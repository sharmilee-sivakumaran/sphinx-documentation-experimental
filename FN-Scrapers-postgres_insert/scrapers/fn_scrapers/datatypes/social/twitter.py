from __future__ import absolute_import

import json
import oauth2 as oauth
import re
from dateutil.parser import parse
import injector

from fn_service.server import BlockingEventLogger, fmt

from fn_scrapers.api.utils import JSONEncoderPlus
from fn_scrapers.api.scraper import argument, scraper, tags
from fn_scrapers.api.resources import PillarDb, ScraperConfig
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher


tweet_base_url = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=%s&count=100"\
                 "&trim_user=true&exclude_replies=false&include_rts=true"


@scraper()
@argument(
    "--max-users",
    type=int,
    help="The maximum number of users to scrape. If a user fails, it is not counted toward this limit.")
@tags(type="social", group="social")
class TwitterScraper(object):
    @injector.inject(
        logger=BlockingEventLogger,
        config=ScraperConfig, 
        scrape_item_publisher=ScrapeItemPublisher,
        pillar_db=PillarDb)
    def __init__(self, logger, config, scrape_item_publisher, pillar_db):
        self.scrape_item_publisher = scrape_item_publisher
        self.logger = logger
        self.pillar_db = pillar_db

        consumer_key = config['twitter']['consumer_key']
        consumer_secret = config['twitter']['consumer_secret']
        access_token = config['twitter']['access_token']
        access_token_secret = config['twitter']['access_token_secret']
        consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
        access_token = oauth.Token(key=access_token, secret=access_token_secret)
        self.client = oauth.Client(consumer, access_token)

    def scrape(self, max_users):
        rows = self.pillar_db.execute(
            "SELECT DISTINCT legislation.person_websites.website_url, legislation.legislators.id "\
            "from legislation.person_websites, legislation.legislators, legislation.legislator_committee_memberships "\
            "where legislation.person_websites.website_type = 'twitter' "\
            "AND legislation.person_websites.person_id = legislation.legislators.person_id"\
            " AND legislation.legislators.id = legislation.legislator_committee_memberships.legislator_id"\
            " AND legislation.legislator_committee_memberships.active=True")

        user_count = 0
        for row in rows:
            if max_users is not None and user_count >= max_users:
                break
            screen_name = re.findall(r'twitter.com/(.*)', row[0])[0]
            if self.scrape_tweets(screen_name, row[1]):
                user_count += 1

    def scrape_tweets(self, screen_name, legislator_id):
        self.logger.debug(__name__, fmt("Scraping {}", screen_name))

        _, data = self.client.request(tweet_base_url%screen_name)
        tweets = json.loads(data)
        if 'errors' in tweets or 'error' in tweets:
            self.logger.warning(__name__, "bad_tweet", fmt("Bad Tweet Data for {}", screen_name))
            return False
        
        for tweet in tweets:
            record = {}
            record['screen_name'] = screen_name
            record['legislator_id'] = legislator_id
            create_at = parse(tweet['created_at'])
            record['create_at'] = create_at
            tweet_id = tweet['id']
            record['tweet_id'] = tweet_id
            tweet_text = tweet['text']
            record['tweet_text'] = tweet_text

            self.scrape_item_publisher.publish_json_item("", "twitter", record, json_encoder=JSONEncoderPlus)
        
        return True

from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.utils import iter_months
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from datetime import datetime, timedelta
import lxml.html
import pytz


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-ND")
class NDEventScraper(EventScraper):
    """
    Scraper workflow:
    1. For each year in the session container, and then each month in that year,
       generate a url for that month, and navigate to that page.
       Ex: http://www.legis.nd.gov/events/list/2018-01
    2. If there are multiple pages of events, we iterate through those.
       Ex: http://www.legis.nd.gov/events/list/2017-12
    3. We get the location from the event list page, and then grab the url for the
       individual event page. We get the Committee name from the link text, skipping
       any events that aren't associated with legislative committees.
    4. Note that some events have the same event page and share an agenda,
       but occur on different days. We scrape all events from that single page
       as separate events. These events are usually listed separately on the
       list page, but link to the same event page. Because of this, we cache
       the list of urls we've scraped for that month, so we don't scrape the same
       page twice and get duplicate events.
       Ex: http://www.legis.nd.gov/events/2018/01/30/higher-education-committee
    5. We navigate to the event page to get the date and time of the event.
       Ex: http://www.legis.nd.gov/events/2018/01/24/energy-development-and-transmission-committee
    6. We save the event.
    """

    jurisdiction = 'nd'
    root_url = 'http://www.legis.nd.gov'
    committee_re = re.compile('(.*? )Committee', re.I)
    date_re = re.compile('(.* - \d+:\d+[ap]m)( to \d+:\d+[ap]m)?', re.I)

    def __init__(self, *args, **kwargs):
        super(NDEventScraper, self).__init__('nd', __name__, **kwargs)

    def scrape(self):
        self.scrape_committee_events()
        self.save_events_calendar()

    def scrape_committee_events(self):
        today = datetime.utcnow().replace(tzinfo=pytz.UTC)
        today = today.astimezone(self._tz).date()
        events_list_url = 'http://www.legis.nd.gov/events/list/'
        for month in iter_months(today - timedelta(days=2), today + timedelta(days=365)):
            scraped_urls = set()
            # Get page listing events in month
            req_url = events_list_url + month.strftime("%Y-%m")
            while req_url:
                page = lxml.html.fromstring(self.get(req_url).text)
                page.make_links_absolute(req_url)
                req_url = None
                list_elements = page.xpath(
                    '//div[@id="block-system-main"]//div[contains(concat(" ", @class, " "), " calendar-list ")]')

                for list_ele in list_elements:
                    if list_ele.xpath("//div[contains(text(), 'CANCELED')]"):
                        continue
                    title_ele = list_ele.xpath('./h2/a')[0]
                    src_url = title_ele.get('href')
                    # Sometimes, there are two links in the list that go to the same page.
                    # In that case, we will scrape both events the first time we go to that
                    # page. If we scrape the url twice, we will get duplicate events, so we skip it.
                    if src_url in scraped_urls:
                        continue
                    scraped_urls.add(src_url)

                    title = title_ele.text_content()
                    comm_name_search = self.committee_re.search(title)

                    if not self.committee_re.search(title):
                        continue

                    comm_name = comm_name_search.group(1).strip()

                    location_ele = list_ele.xpath('.//div[contains(@class, "event-location")]')
                    if location_ele:
                        location = location_ele[0].text_content().strip()
                        location = re.sub(' +', ' ', location)
                        location = re.sub('[\r\n]* *[\r\n]+', ',', location)

                    if not location:
                        location = "Tentative Event"

                    event_page = lxml.html.fromstring(self.get(src_url).text)

                    # Some events pages contain multiple events occuring on different days,
                    # but with the same agenda. Each one should be saved as a separate event, though
                    # all the rest of the data is the same.
                    raw_dates = event_page.xpath('//span[@class="date-display-single"]')
                    for raw_date in raw_dates:
                        raw_date_str = raw_date.text_content()
                        if "-" in raw_date_str:
                            date_str = self.date_re.search(raw_date_str).group(1)
                            date = datetime.strptime(date_str, '%A, %B %d, %Y - %I:%M%p')
                            date = self._tz.localize(date)
                            has_time = True
                        else:
                            date = datetime.strptime(raw_date_str, '%A, %B %d, %Y')
                            has_time = False

                        # Set up event object
                        event = Event(
                            date, "Committee Hearing", location, 'committee_markup', start_has_time=has_time)

                        event.add_source(src_url)

                        # Committee meetings appear to all be joint meetings
                        event.add_participant('host', comm_name, chamber="joint")

                        self.save_event(event)

                # If there's a next page link, we follow that to get additional events
                next_url = page.xpath("//li[@class = 'pager-next']/a/@href")
                if next_url:
                    req_url = next_url[0]
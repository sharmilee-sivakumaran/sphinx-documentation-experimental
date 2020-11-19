from __future__ import absolute_import

import injector

from fn_scrapers.api.scraper import scraper, argument, tags

from fn_service.server import BlockingEventLogger


@scraper()
@argument("--message", default=u"Default Message", help=u"Message to display")
@tags(type="example", group="example")
class ExampleScraper(object):
    @injector.inject(log=BlockingEventLogger)
    def __init__(self, log):
        self.log = log

    def scrape(self, message):
        self.log.info(__name__, u"Running Example Scraper: {}".format(message))


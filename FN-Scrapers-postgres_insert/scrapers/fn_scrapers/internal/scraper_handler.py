from __future__ import absolute_import

import injector
import inspect

from fn_scrapers.api.utils import map_kwargs
from fn_service.server import run_in_new_thread, watchdog

from fn_scrapers.api.resources import ScraperArguments

def create_scraper_handler(scraper_class):
    @run_in_new_thread("scraper_thread")
    @watchdog(None)
    class ScraperHandler(object):
        @injector.inject(inj=injector.Injector, args=ScraperArguments)
        def __init__(self, inj, args):
            self.scraper = inj.get(scraper_class)
            self.args = args

        def scrape(self):
            return map_kwargs(self.scraper.scrape, self.args)

    return ScraperHandler

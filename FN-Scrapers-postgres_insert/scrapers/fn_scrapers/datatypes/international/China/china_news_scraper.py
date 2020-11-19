from __future__ import absolute_import

import injector

from fn_service.server import BlockingEventLogger
from fn_scrapers.api.scraper import scraper, tags
from ..common.utils import parse_date_as_str
from ..China.china_base_scraper import ChinaBaseScraper

@scraper()
@tags(type="bills", country_code="CN", group="international")
class ChinaNewsScraper(ChinaBaseScraper):

    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ChinaNewsScraper, self).__init__("china_news", logger)

    @property
    def schema(self):
        return self.get_schema("china_news.json")

    @property
    def libraries(self):
        return ["NEWS"]

    def scrape_spec(self, detail_response, bill_doc):
        keywords = detail_response.get("keyword")
        try:
            keywords = keywords.split(u"\uff1b")
        except AttributeError:
            pass
        self.set_val("issue_date", parse_date_as_str(detail_response.get("issueDate")), bill_doc)
        self.set_val("categories", self.get_codename_list(detail_response.get("category")), bill_doc)
        self.set_val("source_publication", detail_response.get("source"), bill_doc)
        self.set_val("keywords", keywords, bill_doc)
        self.set_val("author", detail_response.get("author"), bill_doc)

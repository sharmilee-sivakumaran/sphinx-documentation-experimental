from __future__ import absolute_import

import re
import injector

from fn_service.server import BlockingEventLogger
from fn_scrapers.api.scraper import scraper, tags
from ..common.utils import parse_date_as_str
from ..China.china_base_scraper import ChinaBaseScraper
from ast import literal_eval

@scraper()
@tags(type="bills", country_code="CN", group="international")
class ChinaRegScraper(ChinaBaseScraper):

    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ChinaRegScraper, self).__init__("china_regulation", logger)

    @property
    def schema(self):
        return self.get_schema("china_regulation.json")

    @property
    def libraries(self):
        return ["CHL", "LAR", "PROTOCOL"]

    def scrape_spec(self, detail_response, bill_doc):
        related_items = set()
        # Related ids are linked in the fulltext
        try:
            for gid in re.findall("href\S+\((\d+),\d\)", detail_response.get("fullText")):
                related_items.add(gid)
        except TypeError:
            pass

        self.set_val("related_items", list(related_items), bill_doc)
        self.set_val("revision_note", self.html_to_text(detail_response.get("revisedBasis")), bill_doc)
        self.set_val("partial_failure_note", self.html_to_text(detail_response.get("partialFailureBasis")), bill_doc)
        self.set_val("failure_note", self.html_to_text(detail_response.get("failureBasis")), bill_doc)
        self.set_val("document_number", detail_response.get("documentNO"), bill_doc)
        self.set_val("issue_date", parse_date_as_str(detail_response.get("issueDate")), bill_doc)
        self.set_val("implementation_date", parse_date_as_str(detail_response.get("implementDate")), bill_doc)
        self.set_val("status", self.get_codename(detail_response.get("timelinessDic"), bill_doc["download_id"]), bill_doc)
        # For protocol we want to store their category as document_type
        if bill_doc["library"] == "PROTOCOL":
            self.set_val("document_type", self.get_codename(detail_response.get("category"), bill_doc["download_id"]), bill_doc)
        else:
            self.set_val("categories", self.get_codename_list(detail_response.get("category")), bill_doc)
        self.set_val("issuing_department", self.get_codename_list(detail_response.get("issueDepartment")), bill_doc)
        self.set_val("scope_of_authority", self.get_codename(detail_response.get("effectivenessDic"), bill_doc["download_id"]), bill_doc)
        if detail_response.get("ratifyDepartment"):
            self.set_val("ratifying_department",
                         self.get_codename_list(literal_eval(detail_response.get("ratifyDepartment"))), bill_doc)
        self.set_val("expiration_date", parse_date_as_str(detail_response.get("expirationDate")), bill_doc)

# -*- coding: utf-8 -*-
from __future__ import division
import logging
import injector
import re
from ..common.base_scraper import ScraperBase
from .schema_handler import NIGERIABill
from fn_scraperutils.events.reporting import EventComponent
import json

from fn_scrapers.api.scraper import scraper, argument, tags
logger = logging.getLogger(__name__)
from fn_service.server import BlockingEventLogger, fmt
from datetime import datetime


# class for URL formation at different levels
class NigeriaURL:

    base_url = u"http://www.nassnig.org/document/bills"
    document_url = "http://www.nassnig.org/document/download"

    @staticmethod
    def get_bill_page_url(bill_url):
        bill_page_url = "{base_url}{bill_url}".format(base_url=NigeriaURL.base_url, bill_url=bill_url)
        return bill_page_url

    @staticmethod
    def get_document_page_link(id):
        document_page_link = "{document_url}/{id}".format(document_url=NigeriaURL.document_url, id=id)
        return document_page_link

@scraper()
@tags(type="bills", country_code="NG", group="international")
# Nigeria Docscraper class
class NIGERIADocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(NIGERIADocScraper, self).__init__(EventComponent.scraper_bills, "nigeria", "nigeria")
        self.logger = logger

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
            comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
            match = comp.search(html)
            if not match:
                return match
            else:
                resu = re.sub('&\S+;|\s{2,}|;', '', match.group(group))
                return resu

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    # function for downloading html from page
    def download_html(self, url):
            self.http_get(url, self.scraper_policy.doc_list)
            html = self.get_content_from_response()
            return html
    
    @staticmethod
    def trim_content(content):
        content = re.sub('<.*?>', '', content)
        content = re.sub('<.*?>|\s{2,}', ' ', content)
        content = re.sub('^\s+|\s+$|\r?\n', '', content)
        return content

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)|\\\\', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self):

        home_page_url = NigeriaURL.base_url
        home_page_html = self.download_html(home_page_url)
        bill_blocks = self.find_pattern(home_page_html, '(\{"id":"\d+".*?\})')
        for bill in bill_blocks:
            self.scrape_bill(bill)

    # function for scraping bills
    def scrape_bill(self, bill):

        nigeriabill = NIGERIABill()

        # title
        title = self.single_pattern(bill, '"title":"(.*?)"', 1)
        nigeriabill.add_title(title)

        # description
        description = self.single_pattern(bill, '"description":"(.*?)"', 1)
        if description:
            description = re.sub('\r?\n|\\\\r\\\\n', ' ', description)
            nigeriabill.add_description(description)

        # parliament
        parliament = self.single_pattern(bill, '"parliament":"(\d+).*?"', 1)
        nigeriabill.add_parliament(int(parliament))

        # session
        session = self.single_pattern(bill, '"session":"(\d+).*?"', 1)
        nigeriabill.add_session(int(session))

        # chamber_of_origin
        chamber_of_origin = self.single_pattern(bill, '"chamber":"(.*?)"', 1)
        if chamber_of_origin == "senate":
            chamber_of_origin = "Senate"
        elif chamber_of_origin == "House of Representatives":
            chamber_of_origin = "House of Representatives"
        nigeriabill.add_chamber_of_origin(chamber_of_origin)

        # date
        date = self.single_pattern(bill, '"document_date":"(.*?)"', 1)
        date = self.get_formatted_date(date)
        nigeriabill.add_date(date)

        # documents
        bill_id = self.single_pattern(bill, '"id":"(\d+)"', 1)
        document_link = NigeriaURL.get_document_page_link(bill_id)
        extraction_type = self.extraction_type.unknown
        content_type = "application/pdf"

        download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                       self.scraper_policy.doc_service,
                                                                       extraction_type, True,
                                                                       content_type=content_type)
        if len(doc_ids) > 0:
            document_id = doc_ids[0]
            nigeriabill.add_document_id(document_id)
            nigeriabill.add_download_id(download_id)

        if self.validate_doc(nigeriabill):
            self.save_doc(nigeriabill)
        else:
            self.logger.critical(__name__, "individual_bill_scrape_failed",
                                 fmt("JsonSchema validation failed for bill having title: {}", title))
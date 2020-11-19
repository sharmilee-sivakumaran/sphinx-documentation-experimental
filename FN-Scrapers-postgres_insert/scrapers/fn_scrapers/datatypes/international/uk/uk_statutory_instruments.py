# -*- coding: utf-8 -*-
'''
UK Notice Scraper

NOTE: Current implementation uses the HTML website. There is an xml api
available [1] [2] that would make parsing much easier and more reliable.

[1] https://www.legislation.gov.uk/uksi/2018/data.feed
[2] https://www.legislation.gov.uk/ukdsi/2018/data.feed
'''

from datetime import datetime
import itertools
import json
import logging
import os
import re
from urlparse import urljoin

import injector
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory

log = logging.getLogger(__name__)

@scraper()
@tags(type="notices", country_code="GB", group="international")
@argument('--startyear', help='year in the format YYYY i.e.2017..', required=True)
@argument('--endyear', help='year in the format YYYY i.e.2018..', required=True)
# UK Statutory Instruments scraper
class UKstatutoryinstrumentsscraper(ScraperBase):
    base_url = "https://www.legislation.gov.uk/"

    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(UKstatutoryinstrumentsscraper, self).__init__(EventComponent.scraper_bills, "uk", "uk")
        self.logger = logger
        cwd = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(cwd, "schemas", "rules.json")
        self.model_factory = ModelFactory(path, path)

    @classmethod
    def format_url(cls, urltype, start_year, end_year, page=None):
        '''Formats a url with an optional page number. '''
        url = urljoin(cls.base_url, '{}/{}-{}'.format(
            urltype, start_year, end_year))
        if page is None:
            return url
        return '{}?page={}'.format(url, page)

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            return match.group(group)

    # for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)|\.', '', date)
        date = re.sub('(\d)(nd|st|rd|th)', r'\1', date)
        try:
            date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
        return date

    def validate_and_send(self, doc):
        if self.validate_doc(doc):
            self.save_doc(doc)
        else:
            log.critical(
                "JsonSchema validation failed for : " + json.dumps(doc.to_json()),
                event_type="schema_failed"
            )

    # function for downloading html from page
    def download_html(self, url):
        self.http_get(url, self.scraper_policy.doc_list)
        html = self.get_content_from_response()
        return html

    # function for scrape data
    def scrape(self, startyear, endyear):
        for url in ["uksi", "ukdsi"]:
            for i in itertools.count(1,):
                bill_page_url = self.format_url(url, startyear, endyear, i)
                log.info('Fetching page with url: %s', bill_page_url)
                page_html = self.download_html(bill_page_url)
                self.scrape_reg(page_html)
                if ">Next<" not in page_html:
                    break

    # function for scraping bill details
    def scrape_reg(self, page_html):
        self.set_selector(text=page_html)
        bill_rows = self.xpath('//div[@id="content"]/table/tbody/tr')
        for bill_row in bill_rows:
            try:
                ukstbill = self.model_factory.create_bill_doc()
                ukstbill.country = "United Kingdom"

                ukstbill.publication_name = "UK Legislation.gov.uk"

                rule_type = self.extract_single('./td[3]/text()', sel = bill_row)
                if not rule_type:
                    continue
                ukstbill.notice_type = rule_type

                ukstbill.title = self.extract_single('./td[1]/a/text()', sel = bill_row)

                bill_link = self.extract_single('./td[1]/a/@href' ,sel = bill_row)
                bill_link = urljoin(self.base_url, bill_link)
                self.http_get(bill_link, self.scraper_policy.doc_list)

                rule_id = self.extract_single('//div[@id="breadCrumb"]/ul/li[1]/a/text()')
                ukstbill.notice_id = rule_id
                source_url = bill_link
                content_link = self.extract_single('//li[@id="legContentLink"]/a/@href')
                if content_link:
                    content_link = urljoin(self.base_url, content_link)
                    self.http_get(content_link, self.scraper_policy.doc_list)
                    publication_date = self.extract_single('//div[@id="viewLegSnippet"]/div[1]/div[1]/p[2]')
                    publication_date = re.sub('<.*?>|.*? on ', '', publication_date)
                    publication_date = self.single_pattern(publication_date, '\d+(st|nd|rd|th)?\s+\w+\s+\d+', 0)
                    if publication_date:
                        publication_date = self.get_formatted_date(publication_date)
                        ukstbill.publication_date = publication_date

                    source_url = content_link
                ukstbill.source_url = source_url

                documents_page_link = self.extract_single('//li[@id="legResourcesLink"]/a[1]/@href')
                if not documents_page_link:
                    self.validate_and_send(ukstbill)
                    continue

                documents_page_link = urljoin(self.base_url, documents_page_link)
                self.http_get(documents_page_link, self.scraper_policy.doc_list)
                all_documents_link = self.xpath('//ul[@class="plainList"]/li/a')
                for individual_document_link in all_documents_link:
                    document_link = self.extract_single("./@href", sel=individual_document_link)
                    if "pdf" in document_link:
                        flag = 0
                        document_link = urljoin(self.base_url, document_link)
                        document_title = self.extract_single("./text()", sel=individual_document_link)
                        if "Download" in document_title:
                            if "Welsh" not in document_title:
                                document_title = "Original Print PDF"
                            else:
                                flag = 1
                        if flag == 0:
                            ukstbill.document_title = document_title
                            extraction_type = self.extraction_type.unknown
                            content_type = "application/pdf"

                            download_id, _, doc_ids = self.register_download_and_documents(
                                document_link, self.scraper_policy.doc_service,
                                extraction_type, True, content_type=content_type)
                            if doc_ids:
                                ukstbill.document_id = doc_ids[0]
                                ukstbill.download_id = download_id

                            self.validate_and_send(ukstbill)
            except Exception as e:
                log.critical("Error occured: %s", e, exc_info=True,
                             event_type='individual_bill_scrape_failed')
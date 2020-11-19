# -*- coding: utf-8 -*-
from __future__ import division
import logging
import re
from fn_scraperutils.events.reporting import EventComponent
import json

from fn_scrapers.api.scraper import scraper, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import SingaporeBill

from pyvirtualdisplay import Display
from selenium import webdriver

from datetime import datetime

import time

logger = logging.getLogger(__name__)


# class for URL formation at different levels
class SingaporeURL:

    base_url = u"https://www.parliament.gov.sg/parliamentary-business/bills-introduced"

    first_page_url = u"https://www.parliament.gov.sg/parliamentary-business/bills-introduced?keyword=&title=&year=&page=1&pageSize=500"

    @staticmethod
    def get_next_page_url(page_number):
        next_page_url = u"{base_url}?keyword=&title=&year=&page={page_number}&pageSize=500".format(base_url=SingaporeURL.base_url, page_number=page_number)
        return next_page_url


# Singapore Docscraper class
@scraper()
@tags(type="bills", country_code="SG", group="international")
class SingaporeDocScraper(ScraperBase):
    def __init__(self):
        super(SingaporeDocScraper, self).__init__(EventComponent.scraper_bills, "singapore", "singapore")

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            resu = re.sub('&\S+;|\s{2,}|;|<.*?>', '', match.group(group))
            return resu

    @staticmethod
    def single_pattern_link(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            return match.group(group)

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%y').strftime('%Y-%m-%d')
        return date

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL)
        match = comp.findall(html, re.IGNORECASE)
        return match

    # function for downloading html from page
    @staticmethod
    def download_html(url):
        display = Display(visible=0, size=(800, 600))
        display.start()
        time.sleep(5)
        browser = webdriver.Chrome()
        browser.get(url)
        html = browser.page_source
        browser.quit()
        display.stop()
        return html

    # function for getting next page links, total number of bills and bill page urls
    def scrape(self):
        main_page_html = self.download_html(SingaporeURL.first_page_url)
        total_number_of_bills = int(self.single_pattern(main_page_html, '1 to \d+ of (\d+) results', 1))
        total_number_of_pages = int((total_number_of_bills/500))+2
        self.scrape_bill(main_page_html)
        if total_number_of_pages > 2:
            for page_number in range(2, total_number_of_pages):
                next_page_url = SingaporeURL.get_next_page_url(page_number)
                next_page_html = self.download_html(next_page_url)
                self.scrape_bill(next_page_html)

    # function to extract data from bill page
    def scrape_bill(self, bill_page_html):

        bill_blocks = self.find_pattern(bill_page_html, '<div class="indv-bill">.*?</div>\s*</div>\s*</div>')

        for bill_block in bill_blocks:
            singapore_bill = SingaporeBill()

            # title
            title = self.single_pattern(bill_block, '<h5>(.*?)</h5>', 1)
            singapore_bill.add_title(title)

            # bill_number
            bill_number = self.single_pattern(bill_block, 'Bill No\s*:\s*</b>\s*(.*?)\s*<', 1)
            singapore_bill.add_bill_number(bill_number)

            # introduction_date
            introduction_date = self.single_pattern(bill_block, 'Date Introduced\s*:\s*</b>\s*<[^>]*>\s*(\d{1,2}\.\d{1,2}.\d{4})', 1)
            introduction_date= self.get_formatted_date(introduction_date)
            singapore_bill.add_introduction_date(introduction_date)

            # date_of_second_reading
            date_of_second_reading = self.single_pattern(bill_block, 'Date of 2nd Reading\s*:\s*</b>\s*<[^>]*>\s*(\d{1,2}\.\d{1,2}.\d{4})', 1)
            if date_of_second_reading == ' ' or date_of_second_reading == '':
                pass
            else:
                date_of_second_reading = self.get_formatted_date(date_of_second_reading)
                singapore_bill.add_date_of_second_reading(date_of_second_reading)

            # date_of_passage
            date_of_passage = self.single_pattern(bill_block, 'Date Passed\s*:\s*</b>\s*<[^>]*>\s*(\d{1,2}\.\d{1,2}.\d{4})', 1)
            if date_of_passage == ' ' or date_of_passage == '':
                pass
            else:
                date_of_passage = self.get_formatted_date(date_of_passage)
                singapore_bill.add_date_of_passage(date_of_passage)

            # document_link
            document_link = self.single_pattern_link(bill_block, 'href="(http[^"]*\.pdf).*?"', 1)
            document_link = re.sub('http', 'https', document_link)
            extraction_type = self.extraction_type.unknown
            content_type = "application/pdf"
            download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type)
            if len(doc_ids) > 0:
                document_id = doc_ids[0]
                singapore_bill.add_document_id(document_id)
                singapore_bill.add_download_id(download_id)

            if self.validate_doc(singapore_bill):
                self.save_doc(singapore_bill)
            else:
                logging.debug(json.dumps(singapore_bill.to_json()))

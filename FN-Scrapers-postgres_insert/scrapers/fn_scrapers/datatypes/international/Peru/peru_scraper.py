# -*- coding: utf-8 -*-
from __future__ import division
import logging
import re

from .schema_handler import PERUBill, DocumentActions

from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase

import injector
from fn_service.server import BlockingEventLogger,fmt
import json
from unidecode import unidecode

import itertools
from datetime import datetime

# class for URL formation at different levels
class PeruURL:

    base_url = u"http://www2.congreso.gob.pe"

    @staticmethod
    def get_bill_page_url(bill_url):
        bill_page_url = "{base_url}{bill_url}".format(base_url=PeruURL.base_url, bill_url=bill_url)
        return bill_page_url

    @staticmethod
    def get_page_url(year, page_number):
        page_url = "{base_url}/Sicr/TraDocEstProc/CLProLey{year}.nsf/Local%20Por%20Numero%20Inverso?OpenView&Start={page_number}"\
                                                                                            .format(base_url=PeruURL.base_url, year=year, page_number=page_number)
        return page_url

@scraper()
@argument('--year', help='Year for the bills you want to scrape in the format yyyy i.e. 2016 for 2016-2021 session', required=True)
@tags(type="bills", country_code="PE", group="international")
# Peru Docscraper class
class PERUDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(PERUDocScraper, self).__init__(EventComponent.scraper_bills, "peru", "peru")
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

    @staticmethod
    def single_pattern_link(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            return match.group(group)

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL)
        match = comp.findall(html, re.IGNORECASE)
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

    # getting the correct date format for introduction date
    @staticmethod
    def get_formatted_intro_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        return date

    # getting the correct date format for action_date
    @staticmethod
    def get_formatted_action_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self, year):

        try:
            if PERUDocScraper.check_year(year):
                for page in itertools.count(1,99):
                    home_page_url = PeruURL.get_page_url(year, page)
                    home_page_html = self.download_html(home_page_url)
                    if u"<h2>No documents found</h2>" not in home_page_html:
                        bill_test = self.find_pattern(home_page_html, '(<tr valign="top".*?</tr>)')
                        self.scrape_bill(bill_test)
                    else:
                        break

        except Exception as e:

            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc, fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                              self.home_page_url)

    # function for scraping bills
    def scrape_bill(self, home_html):
            for bill_block in home_html:
                try:
                    # source url
                    source_url = self.single_pattern_link(bill_block, '<a\s*href="(.*?)"', 1)
                    if source_url:
                        perubill = PERUBill()

                        if source_url.startswith("http"):
                            source_url = source_url
                        else:
                            source_url = PeruURL.get_bill_page_url(source_url)
                        perubill.add_source_url(source_url)

                        bill_details = self.find_pattern(bill_block, '<td.*?>(.*?)</td>')

                        # bill_id
                        bill_id = bill_details[0]
                        bill_id = self.trim_content(bill_id)
                        perubill.add_bill_id(unidecode(bill_id))

                        # status
                        status = bill_details[3]
                        status = self.trim_content(status)
                        if status != ' ' and status != '':
                            perubill.add_status(unidecode(status))

                        # title
                        title = bill_details[4]
                        title = self.trim_content(title)
                        if title:
                            mod_title = title.title()
                            perubill.add_title(mod_title)

                        bill_page_html = self.download_html(source_url)

                        # session
                        session = self.single_pattern(bill_page_html, 'Per.odo\s*:(.*?)</tr', 1)
                        session = self.single_pattern(session, '\d+\s*\-\s*\d+', 0)
                        perubill.add_session(unidecode(session))

                        # legislature
                        legislature = self.single_pattern(bill_page_html, 'Legislatura\s*:(.*?)</tr', 1)
                        legislature = self.trim_content(legislature)
                        if legislature:
                            perubill.add_legislature(unidecode(legislature))

                        # introduction_date
                        introduction_date = self.single_pattern(bill_page_html, 'Fecha Presentaci.n\s*:(.*?)</tr', 1)
                        introduction_date = self.trim_content(introduction_date)
                        if introduction_date:
                            introduction_date = self.get_formatted_intro_date(introduction_date)
                            perubill.add_introduction_date(introduction_date)

                        # proponent
                        proponent = self.single_pattern(bill_page_html, 'Proponente\s*:(.*?)</tr', 1)
                        proponent = self.trim_content(proponent)
                        if proponent:
                            perubill.add_proponent(unidecode(proponent))

                        # parliamentary_group
                        parliamentary_group = self.single_pattern(bill_page_html, 'Grupo Parlamentario\s*:(.*?)</tr', 1)
                        parliamentary_group = self.trim_content(parliamentary_group)
                        if parliamentary_group:
                            perubill.add_parliamentary_group(unidecode(parliamentary_group))

                        # description
                        description = self.single_pattern(bill_page_html, 'Sumilla\s*:(.*?)</tr', 1)
                        description = self.trim_content(description)
                        if description:
                            perubill.add_description(unidecode(description))

                        # sponsors
                        sponsors = self.single_pattern(bill_page_html, 'Autores.*?<td[^>]*>(.*?)</tr', 1)
                        sponsors = self.trim_content(sponsors)
                        sponsors = re.sub(r"(\w)([A-Z])", r"\1 \2", sponsors)
                        if sponsors:
                            sponsor_array = sponsors.split(',')
                            sponsor_array = set(sponsor_array)
                            sponsor_array = list(sponsor_array)
                            perubill.add_sponsors(sponsor_array)

                        try:
                            # documents
                            document_page_link = self.single_pattern_link(bill_page_html, """onclick="return _doClick\('(.*?)'""", 1)
                            if document_page_link:
                                document_page_link = source_url +'&Click='+document_page_link
                                document_page_html = self.download_html(document_page_link)
                                document_links = self.find_pattern(document_page_html, '<a href="([^"]*\.pdf)"')

                                if not document_links:
                                    document_link = self.single_pattern_link(document_page_html, '<a href="([^>]*\.pdf)"', 1)
                                else:
                                    document_link = document_links[-1]
                                max_size_flag = False
                                MAX_FILE_DOWNLOAD_SIZE = 200 * 1024 * 1024
                                resp = self.http_request(document_link, "HEAD")
                                if "Content-Length" in resp.headers:
                                    if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                                        max_size_flag=True
                                        error_message = "File @ '{}' is larger than max size {} bytes.".format(
                                            document_link, MAX_FILE_DOWNLOAD_SIZE)
                                        self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                                             fmt(
                                                                 'While extracting document Doc-Service is failing with error: {}',
                                                                 error_message))
                                if max_size_flag is False:
                                        extraction_type = self.extraction_type.unknown
                                        content_type = "application/pdf"

                                        download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                                           self.scraper_policy.doc_service,
                                                                                           extraction_type, True,
                                                                                           content_type=content_type)
                                        if len(doc_ids) > 0:
                                            document_id = doc_ids[0]
                                            perubill.add_document_id(document_id)
                                            perubill.add_download_id(download_id)
                        except Exception as e:
                            self.logger.critical(__name__, 'individual_bill_document_extraction_failed',
                                                             fmt("Error occured: {}", e), exc_info=True)
                        # actions
                        actions_block = self.single_pattern(bill_page_html,
                                                            '<tr valign="top"[^>]*>.*?>\s*Seguimiento\s*(<.*?)</tr', 1)
                        validate_action = self.single_pattern(actions_block, '\d+\/\d+\/\d+', 0)
                        if validate_action:
                            actions_block = re.sub('<br>', '----', actions_block)
                            actions_block = self.trim_content(actions_block)
                            if actions_block:
                                if "----" in actions_block:
                                    actions_block = actions_block.split('----')
                                else:
                                    em_block = []
                                    em_block.append(actions_block)
                                    actions_block = em_block

                                for action_bl in actions_block:
                                    action_obj = DocumentActions()

                                    # action date
                                    action_date = self.single_pattern(action_bl, '\d+\/\d+\/\d+', 0)
                                    action_date = self.get_formatted_action_date(action_date)
                                    action_obj.add_action_date(action_date)

                                    # action
                                    action = re.sub('\d+\/\d+\/\d+\s*', '', action_bl)
                                    action = re.sub('^\-|^\s+|\s+$|\-$', '', action)
                                    action_obj.add_action(action)

                                    perubill.add_action_by_obj(action_obj)

                        if self.validate_doc(perubill):
                            self.save_doc(perubill)
                        else:
                            self.logger.critical(__name__, "schema_failed",
                                                 fmt("JsonSchema validation failed for : {}",
                                                     json.dumps(perubill.to_json())))
                except Exception as e:
                    self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)


    @staticmethod
    def check_year(year):
        if re.search(r'\d{4}', year):
            return True
        else:
            return False

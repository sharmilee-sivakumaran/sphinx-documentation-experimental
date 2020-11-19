# -*- coding: utf-8 -*-
from __future__ import division
import logging
import injector
from datetime import datetime
import re
import math
from fn_scraperutils.events.reporting import EventComponent
import json

from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import DocumentActions, DocumentDocuments, PuertoRicoBill

from fn_service.server import BlockingEventLogger, fmt
from HTMLParser import HTMLParser

logger = logging.getLogger(__name__)

# class for URL formation at different levels
class PuertoRicoURL:
    base_url = u""

    @staticmethod
    def get_data_for_first_page(startdate, enddate, parliament):
        data = {'sdata': 'maxln:10;hl:off;sf:akt;sort:vutc;sorder:desc;wp:{parliament};fbwp:del;df:{startdate};dt:{enddate}'.
            format(parliament=parliament, startdate=startdate, enddate=enddate)}
        return data

@scraper()
@argument('--startyear', help='Year in the format YYYY 2017', required=True)
@argument('--endyear', help='Year in the format YYYY 2018', required=True)
@tags(type="bills", country_code="PR", group="international")
# PuertoRico Docscraper class
class Puerto_RicoDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(Puerto_RicoDocScraper, self).__init__(EventComponent.scraper_bills, "puerto rico", "puerto rico")

        self.logger = logger

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            h = HTMLParser()
            resu = re.sub('&nbsp;', ' ', match.group(group))
            resu = re.sub('\s{2,}', ' ', resu)
            resu = re.sub('\s+$|^\s+', '', resu)
            resu = h.unescape(resu)
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

    # date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        return date

    # Function for getting html from bill page
    def get_html_from_page(self, year, start_date_of_given_year, end_date_of_given_year, tipo):

        year_for_url = str(year - (year - 1) % 4)

        # parameter for request
        request_url = "http://www.oslpr.org/legislatura/tl"+year_for_url+"/buscar2.asp"

        cookie = {"cuerpo":"N", "fecha1":start_date_of_given_year, "fecha2":end_date_of_given_year,
                  "tipo_autor":"N", "autor":"NA", "autor2":"NA", "autor3":"NA", "comision":"NA", "tipo":tipo}
        html = self.http_post(request_url, self.scraper_policy.doc_list, request_args={'cookies': cookie})
        return year_for_url, html

    # function for scrape data
    def scrape(self, startyear, endyear):

        if Puerto_RicoDocScraper.check_year(startyear, endyear):

            startyear = int(startyear)
            endyear = int(endyear)

            if startyear == endyear:
                start_date_of_given_year = "01-01-"+str(startyear)
                end_date_of_given_year = "12-31-"+str(startyear)
                tipo_list = ['R', 'RK', 'RC', 'PR', 'P']
                for tipo in tipo_list:
                    year_for_url, page_html = self.get_html_from_page(startyear, start_date_of_given_year, end_date_of_given_year, tipo)
                    bill_ids = self.find_pattern(page_html, '<tr id="(.*?)"')
                    if bill_ids:
                        for id in bill_ids:
                            bid = re.sub(' ', '%20', id)
                            bill_url = "http://www.oslpr.org/legislatura/tl"+year_for_url+"/tl_medida_print2.asp?r="+bid
                            self.scrape_bill(bill_url, id, year_for_url, tipo)
            else:
                if startyear > endyear:
                    logger.error(u'startyear can not be greater than endyear')
                else:
                    endyear = int(endyear) + 1
                    for year in range(startyear, endyear):
                        start_date_of_given_year = "01-01-" + str(year)
                        end_date_of_given_year = "12-31-" + str(year)
                        tipo_list = ['R', 'RK', 'RC', 'PR', 'P']
                        for tipo in tipo_list:
                            year_for_url, page_html = self.get_html_from_page(year, start_date_of_given_year, end_date_of_given_year, tipo)
                            bill_ids = self.find_pattern(page_html, '<tr id="(.*?)"')
                            if bill_ids:
                                for id in bill_ids:
                                    bid = re.sub(' ', '%20', id)
                                    bill_url = "http://www.oslpr.org/legislatura/tl" + year_for_url + "/tl_medida_print2.asp?r=" + bid
                                    self.scrape_bill(bill_url, id, year_for_url, tipo)

    # function for scraping bills
    def scrape_bill(self, source_url, bill_id, year_for_url, tipo):

        bill_page_html = self.download_html(source_url)
        prbill = PuertoRicoBill()

        # bill_id
        prbill.add_bill_id(bill_id)

        # government_bill_id
        government_bill_id = self.single_pattern(bill_page_html, 'N&uacute;m\. de Fortaleza\s*:\s*(.*?)<b>', 1)
        if government_bill_id:
            government_bill_id = re.sub('\s*<.*?>\s*', '', government_bill_id)
            prbill.add_government_bill_id(government_bill_id)

        # session
        year_for_url = int(year_for_url)
        session = str(year_for_url)+"-"+str(year_for_url+3)
        prbill.add_session(session)

        # companion_bill_id
        companion_bill_id = self.single_pattern(bill_page_html, 'Equiv\s*:\s*(.*?)</a>', 1)
        if companion_bill_id:
            companion_bill_id = re.sub('\s*<.*?>\s*', '', companion_bill_id)
            prbill.add_companion_bill_id(companion_bill_id)

        # title
        title = self.single_pattern(bill_page_html, '>\s*T&iacute;tulo\s*(<.*?)</tr>', 1)
        title = re.sub('\s*<.*?>\s*', '', title)
        prbill.add_title(title)

        # sponsors
        sponsors = self.single_pattern(bill_page_html, 'Autor\(es\)\s*:\s*</b>(.*?)</td', 1)
        if sponsors:
            sponsors = re.sub('\s*,\s*', ',', sponsors)
            sponsors = sponsors.split(',')
            prbill.add_sponsors(sponsors)

        # source_url
        prbill.add_source_url(source_url)

        # bill_type
        tipos = {'R':'Resolution', 'RK':'Concurrent Resolution', 'RC':'Joint Resolution', 'PR':'Reorganization Plan', 'P':'Bill'}
        prbill.add_bill_type(tipos[tipo])

        # actions
        actions_html = self.single_pattern(bill_page_html,
                                            '<table width="98%" style="font-family:tahoma;font-size:8pt;" cellspacing=0 cellpading=0 border=0 bgcolor="#ffffff">(.*?)</table>',
                                            1)
        actions_blocks = self.find_pattern(actions_html, '(<tr>.*?\d+\/\d+\/\d+.*?</tr>)')
        for block in actions_blocks:

            document_action = DocumentActions()

            # action_text
            action_text = self.single_pattern(block, '\d+\/\d+\/\d+\s*</td>(.*?)</td>', 1)
            action_text = re.sub('<.*?>', '', action_text)
            document_action.add_action_text(action_text)

            # action_date
            action_date = self.single_pattern(block, 'valign=top>\s*(\d{1,2}\/\d{1,2}\/\d{2,4})\s*</td>', 1)
            action_date = self.get_formatted_date(action_date)
            document_action.add_action_date(action_date)

            prbill.add_actions_by_obj(document_action)

        # documents
        documents_html = self.single_pattern(bill_page_html,
                                            '<table width="98%" style="font-family:tahoma;font-size:8pt;" cellspacing=0 cellpading=0 border=0 bgcolor="#ffffff">(.*?)</table>',
                                            1)
        documents_blocks = self.find_pattern(documents_html, '(<tr>.*?\d+\/\d+\/\d+.*?</tr>)')
        for doc_block in documents_blocks:

            doc_link = self.single_pattern(doc_block, '<a href="(.*?)\s*"', 1)

            if doc_link:

                document = DocumentDocuments()

                resp = self.http_request(doc_link, "HEAD")

                if 'pdf' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.unknown
                    content_type = "application/pdf"

                elif 'msword' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.msword_doc
                    content_type = 'application/msword'

                elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.msword_docx
                    content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

                download_id, _, doc_ids = self.register_download_and_documents(doc_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)
                if len(doc_ids) > 0:
                    document_id = doc_ids[0]
                    document.add_document_id(document_id)
                    document.add_download_id(download_id)

                # document_title
                document_title = self.single_pattern(block, '\d+\/\d+\/\d+\s*</td>(.*?)</td>', 1)
                document_title = re.sub('<.*?>', '', document_title)
                document.add_document_title(document_title)

                # document_date
                document_date = self.single_pattern(block, 'valign=top>\s*(\d{1,2}\/\d{1,2}\/\d{2,4})\s*</td>', 1)
                document_date = self.get_formatted_date(document_date)
                document.add_document_date(document_date)

                prbill.add_documents_by_obj(document)

        if self.validate_doc(prbill):
            self.save_doc(prbill)
        else:
            self.logger.critical(__name__, "individual_bill_scrape_failed",
                                 fmt("JsonSchema validation failed for bill page: {}", source_url))

    @staticmethod
    def check_year(startyear, endyear):
        if re.search(r'\d{4}', startyear):
            if re.search(r'\d{4}', endyear):
                return True
            else:
                return False
        else:
            return False





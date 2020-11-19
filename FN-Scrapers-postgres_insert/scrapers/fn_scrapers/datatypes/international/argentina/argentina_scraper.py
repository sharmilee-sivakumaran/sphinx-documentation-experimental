# -*- coding: utf-8 -*-
from __future__ import division
from datetime import datetime, date, timedelta
import re
import math
from ..common.base_scraper import ScraperBase
from .schema_handler import ARGENTINABill, DocumentAttachment
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
import json
from fn_scrapers.api.scraper import scraper, argument, tags
import injector
from fn_service.server import BlockingEventLogger,fmt
from unidecode import unidecode


# class for URL formation at different levels
class ArgentinaURL:
    base_url = u"http://www.hcdn.gob.ar/proyectos/buscador2016-99.html"

    @staticmethod
    def get_data_and_header_for_first_page(type, startdate, enddate):
        data = {'strTipo': "todos", 'strCamIni': "{type}".format(type=type), "strFechaInicio": "{startdate}".format(startdate=startdate),
                "strFechaFin": "{enddate}".format(enddate=enddate), "strCantPagina": "20",
                'strMostrarTramites': "on", "strMostrarDictamenes": "on", "strMostrarFirmantes": "on",
                "strMostrarComisiones": "on"}
        headers = {'Referer': 'http://www.hcdn.gob.ar/proyectos/buscador2016-99.html'}
        return data, headers

    @staticmethod
    def get_url_headers_cookies_for_next_page(page, jsid):
        url = "http://www.hcdn.gob.ar/proyectos/resultados-buscador.html?pagina="+str(page)
        headers = {'Referer':'http://www.hcdn.gob.ar/proyectos/resultados-buscador.html?pagina='+str(page-1)}
        cookie = {'JSESSIONID': "{jsid}", '_ga': 'GA1.3.474887751.1494837479', '_gid': 'GA1.3.1914050791.1494839120',
                  '_gat': '1'.format(jsid)}

        return url, headers, cookie

@scraper()
@argument(
    '--startdate',
    help='start date from where to scrape in the format dd/mm/yyyy (Defaults to 30 days ago)')
@argument(
    '--enddate',
    help='end date till where to scrape in the format dd/mm/yyyy (Defaults to today)')
@argument(
    '--type',
    dest="types",
    action="append",
    help='Type of the bill to scrap either "diputados" or "senado"',
    required=True)
@tags(type="bills", country_code="AR", group="international")
class ARGENTINADocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ARGENTINADocScraper, self).__init__(EventComponent.scraper_bills, "argentina", "argentina")
        self.logger = logger

    def memo_static_content(self, html_file):
        raw_text = html_file.read()
        required_text = self.single_pattern(raw_text, '(<h1>.*?</body>\s*</html>\s*<html>)', 1)
        return required_text

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
    def roundup(x):
        return int(math.ceil(x / 10.0)) * 10

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return date

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

    # function for getting headers
    def get_headers(self,url,data):
        self.http_post(url,self.scraper_policy.doc_list, request_args={'data' : data})
        headers = self.get_headers_from_response()
        return headers

    # function for downloading html from next page
    def download_next_html_with_cookies(self, url, headers, cookie):
        self.http_post(url, self.scraper_policy.doc_list, request_args={'cookies': cookie, 'headers': headers})
        html = self.get_content_from_response()
        return html

    # function for scrape data
    def scrape(self, types, startdate, enddate):
        today = date.today()
        if startdate is None:
            startdate = (today - timedelta(days=30)).strftime("%d/%m/%Y")
        if enddate is None:
            enddate = today.strftime("%d/%m/%Y")

        jsid = None
        for type in types:
            try:

                if ARGENTINADocScraper.check_date(startdate, enddate):

                    data, headers = ArgentinaURL.get_data_and_header_for_first_page(type, startdate, enddate)
                    first_page_url = "http://www.hcdn.gob.ar/proyectos/resultados-buscador.html"
                    html_for_first_page = self.http_post(
                        first_page_url,
                        self.scraper_policy.doc_list,
                        request_args=dict(data=data, headers=headers)
                    )

                    # It would be best to get this from the requests Session - but, getting access to that
                    # is tricky. In the short term, just checking the response is almost as good.
                    if "JSESSIONID" in self.resp.cookies:
                        jsid = self.resp.cookies["JSESSIONID"]
                    if jsid is None:
                        raise Exception("Couldn't get JSESSIONID")

                    self.scrape_bill(html_for_first_page, type)

                    total_number_of_bills = int(self.single_pattern(html_for_first_page,'(\d+) Proyectos Encontrados',1))
                    if total_number_of_bills > 20:
                        total_number_of_pages = int(total_number_of_bills/20 + 2)
                        for page in range(2, total_number_of_pages):
                            url, headers, cookie = ArgentinaURL.get_url_headers_cookies_for_next_page(page, jsid)
                            html_from_next_page = self.download_next_html_with_cookies(url, headers, cookie)
                            self.scrape_bill(html_from_next_page, type)
            except Exception as e:

                self.logger.critical(__name__, "scraper_failed",
                                     fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
                raise ScrapeError(self.scraper_policy.doc, fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                                  self.first_page_url)

    def scrape_bill(self, html, type):
        bill_blocks = self.find_pattern(html,'<div class="detalle-proyecto">(.*?)</div>\s*</div>')
        for bill_block in bill_blocks:

            try:
                argentina_bill = ARGENTINABill()

                # bill_type
                bill_type = self.single_pattern(bill_block,'<h4>(.*?)</h4>',1)
                if "LEY" in bill_type:
                    bill_type = 'Proyecto de Ley'
                elif "RESOL" in bill_type:
                    bill_type = 'Proyecto de Resolucion'
                elif "DECLAR" in bill_type:
                    bill_type = 'Proyecto de Declaracion'
                elif "MENSAJE" in bill_type:
                    bill_type = 'Mensaje'
                elif "COMUNI" in bill_type:
                    bill_type = 'Comunicacion'
                argentina_bill.add_bill_type(bill_type)

                # chamber_of_origin
                if type == "diputados":
                    chamber_of_origin = "Diputados"
                    argentina_bill.add_chamber_of_origin(chamber_of_origin)
                elif type == "senado":
                    chamber_of_origin = "Senado"
                    argentina_bill.add_chamber_of_origin(chamber_of_origin)

                # file_number
                file_number = self.single_pattern(bill_block, 'Expediente (Diputados|Senado)\s*:\s*</strong>\s*(.*?)\s*</span>', 2)
                argentina_bill.add_file_number(file_number)

                # journal_number
                journal_number = self.single_pattern(bill_block, 'Publicado\s*en\s*:\s*</strong>\s*(.*?)\s*</span>', 1)
                if journal_number:
                    argentina_bill.add_journal_number(journal_number)

                # publication_date
                publication_date = self.single_pattern(bill_block, 'Fecha\s*:\s*</strong>\s*(.*?)\s*</span>', 1)
                publication_date = self.get_formatted_date(publication_date)
                argentina_bill.add_publication_date(publication_date)

                # title
                title = self.single_pattern(bill_block, '<div class="dp-texto">\s*(.*?)\s*</div>', 1)
                if title:
                    argentina_bill.add_title(title.title())

                # sponsors
                sponsors_array = []
                sponsors_table = self.single_pattern(bill_block, '<table class="dp-firmantes table table-condensed table-striped">(.*?)</table>', 1)
                sponsors_tbody = self.single_pattern(sponsors_table, '(<tbody>.*?</tbody>)', 1)
                sponsors_row = self.find_pattern(sponsors_tbody, '(<tr>.*?</tr>)')
                for row in sponsors_row:

                    sponsor_name = self.find_pattern(row, '<td>(.*?)</td>')
                    sponsor_name = sponsor_name[0].strip().title().split(u",")
                    sponsor_name.reverse()
                    sponsor_name = u" ".join(sponsor_name).strip()

                    sponsors_array.append(sponsor_name)
                if sponsors_array:
                    argentina_bill.add_sponsors(sponsors_array)

                # committees
                committees_array = []
                committees_table = self.single_pattern(bill_block,
                                                     '<table class="dp-giros-(diputados|senado) table table-condensed table-striped">(.*?)</table>',
                                                     2)
                committees_tbody = self.single_pattern(committees_table, '(<tbody>.*?</tbody>)', 1)
                committees_row = self.find_pattern(committees_tbody, '(<tr>.*?</tr>)')
                for row in committees_row:
                    committees_list = self.find_pattern(row, '<td>(.*?)</td>')
                    committees_array.append(committees_list[0].strip().title())
                if committees_array:
                    argentina_bill.add_committees(committees_array)

                # documents
                if chamber_of_origin == "Diputados":
                    attachment_link = self.single_pattern(bill_block, 'href="(http://www.hcdn.gob.*?)"', 1)
                    extraction_type = self.extraction_type.html
                    content_type = "text/html"

                    if attachment_link:
                        download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                                       self.scraper_policy.doc_service,
                                                                                       extraction_type, True,
                                                                                       content_type=content_type,
                                                                                       get_static_content=self.memo_static_content)

                elif chamber_of_origin == "Senado":
                    attachment_page_link = self.single_pattern(bill_block, 'href="(http://www.senado.gov.*?)"', 1)
                    attachment_page_html = self.download_html(attachment_page_link)
                    attachment_link = self.single_pattern(attachment_page_html, '<a\s*href="([^"]*)"\s*>\s*Descargar PDF adjunto\s*<', 1)
                    if attachment_link:
                        attachment_link = "http://www.senado.gov.ar" + attachment_link
                        extraction_type = self.extraction_type.unknown
                        content_type = "application/pdf"

                        download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                                       self.scraper_policy.doc_service,
                                                                                       extraction_type, True,
                                                                                       content_type=content_type)

                if attachment_link:
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        document_attachment = DocumentAttachment()
                        document_attachment.add_chamber(chamber_of_origin)
                        document_attachment.add_document_id(document_id)
                        document_attachment.add_download_id(download_id)
                        self.validate_doc(document_attachment)
                        argentina_bill.add_attachment_by_obj(document_attachment)

                if self.validate_doc(argentina_bill):
                    self.save_doc(argentina_bill)
                else:
                    self.logger.critical(__name__, "schema_failed",
                                         fmt("JsonSchema validation failed for : {}",
                                             json.dumps(argentina_bill.to_json())))

            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

    @staticmethod
    def check_date(startdate, enddate):
        if re.search(r'\d{2}\/\d{2}\/\d{4}', startdate):
            if re.search(r'\d{2}\/\d{2}\/\d{4}', enddate):
                return True
            else:
                return False
        else:
            return False
# -*- coding: utf-8 -*-
import re
import os
import json
import injector
import datetime
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info, get_default_http_headers
from lxml import html


@scraper()
@tags(type="gazettes", country_code="CO", group="international")
@argument('--start_date', help='start_date in the format YYYY-mm-dd i.e.2017-02-07', default=(datetime.date.today()-datetime.timedelta(days=30)).strftime("%Y-%m-%d"))
@argument('--end_date', help='end_date in the format YYYY-mm-dd i.e.2018-02-07', default=datetime.date.today().strftime("%Y-%m-%d"))
# by default it will scrap the last 30 days bills
# colombia regulation notice scraper
class ColombiaGazatteScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ColombiaGazatteScraper, self).__init__(EventComponent.scraper_bills, "colombia", "colombia")
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "colombia_gazatte.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("colombia").alpha_2)
        self.base_url = u"http://jacevedo.imprenta.gov.co"
        self.search_url = u"{}/buscador-diario-oficial".format(self.base_url)

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            return match.group(group)

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL|re.IGNORECASE)
        match = comp.findall(html)
        return match

    @staticmethod
    def get_start_end_date(date):
        required_date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%y')
        return required_date

    @staticmethod
    def convert_date_to_str(date_obj):
        return date_obj.strftime('%d/%m/%y')

    @staticmethod
    def return_date_as_date_obj(date_string):
        return datetime.datetime.strptime(date_string, "%Y-%m-%d").date()

    @staticmethod
    def check_date(start_date, end_date):
        date_re = re.compile('\d{4}\-\d{2}\-\d{2}')
        if date_re.match(start_date) and date_re.match(end_date):
            return True
        else:
            return False

    # function for scrape data
    def scrape(self, start_date, end_date):
        if self.check_date(start_date, end_date):
            start_date = self.return_date_as_date_obj(start_date)
            end_date = self.return_date_as_date_obj(end_date)
        else:
            raise ValueError("Invalid date format")

        if end_date < start_date:
            raise ValueError("Invalid Start and end date parameter. Failed to start scraping.")

        start_date = self.convert_date_to_str(start_date)
        end_date = self.convert_date_to_str(end_date)

        # ----- Home page -----
        main_headers = get_default_http_headers()
        main_headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            'Connection': 'keep-alive'
        })
        headers = dict(main_headers)
        req_args = {'headers': headers}

        # First page of colombia gazatte scraper
        self.http_get(self.search_url, self.scraper_policy.doc_list, request_args=req_args)

        # ----- Click Search page tab where we can pass start date and end date of bills-----
        form_id = 'A3828:formMenu'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)

        url = action

        input_fields_dict['A3828:formMenu'] = 'A3828:formMenu'
        input_fields_dict['A3828:formMenu:itemRangoFechas'] = 'A3828:formMenu:itemRangoFechas'

        # Search page of colombia gazatte scraper
        self.http_post(url, self.scraper_policy.doc_list, request_args={'data': input_fields_dict, 'headers': headers})

        # --- Ajax Search result request ----
        # fill the form with start date, end date and other headers and get the response of list page
        headers.update({'X-Requested-With': 'XMLHttpRequest', 'Referer': url, 'Faces-Request': 'partial/ajax',
                        'Accept': 'application/xml, text/xml, */*; q=0.01'})

        form_id = 'A3828:formFechaDiario'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)

        input_fields_dict['A3828:formFechaDiario'] = 'A3828:formFechaDiario'
        input_fields_dict['A3828:formFechaDiario:j_idt28'] = 'A3828:formFechaDiario:j_idt28'
        input_fields_dict['A3828:formFechaDiario:calFechaInicial_input'] = start_date
        input_fields_dict['A3828:formFechaDiario:calFfechaFinal_input'] = end_date
        input_fields_dict[
            'javax.faces.partial.render'] = 'A3828:formFechaDiario:pnlDatosRender A3828:formFechaDiario:pnlMensajes'
        input_fields_dict['javax.faces.partial.execute'] = '@all'
        input_fields_dict['javax.faces.partial.ajax'] = 'true'
        input_fields_dict['javax.faces.source'] = 'A3828:formFechaDiario:j_idt28'
        input_fields_dict['A3828:formFechaDiario:fechaEscrita'] = ''

        url = input_fields_dict['javax.faces.encodedURL']
        req_args = {'data': input_fields_dict, 'headers': headers}

        # Post request with start date and end date
        resp = self.http_request(url, method='POST', request_args=req_args)

        # total number of bills
        total_records = self.single_pattern(resp.text, '"totalRecords":(\d+)', 1)

        if hasattr(self._sel, 'root'):
            xroot = self._sel.root
        else:
            raise ValueError("lxml root not found.")

        self.backup_selector()
        new_html = html.tostring(xroot, encoding='unicode')
        self.set_selector(text=new_html)

        form_id = 'A3828:formFechaDiario'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)

        input_fields_dict['A3828:formFechaDiario'] = 'A3828:formFechaDiario'
        input_fields_dict['A3828:formFechaDiario:dataTable'] = 'A3828:formFechaDiario:dataTable'
        input_fields_dict['A3828:formFechaDiario:dataTable_pagination'] = 'true'
        input_fields_dict['A3828:formFechaDiario:dataTable_first'] = '0'
        input_fields_dict['A3828:formFechaDiario:dataTable_rows'] = total_records
        input_fields_dict['A3828:formFechaDiario:dataTable_encodeFeature'] = 'true'

        input_fields_dict['A3828:formFechaDiario:calFechaInicial_input'] = start_date
        input_fields_dict['A3828:formFechaDiario:calFfechaFinal_input'] = end_date

        input_fields_dict['javax.faces.partial.render'] = 'A3828:formFechaDiario:dataTable'
        input_fields_dict['javax.faces.partial.execute'] = 'A3828:formFechaDiario:dataTable'
        input_fields_dict['javax.faces.partial.ajax'] = 'true'
        input_fields_dict['javax.faces.source'] = 'A3828:formFechaDiario:dataTable'
        input_fields_dict['A3828:formFechaDiario:fechaEscrita'] = ''

        url = input_fields_dict['javax.faces.encodedURL']
        req_args = {'data': input_fields_dict, 'headers': headers}

        # Response from detail pages
        resp = self.http_request(url, method='POST', request_args=req_args)
        resp.encoding = 'utf-8'
        self.scrape_reg(resp.text, action, headers, start_date, end_date)

    # function for srape reg
    def scrape_reg(self, bill_page_source, url, headers, start_date, end_date):
        try:
            viewstate = self.single_pattern(bill_page_source, '\[(\-?\d+:\-?\d+)\]\]>\s*</update>', 1)
            bill_blocks = self.find_pattern(bill_page_source, '(<tr data-ri.*?</tr>)')
            if not viewstate or not bill_blocks:
                raise Exception(
                    "Either viewstate or bill_blocks not found for notice {}".format(
                        url))
        except Exception as e:
            self.logger.critical(__name__, 'individual_gazette_scrape_failed', fmt("Error occured: {}", e),
                                 exc_info=True)
        for bill_block in bill_blocks:
            try:
                self.set_selector(text=bill_block)
                cogazette = self.model_factory.create_bill_doc()
                cogazette.country = "Colombia"



                notice_id = self.extract_single('//span/text()')
                cogazette.notice_id = notice_id

                publication_date = self.extract_single('//td[2]/div/span/text()')

                long_publication_date = parse_date_as_str(publication_date, strftime_format="%b %d, %Y", languages=self.country_languages)
                publication_date = parse_date_as_str(publication_date, languages=self.country_languages)

                cogazette.title = u"{} - {}".format(u"Diario Oficial", long_publication_date)
                cogazette.publication_date = publication_date


                if not notice_id.strip():
                    self.logger.warning(__name__, "no_gazette_id",
                                        fmt("No gazette ID found for publication date: {:%d %B %Y}", publication_date))
                    continue

                # pdf attachment
                data_table_field = self.single_pattern(bill_block, "\{'([^']*)'",1)
                post_data = {'A3828:formFechaDiario': 'A3828:formFechaDiario', 'A3828:formFechaDiario:calFechaInicial_input': start_date,
                             'A3828:formFechaDiario:calFfechaFinal_input': end_date, 'A3828:formFechaDiario:fechaEscrita': '',
                             'javax.faces.ViewState': viewstate, data_table_field: data_table_field}
                req_args = {'data': post_data, 'headers': headers}
                resp = self.http_request(url, method='POST', request_args=req_args)
                document_link = resp.url
                if document_link:
                    cogazette.document_title = notice_id+" PDF"
                    extraction_type = self.extraction_type.extractor_pdftotext
                    content_type = "application/pdf"
                    download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                                   self.scraper_policy.doc_service,
                                                                                   extraction_type,
                                                                                   True,
                                                                                   content_type=content_type)
                    if len(doc_ids) > 0 and doc_ids[0]:
                        document_id = doc_ids[0]
                    else:
                        raise ValueError(
                            "Document ID not found while registering document with url {}".format(
                                document_link))
                    if not download_id:
                        raise ValueError(
                            "Download ID not found while registering document with url {}".format(
                                document_link))
                    cogazette.document_id = document_id
                    cogazette.download_id = download_id

                    if self.validate_doc(cogazette):
                        self.save_doc(cogazette)
                    else:
                        self.logger.critical(__name__, "schema_failed",
                                             fmt("JsonSchema validation failed for : {}",
                                                 json.dumps(cogazette.to_json())))
            except Exception as e:
                self.logger.critical(__name__, 'individual_gazette_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)


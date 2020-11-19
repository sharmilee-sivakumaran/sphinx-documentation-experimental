# -*- coding: utf-8 -*-
import logging
import injector


import re

import os
import sys

from lxml import html


from fn_scraperutils.events.reporting import EventComponent, ScrapeError, EventType
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, get_default_http_headers

from fn_scraperutils.events.reporting import EventComponent

logger = logging.getLogger(__name__)


@scraper()
@argument("--search-term", help="Search term to scrape", type=str, required=True)
@argument("--ignore-archive-bills", default=False, action='store_true', help="Ignore Archive Bills")
@tags(type="bills", country_code="CL", group="international")
class ChileDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ChileDocScraper, self).__init__(EventComponent.scraper_bills, "chile", "chile")

        self.logger = logger

        self.base_url = u'https://www.camara.cl'
        self.search_url = u'{base_url}/pley/pley_buscador.aspx'
        self.bill_detail_url = u'{base_url}/pley/{detail_url}'

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "chile.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("chile").alpha_2)
        self.ignore_archive_bills = False

    def get_search_url(self):
        return self.search_url.format(base_url=self.base_url)

    def get_bill_detail_url(self, detail_url):
        return self.bill_detail_url.format(base_url=self.base_url, detail_url=detail_url)

    def scrape(self, search_term, ignore_archive_bills=False):
        try:

            if ignore_archive_bills:
                self.ignore_archive_bills = ignore_archive_bills

            if ChileDocScraper.check_search_term(search_term):
                bill_ids = self.scrape_bill_ids(search_term)

                for bill_id in bill_ids:
                    self.scrape_bill(bill_id)
            else:
                raise ValueError(u"Invalid session format. Please check and try again.")

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} bills could not be scraped. {}",self.scraper_name, e), exc_info=True)
            raise ScrapeError(fmt("{} bills could not be scraped. {}",self.scraper_name, e))


    def scrape_bill_ids(self, search_term):
        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.


        :param search_term: Search keyword
        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''

        self.search_by_search_term(search_term=search_term)
        # Extract the total number of bills for this session
        bill_ids = []
        bill_rows = self.xpath('//table[re:test(@id,"mainPlaceHolder_grvpley")]/tbody/tr')
        if bill_rows is not None and len(bill_rows) > 0:

            for row in bill_rows:
                row_dict = dict()
                titulo = self.extract_single('.//td[position()=2]/text()', sel=row)
                if titulo:
                    row_dict['title'] = titulo.strip()
                estado_de_tramitacion = self.extract_single('.//td[position()=3]/text()', sel=row)
                if estado_de_tramitacion:
                    row_dict['summary_status'] = estado_de_tramitacion.strip()
                bill_url = self.extract_single('.//a[re:test(@href, "prmBoletin=\d")]/@href',sel=row)
                if bill_url:
                    row_dict['url'] = self.get_bill_detail_url(bill_url)
                m = re.search(r'prmBoletin=(.+?)$',bill_url)
                if m:
                    boltein = m.group(1)
                    row_dict['id'] = boltein.strip()
                    bill_ids.append(row_dict)
        else:
            raise ValueError("No table rows found on bill page.")

        if bill_ids is not None and len(bill_ids) > 0:
            self.logger.debug(__name__, u"Total bills found in search result: {}".format(len(bill_ids)) )
            return bill_ids
        else:
            raise ValueError("Scraper could not scrape bill page url.")

    def scrape_bill(self, bill_info):
        try:

            bill_id = bill_info.setdefault('id',None)
            bill_url = bill_info.setdefault('url',None)
            bill_title = bill_info.setdefault('title',None)
            bill_summary_status = bill_info.setdefault('summary_status',None)

            if bill_url is None or bill_id is None or bill_id.isspace() or bill_url.isspace():
                raise ValueError(u"Bill detail page url or id is not given")

            # Fetching for given bill id and given session

            self.logger.debug(__name__, u"Fetching for bill id - {}".format(bill_id))

            # Bill detail url
            self.http_get(bill_url, self.scraper_policy.doc_list)

            # Extract bill details for given bill detail url
            chile_bill = self.model_factory.create_bill_doc()
            chile_bill.source_url = bill_url
            if bill_summary_status is not None and not bill_summary_status.isspace():
                chile_bill.summary_status = bill_summary_status.strip()

            # wrapperDataGroup
            upper_section_div = self.xpath_single('//div[@class="wrapperDataGroup"]')
            if upper_section_div:
                session_number = self.extract_single('string(.//tr[contains(.,"Legislatura")]/td)', sel=upper_section_div)
                if session_number:
                    session_number = session_number.strip()
                    m = re.search(r'(\d+)\s*$', session_number)
                    if m:
                        session_number = int(m.group(1))
                        if self.ignore_archive_bills and session_number < 362:
                            self.logger.debug(__name__, fmt('Ignoring......archived session: {}', str(session_number)))
                            return None
                        else:
                            chile_bill.session = session_number

                title = self.extract_single('.//h3[@class="caption"]/text()', sel=upper_section_div)
                if title is not None and not title.isspace():
                    title = title.strip()
                    chile_bill.title = title
                elif bill_title is not None and not bill_title.isspace():
                    bill_title = bill_title.strip()
                    chile_bill.title = bill_title

                fecha_de_ingreso = self.extract_single('string(.//tr[contains(.,"Fecha de ingreso")]/td)', sel=upper_section_div)
                if fecha_de_ingreso:
                    introduction_date = parse_date_as_str(fecha_de_ingreso)
                    if introduction_date:
                        chile_bill.introduction_date = introduction_date

                status = self.extract_single('string(.//tr[contains(.,"Estado")]/td)', sel=upper_section_div)
                if status:
                    status = status.strip()
                    chile_bill.status = status

                bill_type = self.extract_single('string(.//tr[contains(.,"Iniciativa")]/td)', sel=upper_section_div)
                if bill_type:
                    bill_type = bill_type.strip()
                    chile_bill.bill_type = bill_type

                numero_de_boletin = self.extract_single('string(.//tr[contains(.,"Numero de bolet")]/td)', sel=upper_section_div)
                if numero_de_boletin:
                    chile_bill.id = numero_de_boletin

                # "Chamber of origin. 'Camara de origen' on source site."
                camara_de_origen = self.extract_single('string(.//tr[contains(.,"mara de origen")]//td)', sel=upper_section_div)
                if camara_de_origen:
                    chile_bill.chamber_of_origin = camara_de_origen

                # mainPlaceHolder_grvtramitacion
                legislative_action_rows = self.xpath('//table[contains(@id,"mainPlaceHolder_grvtramitacion")]/tbody/tr')
                legislative_action_order = 1
                legislative_actions = []
                if legislative_action_rows and len(legislative_action_rows) > 0:
                    for row in legislative_action_rows:

                        legislative_action = self.model_factory.create("LegislativeActionSchema")

                        fecha_col = self.extract_single('.//td[position()=1]/text()',sel=row)
                        if fecha_col:
                            legislative_action_date = parse_date_as_str(fecha_col)
                            if legislative_action_date:
                                legislative_action.date = legislative_action_date

                        session_col = self.extract_single('.//td[position()=2]/text()[normalize-space()]', sel=row)
                        if session_col and not session_col.isspace():
                            m = re.search(r'(\d+)\s*$', session_col)
                            if m:
                                session_col = str(m.group(1))
                                legislative_action.session = session_col

                        etapa_col = self.extract_single('.//td[position()=3]/text()', sel=row)
                        if etapa_col and not etapa_col.isspace():
                            legislative_action.stage = etapa_col

                        sub_etapa_col = self.extract_single('.//td[position()=4]/text()', sel=row)
                        if sub_etapa_col and not sub_etapa_col.isspace():
                            legislative_action.substage = sub_etapa_col

                        documento_col = row.xpath('.//td[position()=5]')
                        if documento_col:
                            documento_col = documento_col[0]
                            doc_urls = documento_col.xpath('.//a[contains(.,"Ver")]/@href').extract()

                            if doc_urls:
                                for doc_url in doc_urls:
                                    document_download_url = urljoin(bill_url, doc_url)

                                    # Head request to get content type, normally don't like extra requests,
                                    # But its otherwise difficult to determine by the URL
                                    extra_http_headers = {'Origin': self.base_url,
                                                          'Referer': bill_url}
                                    http_headers = get_default_http_headers()
                                    http_headers.update(extra_http_headers)
                                    req_args = {'headers': http_headers}

                                    resp = self.http_request(document_download_url, "HEAD", request_args=req_args)


                                    # Default PDF type
                                    extraction_type = self.extraction_type.text_pdf

                                    # Check for msword type
                                    if resp.headers['Content-Type'] == 'application/msword':
                                        extraction_type = self.extraction_type.msword_doc

                                    # IGNORE PDFs for now
                                    if extraction_type == self.extraction_type.text_pdf:
                                        self.logger.debug(__name__, 'Skipping PDF Doc...')
                                        continue

                                    download_id, _, doc_ids = self.register_download_and_documents(document_download_url, self.scraper_policy.doc_service, extraction_type, True, content_type=resp.headers['Content-Type'])

                                    if len(doc_ids) > 0 and doc_ids[0]:
                                        document_id = doc_ids[0]
                                    else:
                                        raise ValueError(
                                            "Document ID not found while registering document with url {}".format(
                                                document_download_url))
                                    if not download_id:
                                        raise ValueError(
                                            "Download ID not found while registering document with url {}".format(
                                                document_download_url))

                                    legislative_action.document_id = document_id
                                    legislative_action.download_id = download_id

                            else:
                                self.logger.debug(__name__, 'MISSING :: No Doc url found in action row...')

                        legislative_action.order = legislative_action_order
                        legislative_action_order += 1
                        if self.validate_doc(legislative_action):
                            legislative_actions.append(legislative_action)

                    if len(legislative_actions)>0:
                        chile_bill.legislative_actions = legislative_actions
                    else:
                        self.logger.debug(__name__, "MISSING :: Legislative action details not found.")
                else:
                    self.logger.debug(__name__, "MISSING :: Legislative table rows not found.")

            else:
                raise ValueError("Div with class=wrapperDataGroup not found on bill page.")

            self.get_authors_via_ajax(bill_url)
            rows = self.xpath('//table[contains(@id,"Autores")]//tr')
            sponsors = []
            if rows and len(rows)>0:
                for row in rows:
                    sponsor = dict()
                    nombre = self.extract_single('./td[position()=1]/text()', sel=row)
                    partido = self.extract_single('./td[position()=4]/text()', sel=row)
                    if nombre:
                        sponsor['sponsor_name'] = nombre.strip()
                    if partido:
                        sponsor['sponsor_party'] = partido.strip()
                    if 'sponsor_name'  in sponsor or 'sponsor_party' in sponsor:
                        sponsors.append(sponsor)
                if len(sponsors)>0:
                    chile_bill.sponsors = sponsors
                else:
                    self.logger.debug(__name__, "MISSING :: Author details not found.")
            else:
                self.logger.debug(__name__, "MISSING :: Author table rows not found.")


            if self.validate_doc(chile_bill):
                self.save_doc(chile_bill)
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill page: {}", bill_url))

        except Exception as e:
            self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Error occured: {}", e),
                                 exc_info=True)

    def search_by_search_term(self, search_term):
        # Search url
        search_url = self.get_search_url()
        self.http_get(search_url, self.scraper_policy.doc_list)
        form_name = 'aspnetForm'
        form_id = 'aspnetForm'
        input_fields_dict, method, action = self.html_form_parser(search_type="both", form_name=form_name, form_id=form_id)
        if input_fields_dict:
            # Set search textbox with given search term
            input_fields_dict['ctl00$mainPlaceHolder$Tbbusqueda'] = search_term
            # -- Search button  ctl00$mainPlaceHolder$Btbusqueda:Buscar
            input_fields_dict['ctl00$mainPlaceHolder$Btbusqueda'] = 'Buscar'
            input_fields_dict['__EVENTARGUMENT'] = ''
            input_fields_dict['__EVENTTARGET'] = ''

        req_args = {'data': input_fields_dict, }
        self.http_post(search_url, self.scraper_policy.doc_list, request_args=req_args)

    def get_authors_via_ajax(self, bill_url):
        # this function is used to fetch tab content via ajax post request

        try:
            self.backup_selector()
            form_name = 'aspnetForm'
            form_id = 'aspnetForm'
            input_fields_dict, method, action = self.html_form_parser(search_type="both", form_name=form_name, form_id=form_id)
            if input_fields_dict:
                input_fields_dict['ctl00$mainPlaceHolder$Tbbusqueda'] = ''
                input_fields_dict[
                    'ctl00$mainPlaceHolder$ScriptManager1'] = 'ctl00$mainPlaceHolder$UpdatePanel1|ctl00$mainPlaceHolder$btnAutores'
                input_fields_dict['__EVENTARGUMENT'] = ''
                input_fields_dict['__EVENTTARGET'] = 'ctl00$mainPlaceHolder$btnAutores'
                input_fields_dict['__ASYNCPOST'] = 'true'

            extra_http_headers = {'Origin': self.base_url,
                                  'Referer': bill_url,
                                  'X-Requested-With': 'XMLHttpRequest',
                                  'X-MicrosoftAjax': 'Delta=true',
                                  'Accept': '*/*'}
            http_headers = get_default_http_headers()
            http_headers.update(extra_http_headers)

            req_args = {'data': input_fields_dict, 'headers': http_headers}

            # create lxml root elem of current bill page
            html_root = html.fromstring(self.resp.text, parser=self._htmlparser)

            # send Ajax POST request to fetch Authors()
            self.resp = self.http_request(bill_url, method='POST', request_args=req_args)

            ajax_response_text = self.resp.text
            ajax_response_item_list = ajax_response_text.split("|")

            if ajax_response_item_list and len(ajax_response_item_list)>60:
                startindex = 0
                lastindex = 4
                while True:
                    newarr = ajax_response_item_list[startindex:lastindex]

                    if "hiddenField" == newarr[1]:
                        elem = html_root.xpath('//input[@type="hidden" and @name=$elem_name]', elem_name=newarr[2])[-1]
                        elem.set('value', newarr[3])
                    elif "updatePanel" == newarr[1]:
                        dynamic_content = newarr[3]
                        dynamic_content = '<div id="ctl00_mainPlaceHolder_UpdatePanel1">' + dynamic_content + '</div>'
                        new_div_update_panel = html.fromstring(dynamic_content)
                        div_detail = html_root.xpath('//div[@id="detail"]')[0]
                        div_update_panel = html_root.xpath('//div[@id="ctl00_mainPlaceHolder_UpdatePanel1"]')[0]
                        div_update_panel = div_detail.xpath('//div[@id="ctl00_mainPlaceHolder_UpdatePanel1"]')[0]
                        div_detail.replace(div_update_panel, new_div_update_panel)
                        div_update_panel = html_root.xpath('//div[@id="ctl00_mainPlaceHolder_UpdatePanel1"]')[0]

                    if lastindex >= 60:
                        break

                    startindex += 4
                    lastindex += 4

                html_text = html.tostring(html_root, encoding = "unicode")

                self.set_selector(text=html_text)
            else:
                raise ScrapeError("Ajax response is not expected one.")

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} Scraper failed while sending ajax request to fetch tab content. Error: {}", self.scraper_name, e), exc_info=True)
            raise ScrapeError(fmt("{} Scraper failed while sending ajax request to fetch tab content. Error: {}", self.scraper_name, e))

    @staticmethod
    def check_search_term(search_term):
        if search_term:
            return True
        else:
            return False


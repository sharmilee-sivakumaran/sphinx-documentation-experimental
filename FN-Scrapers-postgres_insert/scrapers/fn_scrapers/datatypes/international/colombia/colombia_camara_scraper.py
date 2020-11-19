# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import sys
import re
import injector
from fn_scraperutils.events.reporting import EventComponent, ScrapeError, EventType
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, get_default_http_headers

import simplejson as json
from lxml.html import fromstring, tostring
from six.moves.urllib.parse import urlparse, urlunparse, unquote


@scraper()
@argument("--search-term", help="Search term to search and scrape bills.", type=str, required=True)
@tags(type="bills", country_code="CO", group="international")
class ColombiaCamaraDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ColombiaCamaraDocScraper, self).__init__(EventComponent.scraper_bills, "colombia", "colombia")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "colombia.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("colombia").alpha_2)

        self.base_url = u'http://www.camara.gov.co'
        self.search_url = u'{base_url}/secretaria'.format(base_url=self.base_url)
        self.ajax_search_url = u'{base_url}/views/ajax?_wrapper_format=drupal_ajax'.format(base_url=self.base_url)


    def scrape(self, search_term):

        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        :param search_term: Search keyword

        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''

        try:
            self.search_by_search_term(search_term)
            totals = self.extract_single('//header/div[contains(@class,"encabezado_proyectos") and re:test(text(),"\d+\s*proyectos\s*de\s*ley", "i")]/text()', namespaces={'re': "http://exslt.org/regular-expressions"})
            if totals is not None and not totals.isspace():
                m = re.search(r'(\d+)\s*proyectos\s*de\s*ley', totals)
                if m:
                    total_rows = m.group(1)
                    if int(total_rows) > 0:
                        items_per_page = 50
                        number_of_pages = (int(total_rows) + items_per_page - 1) // items_per_page

                        current_page = 1
                        while current_page <= number_of_pages:
                            self.scrape_bills()
                            if current_page == number_of_pages:
                                break
                            current_page += 1
                            self.search_by_search_term(search_term=search_term, page_number=current_page)
            else:
                form_found = self.extract_single('//form[@id="views-exposed-form-proyectos-de-ley-secretaria-block-1"]/@id')


                if form_found is not None:
                    # raise ScrapeError("Results not found.")
                    raise ScrapeError(self.scraper_policy.doc,
                                  fmt(u"{} : Results not found. ", self.scraper_name.title()),
                                  self.search_url)
                else:
                    raise ScrapeError(self.scraper_policy.doc,
                                      fmt(u"{} : Total number of search result not found. ", self.scraper_name.title()),
                                      self.search_url)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("Colombia bills could not be scraped. {}",e), exc_info=True)

            raise ScrapeError(self.scraper_policy.doc_list,
                          fmt(u"{} : Colombia bills could not be scraped. {}", self.scraper_name.title(),e),
                              self.search_url)


    def scrape_bills(self):
        '''
        Scrape Bills from Search result Page

        :return:
        '''
        bill_rows = self.xpath(
            '//tr[contains(@class,"tablacomispro")]')
        bill_short_info_list = []
        if bill_rows  and len(bill_rows)>0:
            self.logger.info(__name__, fmt('Total Bills found: {}', len(bill_rows)))
            for bill_row in bill_rows:

                bill_short_info = dict()
                camara_id_text = self.extract_single('./td[position()=1]/text()', sel=bill_row)
                senado_id_text = self.extract_single('./td[position()=2]/text()', sel=bill_row)

                bill_short_title_cell = bill_row.xpath('./td[position()=3]')
                bill_short_title_url = self.extract_single('./a/@href', sel=bill_short_title_cell)
                bill_short_title_text = self.extract_single('./a/text()', sel=bill_short_title_cell)
                bill_tipo_text = self.extract_single('./td[position()=4]/text()', sel=bill_row)
                bill_legislature_text = self.extract_single('./td[position()=9]/text()', sel=bill_row)

                if camara_id_text:
                    bill_short_info['camara_id'] = camara_id_text.strip()
                    camara_id_text = camara_id_text.replace('215C', '2015C')
                    m = re.search(r'(\d+)\s*[\/\-]\s*(\d\d\d\d)', camara_id_text)
                    if m:
                        year = m.group(2)
                        bill_short_info['year'] = int(year)

                if senado_id_text:
                    bill_short_info['senado_id'] = senado_id_text.strip()
                if bill_short_title_text:
                    bill_short_info['short_title'] = bill_short_title_text.title().strip()



                if bill_short_title_url and not bill_short_title_url.isspace():
                    if bill_short_title_url.startswith("http"):
                        bill_short_info['bill_url'] = bill_short_title_url.strip()
                    else:

                        bill_short_title_url = urljoin(self.base_url, bill_short_title_url.strip())
                        bill_short_info['bill_url'] = bill_short_title_url
                else:
                    raise ValueError("Individual Bill URL not found.")



                if bill_legislature_text:
                    bill_short_info['session'] = bill_legislature_text.replace(" ","")

                bill_short_info['source_chamber'] = 'lower'
                if bill_tipo_text:
                    #bill_tipo_text = re.sub(r'[^\x00-\x7F]+', '', bill_tipo_text)
                    bill_short_info['bill_type'] = bill_tipo_text.strip()
                if len(bill_short_info) > 0:
                    bill_short_info_list.append(bill_short_info)



        else:
            raise ValueError("No table rows found on bill page.")

        for bill_short_info in bill_short_info_list:
            self.scrape_bill_page(bill_short_info)


    def scrape_bill_page(self, bill_short_info):
        '''
        Scrape Individual Bill Page

        :return:
        '''

        if bill_short_info.setdefault('bill_url',None):
            try:
                # 'http://www.camara.gov.co/tratado-antartico-sobre-proteccion-del-medio-ambiente'
                # 'http://www.camara.gov.co/infraestructura-comunicaciones'
                # 'http://www.camara.gov.co/prima-trabajadores-domesticos'
                # 'http://www.camara.gov.co/unidad-nacional-de-proteccion'
                self.logger.info(__name__ , 'Going for individual bill: ' + bill_short_info['bill_url'])
                self.http_get(bill_short_info['bill_url'], self.scraper_policy.doc_list)

                bill_doc = self.model_factory.create_bill_doc()
                bill_doc.source_url = bill_short_info['bill_url']
                bill_doc.short_title = bill_short_info.setdefault('short_title', None)
                bill_doc.session = bill_short_info.setdefault('session', None)
                if bill_short_info.setdefault('year', None):
                    bill_doc.year = bill_short_info['year']
                    if not bill_short_info.setdefault('session', None):
                        session = str(bill_short_info['year']) + '-' + str(int(bill_short_info['year']) + 1)
                        bill_doc.session = session.replace(" ","")

                if bill_short_info.setdefault('source_chamber', None):
                    bill_doc.source_chamber = bill_short_info['source_chamber']

                if bill_short_info.setdefault('bill_type', None):
                    bill_doc.bill_type = bill_short_info['bill_type']

                # camara
                camara_text = self.extract_as_one(
                    '//div[contains(./div,"No. de proyecto:") and contains(./span,"mara:")]/text()')
                if camara_text:
                    bill_doc.camara_id = camara_text.strip()

                # senado
                senado_text = self.extract_as_one('//div[contains(../div/div,"No. de proyecto:") and contains(./span,"Senado:")]/text()')
                if senado_text:
                    senado_text = senado_text.strip()
                    senado_text = re.sub(r'^\s*0\s*$', '', senado_text)
                    bill_doc.senado_id = senado_text

                # Origen
                origen_text = self.extract_single(
                    '//div[contains(./span,"Origen:")]//span[@class="field__item"]/text()')

                if origen_text:
                    # Camara == lower, Senado == upper
                    if 'mara' in origen_text.lower():
                        bill_doc.chamber_of_origin = 'lower'
                    elif 'senado' in origen_text.lower():
                        bill_doc.chamber_of_origin = 'upper'

                # Tipo
                bill_type_text = self.extract_single(
                    '//div[contains(./span,"Tipo:")]//span[@class="field__item"]/text()')
                if bill_type_text:
                    bill_doc.bill_type = bill_type_text
                elif bill_short_info.setdefault('bill_type',None):
                    bill_doc.bill_type = bill_short_info['bill_type']

                # TÍTULO:
                titulo_text = self.extract_single('//div[contains(./span,"tulo:")]/div/text()')
                if titulo_text:
                    titulo_text = titulo_text.title().strip()
                    titulo_text = titulo_text.strip('"')
                    titulo_text = titulo_text.strip(u'\u201c')
                    titulo_text = titulo_text.strip(u'\u201d')
                    # bill_doc.full_title = titulo_text
                    bill_doc.title = titulo_text



                # Fecha de radicación:
                # Cámara:
                fecha_de_radicacion_camara_text = self.extract_as_one(
                    '//div[contains(./div,"Fecha de radicaci") and contains(./span,"mara:")]/text()')
                if fecha_de_radicacion_camara_text:
                    fecha_de_radicacion_camara_text = fecha_de_radicacion_camara_text.strip()
                    camara_filing_date = parse_date_as_str(fecha_de_radicacion_camara_text,
                                                           languages=['en'])
                    if camara_filing_date:
                        bill_doc.camara_filing_date = camara_filing_date

                # Fecha de radicación:
                # Senado:
                fecha_de_radicacion_senado_text = self.extract_as_one(
                    '//div[contains(./div,"Fecha de radicaci") and contains(./span,"mara:")]/following-sibling::div/text()')

                if fecha_de_radicacion_senado_text:
                    senate_filing_date = parse_date_as_str(fecha_de_radicacion_senado_text, languages=['en'])

                    if senate_filing_date:
                        bill_doc.senate_filing_date = senate_filing_date

                # Comisión:
                comision_text = ''
                comision_text = self.extract_as_one(
                    '//div[contains(./span,"Comisi")]//span[@class="field__item"]/a/text()')
                committees = []
                if comision_text:
                    val_list = comision_text.split(',')
                    if val_list:
                        if len(val_list) > 0:
                            for x in val_list:
                                x = x.strip()
                                if not x.isspace() and x != u'':
                                    committees.append(x)
                        if len(committees) > 0:
                            bill_doc.committees = committees
                            #committees = []

                # Contenido:
                doc_url = self.xpath(
                    '//div[contains(./span,"Contenido:")]/span/following-sibling::div/a[contains(@href,"http") or contains(@href,"sites/")]')
                publicacion_gaceta_del_congreso_text = ''
                if doc_url:
                    doc_title = self.extract_single('text()', sel=doc_url)

                    doc_href = self.extract_single('@href', sel=doc_url)
                    if doc_title and doc_href:
                        document_title = None
                        if 'sites/' in doc_href:
                            document_download_url = urljoin(self.base_url, doc_href)
                            url_parsed = urlparse(document_download_url)
                            if url_parsed is not None and hasattr(url_parsed, 'path'):
                                document_title = os.path.basename(unquote(url_parsed.path))
                        else:
                            url_parsed = list(urlparse(doc_href))
                            if url_parsed is not None and len(url_parsed)>0:
                                if 'servoaspr.imprenta.gov.co' in url_parsed[1]:
                                    url_parsed[1] = 'www.imprenta.gov.co'
                                doc_href = urlunparse(url_parsed)

                            document_download_url = doc_href

                        bill_doc.document_title=doc_title.strip()
                        req_args = {'timeout': (60, 120) }
                        resp = self.http_request(document_download_url, "HEAD", request_args=req_args)
                        if 'pdf' in resp.headers['Content-Type']:
                            extraction_type = self.extraction_type.unknown_new
                            if document_title is None:
                                document_title = doc_title + '.pdf'
                            content_type = 'application/pdf'
                            download_id, _, doc_ids = self.register_download_and_documents(
                                document_download_url,
                                self.scraper_policy.doc_service,
                                extraction_type, True,
                                content_type=content_type)
                        elif 'msword' in resp.headers['Content-Type']:
                            if document_title is None:
                                document_title = doc_title + '.doc'
                            extraction_type = self.extraction_type.msword_doc
                            content_type = 'application/msword'
                            download_id, _, doc_ids = self.register_download_and_documents(
                                document_download_url,
                                self.scraper_policy.doc_service,
                                extraction_type, True,
                                content_type=content_type)
                        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in resp.headers['Content-Type']:
                            if document_title is None:
                                document_title = doc_title + '.docx'
                            extraction_type = self.extraction_type.msword_docx
                            content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                            download_id, _, doc_ids = self.register_download_and_documents(
                                document_download_url,
                                self.scraper_policy.doc_service,
                                extraction_type, True,
                                content_type=content_type)
                        elif 'html' in resp.headers['Content-Type']:
                            if document_title is None:
                                document_title = doc_title + '.html'
                            extraction_type = self.extraction_type.html
                            content_type = 'text/html'
                            download_id, _, doc_ids = self.register_download_and_documents(
                                document_download_url,
                                self.scraper_policy.doc_service,
                                extraction_type, True,
                                content_type=content_type, get_static_content=get_static_content,should_download=True, should_skip_checks=True)

                        else:
                            self.logger.debug(__name__, resp.headers['Content-Type'])
                            raise ValueError('Unknown doc type found.........')

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

                        if document_id and download_id:
                            bill_doc.document_title = document_title
                            bill_doc.document_id = document_id
                            bill_doc.download_id = download_id



                # ESTADO ACTUAL:
                estado_actual_text = self.extract_as_one(
                    '//div[contains(.,"Estado actual:")]/div[contains(@class,"field__item")]/text()')
                if estado_actual_text and not estado_actual_text.isspace():
                    bill_doc.short_status=estado_actual_text

                # OBSERVACIONES:
                observaciones_text = self.extract_as_one(
                    '//div[contains(.,"Observaciones:")]/div[contains(@class,"field__item")]//text()')
                if observaciones_text and not observaciones_text.isspace():
                    bill_doc.full_status = observaciones_text

                # AUTOR:
                author_text = self.extract_as_one('normalize-space(//div[contains(./span,"Autor:")])')
                if author_text and not author_text.isspace():
                    author_text = re.sub(r'Autor:', '', author_text)
                    author_text = re.sub(r'[\,|]?\s*H\.([RS])\.', ',H.\\1.', author_text)
                    author_text = author_text.strip()
                    author_text = re.sub(r'\s+', ' ', author_text)

                    val_list = author_text.split(',')

                    if val_list:
                        if len(val_list) > 0:
                            author_sets = set()
                            for x in val_list:
                                x = x.strip()
                                x = x.strip('-')
                                if not x.isspace() and x != u'':
                                    author_sets.add(x)
                            if len(author_sets) > 0:
                                bill_doc.authors = list(author_sets)

                if self.validate_doc(bill_doc):
                    self.save_doc(bill_doc)
                else:
                    self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("JsonSchema validation failed for bill page: {}", bill_short_info['bill_url']))
            except Exception as e:
                self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Error occured: {}",str(e)), exc_info=True)
        else:
            raise ValueError("URL not found for bill detail page.")


    def search_by_search_term(self, search_term, page_number=None):
        try:
            search_url = self.search_url
            if page_number is not None:
                self.restore_selector()
            else:
                self.http_get(search_url, self.scraper_policy.doc_list)

            drupal_script_tag = self.extract_single('//script[@data-drupal-selector="drupal-settings-json"]/text()')

            data = dict()
            data['_drupal_ajax'] = 1
            if page_number:
                data['page'] = int(page_number) - 1

            if drupal_script_tag is not None and not drupal_script_tag.isspace():
                drupal_settings = json.loads(drupal_script_tag)
                if drupal_settings is not None and len(drupal_settings):
                    if 'ajaxPageState' in drupal_settings:
                        for key in drupal_settings['ajaxPageState']:
                            val = drupal_settings['ajaxPageState'][key]
                            data['ajax_page_state['+key+']'] = val if val else ''

                    if 'views' in drupal_settings:
                        for keyid in drupal_settings['views']['ajaxViews']:
                            if 'dom_id' in keyid:
                                m = re.search(r'\:(.+?)$', string=keyid)
                                if m:
                                    view_dom_id = m.group(1)
                                    data['view_dom_id'] = view_dom_id
                                    for key in drupal_settings['views']['ajaxViews'][keyid]:
                                        data[key] = drupal_settings['views']['ajaxViews'][keyid][key]
                    if 'view_path' in data:
                        data['view_path'] = '/views/ajax'
                    if '_drupal_ajax' in data:
                        data['_wrapper_format'] = 'drupal_ajax'

                    form_id = 'views-exposed-form-proyectos-de-ley-secretaria-block-1'
                    input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)



                    if input_fields_dict:
                        input_fields_dict.update(data)
                        if 'combine' in input_fields_dict and search_term is not None:
                            input_fields_dict['combine'] = search_term
                        elif search_term is not None and 'combine' not in input_fields_dict:
                            raise Exception('Search field not found in html form on search page.')

                    try:
                        extra_http_headers = {'X-Requested-With': 'XMLHttpRequest',
                                   'Accept': 'application/json, text/javascript, */*; q=0.01',
                                   'Origin': self.base_url,
                                   'Referer': self.search_url}
                        http_headers = get_default_http_headers()
                        http_headers.update(extra_http_headers)
                        req_args = {'data': input_fields_dict, 'headers': http_headers}
                        ajax_search_url = self.ajax_search_url

                        resp = self.http_request(ajax_search_url, method='POST', request_args=req_args)
                        encoding = 'utf-8'
                        resp.encoding = encoding
                        ajax_json_text = resp.json()
                        first_id = 0
                        last_id = len(ajax_json_text) - 1

                        if 'data' in ajax_json_text[last_id]:
                            ajax_html_selector = ajax_json_text[last_id]['selector']
                            ajax_html_selector = ajax_html_selector.strip('.')
                            ajax_html_text = ajax_json_text[last_id]['data']
                            ajax_script_text = ajax_json_text[first_id]['settings']
                            ajax_script_text = '<script type="application/json" data-drupal-selector="drupal-settings-json">' + \
                                               json.dumps(ajax_script_text) + '</script> '
                            ajax_html_text = ajax_script_text + ajax_html_text
                            if hasattr(self._sel, 'root'):
                                xroot = self._sel.root
                            else:
                                raise ValueError("lxml root not found.")

                            prev_script = xroot.xpath('//script[@data-drupal-selector="drupal-settings-json"]')[0]
                            new_script = fromstring(ajax_script_text)

                            head = xroot.xpath('//head')[0]
                            head.replace(prev_script, new_script)
                            new_div = fromstring(ajax_html_text)
                            try:
                                prev_div = xroot.xpath("//div[contains(@class,$selector)]", selector= ajax_html_selector)
                                if prev_div:
                                    prev_div = prev_div[0]
                                    prev_selector_span = xroot.xpath("//div[contains(@class,$selector)]/parent::span",
                                                                     selector=ajax_html_selector)
                                    if prev_selector_span:
                                        prev_selector_span = prev_selector_span[0]
                                        prev_selector_span.replace(prev_div, new_div)
                                        new_html = tostring(xroot, encoding='unicode')
                                        self.set_selector(text=new_html)
                                        self.backup_selector()
                            except Exception as e:
                                self.logger.critical(__name__, "scraper_failed",
                                                  "Colombia Scraper Failed. Error occured while updating html DOM. Error:" + str(e), exc_info=True)
                        else:
                            raise ValueError("Data json field not found in ajax response.")
                    except Exception as e:
                        self.logger.critical(__name__, "scraper_failed",
                                          "Colombia Scraper Failed. Error occured while sending AJAX request for searching bills. Error:" + str(e), exc_info=True)
                else:
                    raise ScrapeError(self.scraper_policy.doc_list,
                                  u"Drupal settings json not found.", self.search_url)
            else:
                raise ScrapeError(self.scraper_policy.doc_list,
                                  u"Script tag for drupal settings json not found.", self.search_url)

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("Colombia Scraper Failed. Error occured in get_search_result. Error: {}", e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"Colombia Scraper Failed. Error occured in get_search_result. Error: {}", e),self.search_url)


def get_static_content(html_file):


    root = None
    raw_text = html_file.read()

    try:
        unicode_text = raw_text.decode('WINDOWS-1252', 'ignore')
        root = fromstring(unicode_text)
    except ValueError:
        pass

    if root is None:
        root = fromstring(raw_text)

    elem = root.xpath(u"//body")
    if elem is not None and len(elem)>0:
        body_text = tostring(elem[0], encoding='unicode')
        return u"<html>{}</html>".format(body_text)
    else:
        html_text = tostring(root, encoding='unicode')
        if unicode_text is not None:
            html_text_to_check = unicode_text
        else:
            html_text_to_check = raw_text
        if '<body' in html_text_to_check:
            m = re.search('(<html.*?>.+?</html>)', html_text_to_check, re.DOTALL | re.I | re.U)
            if m:
                new_html_text = m.group(1)
                m = re.search('(^.+?)<html.*?>', html_text_to_check, re.DOTALL | re.I | re.U)
                if m:
                    above_html_text = m.group(1)
                    above_html_text = re.sub(r'<!doctype.*?>','',above_html_text,re.DOTALL|re.I|re.U)
                    child_nodes = fromstring(above_html_text)

                root = fromstring(new_html_text)
                elem = root.xpath(u"//body")
                if elem is not None and len(elem)>0:
                    body_node = elem[0]
                    if child_nodes is not None:
                        body_node.insert(0, child_nodes)
                    body_text = tostring(body_node, encoding='unicode')
                    return u"<html>{}</html>".format(body_text)
        else:
            return html_text


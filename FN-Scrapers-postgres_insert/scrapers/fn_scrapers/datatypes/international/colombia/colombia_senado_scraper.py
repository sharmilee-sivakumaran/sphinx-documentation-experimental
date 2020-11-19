# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import re
import six
import injector
import datetime
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, two_digit_year_to_four_digit_year

from six.moves.urllib.parse import unquote


@scraper()
@argument("--session", help="Session to scrape.", type=str, required=True, default='2017-2018', choices=['2017-2018','2016-2017', '2015-2016', '2014-2015', '2013-2014', '2012-2013', '2011-2012', '2010-2011'])
@argument("--search-term", help="Search term to search and filter search results to scrape bills.", type=str, required=True)
@tags(type="bills", country_code="CO", group="international")
class ColombiaSenadoDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ColombiaSenadoDocScraper, self).__init__(EventComponent.scraper_bills, "colombia", "colombia")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "colombia.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("colombia").alpha_2)
        self.base_url = u'http://leyes.senado.gov.co/proyectos'


    def get_search_url(self, session='2016-2017'):
        session_url_dict = {
            'fast-track': '/periodo-legislativo-2014-2018/fasttrack',
            '2017-2018': '/periodo-legislativo-2014-2018/2017-2018',
            '2016-2017': '/periodo-legislativo-2014-2018/2016-2017',
            '2015-2016': '/periodo-legislativo-2014-2018/2015-2016',
            '2014-2015': '/periodo-legislativo-2014-2018/2014-2015',
            '2013-2014': '/periodo-legislativo-2010-2014/2013-2014',
            '2012-2013': '/periodo-legislativo-2010-2014/2012-2013',
            '2011-2012': '/periodo-legislativo-2010-2014/2011-2012',
            '2010-2011': '/periodo-legislativo-2010-2014/2010-2011',
        }
        if session in session_url_dict:
            return u'{base_url}/index.php/proyectos-ley{session_url}'.format(
                base_url=self.base_url, session_url=session_url_dict[session])

    def scrape(self, session='2016-2017', search_term=None):
        self.scrape_bills(session, search_term)


    def scrape_bills(self, session='2016-2017', search_term=None):
        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        :param search_term: Search keyword
        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''
        search_url = self.get_search_url(session)
        try:
            self.get_search_result(session=session, search_term=search_term)
            bill_rows = self.xpath('//tr[contains(@class,"odd") or contains(@class,"even")]')
            bill_short_info_list = []
            self.logger.info(__name__, fmt('Total Bills found: {}', len(bill_rows)))
            for bill_row in bill_rows:
                bill_short_info = dict()

                bill_short_info['session'] = session
                bill_short_info['source_chamber'] = 'upper'
                search_url = self.get_search_url(session)
                m = re.search(r'periodo\-legislativo\-(.+?)\/', search_url)
                if m:
                    bill_short_info['cuatrenio'] = m.group(1)

                bill_short_title_cell = bill_row.xpath('./td[position()=3]')

                bill_short_title_url = self.extract_single('.//h3/a/@href', sel=bill_short_title_cell)
                if bill_short_title_url:
                    bill_url = bill_short_title_url.strip()
                    bill_url = urljoin(self.base_url, bill_url)
                    bill_short_info['bill_url'] = bill_url

                bill_short_title_text = self.extract_single('.//h3/a/text()', sel=bill_short_title_cell)
                if bill_short_title_text:
                    bill_short_info['title'] = bill_short_title_text.strip()

                    if isinstance(bill_short_info['title'], six.string_types):
                        bill_short_info['title'] = bill_short_info['title'].title()

                comision_text = self.extract_as_one('./td[position()=1]/text()', sel=bill_row)
                if comision_text:
                    comision_text = comision_text.strip()
                    comision_text = re.sub(r'\s+', ' ', comision_text)
                    bill_short_info['comision'] = comision_text.title()
                    if isinstance(bill_short_info['comision'],basestring):
                        bill_short_info['comision'] = bill_short_info['comision'].title()

                estado_text = self.extract_as_one('./td[position()=2]//text()', sel=bill_row)
                if estado_text:
                    estado_text = estado_text.strip()
                    estado_text = re.sub(r'\s+', ' ', estado_text)
                    bill_short_info['estado'] = estado_text
                    if isinstance(bill_short_info['estado'],basestring):
                        bill_short_info['estado'] = bill_short_info['estado'].title()

                small_div = bill_row.xpath('./td[position()=3]/div[@class="small"]')
                senado_text = self.extract_as_one('./font[contains(.,"Senado:")]/text()', sel=small_div)

                if senado_text:
                    senado_text = senado_text.strip()
                    senado_text = re.sub(r'\s+', ' ', senado_text)
                    m = re.search(r'((\d+)\s*\/\s*(\d+))', senado_text)
                    if m:
                        senado_id = m.group(1)
                        bill_short_info['senado_id'] = senado_id
                        bill_short_info['year'] = two_digit_year_to_four_digit_year(int(m.group(3)))
                else:
                    senado_text = self.extract_as_one('./b[position()=2]/text()', sel=small_div)
                    if senado_text:
                        senado_text = senado_text.strip()
                        senado_text = re.sub(r'\s+', ' ', senado_text)
                        m = re.search(r'((\d+)\s*\/\s*(\d+))', senado_text)
                        if m:
                            senado_id = m.group(1)
                            bill_short_info['senado_id'] = senado_id
                            bill_short_info['year'] = two_digit_year_to_four_digit_year(int(m.group(3)))

                camara_text = self.extract_as_one('./font[contains(.,"Camara:")]/text()', sel=small_div)
                if camara_text:
                    camara_text = camara_text.strip()
                    camara_text = re.sub(r'\s+', ' ', camara_text)
                    m = re.search(r'(\d+\s*\/\s*\w+)', camara_text)
                    if m:
                        camara_id = m.group(1)
                        bill_short_info['camara_id'] = camara_id
                else:
                    camara_text = self.extract_as_one('./b[position()=3]/text()', sel=small_div)
                    if camara_text:
                        camara_text = camara_text.strip()
                        camara_text = re.sub(r'\s+', ' ', camara_text)
                        m = re.search(r'(\d+\s*\/\s*\w+)', camara_text)
                        if m:
                            camara_id = m.group(1)
                            bill_short_info['camara_id'] = camara_id

                author_para = bill_row.xpath('./td[position()=3]/p[contains(.,"Autor:")]')
                # Author:
                author_cell_text = self.extract_as_one('./font[contains(.,"Author:")]/following-sibling::b//text()', sel=author_para)
                if author_cell_text:
                    author_cell_text = author_cell_text.strip()
                    author_cell_text = re.sub(r'\s+', ' ', author_cell_text)
                    author_cell_text = re.sub(r'H\.R\.', ',H.R.', author_cell_text)
                    authors = set()
                    if author_cell_text:
                        val_list = author_cell_text.split(',')
                        if val_list:
                            if len(val_list) > 0:
                                for x in val_list:
                                    x = x.strip()
                                    if not x.isspace() and x != u'':
                                        authors.add(x.title())
                    bill_short_info['authors'] = list(authors)
                else:
                    author_cell_text = self.extract_as_one('./b[position()=1]/text()', sel=author_para)
                    if author_cell_text:
                        author_cell_text = author_cell_text.strip()
                        author_cell_text = re.sub(r'\s+', ' ', author_cell_text)
                        author_cell_text = re.sub(r'H\.R\.', ',H.R.', author_cell_text)
                        authors = set()
                        if author_cell_text:
                            val_list = author_cell_text.split(',')
                            if val_list:
                                if len(val_list) > 0:
                                    for x in val_list:
                                        x = x.strip()
                                        if not x.isspace() and x != u'':
                                            authors.add(x.title())
                        bill_short_info['authors'] = list(authors)

                bill_short_info_list.append(bill_short_info)

            for bill_short_info in bill_short_info_list:

                if bill_short_info.setdefault('bill_url', None) is not None:
                    self.logger.info(__name__,fmt('Going for individual bill: {}',bill_short_info['bill_url']))

                    self.http_get(bill_short_info['bill_url'], self.scraper_policy.doc_list)

                    bill_doc = self.model_factory.create_bill_doc()
                    bill_doc.source_url = bill_short_info['bill_url']
                    if bill_short_info.setdefault('title', None) is not None:
                        bill_doc.title = bill_short_info.setdefault('title', None)
                    session = bill_short_info.setdefault('session', None)
                    if session is not None:
                        bill_doc.session = session
                    year = bill_short_info.setdefault('year', None)
                    if year is not None:
                        bill_doc.year = int(year)
                        if session is None:
                            session = str(year) + '-' + str(year-1)
                            bill_doc.session = session
                    if bill_short_info.setdefault('cuatrenio', None) is not None:
                        bill_doc.legislature = bill_short_info.setdefault('cuatrenio', None)
                    if bill_short_info.setdefault('camara_id', None) is not None:
                        bill_doc.camara_id = bill_short_info.setdefault('camara_id', None)
                    if bill_short_info.setdefault('senado_id', None) is not None:
                        bill_doc.senado_id = bill_short_info.setdefault('senado_id', None)
                    if bill_short_info.setdefault('estado', None) is not None:
                        bill_doc.short_status = bill_short_info.setdefault('estado', None)
                    if bill_short_info.setdefault('source_chamber', None) is not None:
                        bill_doc.source_chamber = bill_short_info.setdefault('source_chamber', None)
                    if bill_short_info.setdefault('comision', None) is not None:
                        commmittee = bill_short_info.setdefault('comision', None)
                        if commmittee:
                            bill_doc.committees = [commmittee]
                    if bill_short_info.setdefault('authors', None) is not None:
                        authors = bill_short_info.setdefault('authors', None)
                        if authors and len(authors):
                            bill_doc.authors = authors

                    main_div_block = self.xpath('//div[@class="block"]')
                    # Origen
                    origen_text = ''
                    origen_text = self.extract_single(
                        u'//table[@lang]//tr[contains(.,"Origen")]/td[contains(text(),"Origen:")]/following-sibling::td/text()', sel=main_div_block)

                    if origen_text:
                        origen_text = origen_text.strip()
                        # Camara == lower, Senado == upper
                        if 'mara' in origen_text.lower():
                            bill_doc.chamber_of_origin = 'lower'
                            origen_text = 'lower'
                        elif 'senado' in origen_text.lower():
                            bill_doc.chamber_of_origin = 'upper'
                            origen_text = 'upper'

                    # Fecha de Presentación:
                    fecha_de_presentacion_text = self.extract_single(
                        u'//td[contains(.,"Fecha de Presentac")]/following-sibling::td/text()', sel=main_div_block)
                    if fecha_de_presentacion_text:
                        fecha_de_presentacion_text = fecha_de_presentacion_text.strip()
                        filing_date = parse_date_as_str(fecha_de_presentacion_text, languages=self.country_languages)
                        if filing_date:
                            if origen_text == 'lower':
                                bill_doc.camara_filing_date = filing_date
                            elif origen_text == 'upper':
                                bill_doc.senate_filing_date = filing_date

                    # Resumen:
                    resumen_text = ''
                    resumen_text = self.extract_single(
                        u'//dt[contains(.,"Resumen:")]/following-sibling::table//td//div[@align]/text()', sel=main_div_block)


                    if resumen_text:
                        resumen_text = resumen_text.strip()
                        bill_doc.summary = resumen_text

                    # Doc Page link
                    # Ver o Descargar Texto Radicado
                    doc_page_url = self.extract_single(
                        u'//a[contains(text(),"Ver o Descargar Texto Radicado") and contains(@href,"http")]/@href')

                    doc_url = ''
                    if doc_page_url:
                        doc_page_url = doc_page_url.replace(u'\xa0', u' ')
                        doc_page_url = doc_page_url.replace("http://192.168.0.32", "http://leyes.senado.gov.co")
                        doc_page_url = doc_page_url.replace("http://190.26.211.102", "http://leyes.senado.gov.co")

                        if re.search(r'\.(pdf|doc)\w*\s*$',doc_page_url):
                            doc_url = doc_page_url
                        else:
                            self.logger.info(__name__, 'Going for individual bill document page: ' + doc_page_url )
                            self.http_get(doc_page_url, self.scraper_policy.doc)
                            doc_url = self.extract_single(u'//a[contains(.,"Descargar") and contains(@class,"edocs_link") and contains(@href,"http")]/@href')

                    if doc_url:
                        doc_url = doc_url.replace(u'\xa0', u' ')
                        doc_url = doc_url.replace("http://192.168.0.32", "http://leyes.senado.gov.co")
                        doc_url = doc_url.replace("http://190.26.211.102", "http://leyes.senado.gov.co")
                        doc_title = os.path.basename(unquote(doc_url))
                        doc_title = doc_title.strip()
                        if doc_title:
                            document_download_url = doc_url.encode('utf-8')
                            bill_doc.document_title = doc_title.strip()
                            resp = self.http_request(document_download_url, "HEAD")
                            if 'pdf' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.unknown_new
                                content_type = 'application/pdf'
                                download_id, _, doc_ids = self.register_download_and_documents(
                                    document_download_url,
                                    self.scraper_policy.doc_service,
                                    extraction_type, True,
                                    content_type=content_type)
                            elif 'msword' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.msword_doc
                                content_type = 'application/msword'
                                download_id, _, doc_ids = self.register_download_and_documents(
                                    document_download_url,
                                    self.scraper_policy.doc_service,
                                    extraction_type, True,
                                    content_type=content_type)
                            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.msword_docx
                                content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                                download_id, _, doc_ids = self.register_download_and_documents(
                                    document_download_url,
                                    self.scraper_policy.doc_service,
                                    extraction_type, True,
                                    content_type=content_type)

                            else:
                                self.logger.debug(__name__, resp.headers['Content-Type'])
                                raise ValueError('Unknown doc type found.........')

                            if len(doc_ids) > 0 and doc_ids[0]:
                                document_id = doc_ids[0]
                            else:
                                # HACK! Some documents that this scraper gets seem to erroneously
                                # fail extraction. Once that issue is resolved (DI-1629), this
                                # hack should be removed! If extraction fails, skip the bill
                                # entirely, but, keep going with the scrape.
                                self.logger.warning(
                                    __name__,
                                    "individual_bill_scrape_failed",
                                    fmt(
                                        u"Failed to extract text for: {} on bill page: {}",
                                        doc_url,
                                        bill_short_info['bill_url']))
                                continue

                            if not download_id:
                                raise ValueError(
                                    u"Download ID not found while registering document with url {}".format(
                                        doc_url))

                            if document_id and download_id:
                                bill_doc.document_title = doc_title
                                bill_doc.document_id = document_id
                                bill_doc.download_id = download_id
                        else:
                            self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Bill document url not found on Document viewer page: {}", doc_page_url))
                    else:
                        self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Bill document not found on Bill page: {}", bill_short_info['bill_url']))

                    if self.validate_doc(bill_doc):
                        self.save_doc(bill_doc)
                    else:
                        self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("JsonSchema validation failed for bill page: {}", bill_short_info['bill_url']))

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("Colombia bills could not be scraped. {}", e), exc_info=True)

            raise ScrapeError(self.scraper_policy.doc_list, fmt(u"{} : Colombia bills could not be scraped. {}", self.scraper_name.title(), e), search_url)

    def get_search_result(self, session, search_term):
        search_url = self.get_search_url(session)
        try:
            self.http_get(search_url, self.scraper_policy.doc_list)
            form_name = 'searchForm'
            input_fields_dict, method, action = self.html_form_parser(search_type="name", form_name=form_name)
            if input_fields_dict:
                # Bill status ['all', 'assented', 'passed', 'pending', 'withdrawn', 'negatived', 'lapsed']
                if 'limit' in input_fields_dict:
                    input_fields_dict['limit'] = '0'
                else:
                    raise Exception('limit field not found in html form on search page.')
                if 'search' in input_fields_dict and search_term:
                    input_fields_dict['search'] = search_term
                elif search_term and not 'search' in input_fields_dict:
                    raise Exception('search field not found in html form on search page.')

            req_args = {'params': input_fields_dict}
            self.http_get(search_url, self.scraper_policy.doc_list, request_args=req_args)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("Colombia Scraper Failed. Error occured in get_search_result. Error: {}", e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list, fmt(u"Colombia Scraper Failed. Error occured in get_search_result. Error: {}", e), search_url)
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import re

import injector
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, \
    get_default_http_headers


@scraper()
@argument("--type", help="Bill type to scrape.", type=str, default='all', choices=['iniciativa','proyectos','all'])
@tags(type="bills", country_code="MX", group="international")
class MexicoDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(MexicoDocScraper, self).__init__(EventComponent.scraper_bills, "mexico", "mexico")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "mexico.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("mexico").alpha_2)

        self.base_url = u'http://infosen.senado.gob.mx'
        self.search_url_iniciativas = u'{base_url}/fichas_tecnicas/index.php?w=1'.format(base_url=self.base_url)
        self.search_url_proyectos = u'{base_url}/fichas_tecnicas/index.php?w=4'.format(base_url=self.base_url)


    def scrape(self, type='all'):

        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        '''

        try:
            if type == 'all':
                self.scrape_iniciativas()
                self.scrape_proyectos()
            elif type == 'proyectos':
                self.scrape_proyectos()
            elif type == 'iniciativa':
                self.scrape_iniciativas()

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name, e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list, fmt("{} bills could not be scraped. {}", self.scraper_name, e),self.base_url)

    def scrape_iniciativas(self):
        search_url = self.search_url_iniciativas
        try:
            self.logger.info(__name__, fmt('Going for searching Iniciativas bills: {}', search_url))
            self.http_get(search_url, self.scraper_policy.doc_list)
            bill_url_list = []
            #$x('//table[@class="TamanoFuente"]/tbody/tr[not(contains(./td/div,"NOMBRE")) and position() mod 2 = 0 and position() >= 2]')

            table_rows = self.xpath('//table[@class="TamanoFuente"][contains(.,"NOMBRE")]/tbody/tr[not(contains(./td/div,"NOMBRE")) and position() mod 2 = 0 and position() >= 2]')
            self.logger.info(__name__, fmt("Total records found : {}", len(table_rows)))
            if table_rows is not None and len(table_rows)>0:
                for row in table_rows:
                    bill_url = self.extract_single('.//td/a[contains(.,"Ficha T")]/@href', sel=row)
                    if bill_url is not None and not bill_url.isspace():
                        bill_url = urljoin(search_url, bill_url.strip())
                        bill_url_list.append(bill_url)
                    else:
                        self.logger.critical(__name__, "scraper_failed", fmt("Error occured while scraping all bill url."))
                        break
            if 0 < len(bill_url_list) == len(table_rows):
                for index, bill_url in enumerate(bill_url_list):
                    self.scrape_bill_page(bill_url)
            else:
                return None

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt('Mexico scraper failed. Error occured in scrape_iniciativas() with error message: {}', e))
            raise ScrapeError(self.scraper_policy.doc_list,fmt("Mexico Scraper Failed. Error occured in search_by_search_term. Error: {}", e), search_url)

    def scrape_proyectos(self):
        search_url = self.search_url_proyectos
        try:
            self.logger.info(__name__, fmt('Going for searching Proyectos bills: {}', search_url))
            self.http_get(search_url, self.scraper_policy.doc_list)
            bill_url_list = []
            table_rows = self.xpath(
                '//table[@class="TamanoFuente"][contains(.,"NOMBRE")]/tbody/tr[not(contains(./td/div,"NOMBRE")) and position() mod 2 = 0 and position() >= 2]')
            self.logger.info(__name__, fmt("Total records found : {}", len(table_rows)))
            if table_rows is not None and len(table_rows)>0:
                for row in table_rows:
                    bill_url = self.extract_single('.//td/a[contains(.,"Ficha T")]/@href', sel=row)
                    if bill_url is not None and not bill_url.isspace():
                        bill_url = urljoin(search_url, bill_url.strip())
                        bill_url_list.append(bill_url)
                    else:
                        self.logger.critical(__name__, "scraper_failed", fmt("Error occured while scraping all bill url."))
                        break
            if 0 < len(bill_url_list) == len(table_rows):
                for index, bill_url in enumerate(bill_url_list):
                    self.scrape_bill_page(bill_url)
            else:
                return None

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt('Mexico scraper failed. Error occured in scrape_proyectos() with error message: {}', e))
            raise ScrapeError(self.scraper_policy.doc_list,fmt("Mexico Scraper Failed. Error occured in scrape_proyectos. Error: {}", e), search_url)


    def scrape_bill_page(self, bill_url):
        try:
            self.logger.info(__name__, fmt("Going to scrape bill page with url: {}",bill_url))
            self.http_get(bill_url, self.scraper_policy.doc)

            thead_rows = self.xpath('//table[@id="FichaTecnica"]/thead/tr')
            tbody_rows = self.xpath('//table[@id="FichaTecnica"]/tbody/tr')
            if len(thead_rows) > 0 < len(tbody_rows) == len(thead_rows):

                rows = zip(thead_rows, tbody_rows)
                bill_doc = self.model_factory.create_bill_doc()
                bill_doc.source_url = bill_url

                for row_index, (header_row, data_row) in enumerate(rows, start=1):

                    legislatura = None
                    iniciativa_o_minuta = None
                    camara_de_origen = None
                    periodo = None
                    ano_legislativo = None
                    fecha_de_presentacion = None
                    titulo_de_la_iniciativa = None
                    tipo_de_la_iniciativa_o_minuta = None
                    sinopsis = None
                    author = None
                    turno = None
                    proponentes = None
                    estado_actual = None
                    aprobacion_en_el_senado = None
                    aprobacion_camara_de_disputados = None

                    if row_index == 1:
                        # Legislatura
                        legislatura_pos = self.extract_single('count(.//td[text()="Legislatura"]/preceding-sibling::td)+1', sel=header_row)
                        if legislatura_pos:

                            legislatura = self.extract_single('.//td[position()=$col]/text()', sel=data_row, col=int(float(legislatura_pos)))
                            bill_doc.legislature = legislatura

                        # Iniciativa o Minuta
                        iniciativa_pos = self.extract_single('count(.//td[text()="Iniciativa o Minuta"]/preceding-sibling::td)+1', sel=header_row)
                        if iniciativa_pos:

                            iniciativa_o_minuta = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                              col=int(float(iniciativa_pos)))

                            if 'iniciativa' in iniciativa_o_minuta.lower():
                                bill_doc.bill_type = 'Iniciativa'
                            elif 'minuta' in iniciativa_o_minuta.lower():
                                bill_doc.bill_type = 'Minuta'

                        # Cámara de Origen
                        camara_de_origen_pos = self.extract_single(
                            'count(.//td[contains(text(),"mara de Origen")]/preceding-sibling::td)+1', sel=header_row)
                        if camara_de_origen_pos:
                            camara_de_origen = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                             col=int(float(camara_de_origen_pos)))

                            bill_doc.chamber_of_origin = camara_de_origen


                        # Periodo
                        periodo_pos = self.extract_single(
                            'count(.//td[contains(text(),"Periodo")]/preceding-sibling::td)+1', sel=header_row)
                        if periodo_pos:

                            periodo = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                                  col=int(float(periodo_pos)))

                        # Año Legislativo
                        ano_legislativo_pos = self.extract_single(
                            'count(.//td[contains(text(),"o Legislativo")]/preceding-sibling::td)+1', sel=header_row)
                        if ano_legislativo_pos:

                            ano_legislativo = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                          col=int(float(ano_legislativo_pos)))

                        if periodo is not None and ano_legislativo is not None:
                            session = u"{} - {}".format(ano_legislativo, periodo)
                            bill_doc.session = session

                        # Fecha de Presentación
                        fecha_de_presentacion_pos = self.extract_single('count(.//td[contains(text(),"Fecha de Presentaci")]/preceding-sibling::td)+1', sel=header_row)
                        if fecha_de_presentacion_pos:
                            fecha_de_presentacion = self.extract_single('.//td[position()=$col]/text()', sel=data_row, col=int(float(fecha_de_presentacion_pos)))

                            introduction_date = parse_date_as_str(fecha_de_presentacion, languages=['es'])
                            if introduction_date is not None:
                                bill_doc.introduction_date = parse_date_as_str(fecha_de_presentacion, languages=['es'])

                    elif row_index == 3:
                        # Título de la Iniciativa

                        titulo_de_la_iniciativa_pos = self.extract_single(
                            'count(.//td[contains(text(),"tulo de la Iniciativa")]/preceding-sibling::td)+1', sel=header_row)
                        if titulo_de_la_iniciativa_pos:
                            titulo_de_la_iniciativa = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                                          col=int(float(titulo_de_la_iniciativa_pos)))

                            if titulo_de_la_iniciativa is not None:
                                bill_doc.title = titulo_de_la_iniciativa

                        # Tipo de la Iniciativa o Minuta
                        tipo_de_la_iniciativa_o_minuta_pos = self.extract_single('count(.//td[text()="Tipo de la Iniciativa o Minuta"]/preceding-sibling::td)+1', sel=header_row)
                        if tipo_de_la_iniciativa_o_minuta_pos:
                            tipo_de_la_iniciativa_o_minuta = self.extract_single('.//td[position()=$col]/text()', sel=data_row, col=int(float(tipo_de_la_iniciativa_o_minuta_pos)))

                            if tipo_de_la_iniciativa_o_minuta is not None:
                                bill_doc.bill_subtype = tipo_de_la_iniciativa_o_minuta.strip()

                    elif row_index == 5:
                        # Sinopsis
                        sinopsis_pos = self.extract_single('count(.//td[text()="Sinopsis"]/preceding-sibling::td)+1', sel=header_row)
                        if sinopsis_pos:
                            sinopsis = self.extract_as_one('.//td[position()=$col][not(contains(.,"--"))]//text()', sel=data_row, col=int(float(sinopsis_pos)))

                            if sinopsis is not None:
                                bill_doc.summary = sinopsis.strip()

                    elif row_index == 7:
                        # Autor
                        autor_pos = self.extract_single('count(.//td[text()="Autor"]/preceding-sibling::td)+1',
                                                           sel=header_row)
                        if autor_pos:
                            author = self.extract_single('.//td[position()=$col]/text()', sel=data_row,
                                                           col=int(float(autor_pos)))

                            if author is not None:
                                bill_doc.authors = author

                        # Turno
                        turno_pos = self.extract_single('count(.//td[text()="Turno"]/preceding-sibling::td)+1',
                                                           sel=header_row)
                        if turno_pos:
                            turno = self.extract('.//td[position()=$col]//text()', sel=data_row,
                                                           col=int(float(turno_pos)))

                            if turno is not None:
                                if len(turno) > 0:
                                    committee_sets = set()

                                    for t in turno:
                                        t = t.strip()
                                        if not t.isspace() and t != u'':
                                            committee_sets.add(t)

                                    if len(committee_sets) > 0:
                                        bill_doc.committees = list(committee_sets)



                    elif row_index == 8:
                        #Proponentes
                        proponentes_pos = self.extract_single('count(.//td[text()="Proponentes"]/preceding-sibling::td)+1',
                                                        sel=header_row)
                        if proponentes_pos:
                            proponentes = self.extract('.//td[position()=$col][not(contains(.,"--"))]//text()', sel=data_row,
                                                        col=int(float(proponentes_pos)))

                            if proponentes is not None:
                                if len(proponentes) > 0:
                                    sponsor_sets = set()
                                    for proponente in proponentes:
                                        proponente = proponente.strip()
                                        proponente = proponente.replace(' y ', ',')

                                        if ',' in proponente:
                                            proponente_list = proponente.split(',')
                                            for x in proponente_list:
                                                x = x.strip()
                                                x = x.strip('-')
                                                if not x.isspace() and x != u'':
                                                    sponsor_sets.add(x)
                                        else:
                                            x = proponente.strip()
                                            x = x.strip('-')
                                            if not x.isspace() and x != u'':
                                                sponsor_sets.add(x)
                                    if len(sponsor_sets) > 0:
                                        bill_doc.sponsors = list(sponsor_sets)


                    elif row_index == 9:
                        # Estado Actual
                        estado_actual_pos = self.extract_single(
                            'count(.//td[text()="Estado Actual"]/preceding-sibling::td)+1',
                            sel=header_row)
                        if estado_actual_pos:
                            estado_actual = self.extract_single('.//td[position()=$col][not(contains(.,"--"))]//text()', sel=data_row,
                                                              col=int(float(estado_actual_pos)))

                            if estado_actual is not None:
                                bill_doc.current_status = estado_actual.strip()

                        # Aprobación en el Senado
                        aprobacion_en_el_senado_pos = self.extract_single(
                            'count(.//td[contains(text(),"n en el Senado")]/preceding-sibling::td)+1',
                            sel=header_row)
                        if aprobacion_en_el_senado_pos:
                            aprobacion_en_el_senado = self.extract_single('.//td[position()=$col][not(contains(.,"--"))]//text()', sel=data_row,
                                                                col=int(float(aprobacion_en_el_senado_pos)))


                            senate_approval_date = parse_date_as_str(aprobacion_en_el_senado, languages=['es'])
                            if senate_approval_date:
                                bill_doc.senate_approval_date = senate_approval_date

                        # Aprobación Cámara de Diputados
                        aprobacion_camara_de_disputados_pos = self.extract_single(
                            'count(.//td[contains(text(),"mara de Diputados")]/preceding-sibling::td)+1',
                            sel=header_row)

                        if aprobacion_camara_de_disputados_pos:
                            aprobacion_camara_de_disputados = self.extract_single('.//td[position()=$col]//text()', sel=data_row,
                                                                          col=int(float(aprobacion_camara_de_disputados_pos)))


                            house_approval_date = parse_date_as_str(aprobacion_camara_de_disputados, languages=['es'])
                            if house_approval_date:
                                bill_doc.house_approval_date = house_approval_date


                    elif row_index == 10:
                        # URL de la Gaceta
                        url_de_la_gaceta_pos = self.extract_single(
                            'count(.//td[text()="URL de la Gaceta"]/preceding-sibling::td)+1', sel=header_row)
                        if url_de_la_gaceta_pos:
                            url_de_la_gaceta = data_row.xpath('.//td[position()=$col][not(contains(.,"--"))]/a', col=int(float(url_de_la_gaceta_pos)))
                            if url_de_la_gaceta is not None:
                                bill_doc_url = self.extract_single('@href', sel=url_de_la_gaceta)

                                if bill_doc_url is not None and not bill_doc_url.isspace():

                                    self.logger.info(__name__, fmt("Going to scrape bill page with url: {}", bill_doc_url.strip()))
                                    self.http_get(bill_doc_url.strip(), self.scraper_policy.doc)
                                    # table[@class="info_gaceta"]//*[contains(.,"Gaceta:")]/text()
                                    gaceta = self.extract_single(
                                        '//table[@class="info_gaceta"]//*[contains(.,"XGaceta:")]/text()')

                                    gaceta = self.extract_single('//table[@class="info_gaceta"]//*[contains(.,"Gaceta:")]/text()')
                                    if gaceta is None:
                                        m = re.search(r'Gaceta:(.+?)<\/', self.get_content_from_response(), re.I|re.U|re.DOTALL)
                                        if m:
                                            gaceta = m.group(1)
                                    else:
                                        gaceta = gaceta.replace('Gaceta:','')

                                    if gaceta is not None:
                                        # Descargar Documento
                                        # //div[@class="lista_gaceta"]//a[contains(.,"Descargar Documento")]/@href
                                        document_title = gaceta.strip().replace('/','_')
                                        descargar_documento = self.extract_single('//div[@class="lista_gaceta"]//a[contains(.,"Descargar Documento")]/@href')
                                        if descargar_documento is not None:
                                            document_download_url = descargar_documento
                                        else:
                                            document_download_url = bill_doc_url
                                        extra_http_headers = {'Referer': bill_url}
                                        http_headers = get_default_http_headers()
                                        http_headers.update(extra_http_headers)
                                        req_args = {'headers': http_headers}

                                        resp = self.http_request(document_download_url, "HEAD", request_args=req_args)
                                        if 'pdf' in resp.headers['Content-Type']:
                                            extraction_type = self.extraction_type.unknown
                                            document_title = document_title + '.pdf'
                                            content_type = 'application/pdf'
                                            download_id, _, doc_ids = self.register_download_and_documents(
                                                document_download_url,
                                                self.scraper_policy.doc_service,
                                                extraction_type, True,
                                                content_type=content_type)
                                        elif 'msword' in resp.headers['Content-Type']:
                                            document_title = document_title + '.doc'
                                            extraction_type = self.extraction_type.msword_doc
                                            content_type = 'application/msword'
                                            download_id, _, doc_ids = self.register_download_and_documents(
                                                document_download_url,
                                                self.scraper_policy.doc_service,
                                                extraction_type, True,
                                                content_type=content_type)
                                        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in resp.headers['Content-Type']:
                                            document_title = document_title + '.docx'
                                            extraction_type = self.extraction_type.msword_docx
                                            content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                                            download_id, _, doc_ids = self.register_download_and_documents(
                                                document_download_url,
                                                self.scraper_policy.doc_service,
                                                extraction_type, True,
                                                content_type=content_type)
                                        elif 'html' in resp.headers['Content-Type']:
                                            document_title = document_title + '.html'
                                            extraction_type = self.extraction_type.html
                                            content_type = 'text/html'
                                            download_id, _, doc_ids = self.register_download_and_documents(
                                                document_download_url,
                                                self.scraper_policy.doc_service,
                                                extraction_type, True,
                                                content_type=content_type)

                                        else:
                                            self.logger.info(__name__, resp.headers['Content-Type'])
                                            raise ValueError(
                                                u'Unknown doc type found: {} for: {} on page: {}'.format(
                                                    resp.headers['Content-Type'],
                                                    document_download_url,
                                                    bill_url))

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
                                            bill_document = self.model_factory.create('BillDocumentSchema')
                                            bill_document.document_title = document_title
                                            bill_document.document_id = document_id
                                            bill_document.download_id = download_id
                                            bill_doc.document = bill_document
                                    else:
                                        self.logger.critical(__name__, "individual_bill_scrape_failed",
                                                             fmt("Gaceta not found for bill url: {}", bill_url))

                if self.validate_doc(bill_doc):
                    self.save_doc(bill_doc)
                else:
                    self.logger.critical(__name__, "individual_bill_scrape_failed",
                                         fmt("JsonSchema validation failed for bill url: {}",bill_url))
            else:
                raise ValueError("No table rows found on bill page.")

        except Exception as e:
            self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Error occured {}", e), exc_info=True)







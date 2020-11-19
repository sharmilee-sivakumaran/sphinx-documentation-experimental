# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
import os
import json
import injector
import datetime
import urllib
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy

REQUESTS_RETRY_POLICY = Retry500RequestsRetryPolicy(max_attempts=100,max_retry_time=1000,max_attempt_delay=1000)
@scraper()
@argument('--start', metavar='mm/dd/yy', type=str, default=None,
          help='start date(Default: 30 days before today)(Format:04/01/16)')
@argument('--end', type=str, metavar='mm/dd/yy',
          default=None, help='end date(Default: today)(Format:04/30/16)')
@tags(type="notices", country_code="AR", group="international")
class Argentina_GazetteDocScraper(ScraperBase):
    """
    Many website hits result in the data.
    1) Make a get request to the base url (https://www.boletinoficial.gob.ar/) .
    2) Then have to make a post request to the sub_url_1 (https://www.boletinoficial.gob.ar/secciones/mensajeGeneral) along with referer in the headers.
    3) Then have to make a POST request to the sub_url_2  with some data in the request (https://www.boletinoficial.gob.ar/secciones/secciones.json)
    4) The above url will return id of the session created in the response. This id is used while making a post request
    to sub_url_3 (https://www.boletinoficial.gob.ar/secciones/tiponorma) along with some parameters in the request containing the page number
    """
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(Argentina_GazetteDocScraper, self).__init__(EventComponent.scraper_bills, "argentina_gazette", "argentina_gazette")
        self.base_url = u'https://www.boletinoficial.gob.ar/'
        self.list_url = u"https://www.boletinoficial.gob.ar/secciones/BuscadorAvanzado"
        self.member_url = u'{base_url}{rem_url_part}'
        self.sub_url_1 = 'https://www.boletinoficial.gob.ar/secciones/mensajeGeneral'
        self.sub_url_2 = 'https://www.boletinoficial.gob.ar/secciones/secciones.json'
        self.sub_url_3 = 'https://www.boletinoficial.gob.ar/secciones/tiponorma'
        self.document_url = "https://www.boletinoficial.gob.ar/norma/detallePrimera"
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "regulation_notice.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.session_id = ''

    def scrape(self, start, end):
        if not start:
            start_date = datetime.date.today()-datetime.timedelta(days=30)
        else:
            try:
                start_date = datetime.datetime.strptime(start, '%x').date()
            except ValueError:
                raise Exception("Unknown start date %s" % start)
        if not end:
            end_date = datetime.date.today()
        else:
            try:
                end_date = datetime.datetime.strptime(end, '%x').date()
            except ValueError:
                raise Exception("Unknown end date %s" % end)

        try:
            self.logger.info(__name__, fmt(u"Fetching for main page link  - {} ",self.list_url))
            current_datetime = datetime.datetime.now()
            current_date = current_datetime.date()
            current_date_val = current_datetime.strftime('%Y%m%d')
            headers = {}
            headers.update({
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
                'Connection': 'keep-alive'
            })
            self.http_get(self.base_url,self.scraper_policy.doc_list,request_args={'headers':headers},retry_policy=REQUESTS_RETRY_POLICY)
            headers.update({"Origin": "https://www.boletinoficial.gob.ar"})
            headers.update({'X-Requested-With': 'XMLHttpRequest'})
            headers.update({'Accept': 'application/json, text/javascript, */*; q=0.01'})
            headers.update({'Referer': 'https://www.boletinoficial.gob.ar/'})
            self.http_post(self.sub_url_1,self.scraper_policy.doc_list,request_args={'headers':headers},retry_policy=REQUESTS_RETRY_POLICY)
            data = dict()
            data['nombreSeccion'] = 'primera'
            data['subCat'] = 'all'
            data['offset'] = '1'
            data['itemsPerPage'] = '1'
            data['fecha'] = current_date_val
            self.http_post(self.sub_url_2,self.scraper_policy.doc_list,request_args={'headers':headers,'data':data},retry_policy=REQUESTS_RETRY_POLICY)
            json_data = self.resp.json()
            data = dict()
            self.session_id = urllib.quote(json_data['id'])
            data['idSesion'] = self.session_id
            self.http_post(self.sub_url_3,self.scraper_policy.doc_list,request_args={'headers':headers,'data':data},retry_policy=REQUESTS_RETRY_POLICY)
            json_data = self.resp.json()

            parametros_data = {"numeroPagina": 1, "cantidadPorPagina": 500, "largoMaximoCampo": 200,
                               "edicionCompleta": False,
                               "normasSeleccionadas": [True, False, False], "asuntos": [], "seccion": None, "rubro": [],
                               "tipocontratacion": [], "rubro2da": [], "getFromConvera": False, "voces": [],
                               "emisorPrimera": "",
                               "emisorPrimeraComienzaContiene": 1, "emisorTercera": "",
                               "emisorTerceraComienzaContiene": 1,
                               "fechaDesde": "01/02/2018", "fechaHasta": "02/03/2018", "numeroEjemplar": "",
                               "anioNorma": "",
                               "numeroNorma": "", "textoBuscar": "", "emisorSegunda": "", "nrocontratacion": "",
                               "tipoNorma": "0",
                               "asuntosAlgunaTodas": 1, "opcionPalabraLibre": 1,
                               "fecha_portadas_ant_buscadoravanzado": "",
                               "vocesInclusionAlgunaTodas": 2, "ordenamiento2da": 1, "textoFilter": "",
                               "textoTodasAlguna": 1,
                               "ordering": "2", "tipoNormaDescripcion": "Seleccione un valor", "textoAlgunaTodas": 2}

            start_date_val = start_date.strftime('%d/%m/%Y')
            end_date_val = end_date.strftime('%d/%m/%Y')

            parametros_data["fechaDesde"] = start_date_val
            parametros_data["fechaHasta"] = end_date_val
            data['parametros'] = json.dumps(parametros_data)
            data['idSesion'] = urllib.quote(json_data['id'])
            self.http_post(self.list_url, self.scraper_policy.doc_list, request_args={'headers': headers, 'data': data},retry_policy=REQUESTS_RETRY_POLICY)
            data = self.get_content_from_response()
            if data:
                data = json.loads(data)
                total_records = data['dataList']['CantidadTotal']
                self.logger.info(__name__, fmt("Total Notices from {} to {}: {} ",start_date_val,end_date_val, int(total_records)))
                if total_records:
                    total_pages = (total_records+499)//500
                else:
                    total_pages = 1
                for i in range(1,total_pages+1):
                    parametros_data['numeroPagina'] = i
                    data['parametros'] = json.dumps(parametros_data)
                    self.http_post(self.list_url, self.scraper_policy.doc_list,request_args={'headers': headers, 'data': data},retry_policy=REQUESTS_RETRY_POLICY)
                    data = self.get_content_from_response()
                    if data:
                        data = json.loads(data)
                        data_result = data['dataList']['ResultadoBusqueda']
                        for rule in data_result:
                            self.scrape_notice(rule, headers)
            else:
                self.logger.critical(__name__, u"scraper_run_finished",
                                 fmt(u"{} : No Notices Found  from {} to {}", self.scraper_name.title(),start_date_val,end_date_val))
                raise ScrapeError(self.scraper_policy.doc_list,
                                  fmt(u"{} : No Notices Found  from {} to {}", self.scraper_name.title(),start_date_val, end_date_val),
                                  self.list_url)

        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} notices could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{}  notices could not be scraped.", self.scraper_name.title()),self.list_url)

    def scrape_notice(self,rule , headers):
        try:
            title_content = ''
            document_link = ''
            document_content = ''
            argentina_gazette_doc = self.model_factory.create_bill_doc()
            notice_id = ''
            type = ''
            departments_obj = self.model_factory.create("ArgentinaGazetteDepartmentSchema")
            department = rule['organismo']
            department_list = []
            if department:
                departments_obj.department_name = department.title()
                departments_obj.department_hierarchy = 0
                department_list.append(departments_obj)
                argentina_gazette_doc.departments = department_list

            if rule['numeroNorma'] and rule['anioTramite'] and rule['sintesis']:
                title_content = rule['numeroNorma'] + "/" + str(rule['anioTramite']) + " - " + rule['sintesis']
            elif rule['numeroNorma'] and rule['anioTramite']:
                title_content = rule['numeroNorma'] + "/" + str(rule['anioTramite'])
            else:
                pass
            if title_content:
                argentina_gazette_doc.title = title_content

            argentina_gazette_doc.country = "Argentina"
            argentina_gazette_doc.publication_name = u"Boletín Oficial"

            publication_date = rule['fechaPublicacionDesde']
            if publication_date:
                publication_date = dt.strptime(publication_date.strip(), "%d-%m-%Y").strftime('%Y-%m-%d')
                argentina_gazette_doc.publication_date = publication_date

            rem_url_part = '#!DetalleNormaBusquedaAvanzada' +"/" + str(rule['idTamite']) + "/"+ rule['fechaPublicacion']
            source_url = self.member_url.format(base_url = self.base_url,rem_url_part = rem_url_part)
            if source_url:
                argentina_gazette_doc.source_url = source_url

            data_send = dict()
            data_send['idSesion'] = ''
            data_send['numeroTramite'] = rule['idTamite']
            data_send['fechaPublicacion'] = rule['fechaPublicacion']
            data_send['origenDetalle'] = 1
            self.http_post(self.document_url, self.scraper_policy.doc_list, request_args={'headers': headers,'data':data_send},retry_policy=REQUESTS_RETRY_POLICY)
            response_content = self.get_content_from_response()
            if response_content:
                response_content = json.loads(response_content)
                id = response_content['id']
                type = response_content['dataList']['descRubro']
                document_content = response_content['dataList']['detalleNorma']
                updated_id = id.replace('/','slashBar')
                # It seems argentina generates a random hash each time it serves a pdf, so I've chosen one to keep
                # document_id consistent.
                rem_url_part = "pdf/norma/{}/{}/Primera/zDLml+Ac8J6slashBarKCQELNHMwS1bLS1JVi1bLWbMFcsSvU0KWQmCUgqkFlo="\
                    .format(str(rule['idTamite']), str(rule['fechaPublicacion']))
                document_link = self.member_url.format(base_url = self.base_url,rem_url_part = rem_url_part)
            else:
                raise Exception(fmt("No response from the link : {} for idTamite : {} ", self.document_url, rule['idTamite']))

            if type:
                argentina_gazette_doc.notice_type = type

            if document_content:
                self.set_selector(document_content)
                notice_id_block = self.extract_single('//div[@class="detalle-primera"]/p[position()=last()]/text()')
                if notice_id_block:
                    notice_id_block = notice_id_block.split(u'\xb0\xa0')
                    if len(notice_id_block)==2:
                        notice_id_block = notice_id_block[1]
                        notice_id_block = notice_id_block.split('v.')
                        if len(notice_id_block)==2:
                            notice_id = notice_id_block[0]
                            notice_id = notice_id.strip()

            if notice_id:
                argentina_gazette_doc.notice_id = notice_id

            if document_link:
                doc_details, status = self.document_download_block(document_link)
                if status:
                    download_id, _, doc_ids = doc_details
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        argentina_gazette_doc.document_id = document_id
                        argentina_gazette_doc.download_id = download_id
                        argentina_gazette_doc.document_title = "Texto Publicado"
                    else:
                        raise ValueError(
                            "Document ID not found while registering document with url {}".format(
                                document_link))
                    if not download_id:
                        raise ValueError(
                            "Download ID not found while registering document with url {}".format(
                                document_link))

                else:
                    raise Exception(fmt("No document found on page : {}", source_url))
            else:
                raise Exception(fmt("No document found on page : {}", source_url))

            if self.validate_doc(argentina_gazette_doc):
                self.save_doc(argentina_gazette_doc.for_json())
            else:
                self.logger.critical(__name__, "individual_notice_scrape_failed",
                                     fmt("JsonSchema validation failed for notice having notice id: {}", notice_id))
                self.logger.info(__name__, self.json_dumps(message=argentina_gazette_doc.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e), exc_info=True)

    def document_download_block(self, document_link=None):
        try:
            resp = self.http_request(document_link, "HEAD")
            if resp.status_code != 200:
                self.logger.critical(__name__, "individual_notice_document_extraction_failed",
                                     fmt('http request is failing with error: {} for url  ', document_link))
                return None, False

            if 'Content-Length' in resp.headers:
                if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                    error_message = "File @ '{}' is larger than max size {} bytes.".format(
                        document_link, MAX_FILE_DOWNLOAD_SIZE)
                    self.logger.critical(__name__, "individual_notice_document_extraction_failed",
                                         fmt('While extracting document Doc-Service is failing with error: {}',
                                             error_message))
                    return None, False
            self.logger.info(__name__, fmt("Content type of link : {}", resp.headers['Content-Type']))
            if 'pdf' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"
            else:
                extraction_type = self.extraction_type.html
                content_type = "text/html"

            download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)

            if download_id and _ and doc_ids:
                return (download_id, _, doc_ids), True
            else:
                return None, False
        except Exception as e:
            self.logger.critical(__name__, "Document Download failed", fmt("Content type of link : {} ", document_link))
            return None, False

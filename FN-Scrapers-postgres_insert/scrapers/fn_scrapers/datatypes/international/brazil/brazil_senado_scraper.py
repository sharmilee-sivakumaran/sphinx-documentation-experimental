# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import re
import sys
import injector
from collections import OrderedDict
import xmltodict

from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_document_service.blocking.DocumentService import TApplicationException

from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, \
    get_default_http_headers


@scraper()
@argument("--year", help="Years to scrape data from Brazil Senado API", type=int, required=True, default=2017, choices=range(2011,2018))
@argument('--sigla', type=str, default='all', choices=['all','PLC', 'PLS', 'PLN', 'PDN', 'PDS', 'PEC'], help='Provide Sigla to scrape data from Brazil Senado API')
@tags(type="bills", country_code="BR", group="international")
class BrazilSenadoDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(BrazilSenadoDocScraper, self).__init__(EventComponent.scraper_bills, "brazil", "brazil")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "brazil.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("brazil").alpha_2)

        self.base_url = u'http://legis.senado.leg.br/dadosabertos'
        self.url_lista_pesquisa = u'{base_url}/materia/pesquisa/lista'.format(base_url=self.base_url)
        self.url_get_textos = u'{base_url}/fichas_tecnicas/materia/textos/'.format(base_url=self.base_url)


    def scrape(self, year, sigla):
        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        :param search_term: Search keyword
        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''
        try:
            year_range = range(2011, 2018)
            sigla_list =  ['PLC', 'PLS', 'PLN', 'PDN', 'PDS', 'PEC', 'all']
            if sigla and sigla in sigla_list and year and year in year_range:
                self.scrape_brazil_senado(year, sigla)
            else:
                raise ValueError("Invalid sigla or year provided for searching data from Brazil Senado API")
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} bills could not be scraped. {}",self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list, fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), self.base_url)


    def scrape_brazil_senado(self, year, sigla):
        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        :param search_term: Search keyword
        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''
        sigla_list = []
        if sigla == 'all':
            sigla_list = ['PLC', 'PLS', 'PLN', 'PDN', 'PDS', 'PEC']
        else:
            sigla_list.append(sigla)

        for sigla_val in sigla_list:
            try:
                bill_response = self.lista_pesquisa(sigla=sigla_val, ano=year)

                bill_response_dict = xmltodict.parse(bill_response)
                items = []
                if 'PesquisaBasicaMateria' in bill_response_dict:
                    if 'Materias' in bill_response_dict['PesquisaBasicaMateria']:
                        if 'Materia' in bill_response_dict['PesquisaBasicaMateria']['Materias']:
                            if isinstance(bill_response_dict['PesquisaBasicaMateria']['Materias']['Materia'], list):
                                items = bill_response_dict['PesquisaBasicaMateria']['Materias']['Materia']
                            elif isinstance(bill_response_dict['PesquisaBasicaMateria']['Materias']['Materia'], OrderedDict):
                                items.append(bill_response_dict['PesquisaBasicaMateria']['Materias']['Materia'])
                            self.logger.info(__name__, fmt("Total no. of records for sigla {} and year {}: {}", sigla, year, len(items)))
                            for item in items:
                                try:
                                    brazil_bill = self.model_factory.create_bill_doc()
                                    author_name = None
                                    proposalID = external_id = item['IdentificacaoMateria']['CodigoMateria']
                                    year = item['IdentificacaoMateria']['AnoMateria']
                                    siglasubtipomateria = item['IdentificacaoMateria']['SiglaSubtipoMateria']
                                    numeromateria = item['IdentificacaoMateria']['NumeroMateria']
                                    if numeromateria:
                                        numeromateria = str(numeromateria)
                                        numeromateria = re.sub(r'^\s*0+', '', numeromateria)
                                    anomateria = item['IdentificacaoMateria']['AnoMateria']

                                    proposal_name = siglasubtipomateria + ' ' + numeromateria + '/' + anomateria
                                    submission_date = item['DadosBasicosMateria']['DataApresentacao']
                                    proposal_summary = item['DadosBasicosMateria']['EmentaMateria']
                                    if 'AutoresPrincipais' in item and 'AutorPrincipal' in item['AutoresPrincipais'] and 'NomeAutor' in item['AutoresPrincipais']['AutorPrincipal']:
                                        author_name = item['AutoresPrincipais']['AutorPrincipal']['NomeAutor']

                                    bill_type = None
                                    last_action = ''
                                    last_action_date = ''
                                    current_status = ''
                                    chamber_of_origin = ''
                                    bill_type_text = item[u'IdentificacaoMateria'][u'SiglaSubtipoMateria']
                                    if bill_type_text.strip() in ['PLC', 'PLS', 'PLN', 'PDN', 'PDS', 'PEC']:
                                        bill_type = bill_type_text.strip()
                                    else:
                                        self.logger.critical(__name__, "individual_scraper_failed",
                                                             fmt('Skipping Bill id: {} due mis-match ib bill type, bill type: {}',proposalID,bill_type_text))
                                        continue
                                    bill_page_url = "http://www25.senado.leg.br/web/atividade/materias/-/materia/{}".format(str(proposalID))
                                    if bill_type in ('PLS', 'PDS'):
                                        chamber_of_origin = 'upper'
                                    elif bill_type in ('PLC'):
                                        chamber_of_origin = 'lower'
                                    elif bill_type in ('PLN', 'PDN', 'PEC'):
                                        chamber_of_origin = 'joint'

                                    # source_chamber should be upper for the Senado API
                                    source_chamber = 'upper'

                                    if u'SituacaoAtual' in item:
                                        last_action_date = item[u'SituacaoAtual'][u'Autuacoes'][u'Autuacao'][u'Situacao'][u'DataSituacao']

                                        last_action_date = parse_date_as_str(last_action_date)

                                        last_action = item[u'SituacaoAtual'][u'Autuacoes'][u'Autuacao'][u'Situacao'][u'DescricaoSituacao']

                                        if last_action:
                                            current_status = last_action.title()

                                    brazil_bill.external_id = int(external_id)
                                    brazil_bill.year = int(year)
                                    brazil_bill.proposal_name = proposal_name
                                    brazil_bill.proposal_summary = proposal_summary
                                    if submission_date:
                                        submission_date = parse_date_as_str(submission_date)
                                        brazil_bill.submission_date = submission_date

                                    if bill_type:
                                        brazil_bill.bill_type = bill_type

                                    if author_name:
                                        brazil_bill.author_name = author_name

                                    if last_action:
                                        brazil_bill.last_action = last_action

                                    if last_action_date:
                                        brazil_bill.last_action_date = last_action_date

                                    if current_status:
                                        brazil_bill.current_status = current_status

                                    brazil_bill.chamber_of_origin= chamber_of_origin
                                    brazil_bill.bill_page_url = bill_page_url

                                    brazil_bill.source_chamber = source_chamber
                                    doc_response = self.get_textos(id_prop=proposalID)

                                    doc_response_dict = xmltodict.parse(doc_response)
                                    if 'TextoMateria' in doc_response_dict:
                                        if 'Materia' in doc_response_dict['TextoMateria']:
                                            if 'Textos' in doc_response_dict['TextoMateria']['Materia']:
                                                if 'Texto' in doc_response_dict['TextoMateria']['Materia']['Textos']:
                                                    if isinstance(doc_response_dict['TextoMateria']['Materia']['Textos']['Texto'], list):
                                                        docs_item = doc_response_dict['TextoMateria']['Materia']['Textos']['Texto']
                                                    elif isinstance(doc_response_dict['TextoMateria']['Materia']['Textos']['Texto'], OrderedDict):
                                                        docs_item = []
                                                        docs_item.append(doc_response_dict['TextoMateria']['Materia']['Textos']['Texto'])
                                                    documents_list = []
                                                    for doc_item in docs_item:
                                                        try:
                                                            document_download_url = doc_item['UrlTexto']
                                                            document_title = doc_item['DescricaoTipoTexto'] + '.pdf'
                                                            doc_format = doc_item['FormatoTexto']
                                                            content_type = "application/pdf"

                                                            if document_download_url:
                                                                try:
                                                                    # Head request to get content type, normally don't like extra requests,
                                                                    # But its otherwise difficult to determine by the URL

                                                                    resp = self.http_request(document_download_url, "HEAD")
                                                                    if resp and resp.status_code != 200:
                                                                        self.logger.critical(__name__, "individual_bill_document_extraction_failed", fmt('Http request is failing with HTTP status code - {} for url {}',  resp.status_code,  document_download_url))
                                                                        continue
                                                                    elif resp is None:
                                                                        self.logger.critical(__name__, "individual_bill_document_extraction_failed", fmt( 'Http request is failing with no HTTP response for url {}', document_download_url))
                                                                        continue
                                                                except Exception as e:
                                                                    self.logger.critical(__name__, "individual_bill_document_extraction_failed", fmt('http request is failing with error: {} for url {} ', e, document_download_url))
                                                                    continue

                                                                if 'Content-Length' in resp.headers:
                                                                    if int(resp.headers['Content-Length']) > MAX_FILE_DOWNLOAD_SIZE:
                                                                        error_message = u"File @ '{}' is larger than max size {} bytes.".format(document_download_url, MAX_FILE_DOWNLOAD_SIZE)
                                                                        self.logger.critical(__name__,"individual_bill_document_extraction_failed", fmt( 'While extracting document Doc-Service is failing with error: {}', error_message))
                                                                        continue

                                                                if 'pdf' in doc_format:

                                                                    extraction_type = self.extraction_type.unknown_new
                                                                    download_id, _, doc_ids = self.register_download_and_documents(
                                                                        document_download_url,
                                                                        self.scraper_policy.doc_service,
                                                                        extraction_type, True,
                                                                        content_type=doc_format)

                                                                elif 'msword' in doc_format:
                                                                    document_title = str(proposal_name) + '.doc'
                                                                    extraction_type = self.extraction_type.msword_doc
                                                                    download_id, _, doc_ids = self.register_download_and_documents(
                                                                        document_download_url,
                                                                        self.scraper_policy.doc_service,
                                                                        extraction_type, True,
                                                                        content_type=doc_format)
                                                                elif 'html' in doc_format:
                                                                    document_title = str(proposal_name) + '.html'
                                                                    extraction_type = self.extraction_type.html

                                                                    download_id, _, doc_ids = self.register_download_and_documents(
                                                                        document_download_url,
                                                                        self.scraper_policy.doc_service,
                                                                        extraction_type, True,
                                                                        content_type=doc_format)

                                                                if len(doc_ids) > 0 and doc_ids[0] is not None:
                                                                    document_id = doc_ids[0]
                                                                else:
                                                                    raise ValueError(
                                                                        "Document ID not found while registering document with url {}".format(
                                                                            document_download_url))

                                                                if download_id is None:
                                                                    raise ValueError(
                                                                        "Download ID not found while registering document with url {}".format(
                                                                            document_download_url))

                                                                if document_id and download_id:
                                                                    document_obj = self.model_factory.create(
                                                                        "BrazilBillDocumentSchema")
                                                                    document_obj.title = document_title
                                                                    document_obj.document_id = document_id
                                                                    document_obj.download_id = download_id
                                                                    document_obj.document_url = document_download_url

                                                                    if self.validate_doc(document_obj):
                                                                        documents_list.append(document_obj)

                                                                    else:
                                                                        self.logger.debug(__name__, fmt(
                                                                            'Skipping Document Details: {}',
                                                                            document_obj.for_json()))
                                                        except TApplicationException as e:
                                                            self.logger.critical(__name__,"individual_bill_document_extraction_failed", fmt('While extracting document Doc-Service is failing with error: {}', e))
                                                        except Exception as e:
                                                            self.logger.critical(__name__, "individual_bill_document_extraction_failed", fmt('Doc-Service is failing with error: {}',e))

                                                    if len(documents_list)>0:
                                                        brazil_bill.documents = documents_list
                                                else:
                                                    raise KeyError('Texto key is not found in doc response.')
                                            else:
                                                raise KeyError('Textos key is not found in doc response.')
                                        else:
                                            raise KeyError('Materia key is not found in doc response.')
                                    else:
                                        raise KeyError('TextoMateria key is not found in doc response.')
                                    if self.validate_doc(brazil_bill):

                                        self.save_doc(brazil_bill)
                                    else:
                                        self.logger.critical(__name__,"individual_scraper_failed", fmt('Skipping Document Details: {}',  document_obj.for_json()))
                                except Exception as e:
                                    self.logger.critical(__name__,"individual_scraper_failed",fmt('Error occured while iterating for bill ids. Error: {}', e ))

                    elif 'Metadados' in bill_response_dict['PesquisaBasicaMateria']:
                        raise ValueError("No records found for sigla {}".format(sigla_list))
                    else:
                        raise KeyError("Materias key not found in api response xml for sigla {}.".format(sigla_val))
                else:
                    raise KeyError("PesquisaBasicaMateria key not found in api response xml for sigla {}.".format(sigla_val))
            except Exception as e:
                self.logger.critical(__name__, "individual_scraper_failed",
                                     fmt('Error occured while iterating for bill ids. Error: {}', e))

    def lista_pesquisa(self, sigla, ano):
        if sigla and ano:
            api_url = '{url}?sigla={sigla}&ano={ano}'.format(url=self.url_lista_pesquisa, sigla=str(sigla),
                                                             ano=str(ano))
            try:
                headers = {'Content-Type': 'text/xml; charset=UTF-8'}
                req_args = {'headers': headers}
                # response = requests.post(url, data=body, headers=headers)
                self.http_get(api_url, self.scraper_policy.doc_list, request_args=req_args)
                answer = self.get_content_from_response()
                # answerDecoded = answer.decode('utf-8', errors='ignore')
                return answer
            except Exception as e:
                raise ScrapeError(self.scraper_policy.doc_list, fmt("Error occured in lista_pesquisa function. Error message: {}", e), api_url)
        else:
            raise ValueError('Sigla and Ano are required field.')

    def get_textos(self, id_prop):
        if id_prop:
            headers = {'Content-Type': 'text/xml; charset=UTF-8'}
            api_url = 'http://legis.senado.leg.br/dadosabertos/materia/textos/' + str(id_prop)
            req_args = {'headers': headers}
            self.http_get(api_url, self.scraper_policy.doc_list, request_args=req_args)
            answer = self.get_content_from_response()
            return answer
        else:
            raise ValueError('Prop Id is required field.')

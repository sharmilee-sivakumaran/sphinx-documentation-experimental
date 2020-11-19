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
@argument("--year", help="Years to scrape data from Brazil Camara API", type=int, required=True, default=2017, choices=range(2011,2018))
@argument("--sigla", type=str, default="all", choices=['PL', 'PLP', 'PLV', 'PDC', 'MPV', 'PEC','all'], help="Provide Sigla to scrape data from Brazil Camara API")
@tags(type="bills", country_code="BR", group="international")
class BrazilCamaraDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(BrazilCamaraDocScraper, self).__init__(EventComponent.scraper_bills,"brazil","brazil")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "brazil.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("brazil").alpha_2)

        self.base_url = u'http://www.camara.gov.br'

    def scrape(self,year,sigla):
        try:
            year_range = range(2011, 2018)
            sigla_list = ['PL', 'PLP', 'PLV', 'PDC', 'MPV', 'PEC','all']
            if sigla and sigla in sigla_list and year and year in year_range:
                self.scrape_brazil_camara(year, sigla)
            else:
                raise ValueError("Invalid sigla or year provided for searching data from Brazil Camara API")
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} bills could not be scraped. {}",self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list, fmt("{} bills could not be scraped. {}",self.scraper_name.title(), e),self.base_url)


    def scrape_brazil_camara(self, year, sigla):
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
                sigla_list = ['PL', 'PLP', 'PLV', 'PDC', 'MPV', 'PEC']
            elif sigla in ['PL', 'PLP', 'PLV', 'PDC', 'MPV', 'PEC']:
                sigla_list.append(sigla)
            else:
                raise ValueError("Invalid sigla provided for searching data from Camara API")

            for sigla_val in sigla_list:
                try:
                    bill_response = self.listar_proposicoes(sigla=sigla_val, ano=year)
                    bill_response_dict = xmltodict.parse(bill_response)
                    items = []
                    if 'ListarProposicoesResponse' in bill_response_dict:
                        if 'ListarProposicoesResult' in bill_response_dict['ListarProposicoesResponse']:
                            if 'proposicoes' in bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult']:
                                if 'proposicao' in \
                                        bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult'][
                                            'proposicoes']:
                                    if isinstance(
                                            bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult'][
                                                'proposicoes']['proposicao'], list):
                                        items = bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult'][
                                            'proposicoes']['proposicao']
                                    elif isinstance(
                                            bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult'][
                                                'proposicoes']['proposicao'], OrderedDict):

                                        items.append(
                                            bill_response_dict['ListarProposicoesResponse']['ListarProposicoesResult'][
                                                'proposicoes']['proposicao'])
                                    self.logger.info(__name__,
                                                     fmt("Total no. of records for sigla {} and year {}: {}", sigla,
                                                         year, len(items)))
                                    for item in items:
                                        try:
                                            brazil_bill = self.model_factory.create_bill_doc()
                                            proposalID = external_id = item['id']
                                            year = item['ano']
                                            proposal_name = item['nome']
                                            submission_date = item['datApresentacao']
                                            proposal_summary = item['txtEmenta']
                                            author_name = ''
                                            if 'autor1' in item:
                                                author_name = item['autor1']['txtNomeAutor']

                                            bill_type = ''
                                            last_action = ''
                                            last_action_date = ''
                                            current_status = ''
                                            chamber_of_origin = ''
                                            if 'tipoProposicao' in item:
                                                bill_type = item['tipoProposicao']['sigla']

                                            if u'ultimoDespacho' in item:
                                                if u'datDespacho' in item[u'ultimoDespacho']:
                                                    last_action_date = item[u'ultimoDespacho'][u'datDespacho']
                                                    last_action_date = parse_date_as_str(last_action_date,
                                                                                         languages=self.country_languages)
                                                if u'txtDespacho' in item[u'ultimoDespacho']:
                                                    last_action = item[u'ultimoDespacho'][u'txtDespacho']
                                            if u'situacao' in item:
                                                if u'descricao' in item[u'situacao']:
                                                    current_status = item[u'situacao'][u'descricao']

                                            bill_page_url = "http://www.camara.gov.br/proposicoesWeb/fichadetramitacao?idProposicao={}".format(proposalID)
                                            if bill_type in ('PLS', 'PDS'):
                                                chamber_of_origin = 'upper'
                                            elif bill_type in ('PL', 'PLC', 'PDC'):
                                                chamber_of_origin = 'lower'
                                            elif bill_type in ('PLN', 'PLP', 'PLV', 'PDN', 'PEC', 'MPV'):
                                                chamber_of_origin = 'joint'
                                            source_chamber = 'lower'

                                            brazil_bill.external_id = int(external_id)
                                            brazil_bill.year = int(year)
                                            brazil_bill.proposal_name = proposal_name
                                            brazil_bill.proposal_summary = proposal_summary
                                            if submission_date:
                                                submission_date = parse_date_as_str(submission_date,languages=self.country_languages)
                                                brazil_bill.submission_date = submission_date

                                            brazil_bill.bill_type = bill_type
                                            brazil_bill.author_name = author_name

                                            if last_action:
                                                brazil_bill.last_action = last_action

                                            if last_action_date:
                                                brazil_bill.last_action_date = last_action_date

                                            if current_status:
                                                brazil_bill.current_status = current_status

                                            brazil_bill.chamber_of_origin = chamber_of_origin
                                            brazil_bill.bill_page_url = bill_page_url

                                            brazil_bill.source_chamber = source_chamber
                                            doc_response = self.obter_proposicao_por_id(id_prop=proposalID)
                                            doc_response_dict = xmltodict.parse(doc_response)
                                            document_id = download_id = None
                                            if 'ObterProposicaoPorIDResponse' in doc_response_dict:
                                                if 'ObterProposicaoPorIDResult' in doc_response_dict[
                                                    'ObterProposicaoPorIDResponse']:
                                                    if 'proposicao' in doc_response_dict['ObterProposicaoPorIDResponse'][
                                                        'ObterProposicaoPorIDResult']:
                                                        if 'LinkInteiroTeor' in \
                                                                doc_response_dict['ObterProposicaoPorIDResponse'][
                                                                    'ObterProposicaoPorIDResult']['proposicao']:
                                                            try:
                                                                document_download_url = \
                                                                    doc_response_dict['ObterProposicaoPorIDResponse'][
                                                                        'ObterProposicaoPorIDResult']['proposicao'][
                                                                        'LinkInteiroTeor']
                                                                document_title = str(proposal_name) + '.pdf'


                                                                if document_download_url:
                                                                    try:
                                                                        # Head request to get content type, normally don't like extra requests,
                                                                        # But its otherwise difficult to determine by the URL

                                                                        resp = self.http_request(document_download_url,
                                                                                                 "HEAD")
                                                                        if resp and resp.status_code != 200:
                                                                            self.logger.critical(__name__,
                                                                                                 "individual_bill_document_extraction_failed",
                                                                                                 fmt(
                                                                                                     'Http request is failing with HTTP status code - {} for url {}',
                                                                                                     resp.status_code,
                                                                                                     document_download_url))
                                                                            continue
                                                                        elif resp is None:
                                                                            self.logger.critical(__name__,
                                                                                                 "individual_bill_document_extraction_failed",
                                                                                                 fmt(
                                                                                                     'Http request is failing with no HTTP response for url {}',
                                                                                                     document_download_url))
                                                                            continue
                                                                    except Exception as e:
                                                                        self.logger.critical(__name__,
                                                                                             "individual_bill_document_extraction_failed",
                                                                                             fmt(
                                                                                                 'http request is failing with error: {} for url {} ',
                                                                                                 e,
                                                                                                 document_download_url))
                                                                        continue

                                                                    if 'Content-Length' in resp.headers:
                                                                        if int(resp.headers['Content-Length']) > MAX_FILE_DOWNLOAD_SIZE:
                                                                            error_message = u"File @ '{}' is larger than max size {} bytes.".format(
                                                                                document_download_url,
                                                                                MAX_FILE_DOWNLOAD_SIZE)
                                                                            self.logger.critical(__name__,
                                                                                                 "individual_bill_document_extraction_failed",
                                                                                                 fmt(
                                                                                                     'While extracting document Doc-Service is failing with error: {}',
                                                                                                     error_message))
                                                                            continue

                                                                    if 'pdf' in resp.headers['Content-Type']:
                                                                        extraction_type = self.extraction_type.unknown_new
                                                                        download_id, _, doc_ids = self.register_download_and_documents(
                                                                            document_download_url,
                                                                            self.scraper_policy.doc_service,
                                                                            extraction_type, True,
                                                                            content_type=resp.headers['Content-Type'])

                                                                    elif 'msword' in resp.headers['Content-Type']:
                                                                        document_title = str(proposal_name) + '.doc'
                                                                        extraction_type = self.extraction_type.msword_doc
                                                                        download_id, _, doc_ids = self.register_download_and_documents(
                                                                            document_download_url,
                                                                            self.scraper_policy.doc_service,
                                                                            extraction_type, True,
                                                                            content_type=resp.headers['Content-Type'])
                                                                    elif 'html' in resp.headers['Content-Type']:
                                                                        document_title = str(proposal_name) + '.html'
                                                                        extraction_type = self.extraction_type.html

                                                                        download_id, _, doc_ids = self.register_download_and_documents(
                                                                            document_download_url,
                                                                            self.scraper_policy.doc_service,
                                                                            extraction_type, True,
                                                                            content_type=resp.headers['Content-Type'])

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
                                                                        document_obj = self.model_factory.create("BrazilBillDocumentSchema")
                                                                        document_obj.title = document_title
                                                                        document_obj.document_id = document_id
                                                                        document_obj.download_id = download_id
                                                                        document_obj.document_url = document_download_url
                                                                        if self.validate_doc(document_obj):
                                                                            brazil_bill.documents = [document_obj]

                                                                        else:
                                                                            self.logger.debug(__name__, fmt('Skipping Document Details: {}',document_obj.for_json()))

                                                            except TApplicationException as e:
                                                                self.logger.critical(__name__,"individual_bill_document_extraction_failed",fmt('While extracting document Doc-Service is failing with error: {}',e))
                                                            except Exception as e:
                                                                self.logger.critical(__name__,"individual_bill_document_extraction_failed",  fmt('Doc-Service is failing with error: {}', e),exc_info=True)
                                                    else:
                                                        raise KeyError("proposicao key is missing inj doc response")
                                                else:
                                                    raise KeyError("ObterProposicaoPorIDResult key is missing inj doc response")
                                            else:
                                                raise KeyError("ObterProposicaoPorIDResponse key is missing inj doc response")
                                            if self.validate_doc(brazil_bill):
                                                self.save_doc(brazil_bill)
                                            else:
                                                self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("JsonSchema validation failed for bill having proposal name: {}", proposal_name))

                                        except Exception as e:
                                            self.logger.critical(__name__, "individual_bill_scrape_failed",
                                                                 fmt("Error occured while iterating for bill ids.  Error occured: {}", e), exc_info=True)
                except Exception as e:
                    self.logger.critical(__name__,"individual_category_bill_scrape_failed",fmt("Error occured while fetching bills for sigla {} and year {}. Error message: ",sigla_val, year, e))
                    raise ScrapeError(self.scraper_policy.doc_list, fmt("{} bills could not be scraped for sigla {} and year {}. Error: {}", self.scraper_name.title(), sigla_val, year, e), self.base_url)

    def listar_proposicoes(self, sigla, ano):
        soap_api_url = 'http://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx'
        if sigla and ano:
            try:
                headers = {'Content-Type': 'text/xml; charset=UTF-8',
                           'SOAPAction': 'http://www.camara.gov.br/SitCamaraWS/Proposicoes/ListarProposicoes'}
                # headers = {'content-type': 'text/xml'}
                body = """<?xml version="1.0" encoding="UTF-8"?>
                <SOAP-ENV:Envelope xmlns:ns0="http://www.camara.gov.br/SitCamaraWS/Proposicoes"
                 xmlns:ns1="http://schemas.xmlsoap.org/soap/envelope/"
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
                   <SOAP-ENV:Body>
                      <ns0:ListarProposicoes>
                         <ns0:sigla>%s</ns0:sigla>
                         <ns0:ano>%s</ns0:ano>
                      </ns0:ListarProposicoes>
                   </SOAP-ENV:Body>
                </SOAP-ENV:Envelope>""" % (sigla, str(ano))

                req_args = {'data': body, 'headers': headers}
                # response = requests.post(url, data=body, headers=headers)
                self.http_post(soap_api_url, self.scraper_policy.doc_list, request_args=req_args)
                answer = self.get_content_from_response()
                # answerDecoded = answer.decode('utf-8', errors='ignore')
                xmlMessage = re.search(r'(?iLmsux)<soap\:Body>(.*?)<\/soap\:Body>', answer)
                if xmlMessage:
                    replyFinal = xmlMessage.group(1) + '\n'
                    return replyFinal
                else:
                    raise ValueError("Body not found in response.")
            except Exception as e:
                raise ScrapeError(self.scraper_policy.doc, fmt("Error occured in obter_proposicao_por_id function. Error message: {}", e), soap_api_url)
        else:
            raise ValueError('Sigla and Ano are required field.')

    def obter_proposicao_por_id(self, id_prop):
        soap_api_url = 'http://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx'
        if id_prop:
            try:
                headers = {'Content-Type': 'text/xml; charset=UTF-8',
                           'SOAPAction': 'http://www.camara.gov.br/SitCamaraWS/Proposicoes/ObterProposicaoPorID'}
                # headers = {'content-type': 'text/xml'}
                body = """<?xml version="1.0" encoding="UTF-8"?>
                <SOAP-ENV:Envelope xmlns:ns0="http://www.camara.gov.br/SitCamaraWS/Proposicoes"
                 xmlns:ns1="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
                   <SOAP-ENV:Body>
                        <ns0:ObterProposicaoPorID>
                            <ns0:idProp>%s</ns0:idProp>
                        </ns0:ObterProposicaoPorID>
                   </SOAP-ENV:Body>
                </SOAP-ENV:Envelope>""" % (str(id_prop))
                soap_api_url = 'http://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx'
                req_args = {'data': body, 'headers': headers}
                # response = requests.post(url, data=body, headers=headers)
                self.http_post(soap_api_url, self.scraper_policy.doc_list, request_args=req_args)
                answer = self.get_content_from_response()
                # answerDecoded = answer.decode('utf-8', errors='ignore')
                xmlMessage = re.search(r'(?iLmsux)<soap\:Body>(.+?)<\/soap\:Body>', answer)
                if xmlMessage:
                    replyFinal = xmlMessage.group(1) + '\n'
                    return replyFinal
                else:
                    raise ValueError("Body not found in response.")

            except Exception as e:
                raise ScrapeError(self.scraper_policy.doc_list, fmt("Error occured in obter_proposicao_por_id function. Error message: {}", e), soap_api_url)

        else:
            raise ValueError('Prop Id is required field.')

# -*- coding: utf-8 -*-
from __future__ import absolute_import
import injector
import os
import re
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy
from fn_ratelimiter_common.const import CLIENT_GENERIC_RETRY_PERIOD, CLOCK_DRIFT , CLIENT_CHECK_PERIOD
CLIENT_GENERIC_RETRY_PERIOD = 1000
CLOCK_DRIFT = 20
CLIENT_CHECK_PERIOD = 1000
REQUESTS_RETRY_POLICY = Retry500RequestsRetryPolicy(max_attempts=100,max_retry_time=1500,max_attempt_delay=1000)

@scraper()
@tags(type="bills", country_code="IT", group="international")
class ItalyDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(ItalyDocScraper, self).__init__(EventComponent.scraper_bills, "italy", "italy")
        self.logger = logger
        self.base_url = u'http://www.senato.it'
        self.list_url = u'{base_url}/ric/sddl/nuovaricerca.do?params.legislatura=17'
        self.member_url = u'{base_url}/ric/sddl/risultati.do?params.commissioneBoolOp=AND&params.statoDiv=0,0,0,0,0,0,0&\
                            des=&selmode=&params.interventiBoolOp=AND&params.campoOrdinamento=dataPresentazione&params.gruppoTrattazioneBoolOp=AND\
                            &params.rows=100&livelloTeseo=&params.teseoTuttiTermini=T&params.relatoriBoolOp=AND\
                            &params.start={start_index}&searchName=sddl&params.legislatura=17&params.tipoFirmatari=1&\
                            teseo=&sel=&params.ordinamento=desc'
        self.bill_page_url = u'{base_url}{rem_url_part}'
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "italy.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("italy").alpha_2)

    def scrape(self):
        try:

            self.logger.info(__name__, fmt(u"Fetching for main page link  - {} ",
                                           self.member_url.format(base_url=self.base_url, start_index=0)))
            self.http_get(self.member_url.format(base_url=self.base_url,start_index=0),self.scraper_policy.doc_list,retry_policy=REQUESTS_RETRY_POLICY)
            total_bills = self.extract_single(u'//div[@class="sottoTit"]/strong[3]/text()')
            error_message = self.extract_single(u'//div[@class="rigaTabRic"]//p[@class="messaggioErrore"]/strong/text()')
            if total_bills:
                self.logger.info(__name__, fmt("Total Bills  : {} ", int(total_bills)))
                for i in range(0, int(total_bills) + 100, 100):
                    self.http_get(self.member_url.format(base_url=self.base_url, start_index=i),
                                  self.scraper_policy.doc_list,retry_policy=REQUESTS_RETRY_POLICY)

                    self.logger.info(__name__,fmt("Page No : {} ",int(i/100)+1))
                    all_bill_data = self.xpath(u'//div[@class="rigaTabRic"]/ul[@class="risultati"]/li')
                    all_bill_links = []
                    for data in all_bill_data:
                        bill_page_link = self.extract_single('.//a/@href',sel=data)
                        sponsor = self.extract_single('.//em[1]/text()',sel=data)
                        all_bill_links.append((bill_page_link,sponsor))
                    for bill_data in all_bill_links:
                        self.scrape_bill_page(bill_data)
            elif error_message:
                self.logger.critical(__name__,fmt(u"{} bills could not be scraped.", self.scraper_name.title()),fmt(u" Website Error Message : {}",error_message))
                raise ScrapeError(self.scraper_policy.doc_list,
                                  fmt(u"{}  bills could not be scraped.", self.scraper_name.title()),
                                  self.member_url.format(base_url=self.base_url, start_index=0))
            else:
                self.logger.critical(__name__, u"scraper_run_finished",
                                     fmt(u"{} : No Bills Found ", self.scraper_name.title()))
        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} bills could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{}  bills could not be scraped.", self.scraper_name.title()),
                              self.member_url.format(base_url=self.base_url, start_index=0))


    def scrape_bill_page(self,bill_data):
        try:
            url, sponsors = bill_data
            chamber_of_origin = ''
            bill_page_link = self.bill_page_url.format(base_url=self.base_url, rem_url_part=url)
            self.logger.info(__name__,
                             fmt(u"Fetching for bill page link  - {} ", bill_page_link))
            self.http_get(bill_page_link, self.scraper_policy.doc_list,retry_policy=REQUESTS_RETRY_POLICY)
            italy_doc = self.model_factory.create_bill_doc()
            italy_doc.source_url = self.bill_page_url.format(base_url=self.base_url, rem_url_part=url)
            session = "17th Parliament"
            italy_doc.session = session
            title = self.extract_single(u'//div[@id="content"]/div[@class="sxSmall"]/div[@class="boxTitolo"]/p/text()')
            if title:
                title = title.replace("\r", " ").replace("\n", " ")
                title = re.sub('\s+', ' ', title)
                italy_doc.title = title.strip()
            if sponsors:
                italy_doc.sponsors = sponsors
            external_id = self.extract(u'//div[@class="sxSmall"]/div[@class="bordoNero"]/table[@id="tabellaIter"]//tr/td[1]/strong/text()')
            external_id = [i for i in external_id if i.strip()]
            if external_id:
                external_id = external_id[0].replace("."," ")
                italy_doc.external_id = external_id
                if external_id.startswith('C'):
                    chamber_of_origin = "Chamber of Deputies"
                elif external_id.startswith('S'):
                    chamber_of_origin = "Senate"
                else:
                    pass
            else:
                raise ScrapeError(self.scraper_policy.doc_list,
                                  fmt(u"{}  bill could not be scraped. - Bill External ID not found", self.scraper_name.title()),
                                  bill_page_link)
            if chamber_of_origin:
                italy_doc.chamber_of_origin = chamber_of_origin
            bill_introduction_date = self.extract_single('//h2[contains(.,"Presentazione")]/following-sibling::p/strong/text()')
            if bill_introduction_date:
                italy_doc.introduction_date = parse_date_as_str(bill_introduction_date,languages=self.country_languages)

            natura_bill_type = self.extract(u'//h2[contains(.,"Natura")]/following-sibling::p[1]//text()')
            if natura_bill_type:
                natura_bill_type = natura_bill_type[0].strip()
                if "di approvazione di bilancio" in natura_bill_type:
                    natura_bill_type = "Approvazione Bilancio"
                elif "ordinaria" in natura_bill_type:
                    natura_bill_type = "Ordinaria"
                elif "Costituzionale" in natura_bill_type:
                    natura_bill_type = "Costituzionale"
                elif "di conversione del decreto-legge" in natura_bill_type:
                    natura_bill_type = "Conversione Decreto Legge"
                else:
                    #invalid value assigned . Will be checked later
                    natura_bill_type = None

            iniziativa_bill_type = self.extract(u'//div[@class="sxSmall"]//h2[@class="titoloScheda"]/text()')
            if iniziativa_bill_type:
                iniziativa_bill_type = iniziativa_bill_type[0].split(' ')
                if len(iniziativa_bill_type)>1:
                    iniziativa_bill_type = iniziativa_bill_type[1]
                    if "Governativa" in iniziativa_bill_type:
                        iniziativa_bill_type = "Governo"
                    elif "Parlamentare" in iniziativa_bill_type:
                        iniziativa_bill_type = "Parlamentare"
                    elif "Regionale" in iniziativa_bill_type:
                        iniziativa_bill_type = "Regionale"
                    else:
                        # invalid value assigned . Will be checked later
                        iniziativa_bill_type = None
                else:
                    iniziativa_bill_type = self.extract_single('//div[@class="sxSmall"]//div[@class="testoMedium"]/text()')
                    if "Popolare" in iniziativa_bill_type:
                        iniziativa_bill_type = "Popolare"
                    elif "C.N.E.L." in iniziativa_bill_type:
                        iniziativa_bill_type = "CNEL"
                    else:
                        #invalid value assigned . Will be checked later
                        iniziativa_bill_type = None
            #verifying if both iniziativa_bill_type and natura_bill_type are present else website change error
            if iniziativa_bill_type and natura_bill_type:
                legislation_type = natura_bill_type + " - " + iniziativa_bill_type
                italy_doc.legislation_type = legislation_type
            else:
                raise ScrapeError(self.scraper_policy.doc_list,fmt(u"{}  bills could not be scraped. - Bill type not found : Website Structure change", self.scraper_name.title()),bill_page_link)

            action_list = []
            actions = self.xpath(u'//div[@class="sxSmall"]//div[@class="bordoNero"]/table[@id="tabellaIter"]//tr')
            for action in actions:
                action_obj = self.model_factory.create("BillActionSchema")
                action_data_id = self.extract(u'.//td[1]//text()',sel=action)
                action_data_id = [val.strip() for val in action_data_id if val.strip()]
                if action_data_id:
                    action_data_id_val = action_data_id[0].strip()

                action_data_status = self.extract(u'.//td[2]//text()',sel=action)
                if action_data_status:
                    action_data_status = [val.strip() for val in action_data_status if val.strip()]
                    action_data_status_val = ' '.join(action_data_status)
                    action_data_status_val = action_data_status_val.replace("\r", " ").replace("\n", " ")
                    action_data_status_val = re.sub('\s+', ' ', action_data_status_val)

                if action_data_status_val and action_data_id_val:
                    action_obj.action_text =  action_data_id_val + " : " + action_data_status_val

                action_date = self.extract(u'.//td[3]//text()', sel=action)
                action_data_date = [val.strip() for val in action_date if val.strip()]
                action_date = action_data_date[0].strip()
                if action_data_date:
                    action_obj.action_date = parse_date_as_str(action_date,languages=self.country_languages)
                action_list.append(action_obj)

            if action_list:
                italy_doc.actions = action_list

            scraped_status_date = self.extract_single(u'//h2[contains(.,"Iter")]/following-sibling::p[1]/span/strong/text()')
            scraped_status_date = scraped_status_date.replace(":", "").strip()
            if scraped_status_date:
                italy_doc.scraped_status_date = parse_date_as_str(scraped_status_date,languages=self.country_languages)

            teseo_classification = self.extract(u'//h2[contains(.,"Classificazione TESEO")]/following-sibling::p[1]/text()')
            if teseo_classification:
                teseo_classification = [val.strip() for val in teseo_classification if val.strip()]
                teseo_classification = ' '.join(teseo_classification)
                teseo_classification = teseo_classification.replace("\r"," ").replace("\n"," ")
                teseo_classification = re.sub('\s+', ' ', teseo_classification)
                italy_doc.teseo_classification = teseo_classification

            scraped_status = self.extract(u'//h2[contains(.,"Iter")]/following-sibling::p[1]/span/text()')
            if not scraped_status:
                scraped_status = self.extract(
                    u'//div[@class="sxSmall"]/div[@class="bordoNero"]/table[@id="tabellaIter"]//tr/td[2]/strong/text()')
            if scraped_status:
                scraped_status = scraped_status[1].strip()
                scraped_status = scraped_status.replace("\r"," ").replace("\n"," ")
                scraped_status = re.sub('\s+', ' ', scraped_status)
                italy_doc.scraped_status = scraped_status

            document_list = []
            final_document_path = []
            document_link = self.extract_single(u'//div[@class="divNavOrizS"]/ul/li[contains(.,"Testi ed emendamenti")]/a/@href')
            if document_link:
                document_link_main = self.bill_page_url.format(base_url=self.base_url,rem_url_part=document_link)
                self.http_get(document_link_main,self.scraper_policy.doc)
                document_path = self.xpath(u'//div[@class="sxSmall"]//h2[contains(.,"Testi disponibili")]/following-sibling::ol[@class="schede"]/li')
                second_document_path = self.xpath(u'//div[@class="sxSmall"]//h2[contains(.,"Testi disponibili")]/following-sibling::ol[@class="schede"]/li/ol[@class="schede"]/li')
                final_document_path.extend(document_path)
                final_document_path.extend(second_document_path)
                for document in final_document_path:
                    if self.extract('./ol[@class="schede"]/li',sel=document):
                        document_title = self.extract(u'./a/@title',sel=document)

                        if not document_title:
                            document_title = self.extract(u'./text()', sel=document)
                            document_title = [val.strip() for val in document_title if val.strip()]

                        if document_title:
                            document_title = document_title[0].strip()

                    else:
                        document_title = self.extract_as_one(u'.//text()',sel=document)
                    if document_title:
                        document_title = document_title.replace("\r"," ").replace("\n"," ")
                        document_title = document_title.strip()
                        document_title = re.sub('\([^\)]*\)$', '', document_title)

                    document_file_path = self.extract(u'./span[@class="annotazione"]/a[contains(.,"PDF")]/@href',sel=document)
                    if not document_file_path:
                        document_file_path = self.extract(u'./a/@href',sel=document)

                    if document_title and document_file_path:
                        for file_path in document_file_path:
                            document_list.append((document_title, file_path))
                document_obj_list = []

                for document_title,document_link in document_list:
                    document_link = self.bill_page_url.format(base_url=self.base_url,rem_url_part=document_link)
                    doc_details, status = self.document_download_block(document_link)
                    if status:
                        download_id, _, doc_ids = doc_details
                        document = self.model_factory.create("ItalyDocumentSchema")
                        if len(doc_ids) > 0:
                            document_id = doc_ids[0]
                            document.document_id = document_id
                            document.download_id = download_id
                            document.document_title = document_title
                            if self.validate_doc(document):
                                document_obj_list.append(document)
                            else:
                                self.logger.info(__name__, 'Skipping Attachment: {}'.format(document.for_json()))

                        else:
                            raise ValueError(
                                "Document ID not found while registering document with url {}".format(
                                    document_link))
                        if not download_id:
                            raise ValueError(
                                "Download ID not found while registering document with url {}".format(
                                    document_link))

                if len(document_obj_list) > 0:
                    italy_doc.documents = document_obj_list
                else:
                    self.logger.info(__name__, fmt(u"No Documents Found on url : {}", document_link_main))
            if self.validate_doc(italy_doc):
                self.save_doc(italy_doc.for_json())
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill having link: {}",
                                         bill_page_link))

                self.logger.critical(__name__,self.json_dumps(message = italy_doc.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)


    def document_download_block(self,document_link):
        try:
            try:
                resp = self.http_request(document_link, "HEAD")
                if resp.status_code != 200:
                    self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                         fmt('http request is failing with error: {} for url  ', document_link))
                    return None, False
            except Exception as e:
                self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                     fmt('http request is failing with error: {} for url {} ', e, document_link))
                return None, False

            if resp:
                if 'Content-Length' in resp.headers:
                    if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                        error_message = "File @ '{}' is larger than max size {} bytes.".format(
                            document_link, MAX_FILE_DOWNLOAD_SIZE)
                        self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                             fmt('While extracting document Doc-Service is failing with error: {}',
                                                 error_message))
                        return None, False
                self.logger.info(__name__,fmt("Content type of link : {}",resp.headers['Content-Type']))
                if 'pdf' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.unknown
                    content_type = "application/pdf"

                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)
                if download_id and _ and doc_ids:
                    return (download_id , _ , doc_ids),True
                else:
                    return None, False
            else:
                return None, False
        except Exception as e:
            self.logger.critical(__name__,"Document Download failed" , fmt("Content type of link : {} ", document_link))
            return None, False















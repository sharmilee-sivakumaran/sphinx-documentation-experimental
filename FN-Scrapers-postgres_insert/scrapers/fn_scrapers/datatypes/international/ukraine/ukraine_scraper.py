# -*- coding: utf-8 -*-
from __future__ import division
import os
import injector
from time import sleep
from random import randint
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, tags
from fn_service.server import BlockingEventLogger, fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from datetime import datetime


# NOTE: The Ukraine scraper can only handle the 8th convocation of parliament.
# That session is going to close on November 27, 2019 - ie, that's far enough
# away from now that its unclear if making changes to the scraper to support
# other convocations has any point - the website may well change before then.
@scraper()
@tags(type="bills", country_code="UA", group="international")
class UkraineDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(UkraineDocScraper, self).__init__(EventComponent.scraper_bills,"ukraine","ukraine")
        self.base_url = u"http://w1.c1.rada.gov.ua"
        self.member_page_url = u"{base_url}/pls/zweb2/{rem_url_part}"
        self.main_page_url = u"{base_url}/pls/zweb2/webproc2_5_1_J?ses=10009&num_s=2&num=&date1=&date2=&name_zp=&out_type=&id="
        self.page_url = u"{base_url}/pls/zweb2/webproc2_5_1_J?ses=10009&num_s=2&num=&date1=&date2=&name_zp=&out_type=&id=&page=1&zp_cnt=-1"
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "ukraine.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    def scrape(self):
        try:
            self.logger.info(__name__, fmt(u"Fetching for main page link  - {} ",
                                           self.page_url.format(base_url=self.base_url)))
            self.http_get(self.page_url.format(base_url=self.base_url), self.scraper_policy.doc_list,
                          encoding="windows_1251", request_args={'timeout': (180, 180)})
            total_bills_text = self.extract_single(
                '//div[@id="content-all"]/div[@class="heading"]//div[@class="information_block_ins"]/h3/a[@class="show_section"]/text()')
            if total_bills_text:
                total_bills = total_bills_text.split(":")[1].strip()
                self.logger.info(__name__, fmt("Total Bills {}",total_bills))
                if total_bills:
                    page_link = self.page_url.format(base_url=self.base_url)
                    self.scrape_bill(page_link)
                else:
                    self.logger.critical(__name__, u"No bills Found", u"ukraine scraping failed : No pages Found", )
                    raise ScrapeError(self.scraper_policy.doc,
                                      fmt(u"{}  bills could not be scraped.", self.scraper_name.title()),
                                      self.page_url.format(base_url=self.base_url))
        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} bills could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{}  bills could not be scraped.", self.scraper_name.title()),
                              self.page_url.format(base_url=self.base_url))

    def scrape_bill(self,page_link):
        self.logger.info(__name__, fmt(u"Fetching for page link  - {} ", page_link))
        try:
            total_bills_on_page = self.xpath('//table[@class="striped Centered"]/tr[position()>1]')
            bill_link_list = []
            for bill_data in total_bills_on_page:
                registration_number = self.extract_single('./td[position()=1]/a/text()', sel=bill_data)
                bill_page_link = self.extract_single('./td[position()=1]/a/@href', sel=bill_data)
                if bill_page_link:
                    bill_page_link = self.member_page_url.format(base_url=self.base_url,rem_url_part=bill_page_link)
                    bill_link_list.append(bill_page_link)
                else:
                    raise ValueError(
                        "No Bill Link found for Bill {}".format(
                            registration_number))
            for link in bill_link_list:
                sleep(randint(1,5))
                self.scrape_page_bill(link)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{}  bills could not be scraped.", self.scraper_name.title()),page_link)

    def scrape_page_bill(self,bill_page_link):
        try:
            self.logger.info(__name__,
                             fmt(u"Fetching for bill page link  - {} ", bill_page_link))
            ukraine_bill = self.model_factory.create_bill_doc()
            self.http_get(bill_page_link,self.scraper_policy.doc_list,encoding="windows_1251", request_args={'timeout':(180,180)})
            bill_title = self.extract_single(
                u'//div[@class="information_block"]/div[@class="information_block_ins"]//h3/text()')
            if bill_title:
                ukraine_bill.title = bill_title
            source_url = bill_page_link
            ukraine_bill.source_url = source_url
            block_section=self.xpath('//div[@class="zp-info"]')
            bill_num_and_date = self.extract_single(u'//dt[contains(.,"Номер, дата реєстрації:")]/following-sibling::dd[1]/text()',
                                          sel=block_section)
            bill_detail = bill_num_and_date.split(u"від")
            registration_number = bill_detail[0].strip()
            date_of_registration = bill_detail[1].strip()
            if registration_number:
                ukraine_bill.registration_number = registration_number
            if date_of_registration:
                ukraine_bill.registration_date = datetime.strptime(date_of_registration,"%d.%m.%Y").strftime('%Y-%m-%d')
            session = self.extract_single(u'//dt[contains(.,"Сесія реєстрації")]/following-sibling::dd[1]/text()',sel=block_section)
            if session:
                ukraine_bill.session = session
            proponent = self.extract_single(u'//dt[contains(.,"Суб\'єкт права законодавчої ініціативи:")]/following-sibling::dd[1]/text()',sel=block_section)
            if proponent:
                ukraine_bill.proponent = proponent
            sponsors_list = self.extract(u'//dt[contains(.,"Ініціатор(и) законопроекту:")]/following-sibling::dd[1]/li//text()',sel=block_section)
            if sponsors_list:
                ukraine_bill.sponsors =  sponsors_list
            bill_topic = self.extract_single(u'//dt[contains(.,"Рубрика законопроекту:")]/following-sibling::dd[1]/text()',sel=block_section)
            if bill_topic:
                ukraine_bill.topic = bill_topic

            main_commitee = self.extract(u'//dt[contains(.,"Головний комітет:")]/following-sibling::dd[1]/li/text()',sel=block_section)
            committee_type = "Primary"
            committee_list = []
            if main_commitee:
                for committee_name in main_commitee:
                    commitee_mf = self.model_factory.create("UkraineCommitteesSchema")
                    commitee_mf.committee_name = committee_name
                    commitee_mf.committee_type = committee_type
                    if self.validate_doc(commitee_mf):
                        committee_list.append(commitee_mf)
                    else:
                        raise ScrapeError(self.scraper_policy.doc_list, fmt(
                            u"{}  bills could not be scraped . Validation Failed  - Main Commitee Found : Website Structure change",
                            self.scraper_name.title()), bill_page_link)

            elif self.extract(u'//dt[contains(.,"Головний комітет:")]',sel=block_section):
                # if Committee label "Головний комітет:" is present on website, then we are checking for text data of adjacent <dd> tag
                # If we find text then we are considering that there is some change in html source and that's why raising error.
                if self.extract(u'//dt[contains(.,"Головний комітет:")]/following-sibling::dd[1]//text()',sel=block_section):
                    raise ScrapeError(self.scraper_policy.doc_list,fmt(u"{}  bills could not be scraped. - Main Commitee Found : Website Structure change", self.scraper_name.title()),bill_page_link)
                else:
                    pass
            else:
                pass
            other_commitees = self.extract(u'//dt[contains(.,"Інші комітети:")]/following-sibling::dd[1]/li/text()',sel=block_section)
            other_commitee_type = "Secondary"
            if other_commitees:
                for o_committee_name in other_commitees:
                    commitee_omf = self.model_factory.create("UkraineCommitteesSchema")
                    commitee_omf.committee_name = o_committee_name
                    commitee_omf.committee_type = other_commitee_type
                    if self.validate_doc(commitee_omf):
                        committee_list.append(commitee_omf)
                    else:
                        self.logger.info(__name__, 'Skipping other commitee: {}'.format(commitee_omf.for_json()))
            if committee_list:
                ukraine_bill.committees = committee_list

            document_url_list = []
            text_documents = self.xpath(
                u'//div[@class="zp-info"]//dt[contains(.,"Текст законопроекту та супровідні документи:")]/following-sibling::dd[1]/li')

            if text_documents:
                for doc in text_documents:
                    document_title = self.extract_single("./a/text()",sel=doc)
                    document_url = self.extract_single("./a/@href",sel=doc)
                    document_type = "Bill Text"
                    if document_url:
                        document_link = self.member_page_url.format(base_url=self.base_url, rem_url_part=document_url)
                    else:
                        document_link = None
                    document_url_list.append((document_title,document_link,document_type))

            work_documents = self.xpath(u'//div[@class="zp-info"]//dt[contains(.,"Документи, пов\'язані із роботою:")]/following-sibling::dd[1]/li')
            if work_documents:
                for doc in work_documents:
                    document_title = self.extract_single("./a/text()",sel=doc)
                    document_url = self.extract_single("./a/@href",sel=doc)
                    document_type = "Working Document"
                    if document_url:
                        document_link = self.member_page_url.format(base_url=self.base_url, rem_url_part=document_url)
                    else:
                        document_link = None
                    document_url_list.append((document_title, document_link, document_type))

            documents_list = []
            for doc_title, doc_link, doc_type in document_url_list:
                if doc_link:
                    doc_details, status = self.document_download_block(doc_link)
                    if status:
                        download_id, _, doc_ids = doc_details
                        document = self.model_factory.create("UkraineDocumentSchema")
                        if len(doc_ids) > 0:
                            document_id = doc_ids[0]
                            document.document_id = document_id
                            document.download_id = download_id
                            document.document_title = doc_title
                            document.document_type = doc_type
                        else:
                            raise ValueError(
                                "Document ID not found while registering document with url {}".format(
                                    doc_link))
                        if not download_id:
                            raise ValueError(
                                "Download ID not found while registering document with url {}".format(
                                    doc_link))

                        if self.validate_doc(document):
                            documents_list.append(document)
                        else:
                            self.logger.info(__name__, 'Skipping Attachment: {}'.format(document.for_json()))

            if len(documents_list) > 0:
                ukraine_bill.documents = documents_list
            else:
                self.logger.info(__name__, fmt("No Documents Found on url : {}", bill_page_link))

            action_list = []

            action_details = self.xpath('//div[@id="flow_tab"]/table/tr[position()>1]')
            if action_details:
                for action in action_details:
                    action_data = self.model_factory.create("UkraineActionSchema")
                    action_date = self.extract_single('./td[position()=1]/text()', sel=action)
                    if action_date:
                        action_data.action_date = datetime.strptime(action_date,"%d.%m.%Y").strftime('%Y-%m-%d')
                    action_status = self.extract_single('./td[position()=2]/text()', sel=action)
                    if action_status:
                        action_data.action_text = action_status
                    if self.validate_doc(action_data):
                        action_list.append(action_data)
                    else:
                        self.logger.info(__name__, 'Skipping action details: {}'.format(action_data.for_json()))

            status_details = self.extract_single(u'//div[@id="flow_tab"]/table/tr[position()=1]/th[2]/text()')
            if status_details:
                ukraine_bill.status = status_details

            if action_list:
                ukraine_bill.actions = action_list

            if self.validate_doc(ukraine_bill):
                self.save_doc(ukraine_bill.for_json())
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill having link: {}",
                                         bill_page_link))

                self.logger.critical(__name__,self.json_dumps(message = ukraine_bill.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)

    def document_download_block(self,document_link):
        try:
            resp = self.http_request(document_link, "HEAD")
            if resp:
                self.logger.info(__name__,fmt("Content type of link : {}",resp.headers['Content-Type']))
                if 'pdf' in resp.headers['Content-Type'] or '.pdf' in resp.headers.get("Content-Disposition", ""):
                    extraction_type = self.extraction_type.unknown
                    content_type = "application/pdf"

                elif 'msword' in resp.headers['Content-Type'] or '.doc' in resp.headers.get("Content-Disposition", ""):
                    extraction_type = self.extraction_type.msword_doc
                    content_type = 'application/msword'

                elif 'rtf' in resp.headers['Content-Type'] or '.rtf' in resp.headers.get("Content-Disposition", ""):
                    extraction_type = self.extraction_type.rtf
                    content_type = 'application/rtf'

                elif 'application/vnd.ms-excel' in resp.headers['Content-Type'] or '.xls' in resp.headers.get("Content-Disposition", ""):
                    extraction_type = self.extraction_type.msexcel_xls
                    content_type = 'application/vnd.ms-excel'

                elif 'text/html' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.html
                    content_type = 'text/html'

                else:
                    self.logger.critical(
                        __name__,
                        "Document Download failed",
                        fmt(
                            "Could not determine document type - content-type: {}; content-disposition: {}",
                            resp.headers.get("Content-Type"), resp.headers.get("Content-Disposition")),
                        exc_info=True)
                    return None, False

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
            self.logger.critical(__name__,"Document Download failed" , fmt("Content type of link : {} ", document_link), exc_info=True)
            return None, False

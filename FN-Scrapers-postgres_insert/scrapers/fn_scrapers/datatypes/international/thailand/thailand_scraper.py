# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import injector
from fn_scraperutils.events.reporting import EventComponent
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from uritools import urijoin

@scraper()
@tags(type="bills", country_code="TH", group="international")
class ThailandDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(ThailandDocScraper, self).__init__(EventComponent.scraper_bills, "thailand", "thailand")
        self.base_url = u'http://web.senate.go.th'
        self.advance_search_url = self.base_url + u'/w3c/senate/lawdraft/index.php?kw={term}&page=1&orby=&orrg=ASC'
        self.member_url = self.base_url + u'/w3c/senate/lawdraft/{rem_url}'
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "thailand.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)

    def scrape(self):
        page_link = self.advance_search_url.format(term='')
        self.logger.info(__name__, fmt('Visiting page link : {}', page_link ))
        self.http_get(page_link, self.scraper_policy.doc_list)

        total_pages = self.extract('//ul[@class="pagination"]/li/a/@href')
        page_no = 1
        self.logger.info(__name__, fmt("Total pages to be scraped : {} ", str(len(total_pages) +1)))
        self.parse_bills(page_no, page_link)
        for page_link in total_pages:
            page_no +=1
            page_link = self.member_url.format(rem_url=page_link)
            self.http_get(page_link, self.scraper_policy.doc_list)
            self.logger.info(__name__, fmt('Visiting page link : {}', page_link))
            self.parse_bills(page_no, page_link)

    def parse_bills(self, page_no, page_link):
        document_title_list = [u"ร่างวาระ หนึ่ง", u"ผลการพิจารณาในวาระที่ หนึ่ง", u"รายงานกรรมาธิการ", u"ร่างที่เห็นชอบ", u"ข้อสังเกตของคณะกรรมาธิการ", u"ผลการดำเนินการตามข้อสังเกต" , u"ราชกิจจานุเบกษา"]
        rows = self.xpath('//table[@class="table"]/tbody/tr')

        for row in rows: #[1:]
            document_links_list = []
            bill_id = ''
            title = self.extract_single('td[2]/text()',sel=row)
            sponsor = self.extract_single('td[3]/text()',sel=row)
            if not (title and sponsor):
                self.logger.warning(__name__, "missing_title_or_sponsor", "Unable to find title or sponsor")
                continue
            current_status = self.extract_single('td[4]/text()',sel=row)
            for i in range(1,8):
                path = 'td[{}]/a/@href'.format(str(i+4))
                document_link = self.extract_single(path, sel=row)
                if document_link:
                    document_link = urijoin(page_link, document_link)
                    data = (document_link, document_title_list[i-1])
                    document_links_list.append(data)


            if document_links_list:
                #bill id is being getting fetched from the hyperlink of pdf documents.
                # This id is unique irespective of ranking of bills on page.
                bill_id = document_links_list[0][0].rsplit('/',1)
                bill_id = bill_id[-1].split(".")[0].split('-')[0]
                self.parse_bill_page(bill_id, title, sponsor, current_status, document_links_list, page_no, page_link)

    def parse_bill_page(self, bill_id, title, sponsor, current_status, document_links_list, page_no, page_link):
        try:
            thailand_doc = self.model_factory.create_bill_doc()
            thailand_doc.title = title
            thailand_doc.sponsor = sponsor
            thailand_doc.current_status = current_status
            thailand_doc.bill_id = bill_id
            document_list = []

            for link, document_title in document_links_list:
                doc_details, status = self.document_download_block(link)
                if status:
                    download_id, _, doc_ids = doc_details
                    document = self.model_factory.create("ThailandDocumentSchema")
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        document.document_id = document_id
                        document.download_id = download_id
                        document.document_title = document_title
                        if self.validate_doc(document):
                            document_list.append(document)
                        else:
                            self.logger.info(__name__, 'Skipping Attachment: {}'.format(document.for_json()))
                    else:
                        raise ValueError(
                            "Document ID not found while registering document with url {}".format(
                                link))
                    if not download_id:
                        raise ValueError(
                            "Download ID not found while registering document with url {}".format(
                                link))

                else:
                    self.logger.critical(
                        __name__, "individual_bill_scrape_failed", fmt(u"No Documents Found on url : {} .", link), exc_info=True)

            if len(document_list) > 0:
                thailand_doc.documents = document_list
            else:
                self.logger.info(__name__, fmt(u"No Documents Found on bill_id : {}", bill_id))
            if self.validate_doc(thailand_doc):
                self.save_doc(thailand_doc.for_json())
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill having id : {} on page : {}", bill_id , page_no))
                self.logger.info(__name__, self.json_dumps(message=thailand_doc.for_json()))

        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)

    def document_download_block(self, document_link=None):
        try:
            resp = self.http_request(document_link, "HEAD")
            if resp.status_code != 200:
                self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                     fmt('http request is failing with error: {} for url  ', document_link))
                return None, False
            if 'Content-Length' in resp.headers:
                if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                    error_message = "File @ '{}' is larger than max size {} bytes.".format(
                        document_link, MAX_FILE_DOWNLOAD_SIZE)
                    self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                         fmt('While extracting document Doc-Service is failing with error: {}',
                                             error_message))
                    return None, False
            self.logger.info(__name__, fmt("Content type of link : {}", resp.headers['Content-Type']))
            extraction_type = self.extraction_type.tesseract
            content_type = "application/pdf"
            download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type,
                                                                           extract_args={
                                                                            'language': 'tha',
                                                                            'pageCount': 25
                                                                           })

            if download_id and _ and doc_ids:
                return (download_id, _, doc_ids), True
            else:
                return None, False
        except Exception as e:
            self.logger.critical(__name__, "Document Download failed", fmt("Content type of link : {} ", document_link))
            return None, False




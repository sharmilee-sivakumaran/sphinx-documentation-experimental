# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import re
import injector
from datetime import datetime, date, timedelta
import lxml.html as LH
from lxml.html import fromstring, tostring
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info

@scraper()
@argument('--start_date', help='startdate in the format YYYY-mm-dd i.e.2017-06-07')
@argument('--end_date', help='enddate in the format YYYY-mm-dd i.e.2018-06-07')
@tags(type="notices", country_code="PE", group="international")
class Peru_GazetteDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(Peru_GazetteDocScraper, self).__init__(EventComponent.scraper_bills, "peru_gazette", "peru_gazette")
        self.base_url = u'https://diariooficial.elperuano.pe/Normas'
        self.member_url = u'{base_url}{list_url}'
        self.main_document_link = u"https://busquedas.elperuano.pe/"
        self.logger = logger
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "regulation_notice.json")
        self.model_factory = ModelFactory(notice_json_schema_file_path,notice_json_schema_file_path)
        self.document_link = ''

    def fetch_static_content(self, html_file):
        html_page = html_file.read()
        page_content = html_page.decode('utf-8')
        self.set_selector(page_content)
        required_block = self.xpath('//div[@class="story"]').extract()
        if required_block:
            xroot = self._sel.root
            xroot.make_links_absolute(self.main_document_link)
            body_tag = xroot.xpath('//body')
            data = "<body>" + required_block[0] + "</body>"
            xroot.replace(body_tag[0],LH.fromstring(data))
            #removing style tags
            style_tag = xroot.xpath('//style')
            if style_tag:
                for val in style_tag:
                    val.getparent().remove(val)
            #removing script tags
            script_tag = xroot.xpath('//script')
            if script_tag:
                for value in script_tag:
                    value.getparent().remove(value)

            html_text = tostring(xroot)
        else:
            raise Exception(fmt("No HTML content on link : {}", self.document_link))
        return html_text

    def scrape(self, start_date, end_date):
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            start_date = date.today()-timedelta(days=30)
        if end_date:
            end_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            end_date = date.today()
        try:
            self.logger.info(__name__, fmt(u"Fetching for main page link  - {} ",self.base_url))
            self.http_get(self.base_url,self.scraper_policy.doc_list)

            if end_date < start_date:
                raise ValueError("Invalid Start and end date parameter. Failed to start scraping.")
            
            has_rules = False
            for rule in self.get_notices(start_date, end_date):
                has_rules = True
                self.scrape_notice(rule)
            if not has_rules:
                self.logger.critical(__name__, u"scraper_run_finished",
                                 fmt(u"{} : No Notices Found ", self.scraper_name.title()))

        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} notices could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),
                                 exc_info=True)
            raise

    def get_notices(self, start_date, end_date):
        left = start_date
        while True:
            right = min(end_date, left + timedelta(days=30))
            self.logger.info(__name__, fmt(
                u"Requesting date range: {:%Y/%m/%d}-{:%Y/%m/%d} ", left, right
            ))
            rem_part = "/Filtro?dateparam={:%m/%d/%Y} 00:00:00".format(right)
            data = {
                'cddesde': left.strftime('%d/%m/%Y'),
                'cdhasta': right.strftime('%d/%m/%Y'),
                'btnBuscar': 'Filtrar',
                'X-Requested-With': 'XMLHttpRequest',
            }
            self.http_post(
                self.member_url.format(base_url=self.base_url, list_url=rem_part),
                self.scraper_policy.doc_list,
                request_args={'timeout':(180,200), 'data':data})
            rules = self.xpath('//article[@class="edicionesoficiales_articulos"]')
            if not rules:
                self.logger.critical(__name__, u"scraper_run_finished", fmt(
                    u"{} : No Notices Found ", self.scraper_name.title()))
            else:
                for rule in rules:
                    yield rule
            if right == end_date:
                break
            left = right + timedelta(days=1)

    def scrape_notice(self,rule):
        try:
            peru_gazette_doc = self.model_factory.create_bill_doc()
            department_list = []
            department_name = self.extract_single('.//div[@class="ediciones_texto"]/h4/text()',sel=rule)
            department_hierarchy = 0
            department_obj = self.model_factory.create("PeruGazetteDepartmentSchema")
            if department_name:
                department_obj.department_name = department_name.title()
                department_obj.department_hierarchy = department_hierarchy
                department_list.append(department_obj)
            if len(department_list)>0:
                peru_gazette_doc.departments = department_list

            title = self.extract_single('.//div[@class="ediciones_texto"]/p[2]/text()',sel=rule)
            if title:
                peru_gazette_doc.title = title

            publication_date = self.extract_single('.//div[@class="ediciones_texto"]/p[1]//text()',sel=rule)
            if publication_date:
                publication_date = publication_date.split(":")
                if len(publication_date)==2:
                    publication_date = publication_date[1].strip()
                publication_date = parse_date_as_str(publication_date, languages=['es'])
                peru_gazette_doc.publication_date = publication_date

            notice_id = self.extract_single('.//div[@class="ediciones_texto"]/h5/a/text()',sel=rule)
            if notice_id:
                notice_id = notice_id.title()
                peru_gazette_doc.notice_id = notice_id

            peru_gazette_doc.publication_name = "El Peruano"
            peru_gazette_doc.country = "Peru"
            peru_gazette_doc.source_url = "https://diariooficial.elperuano.pe/Normas"
            document_link = self.extract_single('.//div[@class="ediciones_texto"]/h5/a/@href',sel=rule)
            if document_link:
                self.document_link = document_link
                peru_gazette_doc.source_url = document_link
                doc_details, status = self.document_download_block(document_link, document_block_status = True)
                if status:
                    download_id, _, doc_ids = doc_details
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        peru_gazette_doc.document_id = document_id
                        peru_gazette_doc.download_id = download_id
                        if notice_id:
                            peru_gazette_doc.document_title = notice_id.title()
                    else:
                        raise ValueError(
                            "Document ID not found while registering document with url {}".format(
                                document_link))
                    if not download_id:
                        raise ValueError(
                            "Download ID not found while registering document with url {}".format(
                                document_link))

                else:
                    raise Exception(fmt("No document found for url : {}", document_link))
            else:
                raise Exception("No document found")

            if self.validate_doc(peru_gazette_doc):
                self.save_doc(peru_gazette_doc.for_json())
            else:
                self.logger.critical(__name__, "individual_notice_scrape_failed", "JsonSchema validation failed for notice")
                self.logger.info(__name__,self.json_dumps(message = peru_gazette_doc.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e), exc_info=True)


    def document_download_block(self,document_link=None, document_block_status=True):
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
            self.logger.info(__name__,fmt("Content type of link : {}",resp.headers['Content-Type']))
            if 'pdf' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"
            elif 'octet-stream' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = "application/octet-stream"
            else:
                extraction_type = self.extraction_type.html
                content_type = "text/html"

            if document_block_status:
                download_id, _, doc_ids = self.register_download_and_documents(
                    document_link, self.scraper_policy.doc_service,
                    extraction_type, True, content_type=content_type,
                    get_static_content=self.fetch_static_content,
                    should_skip_checks=True)
            else:
                download_id, _, doc_ids = self.register_download_and_documents(
                    document_link, self.scraper_policy.doc_service,
                    extraction_type, True, content_type=content_type)

            if download_id and _ and doc_ids:
                return (download_id , _ , doc_ids),True
            else:
                return None, False

        except Exception as e:
            self.logger.critical(__name__,"Document Download failed" , fmt("Content type of link : {} ", document_link))
            return None, False

# -*- coding: utf-8 -*-
import re
import os
import textwrap
import injector
import datetime
import requests
import itertools
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy

@scraper()
@argument("--start-year", help="Start Year to scrape data from Brazil gazette website", type=int, default=0, choices=range(2017, datetime.date.today().year+1))
@tags(type="notices", country_code="BR", group="international")
class BrazilRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(BrazilRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "brazil_reg_notice", "brazil_reg_notice")
        self.logger = logger
        self.base_url = u'http://portal.imprensanacional.gov.br/'
        self.search_url = u'http://portal.imprensanacional.gov.br/web/guest'
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "brazil_notice.json")
        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("brazil").alpha_2)
        self.retry_policy = Retry500RequestsRetryPolicy(max_attempts=100,max_retry_time=2000,max_attempt_delay=1000)

    def scrape(self, start_year):
        try:
            self.scrape_notices(int(start_year))
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt(u"{} notices could not be scraped. {}",self.scraper_name, e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list, fmt(u"{} notices could not be scraped. {}",self.scraper_name, e), self.search_url)

    def scrape_notices(self, start_year):
        end_date = datetime.date.today()
        if start_year == 0:
            start_date = end_date - datetime.timedelta(days=30)
        elif start_year in range(2017, start_year + 1):
            start_date = datetime.date(start_year, 1, 1)
        else:
            raise ValueError("Invalid Start year parameter. Failed to start scraping.")

        # --------------------------------------------
        # Visit Home page
        # --------------------------------------------
        self.logger.info(__name__,fmt(u"Going to home page."))
        self.http_get(self.search_url, self.scraper_policy.doc_list, retry_policy=self.retry_policy)
        # Search url
        search_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=search_url)
        self.logger.info(__name__, fmt(u"Going forward with start date {} and end date {}", start_date, end_date))
        # --------------------------------------------------
        #      Search javascript code to find minimum
        #      start date applicable on website
        # --------------------------------------------------
        m = re.search(u"var\W+dou_min_date\W+year\W+(.+?)\W+month\W+(.+?)\,\W+day\W+(.+?)\W",
                      self.get_content_from_response(), re.I | re.DOTALL | re.U)
        if not m:
            raise ValueError("Website may have changed - Minimum start date can not be parsed.")

        year = m.group(1)
        month = m.group(2).split(u'-')[0]
        day = m.group(3)
        website_minimum_start_date = datetime.date(int(year), int(month), int(day))
        start_date_scrape = start_date
        if website_minimum_start_date > start_date_scrape:
            self.logger.warning(__name__, fmt(u"Scraper start date is less than the website minimum start date so reseting start date to {} ", website_minimum_start_date.strftime("%d/%m/%Y")))
            start_date_scrape = website_minimum_start_date

        # --------------------------------------------
        #          Set Advanced Search form
        # --------------------------------------------
        form_id = 'imprensa_fm'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if not input_fields_dict:
            raise Exception("Website may have changed - Input fields can not be parsed.")

        input_fields_dict['optionsSecao'] = 'secao1'
        input_fields_dict['p_p_id'] = '3'
        input_fields_dict['p_p_lifecycle'] = '0'
        input_fields_dict['p_p_state'] = 'maximized'
        input_fields_dict['p_p_mode'] = 'view'
        input_fields_dict['_3_struts_action'] = '/search/search'
        input_fields_dict['_3_cur'] = '1'
        input_fields_dict['_3_groupId'] = '0'
        input_fields_dict['_3_modifiedselection'] = '1'
        input_fields_dict['_3_ddm_21040_pubName_pt_BR_sortable'] = 'do1, do1a, do1e'
        input_fields_dict['_3_modifiedfrom'] = start_date_scrape.strftime("%d/%m/%Y")
        input_fields_dict['_3_modifieddayFrom'] = start_date_scrape.strftime("%e")
        input_fields_dict['_3_modifiedmonthFrom'] = start_date_scrape.month - 1
        input_fields_dict['_3_modifiedyearFrom'] = start_date_scrape.strftime("%Y")
        input_fields_dict['_3_modifiedto'] = end_date.strftime("%d/%m/%Y")
        input_fields_dict['_3_modifieddayTo'] = end_date.strftime("%e")
        input_fields_dict['_3_modifiedmonthTo'] = end_date.month - 1
        input_fields_dict['_3_modifiedyearTo'] = end_date.strftime("%Y")
        search_range = "[" + start_date_scrape.strftime("%Y%m%d") + "000000 TO " + end_date.strftime(
            "%Y%m%d") + "235959]"
        input_fields_dict['_3_modified'] = search_range

        action = urljoin(self.search_url, '/consulta')
        req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
        self.http_post(action, self.scraper_policy.doc_list, request_args=req_args, retry_policy=self.retry_policy)

        self._sel.root.make_links_absolute(base_url=action)
        page_num = 1
        next_page = self.extract(u'//a[contains(@href,"&_3_cur") and contains(.,"Mais")]/@href')
        next_page = next_page[0] if next_page else None
        self.parse_notices(page_num)
        while next_page:
            page_num += 1
            # Search url
            self.http_get(next_page, self.scraper_policy.doc_list, retry_policy=self.retry_policy)
            next_page = self.resp.url
            self._sel.root.make_links_absolute(base_url=next_page)
            np = self.extract(u'//a[contains(@href,"&_3_cur") and contains(.,"Mais")]/@href')
            np = np[0] if np else None
            self.parse_notices(page_num)
            if np == next_page:
                break
            else:
                next_page = np
                self.logger.info(__name__, fmt(u"Next page url : {}", next_page))

    def parse_notices(self, page_num):
        notice_rows = self.xpath(u'//tbody[@class="table-data"]/tr/td')
        if not notice_rows:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notices not found.")
            return
        notice_page_urls = self.extract(u'//tbody[@class="table-data"]/tr/td/span/span[@class="asset-entry-title"]/a/@href')
        if not notice_page_urls:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notices not found.")
            return
        self.logger.info(__name__, fmt(u"No. of notice urls on search page: {}", len(notice_page_urls)))
        for notice_row in notice_rows:
            notice_type = None
            notice_page_url = self.extract_single(u'./span/span[@class="asset-entry-title"]/a/@href', sel=notice_row)
            if not notice_page_url:
                self.logger.warning(__name__, u"individual_notice_scrape_failed", fmt(u"No notice page url for a notice on page number : {} ", page_num))
                return
            self.logger.info(__name__, fmt(u"Notice page url: {}", notice_page_url))
            #continue
            notice_id = self.extract_as_one(u'./span/span[@class="asset-entry-title"]/a/text()', sel=notice_row)
            self.logger.info(__name__, fmt(u"Notice id --{}--", notice_id))
            m = re.search(ur'([\w\s\-]+?)(N[º|O]\s+\d+)', notice_id, re.I | re.U | re.DOTALL)
            if not m:
                # ---------------------------------------------------
                # Notice type is not a must required field that's
                # why we are logging message and processing further
                # ---------------------------------------------------
                notice_type = self.extract_single(u'.//div[@class="asset-entry-categories"]//a[@class="asset-category"]/text()', sel=notice_row)
                if not notice_type:
                    self.logger.info(__name__, u"Notice type not found.")
                else:
                    notice_type = notice_type.split(u"-")[0]
                    notice_type = notice_type.strip().title()
                    self.logger.info(__name__, fmt(u"Notice type found: {}", notice_type))
            else:
                notice_type = m.group(1).split(u"-")[0]
                notice_type = notice_type.strip().title()
            notice_title = self.extract_single(u'./span/div[@class="asset-entry-content"]/span[@class="asset-entry-summary"]/text()', sel=notice_row)
            notice_title = notice_title.strip()
            lines = textwrap.wrap(notice_title, 200, break_long_words=False)
            notice_title = lines[0] + ("..." if len(lines) > 1 else "")
            self.parse_notice_details(notice_page_url, notice_id, notice_type, notice_title)

    def parse_notice_details(self, notice_page_url, notice_id, notice_type, notice_title):
        try:
            brazil_notice = self.model_factory.create_bill_doc()
            brazil_notice.country = u"Brazil"
            brazil_notice.publication_name = u"Official Diary (Diário Oficial da União DOU Seção 1)"
            if notice_title:
                brazil_notice.title = notice_title

            if notice_id:
                brazil_notice.notice_id = notice_id.title()

            if notice_type:
                brazil_notice.notice_type = notice_type

            brazil_notice.source_url = notice_page_url
            self.logger.info(__name__, fmt(u"Going to Notice page. Url: {}", notice_page_url))
            self.http_get(notice_page_url, self.scraper_policy.doc_list, request_args={'timeout': (300, 300)}, retry_policy=self.retry_policy)
            page_url = self.resp.url
            self._sel.root.make_links_absolute(base_url=page_url)
            publication_date = self.extract_single('//div[@class="detalhes-dou"]/p[@class="centralizar"][1]/span[@class="publicado-dou-data"]/text()')

            if publication_date:
                brazil_notice.publication_date = parse_date_as_str(publication_date, languages=self.country_languages)

            publication_issue = self.extract_single(
                '//div[@class="detalhes-dou"]/p[@class="centralizar"][1]/span[@class="edicao-dou-data"]/text()')
            if publication_issue:
                brazil_notice.publication_issue = publication_issue

            department_name = self.extract_single(
                '//div[@class="detalhes-dou"]/p[@class="centralizar"][2]/span[@class="orgao-dou-data"]/text()')

            if not department_name:
                # ---------------------------------------------------
                # Department is not a must required field that's
                # why we are logging warning and processing further
                # ---------------------------------------------------
                self.logger.info(__name__, u"Department not found.")
            else:
                department_list = []
                department_hierarchy = 0
                department_obj = self.model_factory.create("DepartmentSchema")
                department_obj.department_name = department_name.title()
                department_obj.department_hierarchy = department_hierarchy
                department_list.append(department_obj)
                brazil_notice.departments = department_list
            # ------------------------------------------------------------------------------------------
            # https://fiscalnote2.atlassian.net/browse/DI-2122
            # Please scrape the HTML page instead of the "Versão certificada em PDF"
            # Most of the "Versao certificada" links appear to be broken.
            # For example:
            # http://pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp?data=22/01/2018&jornal=515&pagina=88
            # ------------------------------------------------------------------------------------------
            document_link = notice_page_url
            resp = self.http_request(document_link, "HEAD")

            if resp.status_code != 200:
                self.logger.critical(__name__, "individual_notice_document_extraction_failed",
                                     fmt(u'http request is failing with status code : {} for url {}', resp.status_code, document_link))
                return

            if 'Content-Length' in resp.headers:
                if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                    error_message = u"File @ '{}' is larger than max size {} bytes.".format(document_link, MAX_FILE_DOWNLOAD_SIZE)
                    self.logger.warning(__name__, "individual_notice_document_extraction_failed",
                                         fmt(u'While extracting document Doc-Service is failing with error: {}', error_message))
                    return

            content_type, encoding, return_headers = self.parse_headers(resp.headers)

            if 'html' in content_type:
                extraction_type = self.extraction_type.html
            else:
                extraction_type = self.extraction_type.unknown

            download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type)

            if len(doc_ids) > 0:
                document_id = doc_ids[0]
            else:
                raise ValueError(
                    "Document ID not found while registering document with url {}".format(
                        document_link))
            if not download_id:
                raise ValueError(
                    "Download ID not found while registering document with url {}".format(
                        document_link))

            brazil_notice.document_id = document_id
            brazil_notice.download_id = download_id
            brazil_notice.document_title = u"Versão certificada em PDF"

            if self.validate_doc(brazil_notice):
                self.save_doc(brazil_notice.for_json())
            else:
                self.logger.critical(__name__, "individual_notice_scrape_failed",
                                     fmt(u"JsonSchema validation failed for notice having url: {}", notice_page_url))
        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt(u"Error occured: {}", e),
                                 exc_info=True)

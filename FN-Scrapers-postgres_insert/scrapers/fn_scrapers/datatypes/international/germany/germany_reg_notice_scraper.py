# -*- coding: utf-8 -*-
import re
import os
import injector
import datetime

from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scrapers.api.resources import ScraperConfig
from fn_service.server import BlockingEventLogger, fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info, urljoin

@scraper()
@argument('--start-date', help='startdate in the format YYYY-mm-dd i.e.2017-02-07', default=(datetime.date.today()-datetime.timedelta(days=30)).strftime("%Y-%m-%d") )
@argument('--end-date', help='enddate in the format YYYY-mm-dd i.e.2018-02-07', default=datetime.date.today().strftime("%Y-%m-%d"))
@tags(type="notices", country_code="DE", group="international")
class GermanyRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger, config=ScraperConfig)
    def __init__(self, logger, config):
        super(GermanyRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "germany_reg_notice", "germany_reg_notice")

        self.logger = logger
        self.base_url = u'https://www1.recht.makrolog.de/'
        self.login_page_url = u'https://www1.recht.makrolog.de/irfd/search?view=biblio'
        self.login_action_url = u'https://www1.recht.makrolog.de/login.php'
        self.issue_search_page = u'https://www1.recht.makrolog.de/irfd/fshow?region=bund&notesdb=BD_BGBL1&year={}'
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "germany_notice.json")

        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("germany").alpha_2)

        self.username = config['username']
        self.password = config['password']

        self.logout_url = ''

    def scrape(self, start_date, end_date):
        if self.check_date(start_date, end_date):
            start_date = self.return_date_as_date_obj(start_date)
            end_date = self.return_date_as_date_obj(end_date)
        else:
            raise ValueError("Invalid date format")

        if end_date < start_date:
            raise ValueError("Invalid Start and end date parameter. Failed to start scraping.")

        self.do_login()
        self.logout_another_session_if_exist()
        if self.is_logged_in():
            self.scrape_notices(start_date, end_date)
            if self.do_logout():
                self.logger.info(__name__, 'Successfully Logout.')
            else:
                self.logger.info(__name__, "Scraper couldn't Logout.")
        else:
            raise Exception("Scraper couldn't logged in.")

    def scrape_notices(self, start_date, end_date):

        self.logger.info(__name__, fmt(u"Going forward with start date {} and end date {}", start_date, end_date))
        for issue_year in xrange(start_date.year, end_date.year + 1):
            issue_search_page = self.issue_search_page.format(issue_year)
            self.logger.info(__name__, fmt(u"Going to Issue search page. {}",issue_search_page))
            self.http_get(issue_search_page, self.scraper_policy.doc_list)
            issue_search_page = self.resp.url
            self._sel.root.make_links_absolute(base_url=issue_search_page)
            issue_number_list = self.extract('//select[@id="heftauswahl"]/option/@value')
            for issue_number in issue_number_list:
                self.parse_notices(issue_year, issue_number, start_date, end_date)

    def parse_notices(self, issue_year, issue_number, start_date, end_date):
        issue_search_page = self.issue_search_page.format(issue_year)
        issue_search_page = issue_search_page + '&number=' + issue_number
        self.logger.info(__name__, fmt(u"Going to Issue search page. {}", issue_search_page))
        self.http_get(issue_search_page, self.scraper_policy.doc_list)

        # ----------------------------------------------------
        # 1) Sachverzeichnis [https://www1.recht.makrolog.de/irfd/fshow?region=bund&notesdb=BD_BGBL1&year=2017&number=80_Sachverzeichnis]
        # 2) Jahresinhaltsverzeichnis [https://www1.recht.makrolog.de/irfd/fshow?region=bund&notesdb=BD_BGBL1&year=2017&number=80_Jahresinhaltsverzeichnis]
        # 3) Fundstellennachweis (Teil 1) till (Teil 5) [https://www1.recht.makrolog.de/irfd/fshow?region=bund&notesdb=BD_BGBL1&year=2017&number=80_Fundstellennachweis]
        # Skip labels on https://www1.recht.makrolog.de/irfd/fshow?region=bund&notesdb=BD_BGBL1&year=2017&number=63_Anlage
        # ----------------------------------------------------
        row_xpath = u'//div[@id="main"]/table//table//tr[not(contains(./td/a,"Inhaltsverzeichnis") or contains(./td/a,"Sachverzeichnis") or contains(./td/a,"Jahresinhaltsverzeichnis") or contains(./td/a,"Fundstellennachweis")) and (./td[2][contains(@class,"ebene1") and starts-with(text(),"Hinweis auf andere Verkündungen")] or contains(./td/a/@href,"show?") or contains(./td[2],"Ausgegeben")) ]'
        notice_rows = self.xpath(row_xpath)
        if not notice_rows:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notices not found.")
            return
        publication_issue = u"{} Issue {}".format(issue_year, issue_number)
        issue_date = None
        for notice_row in notice_rows:
            second_td = notice_row.xpath(u'./td[2]')[0]
            if not issue_date:
                issue_date_string = self.extract_as_one('.//text()', sel=second_td)
                issue_date_string = issue_date_string.replace(u'Ausgegeben am ','')
                issue_date = parse_date_as_str(issue_date_string, languages=self.country_languages)
                continue

            if second_td.re(u'Hinweis\s+auf\s+andere\s+Verkündungen'):
                continue

            notice_page_url = self.extract_single('./a/@href', sel=second_td)
            notice_page_url = urljoin(issue_search_page,notice_page_url)
            notice_title = self.extract_as_one('./a//text()', sel=second_td)
            notice_date = self.extract_single('./td[1]/text()', sel=notice_row)
            notice_date = parse_date_as_str(notice_date, languages=self.country_languages)
            if not notice_date:
                notice_date = issue_date

            if start_date  <= self.return_date_as_date_obj(notice_date) <= end_date :
                self.parse_notice_details(notice_page_url, notice_title, notice_date, publication_issue)
            else:
                self.logger.info(__name__, fmt(u"Skipping Issue because notice publication date is {} and not in range start date {} end date {}", notice_date, start_date, end_date))
                continue


    def parse_notice_details(self, notice_page_url, notice_title, notice_date, publication_issue):

        self.http_get(notice_page_url, self.scraper_policy.doc)
        notice_document_viewer_url = self.extract_single('//div[@id="search_menu"]//a[contains(text(),"Single page PDF")]/@href')
        if not notice_document_viewer_url:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notice Single page PDF url not found.")
            return None

        notice_document_viewer_url = urljoin(notice_page_url, notice_document_viewer_url)
        self.http_get(notice_document_viewer_url, self.scraper_policy.doc)
        notice_document_iframe_url = self.extract_single('//iframe/@src')
        if not notice_document_iframe_url:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Iframe with Notice document url not found.")
            return None

        notice_document_iframe_url = urljoin(notice_page_url, notice_document_iframe_url)
        req_args = {'cookies': self.resp.cookies, 'headers': {'Referer': notice_document_viewer_url}}

        resp = self.http_request(notice_document_iframe_url, "HEAD")

        if 'html' in resp.headers['Content-Type']:
            extraction_type = self.extraction_type.html
            content_type = resp.headers['Content-Type']
        else:
            extraction_type = self.extraction_type.unknown
            content_type = resp.headers['Content-Type']

        download_id, _, doc_ids = self.register_download_and_documents(notice_document_iframe_url,
                                                                       self.scraper_policy.doc_service,
                                                                       extraction_type, True,
                                                                       content_type=content_type, download_args=req_args)

        if len(doc_ids) > 0 and doc_ids[0]:
            document_id = doc_ids[0]
        else:
            raise ValueError(
                "Document ID not found while registering document with url {}".format(
                    notice_document_iframe_url))
        if not download_id:
            raise ValueError(
                "Download ID not found while registering document with url {}".format(
                    notice_document_iframe_url))

        germany_notice = self.model_factory.create_bill_doc()
        germany_notice.country = u"Germany"
        germany_notice.publication_name = u"Federal Law Gazette (Bundesgesetzblatt Teil I)"
        germany_notice.publication_issue = publication_issue
        germany_notice.publication_date = notice_date
        germany_notice.title = notice_title
        germany_notice.source_url = notice_page_url
        germany_notice.document_title = u'Notice PDF'
        germany_notice.document_id = document_id
        germany_notice.download_id = download_id
        if self.validate_doc(germany_notice):
            self.save_doc(germany_notice.for_json())
        else:
            self.logger.critical(__name__, "individual_notice_scrape_failed",
                                 fmt(u"JsonSchema validation failed for notice having url: {}", notice_page_url))

    def do_login(self):
        search_url = self.login_page_url
        self.http_get(search_url, self.scraper_policy.doc_list)
        form_id = 'loginForm'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if not input_fields_dict:
            raise Exception("Website may have changed - Input fields can not be parsed.")
        input_fields_dict['tmpUsername'] = input_fields_dict['username'] = self.username
        input_fields_dict['tmpPassword'] = input_fields_dict['password'] = self.password
        req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
        self.http_post(self.login_action_url, self.scraper_policy.doc_list, request_args=req_args)

    def logout_another_session_if_exist(self):

        search_url = self.login_page_url
        previous_session_exist = self.xpath_single('//a[contains(@href,"setProcess")]/@href')
        if previous_session_exist:
            m = re.search('setProcess\(\'(.+)\'\)',previous_session_exist.extract(), re.DOTALL|re.I)
            if not m:
                raise ValueError("Website may have changed - Javascript cookie can not be parsed.")
            vcookie = m.group(1)
            form_name = 'sessions'
            input_fields_dict, method, action = self.html_form_parser(search_type="name", form_name=form_name)
            if not input_fields_dict:
                raise Exception("Website may have changed - Input fields can not be parsed.")

            input_fields_dict['id'] = vcookie
            req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
            self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)
            previous_url = self.resp.url

            refresh_meta_tag = self.xpath_single('//meta[contains(@http-equiv,"refresh")]/@content')
            if not refresh_meta_tag:
                raise Exception("Website may have changed - Meta refresh tag can not be parsed.")
            m = re.search('url\W+(.+)', refresh_meta_tag.extract(), re.DOTALL|re.I)
            if not m:
                raise Exception("Website may have changed - Url from Meta refresh tag can not be parsed.")
            refresh_url = m.group(1)
            req_args = {'headers': {'Referer': previous_url}}
            self.http_get(refresh_url, self.scraper_policy.doc_list, request_args=req_args)
            return True
        else:
            return False

    def is_logged_in(self):
        logout_url = self.xpath_single(
            '//a[contains(text(),"Logout") and contains(text(),"{username}")]/@href'.format(username=self.username))
        if logout_url:
            self.logout_url = urljoin(self.base_url, logout_url.extract())
            return True
        else:
            return False

    def do_logout(self):
        response_page = self.http_get(self.logout_url, self.scraper_policy.doc_list)
        if u'Your session at' in response_page and u'was successfully closed.' in response_page:
            return True
        else:
            return False

    @staticmethod
    def return_date_as_date_obj(date_string):
        return datetime.datetime.strptime(date_string, "%Y-%m-%d").date()

    @staticmethod
    def check_date(start_date, end_date):
        date_re = re.compile('\d{4}\-\d{2}\-\d{2}')
        if date_re.search(start_date) and date_re.search(end_date):
            return True
        else:
            return False
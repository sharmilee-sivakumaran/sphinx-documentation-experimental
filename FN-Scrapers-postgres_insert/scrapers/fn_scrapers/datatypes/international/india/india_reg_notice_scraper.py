# -*- coding: utf-8 -*-
import re
import os
import injector
import datetime

from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt
from requests import HTTPError

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info


@scraper()
@argument('--start-date', help='startdate in the format YYYY-mm-dd i.e.2017-02-07',
          default=(datetime.date.today()-datetime.timedelta(days=30)).strftime("%Y-%m-%d"))
@argument('--end-date', help='enddate in format YYYY-mm-dd i.e. 2017-02-07',
          default=datetime.date.today().strftime("%Y-%m-%d"))
@tags(type="notices", country_code="IN", group="international")
class IndiaRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(IndiaRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "india_reg_notice", "india_reg_notice")

        self.logger = logger
        self.base_url = u'http://egazette.nic.in'

        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "india_notice.json")

        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("india").alpha_2)

    def scrape(self, start_date, end_date):
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        if end_date < start_date:
            raise ValueError("Start date should be less than end date")
        self.scrape_notices(start_date, end_date)

    def scrape_notices(self, start_date, end_date):
        self.logger.info(__name__,  fmt(u"Going forward with start date {} and end date {}", start_date, end_date))
        # --------------------------------------------
        # Visit Home page
        # --------------------------------------------
        self.logger.info(__name__, fmt(u"Going to home page."))
        self.http_get(self.base_url, self.scraper_policy.doc_list)
        # Search url
        search_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=search_url)

        # --------------------------------------------
        # Click search gazette menu tab
        # --------------------------------------------
        form_id = 'Form1'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if not input_fields_dict:
            raise Exception("Website may have changed - Input fields can not be parsed.")

        '''
        Html source code for postback JS function:

        <li class="topmenu"><a id="sgzt" style="height:20px;line-height:20px;" href="javascript:__doPostBack(&#39;sgzt&#39;,&#39;&#39;)"><img src="css3menu1/find1.png" alt=""/>Search Gazette</a></li>
        '''

        input_fields_dict['__EVENTTARGET'] = 'sgzt'
        req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
        self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)

        # --------------------------------------------
        # Click Search by Ministry button
        # --------------------------------------------
        search_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=search_url)
        form_id = 'form1'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if not input_fields_dict:
            raise Exception("Website may have changed - Input fields can not be parsed.")

        input_fields_dict['ctl00$ContentPlaceHolder1$imgbtn_week.x'] = '96'
        input_fields_dict['ctl00$ContentPlaceHolder1$imgbtn_week.y'] = '24'
        input_fields_dict['__SCROLLPOSITIONY'] = '248'
        req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
        self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)

        self.do_scrape(start_date, end_date)

    def do_scrape(self, start_date, end_date):
        # ---------------------------------------------------------------
        # This flow is for scraping data for given year till current date
        # or past 30 days (default flow). Choose Date-wise radio button
        # on search page
        # ---------------------------------------------------------------
        search_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=search_url)
        form_id = 'form1'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if not input_fields_dict:
            raise Exception("Website may have changed - Input fields can not be parsed.")

        date_wise_radio_button_value = '1'
        input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$rdbtnwise${}'.format(
            date_wise_radio_button_value)
        input_fields_dict['ctl00$ContentPlaceHolder1$rdbtnwise'] = date_wise_radio_button_value
        input_fields_dict['__SCROLLPOSITIONY'] = '128'
        req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
        self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)

        # ---------------------------------------------------------------
        # Choose ministry from dropdown and set date range on search page
        # ---------------------------------------------------------------
        search_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=search_url)
        option_list = self.xpath('//select[@name="ctl00$ContentPlaceHolder1$ddlministry"]/option[not(@value="1") and not(@value="Select Ministry")]')
        if not option_list:
            raise Exception("Website may have changed - Options can not be parsed.")

        for option in option_list:
            option_text = self.extract_single('text()', sel=option)
            option_value = self.extract_single('@value', sel=option)

            form_id = 'form1'
            input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
            if not input_fields_dict:
                raise Exception("Website may have changed - Input fields can not be parsed.")

            input_fields_dict['__SCROLLPOSITIONY'] = '128'
            input_fields_dict['ctl00$ContentPlaceHolder1$Button1'] = 'Search'
            input_fields_dict['ctl00$ContentPlaceHolder1$ddlministry'] = option_value
            input_fields_dict['ctl00$ContentPlaceHolder1$txtDateIssueF'] = start_date.strftime("%d-%b-%Y")
            input_fields_dict['ctl00$ContentPlaceHolder1$txtDateIssueT'] = end_date.strftime("%d-%b-%Y")
            self.logger.info(__name__, fmt(u"Going to select Ministry: {}", option_text))
            req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
            self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)

            search_url = self.resp.url
            self._sel.root.make_links_absolute(base_url=search_url)
            selected_ministry = self.extract_single('//select[@name="ctl00$ContentPlaceHolder1$ddlministry"]/option[@selected]/text()')
            if not selected_ministry:
                self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Selected Ministry name not found.")
                continue
            self.logger.info(__name__, fmt(u"Selected Ministry: {}", selected_ministry))
            result_message = self.extract_single('//span[@id="ContentPlaceHolder1_lblresult"]/text()')
            self.logger.info(__name__, fmt(u"Result message: {}", result_message))
            m = re.search("No\.\s+of\s+Gazettes\s+found:\s+([1-9]{1,})", result_message)
            if m:
                self.logger.info(__name__, fmt(u"Notices found: {}", m.group(1)))
                self.parse_notice_details(selected_ministry, start_date, end_date)
            else:
                self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notices not found.")

    def parse_notice_details(self, department_name, start_date, end_date):

        rows = self.xpath('//table/tr[td/font[contains(.,"S.No.") or contains(., "S. No.")]]/following-sibling::tr')
        if not rows:
            self.logger.warning(__name__, u"individual_notice_scrape_failed", u"Notices not found.")
            return

        country = 'India'
        publication_name = 'India Gazette'

        for row in rows:
            publication_date = self.extract_single('./td[4]/span/text()', sel=row)
            publication_date = parse_date_as_str(publication_date)
            notice_title = self.extract_single('./td[3]/span/text()', sel=row)
            notice_title = re.sub("\s+", u" ", notice_title.strip())
            notice_title = notice_title.strip(u",.").strip()
            document_title = notice_title
            document_input_field_name = self.extract_single('./td[5]/input/@name', sel=row)

            # Extract notice details for given notice search url
            india_notice = self.model_factory.create_bill_doc()
            india_notice.country = country
            india_notice.publication_name = publication_name
            india_notice.publication_date = publication_date
            india_notice.title = notice_title
            india_notice.document_title = document_title

            department_list = []
            department_name = department_name.title()
            department_obj = self.model_factory.create('NoticeDepartmentSchema')
            department_obj.department_name = department_name
            department_obj.department_hierarchy = 0
            department_list.append(department_obj)

            india_notice.departments = department_list

            form_id = 'form1'
            input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
            if not input_fields_dict:
                raise Exception("Website may have changed - Input fields can not be parsed.")

            input_fields_dict['__SCROLLPOSITIONY'] = '128'
            input_field_name = "{}.x".format(document_input_field_name)
            input_fields_dict[input_field_name] = 9
            input_field_name = "{}.y".format(document_input_field_name)
            input_fields_dict[input_field_name] = 9

            search_url = self.resp.url
            req_args = {'data': input_fields_dict, 'headers': {'Referer': search_url}}
            self.http_post(action, self.scraper_policy.doc_list, request_args=req_args)

            search_url = self.resp.url
            self._sel.root.make_links_absolute(base_url=search_url)

            script_tag_for_download_url = self.extract_single('//script[contains(.,"WriteReadData")]/text()')
            if not script_tag_for_download_url:
                self.logger.warning(__name__, "no_script",
                                    fmt("Unable to find script tag\nMinistry: {}\n" +
                                        "Start date: {:%Y-%m-%d}\nEnd date: {:%Y-%m-%d}", department_name,
                                        start_date,
                                        end_date))
                continue
            m = re.search('window\Wopen\W+(.+?)\'', script_tag_for_download_url, re.I | re.DOTALL)
            if not m:
                raise Exception("Website may have changed - couldn't find download_url")

            url = m.group(1)
            document_url = urljoin(self.base_url, url)

            try:
                resp = self.http_request(document_url, "HEAD")
            except HTTPError as e:
                self.logger.warning(__name__, 'invalid_request', str(e))
                continue

            if 'pdf' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = resp.headers['Content-Type']
            else:
                extraction_type = self.extraction_type.html
                content_type = resp.headers['Content-Type']

            download_id, _, doc_ids = self.register_download_and_documents(document_url,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type)

            if len(doc_ids) > 0 and doc_ids[0]:
                document_id = doc_ids[0]
            else:
                self.logger.warning(
                    __name__,
                    "no_document",
                    "Document ID not found while registering document with url {}".format(
                        document_url))
                continue
            if not download_id:
                self.logger.warning(
                    __name__,
                    "no_download",
                    "Download ID not found while registering document with url {}".format(
                        document_url))
                continue

            india_notice.document_id = document_id
            india_notice.download_id = download_id

            if self.validate_doc(india_notice):
                self.save_doc(india_notice.for_json())
            else:
                self.logger.critical(__name__, "validation error", self.json_dumps(message=india_notice.for_json()))

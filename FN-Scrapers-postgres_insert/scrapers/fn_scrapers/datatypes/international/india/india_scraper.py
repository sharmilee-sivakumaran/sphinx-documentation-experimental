# -*- coding: utf-8 -*-
from __future__ import absolute_import

import injector
import os
import re
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, get_default_http_headers

@scraper()
@argument("--search-term", help="Search term to search and scrape bills.", type=str, required=True)
@argument("--year-range", help="Bill introduced between years.Expected value should be year range, starting year to end year. Ex: 2016-2017, 2014-2017, 2017-2017", type=str)
@tags(type="bills", country_code="IN", group="international")
class IndiaDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(IndiaDocScraper, self).__init__(EventComponent.scraper_bills, "india", "india")

        self.logger = logger

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "india.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("india").alpha_2)

        self.base_url = u'http://164.100.47.194'
        self.search_url = u'{base_url}/Loksabha/Legislation/NewAdvsearch.aspx'.format(base_url=self.base_url)


    def scrape(self, search_term=None, year_range=None):

        '''
        This modules retrieve bill urls for the given search term.
        Firstly, it will search for given search term.
        Secondly, it will parse bill url and ids from search result.

        :param search_term: Search keyword
        :type str:
        :return: Array containing details of bill page like url, title, id.
        '''
        try:
            if year_range is not None:
                year_range = self.check_year_range(year_range)

            self.search_by_search_term(search_term=search_term, year_range=year_range)
            totals = self.extract_single('//td[contains(span,"Total Bills")]/span[position()=2]/text()')
            if totals is not None and int(totals) > 0:
                self.logger.info(__name__, fmt("Total search result found: {}",totals))
                items_per_page = 5
                number_of_pages = (int(totals) + items_per_page - 1) // items_per_page
                current_page = 1
                while current_page <= number_of_pages:
                    self.scrape_bills()
                    if current_page == number_of_pages:
                        break
                    current_page += 1
                    self.search_by_search_term(search_term=search_term, page_number=current_page)
            else:
                form_found = self.extract_single('//form[@id="form1"]/@id')

                if form_found is not None:
                    raise ScrapeError(self.scraper_policy.doc,"Results not found.",self.search_url)
                else:
                    raise ScrapeError(self.scraper_policy.doc,"Total number of search result not found.",self.search_url)

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} bills could not be scraped. {}",self.scraper_name, e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc,fmt("{} bills could not be scraped. {}",self.scraper_name, e),self.search_url)

    def search_by_search_term(self, search_term, year_range=None, page_number=None):
        try:
            bill_house = 'both'
            bill_type = 'all'
            bill_status = 'all'

            # Search url
            search_url = self.search_url
            if not page_number:
                self.http_get(search_url, self.scraper_policy.doc_list)

            if not page_number:
                self.choose_house(bill_house)
                self.choose_bill_type(bill_type)

            form_id = 'form1'
            input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
            if input_fields_dict:
                # Bill status ['all', 'assented', 'passed', 'pending', 'withdrawn', 'negatived', 'lapsed']
                if bill_status == 'all':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '6'
                elif bill_status == 'assented':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '0'
                elif bill_status == 'passed':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '1'
                elif bill_status == 'pending':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '2'
                elif bill_status == 'withdrawn':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '3'
                elif bill_status == 'negatived':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '4'
                elif bill_status == 'lapsed':
                    input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '5'
                # Set search textbox with given search term
                if search_term:
                    input_fields_dict['ctl00$ContentPlaceHolder1$STitle'] = search_term
                if year_range:
                    year_from, year_to = year_range.split('-')
                    input_fields_dict['ctl00$ContentPlaceHolder1$ddlYear1'] = year_from
                    input_fields_dict['ctl00$ContentPlaceHolder1$ddlYear2'] = year_to

                if not page_number:
                    input_fields_dict['__EVENTARGUMENT'] = ''
                    input_fields_dict['__EVENTTARGET'] = ''
                    input_fields_dict['ctl00$ContentPlaceHolder1$btnsbmt'] = 'Submit'
                else:
                    # Page$454
                    input_fields_dict['__EVENTARGUMENT'] = 'Page$' + str(page_number)
                    input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$GR1'

            req_args = {'data': input_fields_dict, }
            self.http_post(search_url, self.scraper_policy.doc_list, request_args=req_args)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("India Scraper Failed. Error occured in search_by_search_term. Error: {}", e),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc,fmt("India Scraper Failed. Error occured in search_by_search_term. Error: {}", e),self.search_url)

    def choose_house(self, bill_house):
        # Search url
        search_url = self.search_url

        form_id = 'form1'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if input_fields_dict:
            if bill_house == 'both':
                input_fields_dict['ctl00$ContentPlaceHolder1$RadioBttnhouse'] = 'both'
                input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$RadioBttnhouse$0'
            elif bill_house == 'rajyasabha':
                input_fields_dict['ctl00$ContentPlaceHolder1$RadioBttnhouse'] = 'rs'
                input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$RadioBttnhouse$2'
            elif bill_house == 'loksabha':
                return None
            input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '6'
            input_fields_dict['ctl00$ContentPlaceHolder1$STitle'] = ''
            input_fields_dict['__EVENTARGUMENT'] = ''

            req_args = {'data': input_fields_dict, }
            self.http_post(search_url, self.scraper_policy.doc_list, request_args=req_args)

    def choose_bill_type(self, bill_type):
        # Search url
        search_url = self.search_url

        form_id = 'form1'
        input_fields_dict, method, action = self.html_form_parser(search_type="id", form_id=form_id)
        if input_fields_dict:
            if bill_type == 'all':
                input_fields_dict['ctl00$ContentPlaceHolder1$RadioBttnbilltyp'] = 'All'
                input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$RadioBttnbilltyp$0'
            elif bill_type == 'private_member':
                input_fields_dict['ctl00$ContentPlaceHolder1$RadioBttnbilltyp'] = 'Private Member'
                input_fields_dict['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$RadioBttnbilltyp$2'
            elif bill_type == 'government':
                return None
            input_fields_dict['ctl00$ContentPlaceHolder1$ddlMember'] = '--- Please Select ---'
            input_fields_dict['ctl00$ContentPlaceHolder1$RadioBttnmember'] = 'Current'
            input_fields_dict['ctl00$ContentPlaceHolder1$RadioButtonList1'] = '6'
            input_fields_dict['ctl00$ContentPlaceHolder1$STitle'] = ''
            input_fields_dict['__EVENTARGUMENT'] = ''
            req_args = {'data': input_fields_dict, }
            self.http_post(search_url, self.scraper_policy.doc_list, request_args=req_args)

    def check_member_list(self, member_name):
        option_text_list = self.xpath('//select[@name="ctl00$ContentPlaceHolder1$ddlMember"]/option/text()')
        if option_text_list and len(option_text_list) > 0:
            option_text_list = option_text_list.extract()
            found = False
            for option_text in option_text_list:
                option_text = re.sub(r'\W+', '', option_text)
                option_text = option_text.lower()
                member_name = re.sub(r'\W+', '', member_name)
                member_name = member_name.lower()
                if member_name == option_text:
                    found = True
                    break
                else:
                    found = False
            if found:
                return True
            else:
                return False
        else:
            return False

    def scrape_bills(self):

        # Extract bill details for given bill detail url
        bill_rows = self.xpath('//table[contains(tr/th/text(),"Year")]//tr[not(contains(.//@href,"doPostBack")) and not(contains(@class,"head-style"))]')
        if bill_rows is not None and len(bill_rows)>0:
            for row in bill_rows:

                try:
                    bill_year = None
                    bill_num = None
                    bill_short_title = None
                    bill_type = None
                    member_name = None
                    member_status = 'Ex-member'
                    date_of_intro = None
                    origin_chamber = None
                    debate_passed_in_ls = None
                    debate_passed_in_ls_list = []
                    debate_passed_in_rs = None
                    referred_to_committee_or_report_presented = None
                    assent_date = None
                    synopsis_url = None
                    bill_status = None
                    other_document_list = []
                    other_documents = []
                    bill_version_list = []
                    bill_versions = []
                    actions = []
                    member_list = []
                    referred_to_committee_dates = []
                    bill_status_text = ''

                    # --- Bill Year. ---
                    bill_year = self.extract_single('./td[position()=1]/text()', sel=row)
                    if bill_year:
                        bill_year = bill_year.strip()

                    # --- Bill No. ---
                    bill_num = self.extract_single('./td[position()=2]/text()', sel=row)
                    if bill_num is not None:
                        bill_num = bill_num.strip()

                    # --- Bill Short title & Bill versions ---
                    cell3 = row.xpath('./td[position()=3]')
                    if cell3:
                        cell3 = cell3[0]
                        bill_short_title = self.extract_single('./a[position()=1]/text()', sel=cell3)
                        if bill_short_title is not None:
                            bill_short_title = bill_short_title.strip()

                        cell3 = cell3.xpath('./a[position()>1]')
                        for cell in cell3:
                            link_title = self.extract_single('normalize-space(text())', sel=cell)

                            if link_title is not None and not link_title.isspace():
                                href = self.extract_single('@href', sel=cell)

                                if href is not None:
                                    bill_version = dict()
                                    bill_version['title'] = link_title
                                    bill_version['url'] = href
                                    m = re.search('[^\W_]', link_title, re.I)
                                    if m:
                                        bill_version['locale'] = 'en_IN'
                                    else:
                                        bill_version['locale'] = 'hi_IN'
                                    bill_version_list.append(bill_version)

                    # --- Bill Type ---
                    cell4 = row.xpath('./td[position()=4]/text()')
                    if cell4:
                        cell4 = cell4[0]
                        bill_type = cell4.extract()

                    # --- Bill Member Name ---
                    cell5 = self.extract_single('./td[position()=5]/text()',sel=row)
                    if cell5 is not None and not cell5.isspace():
                        member_list.append({'name': cell5.strip()})

                    # --- Bill Date of Introduction ---
                    cell6 = row.xpath('./td[position()=6]')
                    if cell6:
                        cell6 = cell6[0]
                        span1 = cell6.xpath('./span[position()=1]/text()')
                        if span1:
                            date_of_intro = span1[0].extract()
                            date_of_intro = parse_date_as_str(date_of_intro, languages=self.country_languages)

                        span2 = cell6.xpath('./span[position()=2]/text()')
                        if span2:
                            origin_chamber = span2[0].extract()


                    # --- Bill Debate passed in Loksabha ---
                    cell7 = row.xpath('./td[position()=7]')
                    if cell7:
                        cell7 = cell7[0]
                        # -- For extracting dates of debate passed in ls --
                        cell7_inner = cell7.xpath('./a')
                        if cell7_inner and len(cell7_inner) > 0:
                            date_text = cell7_inner.xpath('text()').extract()
                            if date_text and isinstance(date_text,list) and len(date_text) >= 1:
                                for date_str in date_text:
                                    if date_str and date_str.strip():
                                        debate_passed_in_ls = parse_date_as_str(date_str, languages=self.country_languages)
                                        if debate_passed_in_ls:
                                            debate_passed_in_ls_list.append(debate_passed_in_ls)

                        # -- For extracting dates of debate passed in ls --
                        cell7_inner = cell7.xpath('./a[@href]')
                        if len(cell7_inner) > 0:

                            text_list = cell7_inner.xpath('normalize-space(text())').extract()
                            urls = cell7_inner.xpath('@href').extract()
                            debate_proceedings_dict = zip(text_list, urls)
                            for text, url in debate_proceedings_dict:
                                doc_dict = dict()
                                doc_dict['type'] = "Debate Proceedings"
                                doc_dict['url'] = url
                                doc_dict['title'] = text.strip()
                                other_document_list.append(doc_dict)

                    # --- Bill Debate passed in Rajyasabha ---
                    cell8 = row.xpath('./td[position()=8]/text()')
                    if cell8:

                        cell8 = cell8[0].extract()
                        if cell8 is not None and not cell8.isspace():
                            debate_passed_in_rs = parse_date_as_str(cell8, languages=self.country_languages)

                    # --- Bill Referred to Committee / Report Presented ---
                    cell9 = row.xpath('./td[position()=9]/a|./td[position()=9]/span')
                    if cell9:
                        referred_to_committee_or_report_presented = []
                        for cell in cell9:
                            doc_dict = dict()
                            # cell = cell.extract()
                            cell_text = self.extract_single("text()", sel=cell)
                            url = self.extract_single("@href", sel=cell)
                            type = "Report Proceedings"
                            if cell_text and url:
                                doc_dict['title'] = cell_text
                                doc_dict['url'] = url
                                doc_dict['type'] = type
                                try:
                                    resp = self.http_request(url, "HEAD")
                                    if resp and resp.status_code == 200:
                                        other_document_list.append(doc_dict)
                                    else:
                                        self.logger.critical(__name__, fmt(
                                            "Report Proceedings for url failed : {} . ", url),
                                                             "No Response")
                                except Exception as e:
                                    self.logger.critical(__name__,fmt("Report Proceedings for url failed : {} . Exception :{} ",url,e),"No Response")

                            if cell_text:
                                date_val = parse_date_as_str(cell_text, languages=self.country_languages)

                                if date_val:
                                    referred_to_committee_or_report_presented.append(date_val)

                    # --- Bill Assented date ---
                    cell10 = row.xpath('./td[position()=10]')
                    if cell10:
                        cell10 = cell10[0]
                        cell10 = cell10.xpath('./a')
                        if cell10 and len(cell10) > 0:
                            date_text = cell10[0].xpath('text()').extract()
                            if date_text and len(date_text) >= 1:
                                date_text = date_text[0]
                                date_text = re.sub('^.*(\d\d\/\d\d\/\d\d\d\d)(.+)$', '\\1', date_text, re.DOTALL | re.U)
                                assent_date = parse_date_as_str(date_text, languages=self.country_languages)

                            if len(cell10) > 1:
                                doc_dict = dict()
                                gazette_text = cell10[1].xpath('text()')
                                if len(gazette_text) > 0:
                                    gazette_notification_doc_title = gazette_text[0].extract()
                                    if gazette_notification_doc_title is not None:
                                        doc_dict['title'] = gazette_notification_doc_title.strip()

                                gazette_url = cell10[1].xpath('@href')
                                doc_dict['type'] = "Gazette Notification"
                                if len(gazette_url)>0:
                                    gazette_notification_doc_url = gazette_url[0].extract()
                                    if gazette_notification_doc_url is not None:
                                        doc_dict['url'] = gazette_notification_doc_url.strip()
                                if len(gazette_url) > 0 and len(gazette_text)>0 :
                                    other_document_list.append(doc_dict)

                    # --- Bill synopsis ---
                    cell11 = row.xpath('./td[position()=11]')
                    if cell11:
                        cell11 = cell11[0]
                        synopsis = cell11.xpath('./a[contains(@href,".pdf")]')
                        if synopsis and len(synopsis) > 0:
                            for synopsis_item in synopsis:
                                doc_dict = dict()
                                synopsis_url = self.extract_single('@href',sel=synopsis_item)
                                synopsis_title = self.extract_single("text()",sel=synopsis_item)
                                if synopsis_url and synopsis_title:
                                    doc_dict['type'] = "Synopsis"
                                    doc_dict['title'] = synopsis_title
                                    doc_dict['url'] = synopsis_url
                                other_document_list.append(doc_dict)

                    # --- Bill Status ---
                    cell12 = row.xpath('./td[position()=12]/text()[normalize-space()]')

                    if cell12:
                        cell12 = cell12[0].extract()
                        if cell12 is not None:
                            bill_status_text = cell12.strip()

                    bill_doc = self.model_factory.create_bill_doc()
                    if bill_year:
                        bill_doc.year = int(bill_year)
                    if bill_num:
                        bill_doc.bill_number = bill_num
                    if bill_short_title:
                        bill_doc.short_title = bill_short_title
                    if bill_type:
                        bill_doc.bill_type = bill_type

                        if re.search('government', bill_type, re.I):
                            india_bill_government_detail = self.model_factory.create('IndiaGovernmentIntroducedLegislationSubSchema')
                            if referred_to_committee_or_report_presented and len(referred_to_committee_or_report_presented):
                                for date_item in referred_to_committee_or_report_presented:
                                    action = dict()
                                    action['action'] = 'Referred to Committee / Report Presented'
                                    action['action_date'] = date_item
                                    actions.append(action)

                            if len(debate_passed_in_ls_list)>0:
                                for debate_passed_in_ls in debate_passed_in_ls_list:
                                    action = dict()
                                    action['action'] = 'Debate / Passed in LS'
                                    action['action_date'] = debate_passed_in_ls
                                    actions.append(action)

                            if debate_passed_in_rs:
                                action = dict()
                                action['action'] = 'Debate / Passed in RS'
                                action['action_date'] = debate_passed_in_rs
                                actions.append(action)

                            if assent_date:
                                action = dict()
                                action['action'] = 'Assent Date'
                                action['action_date'] = assent_date
                                actions.append(action)

                            if len(actions)>0:
                                india_bill_government_detail.actions = actions

                            if other_document_list and len(other_document_list) > 0:
                                for other_document in other_document_list:
                                    other_document_url = other_document['url']
                                    if 'title' in other_document:
                                        other_document_title = other_document['title']
                                    else:
                                        continue
                                    other_document_type = other_document['type']

                                    # 200 MB
                                    # MAX_FILE_DOWNLOAD_SIZE = 200 * 1024 * 1024
                                    try:
                                        resp = self.http_request(other_document_url, "HEAD")
                                        if resp.status_code != 200:
                                            self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                                                 fmt('http request is failing with error: {} for url  ',other_document_url))
                                            continue
                                    except Exception as e:
                                        self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                                             fmt('http request is failing with error: {} for url {} ',e,other_document_url))
                                        continue
                                    if 'Content-Length' in resp.headers:
                                        if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                                            error_message = "File @ '{}' is larger than max size {} bytes.".format(
                                                other_document_url, MAX_FILE_DOWNLOAD_SIZE)
                                            self.logger.critical(__name__, "individual_bill_document_extraction_failed",
                                                                 fmt(
                                                                     'While extracting document Doc-Service is failing with error: {}',
                                                                     error_message))
                                            continue
                                    if re.search(r'\.pdf$', other_document_url, re.I):
                                        extraction_type = self.extraction_type.text_pdf

                                        document_download_url = other_document_url
                                        download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                                       self.scraper_policy.doc_service,
                                                                                                       extraction_type, True,
                                                                                                       content_type="application/pdf")

                                    elif re.search(r'\.doc$', other_document_url, re.I):

                                        extraction_type = self.extraction_type.msword_doc

                                        document_download_url = other_document_url
                                        download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                                       self.scraper_policy.doc_service,
                                                                                                       extraction_type, True,
                                                                                                       content_type="application/msword")

                                    elif re.search(r'\=\d+$', other_document_url, re.I):

                                        extraction_type = self.extraction_type.html

                                        document_download_url = other_document_url
                                        download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                                       self.scraper_policy.doc_service,
                                                                                                       extraction_type, True,
                                                                                                       content_type="text/html")

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
                                        other_document = self.model_factory.create('OtherDocumentSchema')
                                        if other_document_title is not None:
                                            other_document.title = other_document_title
                                        if other_document_type is not None:
                                            other_document.document_type = other_document_type

                                        other_document.document_id = document_id
                                        other_document.download_id = download_id
                                        other_documents.append(other_document)

                                if len(other_documents)>0:
                                    india_bill_government_detail.other_documents = other_documents

                            if self.validate_doc(india_bill_government_detail):
                                bill_doc.doc_type_items = india_bill_government_detail
                            else:
                                self.logger.debug(__name__,fmt('Skipping Government Details: {}',india_bill_government_detail.for_json()))




                        elif re.search('member', bill_type, re.I):
                            india_bill_member_detail = self.model_factory.create('IndiaMemberIntroducedLegislationSubSchema')
                            if referred_to_committee_or_report_presented and len(referred_to_committee_or_report_presented):
                                for date_item in referred_to_committee_or_report_presented:
                                    referred_to_committee_dates.append(date_item)
                                if len(referred_to_committee_dates)>0:
                                    india_bill_member_detail.referred_to_committee_dates = referred_to_committee_dates

                            for index, member in enumerate(member_list):
                                member_name = member.setdefault('name', None)
                                if member_name is not None:
                                    if self.check_member_list(member_name=member_name):
                                        member_status = 'Current'
                                    else:
                                        member_status = 'Ex-member'
                                    member_list[index]['member_status'] = member_status

                            if len(member_list)>0:
                                india_bill_member_detail.members = member_list

                            if self.validate_doc(india_bill_member_detail):
                                bill_doc.doc_type_items = india_bill_member_detail
                            else:
                                self.logger.debug(__name__,fmt('Skipping Members: {}',india_bill_member_detail.for_json()))


                    if origin_chamber:
                        if re.search('Lok\s*Sabha', origin_chamber, re.I):
                            bill_doc.chamber_of_introduction = "Lok Sabha"
                        elif re.search('Rajya\s*Sabha', origin_chamber, re.I):
                            bill_doc.chamber_of_introduction = "Rajya Sabha"

                    if date_of_intro:
                        bill_doc.introduction_date = date_of_intro

                    if bill_status_text is None or bill_status_text.isspace() or bill_status_text == u"":
                        #bill_doc.status = 'Lapsed'
                        pass
                    elif bill_status_text is not None and not bill_status_text.isspace():
                        bill_doc.status = bill_status_text.strip()

                    if bill_version_list and len(bill_version_list):
                        for index, bill_version in enumerate(bill_version_list):
                            doc_title = bill_version['title']
                            doc_url = bill_version['url']
                            doc_locale = bill_version['locale']

                            if re.search(r'\.pdf$', doc_url, re.I):
                                extraction_type = self.extraction_type.unknown

                                document_download_url = doc_url
                                download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                               self.scraper_policy.doc_service,
                                                                                               extraction_type, True,
                                                                                               content_type="application/pdf")

                            elif re.search(r'\.doc$', doc_url, re.I):

                                extraction_type = self.extraction_type.msword_doc

                                document_download_url = doc_url
                                download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                               self.scraper_policy.doc_service,
                                                                                               extraction_type, True,
                                                                                               content_type="application/msword")
                            if len(doc_ids) > 0 and doc_ids[0]:
                                document_id = doc_ids[0]
                            else:
                                raise ValueError(
                                    "Document ID not found while registering document with url {}".format(
                                        document_download_url))

                            if not download_id:
                                raise ValueError(
                                    "Download ID not found while registering document with url {}".format(
                                        document_download_url))

                            if document_id and download_id:
                                bill_version_detail = self.model_factory.create('BillVersionSchema')
                                bill_version_detail.title = doc_title
                                bill_version_detail.locale = doc_locale
                                bill_version_detail.document_id = document_id
                                bill_version_detail.download_id = download_id
                                bill_versions.append(bill_version_detail)

                        if len(bill_versions)>0:
                            bill_doc.bill_versions = bill_versions

                    self.logger.debug(__name__, fmt("Dump data: {}", bill_doc.for_json()))
                    if self.validate_doc(bill_doc):
                        self.save_doc(bill_doc)
                    else:
                        self.logger.debug(__name__, fmt("Dump invalidate data: {}", self.json_dumps(bill_doc)))
                        self.logger.critical(__name__, "individual_bill_scrape_failed",
                                             fmt("JsonSchema validation failed for bill having short title: {}",bill_short_title))
                except Exception as e:
                    self.logger.critical(__name__, "individual_bill_scrape_failed", fmt("Error occured: {}", e), exc_info=True)
        else:
            raise ValueError("No table rows found on bill page.")

    @staticmethod
    def check_search_term(search_term):
        if search_term:
            return True
        else:
            return False

    def check_year_range(self, year_range):
        if year_range is not None and isinstance(year_range, basestring):
            if bool(year_range) and bool(year_range.strip()):
                year_from, year_to = year_range.replace(' ','').split('-')
                if year_from.isdigit() and year_to.isdigit():
                    if int(year_from) <= int(year_to):
                        return "{}-{}".format(year_from,year_to)
                    else:
                        raise ValueError("Invalid year range.")
                else:
                    raise ValueError("Invalid year range value.")
            else:
                raise ValueError("Empty year range.")

        else:
            raise ValueError("Year range not given.")
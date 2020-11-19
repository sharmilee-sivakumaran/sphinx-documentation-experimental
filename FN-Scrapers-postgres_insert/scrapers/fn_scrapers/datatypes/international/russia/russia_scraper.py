# -*- coding: utf-8 -*-
import re
import os
import injector
from dateparser import parse
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from datetime import datetime
import json


# class for URL formation at different levels
class RussiaURL:

    @staticmethod
    def get_bill_url(convocation, page_number):
        url = "http://sozd.parlament.gov.ru/oz?b%5BAnnotation%5D=&b%5BNumberSpec%5D=&b%5BConvocation%5D%5B0%" \
                   "5D={session}&b%5BYear%5D=&b%5BFzNumber%5D=&b%5BNameComment%5D=&b%5BfirstCommitteeCond%5D=and&b%5B" \
                   "secondCommitteeCond%5D=and&b%5BExistsEventsDate%5D=&b%5BMaxDate%5D=&b%5BSectorOfLaw%5D=replaceAll" \
                   "&b%5BClassOfTheObjectLawmakingId%5D=34f6ae40-bdf0-408a-a56e-e48511c6b618&date_period_from_Year=" \
                   "&date_period_to_Year=&cond%5BClassOfTheObjectLawmaking%5D=any&cond%5BThematicBlockOfBills%5D=any" \
                   "&cond%5BPersonDeputy%5D=any&cond%5BFraction%5D=any&cond%5BRelevantCommittee%5D=any&cond%5B" \
                   "ResponsibleCommittee%5D=any&cond%5BHelperCommittee%5D=any&cond%5BExistsEvents%5D=any&cond%5B" \
                   "LastEvent%5D=any&cond%5BExistsDecisions%5D=any&cond%5BQuestionOfReference%5D=any&cond%5B" \
                   "SubjectOfReference%5D=any&cond%5BFormOfTheObjectLawmaking%5D=any&date_period_from_ExistsEventsDate=&" \
                   "date_period_to_ExistsEventsDate=&date_period_from_MaxDate=&date_period_to_MaxDate=" \
                   "&page_34F6AE40-BDF0-408A-A56E-E48511C6B618={page_number}#data_source_tab_b"\
            .format(session=convocation, page_number=page_number)
        return url

    @staticmethod
    def get_resolution_url(convocation, page_number):
        url = "http://sozd.parlament.gov.ru/oz/p?p%5BAnnotation%5D=&p%5BNumberSpec%5D=&p%5B" \
              "Convocation%5D%5B0%5D={session}&p%5BYear%5D=&p%5BNameComment%5D=&p%5BExistsEventsDate" \
              "%5D=&p%5BMaxDate%5D=&p%5BSectorOfLaw%5D=replaceAll&p%5BClassOfTheObjectLawmakingId" \
              "%5D=db9b35bb-71f8-4f81-a51d-4fb2b026b913&date_period_from_Year=&date_period_to_Year=&cond%5B" \
              "PersonDeputy%5D=any&cond%5BFraction%5D=any&cond%5BRelevantCommittee%5D=any&cond%5B" \
              "ExistsEvents%5D=any&cond%5BLastEvent%5D=any&cond%5BExistsDecisions%5D=any" \
              "&date_period_from_ExistsEventsDate=&date_period_to_ExistsEvents" \
              "Date=&date_period_from_MaxDate=&date_period_to_MaxDate=" \
              "&page_DB9B35BB-71F8-4F81-A51D-4FB2B026B913={page_number}#data_source_tab_p"\
            .format(session=convocation, page_number=page_number)
        return url

    @staticmethod
    def get_legislation_url(page_number):
        url = "http://sozd.parlament.gov.ru/oz/c?c%5BAnnotation%5D=&c%5BNumberSpec%5D=&c%5BYear%5D=&c%5BExists" \
              "EventsDate%5D=&c%5BMaxDate%5D=&c%5BSectorOfLaw%5D=replaceAll&c%5BClassOfTheObjectLawmakingId%5D" \
              "=4b854ec5-0cf8-4d6e-a2fe-fe99f73c4427&date_period_from_Year=&date_period_to_Year=&cond%5B" \
              "PersonDeputy%5D=any&cond%5BExistsEvents%5D=any&cond%5BLastEvent%5D=any&cond%5BExistsDecisions%5D=" \
              "any&date_period_from_ExistsEventsDate=&date_period_to_ExistsEventsDate=&date_period_from_MaxDate=&" \
              "date_period_to_MaxDate=&page_4B854EC5-0CF8-4D6E-A2FE-FE99F73C4427={page_number}#data_source_tab_c"\
            .format(page_number=page_number)
        return url


@scraper()
@tags(type="bills", country_code="RU", group="international")
@argument('--types', nargs='+', help='type should "type,convocation". Example types are (bill, resolution, or legislative).'
                          'Example convocations are (6, 7)', required=False)
# by default this scraper will scrape bill and resolution for 6 and 7 legislation and all legislative bills - DI-1337.
class RUSSIADocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(RUSSIADocScraper, self).__init__(EventComponent.scraper_bills, "russia", "russia")
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "russia.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
            comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
            match = comp.search(html)
            if not match:
                return match
            else:
                return match.group(group)

    # for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
        return date

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    # function for downloading html from page
    def download_html(self, url):
            self.http_get(url, self.scraper_policy.doc_list)
            html = self.get_content_from_response()
            return html

    # scrape actions
    def scrape_actions(self, action_list):
        action_schema_list = []
        prev_action_date = ''
        for action in action_list:
            if 'class="lnk' in action:
                top_action = self.single_pattern(action, 'class="lnk[^"]*">(.*?)</', 1)
                top_action_blocks = action.split('class="oz_event bh_etap bh_etap_not"')
                for top_action_block in top_action_blocks:
                    bottom_action_1 = self.single_pattern(top_action_block,
                                                          '<span class="name" title=".*?">\s*([^<]*)\s*<',
                                                          1)
                    if bottom_action_1:

                        bottom_action_2_list = self.find_pattern(top_action_block,
                                                                 '<div class="arr">.*?</div>\s*</div>\s*</div>')
                        for bottom_action_2_block in bottom_action_2_list:
                            action_schema_object = self.model_factory.create("RussiaActionsSchema")
                            bottom_action_2 = re.sub('\s+', ' ', bottom_action_2_block)
                            bottom_action_2 = re.sub('<[^>]*data-placement="top"[^>]*>.*?</span>', '',
                                                     bottom_action_2)

                            bottom_action_2 = re.sub('<span class="pun_number pull-right">.*?</span>', '',
                                                     bottom_action_2)
                            bottom_action_2 = re.sub('<span class="flr_mr">.*?</span>', '', bottom_action_2)
                            bottom_action_2 = re.sub('data-placement="top".*?</span>', '', bottom_action_2)
                            bottom_action_2 = re.sub('<span class="mob_not">.*?</span>', '',
                                                     bottom_action_2)
                            bottom_action_2 = re.sub('<span class="norm_not">.*?</span>', '',
                                                     bottom_action_2)
                            bottom_action_2 = re.sub('<div class="bh_etap_date_time">.*?</div>', '',
                                                     bottom_action_2)
                            bottom_action_2 = re.sub('<div class="doc_wrap">.*?</div>', '', bottom_action_2)

                            bottom_action_2 = re.sub('<[^>]*>', '', bottom_action_2)
                            bottom_action_2 = re.sub('\s+', ' ', bottom_action_2).strip()

                            # stage
                            action_schema_object.stage = top_action

                            # action_text
                            final_bottom_action = bottom_action_1 + '- ' + bottom_action_2
                            final_bottom_action = re.sub(';\s*$', '', final_bottom_action)
                            final_bottom_action = re.sub('\s+', ' ', final_bottom_action)
                            action_schema_object.action_text = final_bottom_action

                            # action_date
                            action_date = self.single_pattern(bottom_action_2_block,
                                                              '<span class="mob_not">[^<]*(\d{2}\.\d{2}\.\d{4})[^<]*</span>',
                                                              1)
                            if action_date:
                                action_date = self.get_formatted_date(action_date)
                                action_schema_object.action_date = action_date
                                prev_action_date = action_date
                            elif prev_action_date:
                                action_schema_object.action_date = prev_action_date
                            action_schema_list.append(action_schema_object)
        return action_schema_list

    # function for getting specific url
    def get_specific_url(self, type, convocation, page_number):

        if type == "bill":
            first_page_url = RussiaURL.get_bill_url(convocation, page_number)
            return first_page_url
        elif type == "resolution":
            first_page_url = RussiaURL.get_resolution_url(convocation, page_number)
            return first_page_url
        elif type == "legislative":
            first_page_url = RussiaURL.get_legislation_url(page_number)
            return first_page_url
        else:
            raise ScrapeError(self.scraper_policy.doc,
                              fmt("{} bill type should be one of 'bill', 'resolution' or 'legislative'", self.scraper_name),
                              "http://sozd.parlament.gov.ru/")

    def extract_attachment(self, attachment_link):
        try:
            resp = self.http_request(attachment_link, "HEAD")

            if 'msword' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.msword_doc
                content_type = 'application/msword'

            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in \
                    resp.headers['Content-Type']:
                extraction_type = self.extraction_type.msword_docx
                content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

            elif 'pdf' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"

            elif 'html' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.html
                content_type = "text/html"

            elif 'application/octet-stream' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = None

            else:
                raise Exception(
                    "Unhandled content type for link {}".format(
                        attachment_link))

            download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type)
            if len(doc_ids) > 0:
                document_id = doc_ids[0]
            else:
                document_id = None

            if document_id and download_id:
                return document_id, download_id
            else:
                self.logger.info(__name__, fmt(u"Doc_id, download_id not found for - {}", attachment_link))
                return None, None

        except Exception as e:
            self.logger.info(__name__, fmt(u"Exception occurred while uploading - {}", attachment_link))
            return None, None

    # function for scrape data
    def scrape(self, types):
        try:
            if types is None:
                types = ["bill,6", "bill,7", "resolution,6", "resolution,7", "legislative,all"]
            for item in types:
                type, convocation = item.split(",")

                first_page_url = self.get_specific_url(type, convocation, "1")
                first_page_html = self.download_html(first_page_url)
                total_number_of_pages = int(self.single_pattern(first_page_html,
                                                                '<span>&hellip;</span>.*?<a class="merge_pagination" href="[^>]*">(\d+)',
                                                                1))
                self.scrape_bill(first_page_url, type, convocation)
                for page in range(2, total_number_of_pages + 1):
                    next_page_url = self.get_specific_url(type, convocation, str(page))
                    self.scrape_bill(next_page_url, type, convocation)

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc, fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                              self.first_page_url)

    # function for scraping bill details
    def scrape_bill(self, bill_page_url, type, convocation):
        self.http_get(bill_page_url, self.scraper_policy.doc_list)
        blocks = self.xpath("//div[@id='obj_list']/div")
        for block in blocks:
            try:
                rubill = self.model_factory.create_bill_doc()
                # bill_type
                if type == "bill":
                    rubill.bill_type = "Bill"
                elif type == "resolution":
                    rubill.bill_type = "Draft Resolution"
                elif type == "legislative":
                    rubill.bill_type = "Legislative Initiative"

                # bill number
                bill_number = self.extract_single('.//span[@class="o_num"]/strong/text()', sel=block)
                rubill.bill_number = bill_number

                # session
                if type == "legislative":
                    convocation = self.single_pattern(bill_number, '(\d+)\-\d+', 1)
                    rubill.session = int(convocation)
                else:
                    rubill.session = int(convocation)

                # introduction_date
                introduction_date = self.extract_single('.//span[@class="o_date o_date_div"]/text()', sel=block)
                introduction_date = self.single_pattern(introduction_date, '\d+\.\d+\.\d+', 0)
                introduction_date = self.get_formatted_date(introduction_date)
                rubill.introduction_date = introduction_date

                # source_url
                source_url = "http://sozd.parlament.gov.ru/bill/"+bill_number
                rubill.source_url = source_url

                source_html = self.http_get(source_url, self.scraper_policy.doc_list)

                # title
                upper_title = self.extract_single('//span[@class="oz_naimen"]/text()')
                lower_title = self.extract_single('//p[@class="p_namecomment"]/text()')
                if lower_title:
                    lower_title = lower_title.strip()
                    title = upper_title+' '+lower_title
                    rubill.title = title
                elif upper_title:
                    rubill.title = upper_title.strip()

                # summary_status
                summary_status = self.extract_single('//span[@id="current_oz_status"]/text()')
                if not summary_status:
                    summary_status = self.extract_single('//sup[@class="ico_arhiv"]/text()')
                if summary_status:
                    summary_status = summary_status.strip()
                    rubill.summary_status = summary_status

                # sponsoring_organizations
                sponsoring_organizations = self.extract_single('//div[@id="opc_hild"]/table/tr/td[2]/div/text()')
                sponsoring_organizations = sponsoring_organizations.split(',')
                sponsoring_organizations = [x.strip() for x in sponsoring_organizations]
                first_list = sponsoring_organizations[0]
                first_list = first_list.split('\n')
                first_list = [x.strip() for x in first_list]
                sponsoring_organizations.pop(0)
                first_list.extend(sponsoring_organizations)
                rubill.sponsoring_organizations = first_list

                # topic_keywords
                topic_keywords = self.extract_single(u'//tr/td[@class="td_hild_l w25p"][contains(./div/span'
                                                     u'[@class="opch_l_txt"]/text(),"\u0422\u0435\u043c\u0430\u0442'
                                                     u'\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0431\u043b\u043e'
                                                     u'\u043a \u0437\u0430\u043a\u043e\u043d\u043e\u043f\u0440\u043e'
                                                     u'\u0435\u043a\u0442\u043e\u0432")]'
                                                     u'/following-sibling::td[1]/div/text()')
                if topic_keywords:
                    topic_keywords = re.sub('\s+', ' ', topic_keywords)
                    rubill.topic_keywords = topic_keywords.strip()

                # committees
                committees_array = []

                responsible_committee = self.extract_single(u'//tr/td[@class="td_hild_l w25p"][contains(./div/span'
                                                            u'[@class="opch_l_txt"]/text(),"\u041e\u0442\u0432\u0435'
                                                            u'\u0442'
                                                            u'\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0439 \u043a'
                                                            u'\u043e\u043c\u0438\u0442\u0435\u0442")]'
                                                            u'/following-sibling::td[1]/div/text()')
                if responsible_committee:
                    responsible_committee = re.sub('\s+', ' ', responsible_committee)
                    responsible_dict = {"committee_type": "Responsible", "committee_name": responsible_committee.strip()}
                    committees_array.append(responsible_dict)

                profile_committee = self.extract_single(u'//tr/td[@class="td_hild_l w25p"][contains(./div/span'
                                                        u'[@class="opch_l_txt"]/text(),'
                                                        u'"\u041f\u0440\u043e\u0444\u0438\u043b\u044c\u043d\u044b\u0439'
                                                        u' \u043a\u043e\u043c\u0438\u0442\u0435\u0442")]'
                                                        u'/following-sibling::td[1]/div/text()')
                if profile_committee:
                    profile_committee = re.sub('\s+', ' ', profile_committee)
                    profile_dict = {"committee_type": "Profile", "committee_name": profile_committee.strip()}
                    committees_array.append(profile_dict)

                if committees_array:
                    rubill.committees = committees_array

                # action_list and documents
                action_list = source_html.split('<div class="ttl">')
                action_schema_list = self.scrape_actions(action_list)
                if action_schema_list:
                    rubill.actions = action_schema_list

                # documents
                document_schema_list = []
                all_document_block_1 = self.xpath('//div[@class="oz_event bh_etap"]')
                all_document_block_2 = self.xpath('//div[@class="oz_event bh_etap with_datatime"]')
                final_document_block = all_document_block_1 + all_document_block_2
                for each_document_block in final_document_block:
                    published_date = self.extract_single('./@data-eventdate', sel=each_document_block)
                    published_date = re.sub('T.*', '', published_date)
                    published_date = self.get_formatted_date(published_date)
                    document_blocks = each_document_block.xpath('./div[@class="event_files event_files_drag"]/span/a')
                    hidden_document_blocks = each_document_block.xpath('./div[@class="event_files event_files_drag"]'
                                                                       '/div[@class="hidden_event_files"]/span/a')
                    all_document_blocks = document_blocks+hidden_document_blocks
                    for document_block in all_document_blocks:
                        document_schema_object = self.model_factory.create("RussiaDocumentsSchema")
                        attachment_link = self.extract_single('./@href', sel=document_block)
                        if attachment_link:
                            document_id, download_id = self.extract_attachment(attachment_link)
                            if document_id:
                                document_schema_object.document_id = document_id
                                document_schema_object.download_id = download_id
                        else:
                            self.logger.info(__name__, fmt(u"No attachment found for url - {}", source_url))
                        document_title = self.extract_single('./div/div[2]/div[@class="doc_wrap"]/text()',
                                                             sel=document_block)
                        if document_title:
                            document_title = re.sub('\s+', ' ', document_title)
                            document_schema_object.document_title = document_title.strip()
                            document_schema_object.published_date = published_date

                            document_schema_list.append(document_schema_object)

                rubill.documents = document_schema_list

                if self.validate_doc(rubill):
                    self.save_doc(rubill.for_json())
                else:
                    self.logger.critical(__name__, "individual_bill_scrape_failed",
                                         fmt("JsonSchema validation failed for bill having link: {}",
                                             source_url))
                    self.logger.critical(__name__, self.json_dumps(message=rubill.for_json()))
            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

# -*- coding: utf-8 -*-
from __future__ import absolute_import

from __future__ import division
import logging
import re
from ..common.base_scraper import ScraperBase
from .schema_handler import UKBill, UKActions, DocumentAttachment, DocumentAttachmentWithType
from fn_scraperutils.events.reporting import EventComponent
from datetime import datetime

import json

from fn_scrapers.api.scraper import scraper, argument, tags


logger = logging.getLogger(__name__)

# class for URL formation at different levels
class UKURL:
    base_url = u'http://services.parliament.uk'

    @staticmethod
    def list_url_for_year_range(startyear, endyear):
        endyear = endyear[2:]
        return u'{base_url}/bills/{startyear}-{endyear}.html'.format(base_url=UKURL.base_url, startyear=startyear, endyear=endyear)

    @staticmethod
    def bill_url(link):
        return u'{base_url}{detail_page_url}'.format(base_url=UKURL.base_url, detail_page_url=link)


@scraper()
@argument("--startyear", help="start year from where to scrape in the format yyyy")
@argument("--endyear", help="end year till where to scrape in the format yyyy")
@tags(type="bills", country_code="GB", group="international")
class UKDocScraper(ScraperBase):
    def __init__(self):
        super(UKDocScraper, self).__init__(EventComponent.scraper_bills, "united_kingdom", "united_kingdom")

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            resu = re.sub('&\S+;|\s{2,}|;', '', match.group(group))
            return resu

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%y').strftime('%Y-%m-%d')
        return date

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL)
        match = comp.findall(html, re.IGNORECASE)
        return match

    # function for downloading html from page
    def download_html(self, url):

            self.http_get(url, self.scraper_policy.doc_list)
            # Extract the total number of bills for this session
            html = self.get_content_from_response()
            return html

    # function for getting next page links, total number of bills and bill page urls
    def scrape(self, startyear, endyear):

        if UKDocScraper.check_year(startyear, endyear):

            url = UKURL.list_url_for_year_range(startyear=startyear, endyear=endyear)

            # calling function for getting the html based on startyear and endyear
            html_from_page = self.download_html(url)

            # total bills for selected year as all bills are on one page only
            total_links_on_page = UKDocScraper.find_pattern(html_from_page,'<td class="bill-item-description">\s*<a href="(.*?)">')

            if total_links_on_page == ' ':
                logger.debug(u"No bill found in the search result")
            else:
                total_number_of_bills = len(total_links_on_page)
                logger.debug(u"Total bills found in search result: %s", total_number_of_bills)

                for link in total_links_on_page:
                    bill_page_url = UKURL.bill_url(link)
                    self.scrape_bill(bill_page_url,startyear,endyear)

        else:
                logger.error(u"Invalid date format")

    # function to extract data from bill page
    def scrape_bill(self, bill_page_url, startyear, endyear):

        # this function will accept the final bill url and process it to extract the required fields
        logger.debug(u"Fetching for given bill url - {}".format(bill_page_url))

        html = self.download_html(bill_page_url)
        uk_bill = UKBill()

        # session
        session = startyear + '-' + endyear
        uk_bill.add_session(session)

        # bill short title
        bill_short_title = UKDocScraper.single_pattern(html, '<h1>(.*?)</h1>', 1)
        bill_short_title = re.sub('<.*?>|\s*\[HL\]\s*\d+\-\d+','',bill_short_title)
        bill_short_title = re.sub('\s{2,}|\s+$', '', bill_short_title)
        uk_bill.add_short_title(bill_short_title)

        # description
        description = UKDocScraper.single_pattern(html, '<h2>\s*Summary of the.*?</h2>\s*(.*?)\s*</div>', 1)
        description = re.sub('<.*?>|\s{2,}|\.$', '', description)
        if description:
            uk_bill.add_description(description)

        # type
        type_ = UKDocScraper.single_pattern(html, '<dt>\s*Type of Bill\s*:</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        type_ = re.sub('<.*?>', '', type_)
        uk_bill.add_type(type_)

        # originating_chamber
        originating_chamber = UKDocScraper.single_pattern(html,'Bill started in the\s*<span\s*class="(.*?)">', 1)
        if originating_chamber not in ['lords','commons']:
            originating_chamber = UKDocScraper.single_pattern(html, 'Bill started in the\s*<span.*?>\s*(.*?)\s*</span>', 1)
            if originating_chamber == 'House of Lords':
                originating_chamber = 'lords'
            if originating_chamber == 'House of Commons':
                originating_chamber = 'commons'
        uk_bill.add_originating_chamber(originating_chamber)

        # latest_news
        latest_news = self.single_pattern(html, '>\s*Latest news on.*?</h2>(.*?)<h2>', 1)
        latest_news = re.sub('<.*?>|\r?\n|\s{2,}|^\s+', '', latest_news)
        if latest_news != "" and latest_news != " ":
            uk_bill.add_latest_news(latest_news)

        # source_url
        uk_bill.add_source_url(bill_page_url)

        # actions
        status_link = UKDocScraper.single_pattern(html, '<a href="([^>]*stages.html)">', 1)
        status_url = UKURL.bill_url(status_link)
        status_stages_html = self.download_html(status_url)
        various_stages = UKDocScraper.find_pattern(status_stages_html,
                                                   '<tr class="tr\d+">(.*?)</tr>')
        for stage in various_stages:
            actionobj = UKActions()

            # action date
            action_date = self.single_pattern(stage, '<td>\s*(\d+\.\d+\.\d+)\s*</td>', 1)
            if action_date != ' ':
                action_date = self.get_formatted_date(action_date)
                actionobj.add_action_date(action_date)

            # action text
            action_text = self.single_pattern(stage, 'alt="(.*?)"', 1)
            actionobj.add_action_text(action_text)

            uk_bill.add_actions_by_obj(actionobj)

        # sponsor name
        sponsor_block = UKDocScraper.single_pattern(html,'<dt\s*>\s*Sponsors?\s*:(</dt>.*?</dl>)',1)
        if sponsor_block != ' ':
            sponsor_rows = UKDocScraper.find_pattern(sponsor_block,'(<dd>.*?</dd>)')
            for row in sponsor_rows:
                sponsor_name = UKDocScraper.single_pattern(row,'<dd>\s*(.*?)\s*<',1)
                sponsor_association = UKDocScraper.single_pattern(row,'<a\s*href.*?>(.*?)</a>',1)
                sponsor_association = re.sub('<.*?>|\s{2,}','',sponsor_association)
                if sponsor_association == ' ':
                    sponsor_association = None
                uk_bill.add_sponsors_by_value(sponsor_name,sponsor_association)

        # attachment link
        attachment_page_link = UKDocScraper.single_pattern(html,'<a href="([^>]*)">\s*Bill documents',1)
        if attachment_page_link != ' ':

            # attachments
            attachment_order = 1

            if attachment_page_link != ' ':
                attachment_page_link = UKURL.bill_url(attachment_page_link)

                attachment_html = self.download_html(attachment_page_link)
                attachment_blocks = UKDocScraper.find_pattern(attachment_html,'<table class="bill-items">(.*?)</table>')
                for block in attachment_blocks:
                    document_type = UKDocScraper.single_pattern(block,'<th>\s*House\s*</th>\s*<th>\s*(.*?)\s*</th>',1)
                    document_type = document_type.lower()

                    if document_type == "bill":
                        document_type = "bill_text"

                    elif document_type == "act of parliament":
                        document_type = "act_of_parliament"

                    elif document_type == "delegated powers memoranda":
                        document_type = "delegated_powers_memoranda"

                    elif document_type == "amendment paper":
                        document_type = "amendment_paper"

                    elif document_type == "briefing papers":
                        document_type = "briefing_paper"

                    elif document_type == "library notes":
                        document_type = "library_note"

                    elif "press not" in document_type:
                        document_type = "press_note"

                    elif "selection of amendment" in document_type:
                        document_type = "selection_of_amendment"

                    elif "public bill committee and" in document_type:
                        document_type = "public_bill_committee_and_report_stage_proceeding"

                    else:
                        document_type = "other"

                    bill_block_rows = UKDocScraper.find_pattern(block,'(<tr\s*class="tr\d+">.*?</tr>)')

                    for bill_row in bill_block_rows:

                        # process status
                        process_status = UKDocScraper.single_pattern(bill_row,'<td class="bill-item-description">(.*?)</a>',1)
                        process_status = re.sub('<.*?>|\s{2,}','',process_status)
                        process_status = re.sub('\s*\(PDF.*?\)|\s*\(OCTET\-STREAM.*?\)','',process_status)

                        # publication_date
                        publication_date = UKDocScraper.single_pattern(bill_row,'<td class="bill-item-date">(\d+\.\d+\.\d+)</td>',1)
                        publication_date = UKDocScraper.get_formatted_date(publication_date)

                        # attachment_link
                        attachment_link = UKDocScraper.single_pattern(bill_row,'<span class="application-pdf">\s*<a\s*href="([^>]*\.pdf)"',1)
                        extraction_type = self.extraction_type.unknown
                        content_type = "application/pdf"
                        if attachment_link == ' ':
                            attachment_link = UKDocScraper.single_pattern(bill_row,'<td class="bill-item-description">\s*<a\s*href="([^>]*\.pdf)"',1)
                            extraction_type = self.extraction_type.unknown
                            content_type = "application/pdf"
                            if attachment_link == ' ':
                                attachment_link = UKDocScraper.single_pattern(bill_row,
                                                                              '<td class="bill-item-description">\s*<a\s*href="([^>]*)"',
                                                                              1)
                                extraction_type = self.extraction_type.html
                                content_type = "text/html"
                        try:

                            download_id, _, doc_ids= self.register_download_and_documents(attachment_link,
                                                                                       self.scraper_policy.doc_service,
                                                                                       extraction_type, True,
                                                                                       content_type = content_type)
                            if len(doc_ids) > 0:
                                document_id = doc_ids[0]
                                document_attachment_with_type = DocumentAttachmentWithType()
                                document_attachment = DocumentAttachment()
                                document_attachment.add_document_id(document_id)
                                document_attachment.add_download_id(download_id)
                                document_attachment.add_order(attachment_order)
                                document_attachment.add_process_status(process_status)
                                document_attachment.add_publication_date(publication_date)
                                document_attachment_with_type.add_document_type(document_type)
                                document_attachment_with_type.add_document(document_attachment)
                                self.validate_doc(document_attachment_with_type)
                                uk_bill.add_attachment_by_obj(document_attachment_with_type)
                                attachment_order += 1

                        except Exception as e:
                            logger.error(u"Error occurred - {}".format(str(e)))

        if self.validate_doc(uk_bill):
            self.save_doc(uk_bill)
        else:
            logging.debug(json.dumps(uk_bill.to_json()))


    @staticmethod
    def check_year(startyear, endyear):
        if re.search(r'\d{4}', startyear):
            if re.search(r'\d{4}', endyear):
                return True
            else:
                return False
        else:
            return False

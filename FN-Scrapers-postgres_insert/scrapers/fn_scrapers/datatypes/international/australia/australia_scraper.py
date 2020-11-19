# -*- coding: utf-8 -*-
from __future__ import division, absolute_import
import logging
import dateutil.parser
from datetime import datetime
import re
import math
from ..common.base_scraper import ScraperBase
from .schema_handler import AustraliaBill, DocumentAttachment,  BillAction, DocumentItem, to_unicode
from fn_scraperutils.events.reporting import EventComponent
from unidecode import unidecode
import json

from fn_scrapers.api.scraper import scraper, argument, tags

from lxml import html as HTML

logger = logging.getLogger(__name__)

# class for URL fomration at different levels
class AusURL:
    base_url = u'http://www.aph.gov.au'

    @staticmethod
    def list_url_for_date_range(startdate, enddate, page=1):
        return u'{base_url}/Parliamentary_Business/Bills_Legislation/Bills_Search_Results?page={page}&drt=0&drv=0&drvH=0&pnu=0&pnuH=0&f={startdate:%d/%m/%Y}&to={enddate:%d/%m/%Y}&ps=10&ito=1&q=&bs=1&pbh=1&bhor=1&ra=1&np=1&pmb=1&g=1&st=2'.format(base_url=AusURL.base_url, page=page, startdate=startdate, enddate=enddate)

    @staticmethod
    def bill_url(link):
        return u'{base_url}{detail_page_url}'.format(base_url=AusURL.base_url, detail_page_url=link)


@scraper()
@tags(type="bills", country_code="AU", group="international")
@argument('--parliament', help='Parliament number', required=True, type=int)
class AUBillScraper(ScraperBase):
    def __init__(self):
        super(AUBillScraper, self).__init__(EventComponent.scraper_bills, "australia", "australia")
    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            resu = match.group(group).replace(';', '')
            resu = re.sub(r'&\S+;|\s{2,}', ' ', resu, flags=re.U)
            return resu

    # modified version of single_pattern function to remove the trimming criteria
    @staticmethod
    def single_pattern_link(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            return match.group(group)

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date.strip())
        try:
            date = datetime.strptime(date, '%d/%m/%y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')
        return date

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        return re.findall(pattern, html, flags=re.DOTALL | re.I)

    # function for getting next page links, total number of bills and bill page urls
    def scrape(self, parliament):
        parliament_dates = self.url_to_json("https://www.aph.gov.au/ParliamentNumbers.svc/GetDates",
                                            self.scraper_policy.doc_list)
        date_re = re.compile(r"Date\((\d+)\+")
        for par_dets in parliament_dates['d']:
            if par_dets['ParliamentNumber'] == parliament:
                fromdate = par_dets['DateFrom']
                startdate = int(date_re.search(fromdate).group(1))
                # The timestamps are in miliseconds rather than seconds
                startdate = datetime.fromtimestamp(int(startdate/1000))
                startdate = startdate.strftime("%Y-%m-%d")
                todate = par_dets['DateTo']
                if not todate:
                    enddate = datetime.today().strftime('%Y-%m-%d')
                else:
                    todate = int(date_re.search(todate).group(1))
                    enddate = datetime.fromtimestamp(int(todate/1000))
                    enddate = enddate.strftime("%Y-%m-%d")
                break
        else:
            logger.critical("Invalid Parliament number provided %d", parliament)
            return

        logger.info("Parliament %d, Startdate: %s, Enddate: %s", parliament, startdate, enddate)

        startdate = datetime.strptime(startdate, '%Y-%m-%d')
        enddate = datetime.strptime(enddate, '%Y-%m-%d')
        url = AusURL.list_url_for_date_range(startdate=startdate, enddate=enddate)

        # calling function for getting the html based on startdate and enddate
        html_from_page = self.download_html(url)
        html_page = HTML.fromstring(html_from_page)

        # capturing the total number of bills
        total_number_of_bills = AUBillScraper.single_pattern(html_from_page, 'TOTAL RESULTS\s*:\s*(\d+)\s*<', 1)

        if total_number_of_bills == ' ':
            logger.info(u"No bill found in the search result")
        else:
            total_number_of_bills = int(total_number_of_bills)
            logger.info(u"Total bills found in search result: %s", total_number_of_bills)

            items_per_page_ele = html_page.cssselect('select#pageSize1')

            if not items_per_page_ele:
                logger.critical("Unable to find 'select' with ID 'pageSize1' element on page %s", url)
                return

            items_per_page = int(items_per_page_ele[0].xpath('./option[@selected]')[0].get('value'))

            # Uses division from __future__
            total_number_of_pages = int(math.ceil(total_number_of_bills / items_per_page))

            # Handle pagination
            for i in range(1, total_number_of_pages + 1):
                page_url = re.sub('page=1','page='+str(i),url)
                html_from_next_page = self.download_html(page_url)

                total_links_on_next_page = AUBillScraper.find_links(html_from_next_page)

                for link in total_links_on_next_page:
                    bill_page_url = AusURL.bill_url(link)
                    self.scrape_bill(bill_page_url)

    def download_html(self, url):

            self.http_get(url, self.scraper_policy.doc_list)
            # Extract the total number of bills for this session
            html = self.get_content_from_response()
            return html

    @staticmethod
    def find_links(html):
            total_links_on_page = AUBillScraper.find_pattern(html,'<a id="main_0_content_0_lvResults_hlTitle_\d+" href="(.*?)"')
            if total_links_on_page:
                return total_links_on_page
            else:
                logger.error(u"No links found")
                return []

    # function to extract data from bill page
    def scrape_bill(self, bill_page_url):
        # this function will accept the final bill url and process it to extract the required fields
        logger.info(u"Fetching for given bill url - {}".format(bill_page_url))

        html = self.download_html(bill_page_url)
        australia_bill = AustraliaBill()

        # bill title
        bill_title = AUBillScraper.single_pattern(html, '<span class="current">\s*(.*?)\s*</span>', 1)
        bill_title = re.sub('<.*?>','',bill_title)
        australia_bill.add_title(bill_title)

        # unique bill id
        unique_bill_id = AUBillScraper.single_pattern(bill_page_url, 'bId=([A-Za-z0-9]+)', 1)
        australia_bill.add_unique_bill_id(unique_bill_id)

        # status
        status = AUBillScraper.single_pattern(html, '<dt>\s*Status\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        australia_bill.add_status(status)

        # parliament_number
        parliament_number = AUBillScraper.single_pattern(html, '<dt>\s*Parliament no\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        australia_bill.add_parliament_number(parliament_number)

        # originating_chamber
        originating_chamber = AUBillScraper.single_pattern(html, '<dt>\s*Originating house\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        if originating_chamber and originating_chamber not in {'hor', 'senate'}:
            if re.search(r"house\s+of\s+rep", originating_chamber, re.I | re.U):
                originating_chamber = "hor"
            elif originating_chamber.strip().lower() == "senate":
                originating_chamber = "senate"
            else:
                logger.critical("Unknown bill chamber {!r}".format(originating_chamber))
                return
        australia_bill.add_originating_chamber(originating_chamber)

        # type
        _type = AUBillScraper.single_pattern(html, '<dt>\s*Type\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        _type = re.sub('<.*?>','',_type)
        australia_bill.add_type(_type)

        # introduction_date
        introduction_date = AUBillScraper.single_pattern(html, '<span\s*>\s*Introduced and read a first time\s*</span>\s*</td>\s*<td.*?>\s*(.*?)</', 1)
        if introduction_date != ' ':
            introduction_date = AUBillScraper.get_formatted_date(introduction_date)
            australia_bill.add_introduction_date(introduction_date)

        # sponsor
        sponsor = AUBillScraper.single_pattern(html, '<dt>\s*Sponsor\(s\)\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        if sponsor != ' ':
            sponsor = re.sub('<.*?>','',sponsor)
            australia_bill.add_sponsor(sponsor.title())  # Case normalizing the sponsor name

        # portfolio
        portfolio = AUBillScraper.single_pattern(html, '<dt>\s*Portfolio\s*</dt>\s*<dd>\s*(.*?)\s*</dd>', 1)
        if portfolio != ' ':
            australia_bill.add_portfolio(portfolio)

        # source_url
        source_url = AUBillScraper.single_pattern_link(html, '<a id="main_0_billSummary_permalink"\s*href="(.*?)"', 1)
        if source_url == ' ':
            source_url = bill_page_url
        australia_bill.add_source_url(source_url)

        # actions
        order = 1
        actions_top_frame = AUBillScraper.single_pattern(html,'<div id="main_0_mainDiv".*?>.*?</div>',0)
        top_actions = AUBillScraper.find_pattern(actions_top_frame,'<table class="fullwidth">(.*?)</table>')
        for t_a in top_actions:
            chamber = AUBillScraper.single_pattern(t_a,'<th colspan="2">(.*?)</th>',1)
            actions_list = AUBillScraper.find_pattern(t_a,'(<tr>\s*<td>.*?</tr>)')
            for act in actions_list:
                action = BillAction()
                action_name = AUBillScraper.single_pattern(act,'<span\s*>(.*?)</span>',1)
                action_date = AUBillScraper.single_pattern(act,'<td style="width: 120px;?" >\s*(\d+\s*[A-Za-z]+\s*\d+)',1)
                action_date = AUBillScraper.get_formatted_date(action_date)
                action.add_action(action_name)
                action.add_date(action_date)
                if chamber:
                    if re.search(r"house\s+of\s+rep", chamber, re.I | re.U):
                        chamber = "hor"
                    elif chamber.strip().lower() == "senate":
                        chamber = "senate"
                    if chamber not in {'hor', 'senate'}:
                        logger.warning("Unknown chamber {!r}".format(chamber))
                    else:
                        action.add_chamber(chamber)
                action.add_order(order)
                order += 1
                self.validate_doc(action)
                australia_bill.add_action_by_obj(action)

        # last_action_date
        if australia_bill.get('actions'):
            last_action_date = australia_bill.get('actions')[-1].get('action_date')
            australia_bill.add_last_action_date(last_action_date)

        # attachments
        attachment_order = 1
        attachment_table = AUBillScraper.single_pattern_link(html,'<table class="docs bill-docs">.*?<tr id="main_0_textOfBill.*?</table>',0)
        attachment_rows = AUBillScraper.find_pattern(attachment_table,'(<tr.*?>.*?</tr>)')
        for row in attachment_rows:
            attachment_link = AUBillScraper.single_pattern_link(row,'<a href="([^"]*)"[^>]*>\s*<[^>]*alt="PDF Format"',1)
            extraction_type = self.extraction_type.unknown_new
            content_type = "application/pdf"
            if attachment_link == ' ':
                attachment_link = AUBillScraper.single_pattern_link(row,'<a href="([^"]*)"[^>]*>\s*<[^>]*alt="Word Format"',1)

                if "docx" in attachment_link:
                    extraction_type = self.extraction_type.msword_docx
                    content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                elif re.search(r"\.doc[^a-z]", attachment_link):
                    extraction_type = self.extraction_type.msword_doc
                    content_type = "application/msword"
                else:
                    logger.warning("Unable to determine document type from URL: '%s'", attachment_link)
                    continue
                if attachment_link == ' ':
                    attachment_link = AUBillScraper.single_pattern_link(row,'<a href="([^"]*)"[^>]*>\s*<[^>]*alt="HTML Format"',1)
                    extraction_type = self.extraction_type.html
                    content_type = "text/html"

            download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True, content_type = content_type)
            if len(doc_ids) > 0:
                document_id = doc_ids[0]
                document_attachment = DocumentAttachment()
                document_attachment.add_document_id(document_id)
                document_attachment.add_download_id(download_id)
                document_attachment.add_order(attachment_order)
                attachment_order += 1
                status = AUBillScraper.single_pattern(row,'<ul class="links">(.*?)</ul>',1)
                status = re.sub('<.*?>|\s{2,}','',status)
                document_attachment.add_process_status(status)
                self.validate_doc(document_attachment)
                australia_bill.add_attachment_by_obj(document_attachment)

        if self.validate_doc(australia_bill):
            self.save_doc(australia_bill)
        else:
            logger.critical("Bill Validation failed. External ID: %s\nDump: %s", unique_bill_id,
                            json.dumps(australia_bill.to_json()))

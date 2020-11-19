# -*- coding: utf-8 -*-
from __future__ import division
import logging
from datetime import datetime
import re
import math
from fn_scraperutils.events.reporting import EventComponent
import json

from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import GERMANYBill, DocumentAttachment

logger = logging.getLogger(__name__)

# class for URL formation at different levels
class GermanyURL:
    base_url = u"http://pdok.bundestag.de/pushData.php"

    @staticmethod
    def get_data_for_first_page(startdate, enddate, parliament):
        data = {'sdata': 'maxln:10;hl:off;sf:akt;sort:vutc;sorder:desc;wp:{parliament};fbwp:del;df:{startdate};dt:{enddate}'.
            format(parliament=parliament, startdate=startdate, enddate=enddate)}
        return data

@scraper()
@argument('--startdate', help='start date from where to scrape in the format dd.mm.yyyy', required=True)
@argument('--enddate', help='end date till where to scrape in the format dd.mm.yyyy', required=True)
@argument('--parliament', help='parliament for which the bills to scrape', required=True)
@tags(type="bills", country_code="DE", group="international")
class GERMANYDocScraper(ScraperBase):
    def __init__(self):
        super(GERMANYDocScraper, self).__init__(EventComponent.scraper_bills, "germany", "germany")

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

    @staticmethod
    def single_pattern_link(html, pattern, group):
            comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
            match = comp.search(html)
            if not match:
                match = ' '
                return match
            else:
                return match.group(group)

    @staticmethod
    def get_author_names(bill_block):
        pattern = "Autoren\s*:\s*(.*?)</div>"
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(bill_block)
        if not match:
            match= ' '
            return match
        else:
            authors = re.sub('\s*\.*', '', match.group(1))
            authors = re.sub(',', ' ', authors)
            authors = authors.split(';')
            return authors

    @staticmethod
    def roundup(x):
        return int(math.ceil(x / 10.0)) * 10

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

    # function for getting headers
    def get_headers(self,url,data):
        self.http_post(url,self.scraper_policy.doc_list, request_args={'data' : data})
        headers = self.get_headers_from_response()
        return headers

    # function for downloading html with cookies
    def download_html_with_cookies(self,url,cookie):
        self.http_post(url, self.scraper_policy.doc_list, request_args={'cookies': cookie})
        html = self.get_content_from_response()
        return html

    @staticmethod
    def get_cookie(headers):
        session = headers.get('Set-Cookie')
        session = re.sub('PHPSESSID=|; path=/','',session)
        cookie = {'PHPSESSID': session}
        return cookie

    # function for scrape data
    def scrape(self, startdate, enddate, parliament):

        if GERMANYDocScraper.check_date(startdate, enddate):

            # url = "http://pdok.bundestag.de/pushData.php"
            data = GermanyURL.get_data_for_first_page(startdate, enddate, parliament)
            headers = self.get_headers(GermanyURL.base_url,data)
            cookie = self.get_cookie(headers)
            html_from_page = self.download_html_with_cookies(
                "http://pdok.bundestag.de/treffer.php?h=0&q=" + parliament + "/*", cookie)
            self.scrape_bill(html_from_page)
            total_number_of_bills = self.single_pattern(html_from_page,'Treffer\s*:\s*<[^>]*>(\d+)<[^>]*>',1)

            if total_number_of_bills == "" or total_number_of_bills == " ":
                logger.warn(u"No Bills Found For Given Parliament")
                return
            else:
                total_number_of_bills = int(total_number_of_bills)

            if total_number_of_bills > 10:
                total_number_of_pages = self.roundup(total_number_of_bills)
                for page in range(10,total_number_of_pages,10):
                    html_from_next_page = self.download_html_with_cookies("http://pdok.bundestag.de/treffer.php?h="+str(page)+"&q="+parliament+"/*",cookie)
                    self.scrape_bill(html_from_next_page)

    def scrape_bill(self,html):

        bill_blocks = self.find_pattern(html,'(<div class="linkGeneric">.*?</div></div><div)')
        for bill_block in bill_blocks:
            germany_bill = GERMANYBill()

            # title
            title = self.single_pattern(bill_block,'<div class="linkGeneric">\s*<a href.*?>\s*(.*?)\s*</a>',1)
            title = re.sub('(\s*\-)$','',title)
            germany_bill.add_title(title)

            # printed_copy_number
            printed_copy_number = self.single_pattern(bill_block,'(Drucksache Nr|Plenarprotokoll Nr)\.:\s*<strong>\s*(.*?)\s*</strong>',2)
            germany_bill.add_printed_copy_number(printed_copy_number)

            # copy_date
            copy_date = self.single_pattern(bill_block, '</strong>\s*vom\s*<strong>(\d+\.\d+\.\d+)</strong>', 1)
            if copy_date != ' ':
                copy_date = self.get_formatted_date(copy_date)
                germany_bill.add_copy_date(copy_date)

            # published_date
            published_date = self.single_pattern(bill_block, 'ver&ouml;ffentlicht\s*am\s*<strong>\s*(\d+.\d+.\d+)\s*</strong>', 1)
            published_date = self.get_formatted_date(published_date)
            germany_bill.add_published_date(published_date)

            # legislation_type
            legislation_type = self.single_pattern(bill_block,'Typ\s*:\s*<strong>\s*(.*?)\s*</strong>', 1)
            if legislation_type != ' ':
                germany_bill.add_legislation_type(legislation_type)

            # interested_party
            interested_party = self.single_pattern(bill_block, 'Urheber\s*:\s*<strong>\s*(.*?)\s*</strong>', 1)
            if interested_party != ' ':
                germany_bill.add_interested_party(interested_party)

            # summary
            summary = self.single_pattern(bill_block, '<span id="fullbtd_edok.*?>(.*?)</span>', 1)
            if summary != ' ':
                summary = re.sub('<.*?>|\s{2,}|\r?\n|\s*\.\.\.|,$','',summary)
                germany_bill.add_summary(summary)

            # authors
            authors = self.get_author_names(bill_block)
            if authors != ' ':
                correct_names = []
                for author in authors:
                    author = author.split()
                    rev_author = author[::-1]
                    correct_names.append(' '.join(rev_author))
                germany_bill.add_authors_by_obj(correct_names)

            # source_url
            source_url = self.single_pattern_link(bill_block, '<a href="(http://dipbt.bundestag.de/dip21.*?)"', 1)
            if source_url != "" and source_url != " ":
                germany_bill.add_source_url(source_url)

            # attachments
            attachment_link = self.single_pattern(bill_block,'<div class="linkGeneric">\s*<a href="(http://dipbt.bundestag.de[^"]*\.pdf)"',1)
            if attachment_link != ' ':
                document_title = self.single_pattern(bill_block,'<div class="linkGeneric">\s*<a.*?>(.*?)</a>',1)
                document_title = re.sub('(\s*\-)$', '', document_title)
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"
                download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)
                if len(doc_ids) > 0:
                    document_id = doc_ids[0]
                    document_attachment = DocumentAttachment()
                    document_attachment.add_document_title(document_title)
                    document_attachment.add_document_id(document_id)
                    document_attachment.add_download_id(download_id)
                    self.validate_doc(document_attachment)
                    germany_bill.add_attachment_by_obj(document_attachment)

            if self.validate_doc(germany_bill):
                self.save_doc(germany_bill)
            else:
                logging.debug(json.dumps(germany_bill.to_json()))

    @staticmethod
    def check_date(startdate, enddate):
        if re.search(r'\d{2}\.\d{2}\.\d{4}', startdate):
            if re.search(r'\d{2}\.\d{2}\.\d{4}', enddate):
                return True
            else:
                return False
        else:
            return False

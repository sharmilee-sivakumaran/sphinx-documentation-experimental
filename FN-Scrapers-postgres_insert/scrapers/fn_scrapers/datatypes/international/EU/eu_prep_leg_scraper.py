# -*- coding: utf-8 -*-
from __future__ import division
import logging
import injector
import re

from ..common.base_scraper import ScraperBase
from .prep_leg_schema_handler import EU_Prep_Leg

from fn_scraperutils.events.reporting import EventComponent
from fn_scrapers.api.scraper import scraper, argument, tags, argument_function
from fn_scrapers.api.resources import ScraperArguments
import json
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

from fn_service.server import BlockingEventLogger, fmt

logger = logging.getLogger(__name__)

# class for URL formation at different levels
class EuropeprepURL:

    base_url = u"https://eur-lex.europa.eu"

    @staticmethod
    def list_page_url_recent(event_date, page):
        list_page_url = u'{base_url}/search.html?dom=PRE_ACTS,LEGISLATION&qid=1516600033935&CASE_LAW_SUMMARY=false&type' \
                        u'=advanced&lang=en&SUBDOM_INIT=ALL_ALL&date0=ALL:{event_date}&page={page}' \
            .format(base_url=EuropeprepURL.base_url, event_date=event_date, page=page)
        return list_page_url

    @staticmethod
    def list_page_url(type, year, type_capital, qid):

        list_page_url = u'{base_url}/search.html?qid={qid}&DB_TYPE_OF_ACT={type}&DTS_DOM=EU_LAW&' \
                        u'typeOfActStatus={type_capital}&type=advanced&lang=en&' \
                        u'SUBDOM_INIT=PRE_ACTS&DTS_SUBDOM=PRE_ACTS&DD_YEAR={year}'.format(base_url=EuropeprepURL.base_url,
                                                                                          type=type, type_capital=type_capital, year=year, qid=qid)
        return list_page_url

    @staticmethod
    def list_url_for_legislation(year):
        return u'{base_url}/search.html?qid=1497565332080&DTS_DOM=EU_LAW&type=advanced&lang=en&SUBDOM_INIT' \
               u'=LEGISLATION&DTS_SUBDOM=LEGISLATION&DD_YEAR={year}'.format(base_url=EuropeprepURL.base_url, year=year)

    @staticmethod
    def get_next_page(list_page_url, page_number):
        next_page_url = u'{list_page_url}&page={page_number}'.format(list_page_url=list_page_url, page_number=page_number)
        return next_page_url

    @staticmethod
    def add_base_url(url):
        final_url = u'{base_url}{url}'.format(base_url=EuropeprepURL.base_url, url=url)
        return final_url


def _args(parser):
    subparsers = parser.add_subparsers(help="Scraper modes")

    by_year_parser = subparsers.add_parser("by-year", help="Scrape items in a year range")
    by_year_parser.add_argument(
        '--subtype',
        help='subtype like - "COM", "JOIN", "SEC", "LEG"',
        dest="subtypes",
        action="append",
        required=True)
    by_year_parser.add_argument(
        '--year',
        help='Year in the format YYYY - 2018',
        dest="years",
        action="append",
        required=True)
    by_year_parser.set_defaults(scraper_mode="by-year")

    recently_parser = subparsers.add_parser("recently", help="Scrape items updated recently")
    recently_parser.add_argument("--days", help="Scrape items updated in the last number of days", required=True)
    recently_parser.set_defaults(scraper_mode="recently")


@scraper()
@argument_function(_args)
@tags(type="bills", group="international")
# EU Docscraper class
class EU_Prep_legDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger, args=ScraperArguments)
    def __init__(self, logger, args):

        super(EU_Prep_legDocScraper, self).__init__(EventComponent.scraper_bills, "eu_legislation_stage", "europe")
        self.logger = logger
        self.args = args

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return None
        else:
            return match.group(group)

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

    @staticmethod
    def trim_content(content):
        content = re.sub('<.*?>', '', content)
        content = re.sub('<.*?>|\s{2,}', ' ', content)
        content = re.sub('^\s+|\s+$|\r?\n', '', content)
        return content

    # function to get total member_links present on page
    def get_member_blocks(self, url):
        html = self.download_html(url)
        member_blocks = self.find_pattern(html, '(<li class="listing__item">.*?</li>)')
        return member_blocks

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)|^\s+|\s+$', '', date)
        try:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
        return date

    # function to scrape data
    def scrape(self):
        if self.args.scraper_mode == "by-year":
            self.scrape_by_year(self.args.subtypes, self.args.years)
        elif self.args.scraper_mode == "recently":
            self.scrape_recently(self.args.days)
        else:
            raise ValueError(self.args.scraper_mode)

    def scrape_recently(self, days):
        try:
            d = date.today()
            d1 = d - timedelta(int(days))
            enddate = d.strftime('%d%m%Y')
            startdate = d1.strftime('%d%m%Y')
            self.logger.info(__name__,
                             fmt(u"Considering start date as - {} and end date as - {}", startdate, enddate))
            event_date = startdate + '%7C' + enddate
            list_page_url = EuropeprepURL.list_page_url_recent(event_date, "1")

            # calling function for getting the html based on year provided
            html_from_page = self.download_html(list_page_url)

            bills_total = self.single_pattern(html_from_page, '(<p class="resultNumber">.*?</p>)', 1)
            bills_total = re.sub('&nbsp;', '', bills_total)
            bills_total = int(self.single_pattern(bills_total, '</span>\s*(\d+)</p>', 1))
            logger.debug(u"Total bills found in search result: %s", bills_total)
            total_number_of_pages = int(bills_total / 10 + 2)
            for page in range(1, total_number_of_pages):
                next_page_url = EuropeprepURL.list_page_url_recent(event_date, page)
                next_page_html = self.download_html(next_page_url)
                self.scrape_bill(next_page_html)

        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)

    def scrape_by_year(self, subtypes, years):
        for subtype in subtypes:
            for year in years:
                self.scrape_subtype(subtype, year)

    def scrape_subtype(self, subtype, year):
        type_capital = ' '
        if subtype == "COM":
            subtype = "com"
            type_capital = "COM"
            qid = "1490117937643"
        elif subtype == "JOIN":
            subtype = "join"
            type_capital = "JOIN"
            qid = "1490117981989"
        elif subtype == "SEC":
            subtype = "secSwd"
            type_capital = "SEC_SWD"
            qid = "1490118003051"

        if subtype != "LEG":
            list_page_url = EuropeprepURL.list_page_url(subtype, year, type_capital, qid)
        elif subtype == "LEG":
            list_page_url = EuropeprepURL.list_url_for_legislation(year)

        # calling function for getting the html based on year provided
        html_from_page = self.download_html(list_page_url)

        bills_total = self.single_pattern(html_from_page, '(<p class="resultNumber">.*?</p>)', 1)
        bills_total = re.sub('&nbsp;', '', bills_total)
        bills_total = int(self.single_pattern(bills_total, '</span>\s*(\d+)</p>', 1))
        logger.debug(u"Total bills found in search result: %s", bills_total)
        total_number_of_pages = int(bills_total / 10 + 2)
        for page in range(1, total_number_of_pages):
            next_page_url = EuropeprepURL.get_next_page(list_page_url, page)
            next_page_html = self.download_html(next_page_url)
            self.scrape_bill(next_page_html)

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

            download_id, _, doc_ids = self.register_download_and_documents(attachment_link,
                                                                           self.scraper_policy.doc_service,
                                                                           extraction_type, True,
                                                                           content_type=content_type)
            if len(doc_ids) > 0:
                document_id = doc_ids[0]
            else:
                document_id = None

            if document_id and download_id:
                return document_id, download_id, True
            else:
                self.logger.info(__name__, fmt(u"Doc_id, download_id not found for - {}", attachment_link))
                return None, None, False

        except Exception as e:
            self.logger.info(__name__, fmt(u"Exception occurred while uploading - {}", attachment_link))
            return None, None, False

    # function to scrape bill information
    def scrape_bill(self, html):
        bill_blocks = self.find_pattern(html, '(<tr>\s*<td\s*rowspan=.*?</ul></td></tr>)')

        for bill_block in bill_blocks:
            try:
                eu_prep_leg_bill = EU_Prep_Leg()

                # celex
                celex = self.single_pattern(bill_block, 'CELEX\s*number\s*:\s*(.*?)<', 1)
                if celex:
                    eu_prep_leg_bill.add_celex(celex)

                    # source_type
                    if celex.startswith("5"):
                        source_type = "PrepAct"
                    elif celex.startswith("3"):
                        source_type = "Legislation"
                    else:
                        source_type = None
                    if source_type:

                        eu_prep_leg_bill.add_source_type(source_type)

                        # title
                        title = self.single_pattern(bill_block, '<h3>(.*?)</h3>', 1)
                        title = re.sub('<.*?>|\s{2,3}', '', title)
                        eu_prep_leg_bill.add_title(title.title())

                        # document_reference_id
                        document_reference_id = self.single_pattern(bill_block, '</h3>\s*<br/>(.*?)</td>', 1)
                        if document_reference_id:
                            eu_prep_leg_bill.add_document_reference_id(document_reference_id)

                        # authors
                        authors = self.single_pattern(bill_block, 'Author\s*:\s*(.*?)<', 1)
                        authors = re.sub(',\s+', ',', authors)
                        if ',' in authors:
                            authors_list = authors.split(',')
                        else:
                            authors_list = [authors]
                        eu_prep_leg_bill.add_authors(authors_list)

                        # file_type
                        file_type = self.single_pattern(bill_block, 'Form\s*:\s*(.*?)<', 1)
                        eu_prep_leg_bill.add_file_type(file_type)

                        # publication_date
                        publication_date = self.single_pattern(bill_block, 'Date of document\s*:\s*(\d{2}\/\d{2}\/\d{4})', 1)
                        publication_date = self.get_formatted_date(publication_date)
                        eu_prep_leg_bill.add_publication_date(publication_date)

                        # source_url
                        source_url = self.single_pattern(bill_block, 'class="title" name="(https?://.*?)"', 1)
                        eu_prep_leg_bill.add_source_url(source_url)

                        # eurovoc_descriptors
                        euro_source_url = re.sub('AUTO', 'ALL', source_url)
                        source_url_html = self.download_html(euro_source_url)
                        eurovoc_descriptors = self.single_pattern(source_url_html, '<li>\s*EUROVOC descriptor\s*:\s* <br/>(.*?)\s*</li>', 1)
                        if eurovoc_descriptors:
                            eurovoc_descriptors = re.sub('<br/>', ',', eurovoc_descriptors)
                            eurovoc_descriptors = re.sub('<.*?>|\r?\n|\s{2,}', '', eurovoc_descriptors)
                            if ',' in eurovoc_descriptors:
                                eurovoc_descriptors_list = eurovoc_descriptors.split(',')
                            else:
                                eurovoc_descriptors_list = [eurovoc_descriptors]
                            eu_prep_leg_bill.add_eurovoc_descriptors(eurovoc_descriptors_list)

                        # status
                        if source_type == "Legislation":
                            if "green-on.png" in bill_block:
                                eu_prep_leg_bill.add_status("In Force")

                        # documents
                        attachment_link = self.single_pattern(bill_block,
                                                              'href=".(/legal-content/EN/TXT/HTML.*?)"', 1)
                        if not attachment_link:
                            attachment_link = self.single_pattern(bill_block,
                                                                  'href=".(/legal-content/EN/TXT/PDF.*?)"', 1)
                        if not attachment_link:
                            attachment_link = self.single_pattern(bill_block,
                                                                  'href=".(./legal-content/EN/TXT/DOC.*?)"', 1)
                        if not attachment_link:
                            self.http_get(source_url, self.scraper_policy.doc_list)
                            attachment_link = self.extract_single(
                                    '//table[@class="languageBar"]/tbody/tr/td[8]/a/@href')
                            if attachment_link:
                                # removing unwanted characters from
                                # "href="./../../../legal-content/EN/TXT/DOC/?uri=CELEX:32018M8725&from=EN""
                                # ref - https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32018M8725
                                attachment_link = re.sub("(\.?\.\/)+", "/", attachment_link)
                        if attachment_link:
                            attachment_link = EuropeprepURL.add_base_url(attachment_link)
                            attachment_link = re.sub('&amp;rid', '&rid', attachment_link)
                            document_id, download_id, status = self.extract_attachment(attachment_link)
                            if status:
                                eu_prep_leg_bill.add_document_id(document_id)
                                eu_prep_leg_bill.add_download_id(download_id)
                        else:
                            self.logger.info(__name__, fmt(u"No attachment found for url - {}", source_url))

                        # procedure
                        procedure_url = re.sub('AUTO', 'HIS', source_url)
                        procedure_page_html = self.download_html(procedure_url)
                        if 'class="usermsgWarning"' in procedure_page_html:
                            logging.debug(u'No Linked procedure found')
                        else:

                            # procedure_title
                            procedure_title = self.single_pattern(procedure_page_html,
                                                                  '"description":"([^"]*)"', 1)
                            if procedure_title is None or "no title" in procedure_title:
                                self.logger.info(__name__, fmt(u"No title found for procedure url - {}", procedure_url))
                                pass
                            else:
                                procedure_title = re.sub('\s{2,}', ' ', procedure_title)
                                procedure_title = re.sub('<br>', ': ', procedure_title)
                                eu_prep_leg_bill.add_procedure_title(procedure_title)

                        if self.validate_doc(eu_prep_leg_bill):
                            self.save_doc(eu_prep_leg_bill)
                        else:
                            self.logger.critical(__name__, "individual_bill_scrape_failed",
                                                 fmt("JsonSchema validation failed for bill page: {}", source_url))

                    else:
                        self.logger.info(__name__, fmt(u"Skipping bill with celex number - {}", celex))
                        pass
            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)
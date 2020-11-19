# -*- coding: utf-8 -*-
import re
import os
import injector
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import urljoin
import datetime
import json


class FranceURL:
    base_url = "https://www.legifrance.gouv.fr"

    @staticmethod
    def get_main_url(scrap_date):
        main_url = "{base_url}/eli/jo/{scrap_date:%Y/%-m/%-d}".format(base_url=FranceURL.base_url, scrap_date=scrap_date)
        return main_url


@scraper()
@tags(type="notices", country_code="FR", group="international")
@argument('--startdate', help='startdate in the format YYYY-mm-dd i.e.2017-02-07')
@argument('--enddate', help='enddate in the format YYYY-mm-dd i.e.2018-02-07')
# france regulation notice scraper
class FRANCEregulationnoticescraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(FRANCEregulationnoticescraper, self).__init__(EventComponent.scraper_bills, "france", "france")
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "france_regulation_notice.json")
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
        date = date.strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self, startdate, enddate):
        try:
            daydelta = datetime.timedelta(days=1)
            if not startdate:
                startdate = datetime.date.today()-datetime.timedelta(days=30)
            else:
                startdate = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
            if not enddate:
                enddate = datetime.date.today()
            else:
                enddate = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()
            while startdate <= enddate:
                scrap_main_url = FranceURL.get_main_url(startdate)
                self.scrape_reg(scrap_main_url, startdate)
                startdate += daydelta

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} notices could not be scraped. {}", self.scraper_name.title(), e),
                                 exc_info=True)
            raise

    # function for srape reg
    def scrape_reg(self, bill_page_url, scrap_date):
        self.http_get(bill_page_url, self.scraper_policy.doc_list)
        # checking if notices are present for specific date or not
        if u"aucun JO trouvé pour cette date" in self.resp.text:
            self.logger.info(
                __name__,
                u"No notices Found for {:%Y/%-m/%-d}".format(scrap_date))
            return

        # notice id
        notice_id_main = self.extract_single('//title')
        notice_id1 = self.single_pattern(notice_id_main, 'JORF n.(\d+) ', 1)
        notice_id1 = "JORF n "+notice_id1
        publication_issue = self.single_pattern(notice_id_main, '(JORF.*?)\s*\|', 1)

        # main block
        main_block = self.extract_single('//div[@class="sommaire"]/ul/li')
        seperations = main_block.split('<p class="separationSection"')
        for seperation in seperations:
            self.set_selector(text=seperation)
            h3 = self.extract_single('//h3/text()')
            sub_blocks = self.xpath('ul/li')
            for sub_block in sub_blocks:
                h4 = self.extract_single('./h4/text()', sel=sub_block)
                inner_blocks = sub_block.xpath('./ul/li')
                if not inner_blocks:
                    inner_blocks = [sub_block]
                for inner_block in inner_blocks:
                    department = self.extract_single('h5/text()', sel=inner_block)
                    individual_bills = inner_block.xpath('ul/li')
                    if not individual_bills:
                        individual_bills = [inner_block]
                    for individual_bill in individual_bills:
                        try:
                            frbill = self.model_factory.create_bill_doc()
                            frbill.country = "France"
                            title = self.extract_single('.//a[@class="lienSommaire"]/text()',
                                                               sel=individual_bill)
                            frbill.title = title
                            source_url = self.extract_single('.//span[@class="publishes"]/@resource',
                                                             sel=individual_bill)
                            frbill.source_url = source_url
                            if h4:
                                notice_type = h3+' - '+h4
                            else:
                                notice_type = h3
                            notice_type = notice_type
                            frbill.notice_type = notice_type
                            frbill.publication_date = self.get_formatted_date(scrap_date)
                            notice_id2 = self.extract_single('.//strong[@class="numeroTexte"]/text()', sel=individual_bill)
                            notice_id = notice_id1 + " - " + notice_id2
                            notice_id = re.sub('\s+$','',notice_id)
                            frbill.notice_id = notice_id
                            frbill.publication_name = "Official Journal (Journal Officiel de la Republique Francaise JORF)"
                            publication_issue = publication_issue
                            frbill.publication_issue = publication_issue
                            if department:
                                department_array = [{"department_name": department, "department_hierarchy": 0}]
                                frbill.departments = department_array

                            # documents
                            document_title = "Version initiale"
                            self.http_get(source_url, self.scraper_policy.doc_list)

                            # downloading pdf links
                            download_links = self.extract("//div[@id='exportRTF']//a/@href")
                            if not download_links:
                                raise Exception(
                                    "Download link not found for notice {}".format(
                                        source_url))
                            pdf_link = None
                            for pd_link in download_links:
                                if pd_link.startswith("/jo_pdf.do"):
                                    pdf_link = urljoin("https://www.legifrance.gouv.fr", pd_link)
                            if pdf_link:
                                self.http_get(pdf_link, self.scraper_policy.doc_list)
                                cookies = self.resp.cookies
                                request_args = {
                                    'cookies': cookies, 
                                    'headers': {'referer': pdf_link}
                                }
                                extraction_type = self.extraction_type.unknown
                                content_type = "application/pdf"
                                download_id, _, doc_ids = self.register_download_and_documents(
                                    "https://www.legifrance.gouv.fr/jo_pdf_frame.do?dl",
                                    self.scraper_policy.doc_service,
                                    extraction_type,
                                    True,
                                    content_type=content_type,
                                    download_args=request_args,
                                    should_skip_checks=True)
                                if len(doc_ids) > 0 and doc_ids[0]:
                                    document_id = doc_ids[0]
                                else:
                                    raise ValueError(
                                        "Document ID not found while registering document with url {}".format(
                                            pdf_link))
                                if not download_id:
                                    raise ValueError(
                                        "Download ID not found while registering document with url {}".format(
                                            pdf_link))

                                frbill.document_id = document_id
                                frbill.download_id = download_id
                                frbill.document_title = document_title
                            else:
                                raise Exception(
                                    "PDF link not found for notice {}".format(
                                        source_url))

                            if self.validate_doc(frbill):
                                self.save_doc(frbill)
                            else:
                                self.logger.critical(__name__, "schema_failed",
                                                    fmt("JsonSchema validation failed for : {}",
                                                    json.dumps(frbill.to_json())))
                        except Exception as e:
                            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                                 exc_info=True)
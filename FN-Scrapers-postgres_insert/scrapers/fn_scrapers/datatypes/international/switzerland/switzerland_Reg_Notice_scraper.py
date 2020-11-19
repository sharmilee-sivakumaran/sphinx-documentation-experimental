# -*- coding: utf-8 -*-
from __future__ import absolute_import
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
import dateparser
import json


class SwitzerlandURL:

    base_url = "https://www.admin.ch"

    @staticmethod
    def get_bill_url(year):
        url = "{base_url}/opc/de/official-compilation/{year}/index.html".format(base_url=SwitzerlandURL.base_url, year=year)
        return url


@scraper()
@tags(type="notices", country_code="CH", group="international")
@argument('--year', help='year should be in the format YYYY i.e. 2017,2018')
# Switzerland regulation notice scraper
class SWITZERLANDregulationnoticescraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(SWITZERLANDregulationnoticescraper, self).__init__(EventComponent.scraper_bills, "switzerland", "switzerland")
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "Switzerland_regulation_notice.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    @staticmethod
    def get_date_from_german(date):
        dt = dateparser.parse(date)
        date = dt.strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self, year):
        try:
            if year:
                # This code will run if year is given in the arguments
                # It will scrap all bills corresponding to the given year
                main_page_url = SwitzerlandURL.get_bill_url(year)
                self.http_get(main_page_url, self.scraper_policy.doc_list)
                total_collections = self.xpath('//table[@class="table table-striped"]/tbody/tr')
                for collection in total_collections[1:]:
                    individual_collection_url = self.extract_single('.//td[1]/a/@href', sel=collection)
                    individual_collection_url = urljoin(SwitzerlandURL.base_url, individual_collection_url.strip())
                    introduction_date = self.extract_single('.//td[2]/text()', sel=collection)
                    self.scrape_reg(individual_collection_url, introduction_date)
            else:
                # By Default this code will run
                startdate = datetime.date.today() - datetime.timedelta(days=30)
                enddate = datetime.date.today()
                for year in range(startdate.year, enddate.year + 1):
                    main_page_url = SwitzerlandURL.get_bill_url(year)
                    self.http_get(main_page_url, self.scraper_policy.doc_list)
                    # Getting the bills table from the list page url
                    total_collections = self.xpath('//table[@class="table table-striped"]/tbody/tr')
                    for collection in total_collections[1:]:
                        # Getting the introduction_date from the table present on the list page
                        introduction_date = self.extract_single('.//td[2]/text()', sel=collection)
                        notice_date = dateparser.parse(introduction_date).date()
                        # checking whether the notice date present lies between expected range or not
                        if startdate <= notice_date <= enddate:
                            individual_collection_url = self.extract_single('.//td[1]/a/@href', sel=collection)
                            individual_collection_url = urljoin(SwitzerlandURL.base_url,
                                                                individual_collection_url.strip())
                            self.scrape_reg(individual_collection_url, introduction_date)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc,
                              fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                              self.main_page_url)

    # function for scraping bill details
    def scrape_reg(self, bill_page_url, introduction_date):
        self.http_get(bill_page_url, self.scraper_policy.doc_list)
        publication_title = self.extract_single('//div[@class="col-sm-8 col-md-9"]/h1/text()')
        blocks = self.xpath('//table[@class="table table-striped"]/tbody/tr')
        for block in blocks[1:]:
            try:
                chbill = self.model_factory.create_bill_doc()

                # country
                chbill.country = "Switzerland"

                # title
                chbill.title = self.extract_single('.//td[2]/text()', sel=block).encode('latin_1')

                # departments
                department_name = self.extract_single('.//td[@class="small"]/text()', sel=block)
                department_name = re.sub('\r?\n|^\s+|\s+$', '', department_name)
                department_array = [{"department_name":department_name, "department_hierarchy":0}]
                chbill.departments = department_array

                # notice_id
                notice_id = self.extract_single('.//td[@class="nowrap"]/a', sel=block)
                notice_id = re.sub('<.*?>', ' ', notice_id)
                notice_id = re.sub('\r?\n|^\s+|\s+$', '', notice_id)
                notice_id = re.sub('\s+', ' ', notice_id)
                chbill.notice_id = notice_id

                # publication_date
                chbill.publication_date = self.get_date_from_german(introduction_date)

                # publication_name
                chbill.publication_name = "Official Collection of Federal Law (Amtliche Sammlung AS)"

                # publication_issue
                chbill.publication_issue = publication_title

                # source_url
                chbill.source_url = bill_page_url

                # document_title
                chbill.document_title = notice_id

                # document link
                document_link = self.extract_single('.//td[@class="nowrap"]/a/@href', sel=block)
                document_link = urljoin(SwitzerlandURL.base_url, document_link.strip())

                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"

                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type,
                                                                               True,
                                                                               content_type=content_type)
                if len(doc_ids) > 0 and doc_ids[0]:
                    document_id = doc_ids[0]
                else:
                    raise ValueError(
                        "Document ID not found while registering document with url {}".format(
                            document_link))
                if not download_id:
                    raise ValueError(
                        "Download ID not found while registering document with url {}".format(
                            document_link))

                chbill.document_id = document_id
                chbill.download_id = download_id

                if self.validate_doc(chbill):
                    self.save_doc(chbill)
                else:
                    self.logger.critical(__name__, "schema_failed",
                                         fmt("JsonSchema validation failed for : {}",
                                             json.dumps(chbill.to_json())))
            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

# -*- coding: utf-8 -*-
import os
import re
import injector
import datetime

from fn_scraperutils.events.reporting import EventComponent, ScrapeError, EventType
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info

@scraper()
@argument('--start_date', help='start_date in the format YYYY-mm-dd i.e.2017-02-07', default=(datetime.date.today()-datetime.timedelta(days=30)).strftime("%Y-%m-%d"))
@argument('--end_date', help='end_date in the format YYYY-mm-dd i.e.2018-02-07', default=datetime.date.today().strftime("%Y-%m-%d"))
@tags(type="notices", country_code="ID", group="international")
class IndonesiaRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(IndonesiaRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "indonesia_reg_notice", "indonesia_reg_notice")

        self.logger = logger

        self.base_url = u'http://ditjenpp.kemenkumham.go.id'
        self.search_url = self.base_url + u'/kerja/lnnew.php?tahun={year}'

        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "indonesia_notice.json")

        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info(u"indonesia").alpha_2)

    @staticmethod
    def return_date_as_date_obj(date_string):
        return datetime.datetime.strptime(date_string, "%Y-%m-%d").date()

    @staticmethod
    def check_date(start_date, end_date):
        date_re = re.compile('\d{4}\-\d{2}\-\d{2}')
        if date_re.match(start_date) and date_re.match(end_date):
            return True
        else:
            return False

    # function for scrape data
    def scrape(self, start_date, end_date):

        if self.check_date(start_date, end_date):
            start_date = self.return_date_as_date_obj(start_date)
            end_date = self.return_date_as_date_obj(end_date)
        else:
            raise ValueError("Invalid date format")

        if end_date < start_date:
            raise ValueError("Invalid Start and end date parameter. Failed to start scraping.")

        self.scrape_notices(start_date, end_date)

    def scrape_notices(self, start_date, end_date):
        year_range = xrange(start_date.year, end_date.year+1)
        # --------------------------------------------------
        # Scrape for 2017 and 2018 (From 2017 to till date)
        # --------------------------------------------------
        for year in year_range:
            search_url = self.search_url.format(year=year)
            self.logger.info(__name__, fmt(u"Going to notice page of year: {}", year))
            self.http_get(search_url, self.scraper_policy.doc_list)
            # Search url
            search_url = self.resp.url
            self._sel.root.make_links_absolute(base_url=search_url)

            all_next_pages = self.extract(u'//a[contains(@href,"&page=") and contains(@title,"Data:")]/@href')

            # scrape first page
            self.parse_notice_details(start_date, end_date)
            if not all_next_pages:
                self.logger.info(__name__,
                                 fmt(u"Single page result. Pagination is not available for year {}", year))
            for next_page in all_next_pages:
                self.logger.info(__name__, fmt(u"Next page url : {}", next_page))
                self.http_get(next_page, self.scraper_policy.doc_list)
                # Search url
                next_page = self.resp.url
                self._sel.root.make_links_absolute(base_url=next_page)
                self.parse_notice_details(start_date, end_date)

    def parse_notice_details(self, start_date, end_date):
        page_url = self.resp.url
        self._sel.root.make_links_absolute(base_url=page_url)
        rows = self.xpath(u'//table[@id="boks"]//tr[@class="wr"]')
        country = u'Indonesia'
        publication_name = u'State Gazette (Lembaran Negara)'

        if not rows:
            self.logger.critical(__name__, u"individual_notice_scrape_failed", u"Notices not found.")
        for row in rows:
            try:
                # Extract notice details for given notice search url
                publication_date = self.extract_single(u'./td[7]/text()', sel=row)
                publication_date = parse_date_as_str(publication_date, languages=self.country_languages)
                if start_date <= self.return_date_as_date_obj(publication_date) <= end_date:
                    notice_title = self.extract_single(u'./td[4]/text()', sel=row)

                    notice_type = self.extract_single(u'./td[2]/text()', sel=row)
                    notice_number = self.extract_single(u'./td[5]/text()', sel=row)
                    current_tahun = self.extract_single(u'//ul[contains(@class,"list-tahun")]/li[@style]/text()')
                    notice_id = u"{} - {}".format(current_tahun, notice_number)

                    document_urls = self.extract(u'./td[9]/a/@href', sel=row)
                    for document_url in document_urls:

                        # Assign extracted notice details.
                        indonesia_notice = self.model_factory.create_bill_doc()
                        indonesia_notice.country = country
                        indonesia_notice.title = notice_title
                        indonesia_notice.notice_id = notice_id
                        indonesia_notice.notice_type = notice_type
                        indonesia_notice.publication_name = publication_name
                        indonesia_notice.publication_date = publication_date
                        indonesia_notice.source_url = page_url

                        # The row in the table is called "PDF File" so it should be safe to assume it ends in ".pdf"
                        # ex. "http://ditjenpp.kemenkumham.go.id/arsip/ln/2018/pp1-2018bt.pdf"
                        document_title = re.search(r"20\d{2}\/([^\.\/]+)\.pdf", document_url)
                        if document_title:
                            indonesia_notice.document_title = document_title.group(1).upper()

                        resp = self.http_request(document_url, "HEAD")

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
                            raise ValueError(
                                "Document ID not found while registering document with url {}".format(
                                    document_url))
                        if not download_id:
                            raise ValueError(
                                "Download ID not found while registering document with url {}".format(
                                    document_url))

                        indonesia_notice.document_id = document_id
                        indonesia_notice.download_id = download_id

                        if self.validate_doc(indonesia_notice):
                            self.save_doc(indonesia_notice.for_json())
                        else:
                            self.logger.critical(__name__, u"validation error",
                                                 self.json_dumps(message=indonesia_notice.for_json()))
                else:
                    self.logger.info(__name__, fmt(
                        u"Skipping Issue because notice publication date is {} and not in range start date {} end date {}",
                        publication_date, start_date, end_date))
                    continue

            except Exception as e:
                self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

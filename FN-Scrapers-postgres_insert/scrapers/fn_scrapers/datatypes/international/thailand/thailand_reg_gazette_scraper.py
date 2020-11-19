# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta, date
import logging
import re
import os
import injector
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
import json



logger = logging.getLogger(__name__)

@scraper()
@tags(type="gazettes", country_code="TH", group="international")
@argument('--start', help='YYYY-MM-DD, defaults to last 30d')
@argument('--end', help='YYYY-MM-DD, defaults to today')
# Thailand regulation notice scraper
class THAILANDregulationnoticescraper(ScraperBase):
    SEARCH_DELTA = timedelta(days=90)
    THAI_DIGITS = [u'๐', u'๑', u'๒', u'๓', u'๔', u'๕', u'๖', u'๗', u'๘', u'๙']
    THAI_MONTHS = [
        u'มกราคม', u'กุมภาพันธ์', u'มีนาคม', u'เมษายน', # Jan-Apr
        u'พฤษภาคม', u'มิถุนายน', u'กรกฎาคม', u'สิงหาคม', # May-Aug
        u'กันยายน', u'ตุลาคม', u'พฤศจิกายน', u'ธันวาคม' # Sep-Dec
    ]
    THAI_DATE_PATTERN = re.compile(u'^([{0}]+) ({1}) พ.ศ. ([{0}]+)$'.format(
        u'|'.join(THAI_DIGITS), u'|'.join(THAI_MONTHS)))

    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(THAILANDregulationnoticescraper, self).__init__(
            EventComponent.scraper_bills, "thailand", "thailand")
        self.logger = logger
        schema_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "schemas",
            "thailand_regulation_notice.json"))
        self.model_factory = ModelFactory(schema_path, schema_path)

    @classmethod
    def build_search_url(cls, startdate, enddate, page_number=1):
        return (
            "http://www.mratchakitcha.soc.go.th/search_result.php?"
            "p={page_number}&title=&book=&special=1&type=%E0%B8%81&"
            "section=&start_date={startdate}&end_date={enddate}&group=&"
               "order=&sort=DESC"
        ).format(
            page_number=page_number,
            startdate=cls.thai_date(startdate),
            enddate=cls.thai_date(enddate)
        )

    @classmethod
    def thai_date(cls, date_str):
        '''
        Formats a date in dd/mm/yyyy (thai years)
        '''
        return date_str.strftime('%d/%m/') + str(date_str.year + 543)

    @classmethod
    def translate_thai_date(cls, date_str):
        '''
        Dates are actually ISO-8601 dates but 1) they're in thai, 2) they use the
        Buddhist calendar for the year.
        https://en.wikipedia.org/wiki/Date_and_time_notation_in_Thailand
        '''
        match = cls.THAI_DATE_PATTERN.match(date_str.strip())
        if not match:
            raise ValueError(u"Unable to parse date: " + date_str)
        day, month, year = match.groups()
        month = cls.THAI_MONTHS.index(month) + 1
        day = cls.thai_digits_to_int(day)
        year = cls.thai_digits_to_int(year) - 543
        return '{:04d}-{:02d}-{:02d}'.format(year, month, day)

    @classmethod
    def thai_digits_to_int(cls, int_str):
        '''
        Convert thai digits to ints, stripping whitespace and \u0e01 (ก, assumed
        to be ordinal indicator).
        '''
        int_str = re.sub(r'\u0e01|\s', '', int_str, flags=re.UNICODE)
        return int(''.join(str(cls.THAI_DIGITS.index(c)) for c in int_str))

    @classmethod
    def parse_date(cls, date_str):
        '''
        Parses a date from yyyy-mm-dd format
        '''
        return datetime.strptime(date_str, '%Y-%m-%d').date()

    # function for scrape data
    def scrape(self, start, end):
        if start:
            start = datetime.strptime(start, '%Y-%m-%d').date()
        else:
            start = datetime.now().date() - timedelta(days=30)
        if end:
            end = datetime.strptime(end, '%Y-%m-%d').date()
        else:
            end = datetime.now().date()
        logger.info('Searching over %s - %s', start, end)
        left = start
        notice_total = None
        while True:
            right = min(left + self.SEARCH_DELTA, end)
            logger.info('Querying: %s - %s', left, right)
            page_total = None
            page = 1
            while not page_total or page < page_total:
                url = self.build_search_url(left, right, page)
                self.http_get(url, self.scraper_policy.doc_list)
                if not page_total:
                    page_text = self.extract(
                        ".//div[@class='infopage clearfix']/form/span/text()")[1]
                    try:
                        notice_total = int(re.search(
                            r'(\d+)\s+\u0e23\u0e32\u0e22\u0e01\u0e32\u0e23',
                            page_text).group(1))
                    except:
                        # this can fail if there are no notices for the date
                        # range, which is acceptable. this may become a problem
                        # though if the page gets redesigned as no notices
                        # would be scraped and there would be no criticals.
                        pass
                    if not notice_total:
                        break
                    self.logger.info(__name__, fmt(
                        "Total Notices {}", notice_total))
                    page_total = notice_total / 100 + 2
                self.scrape_reg(url)
                page += 1
            # search page is inclusive on both ends
            left = right + timedelta(days=1)
            if right >= end:
                break

    # function for scraping search results
    def scrape_reg(self, search_result_url):
        self.http_get(search_result_url, self.scraper_policy.doc_list)
        rows = self.xpath(".//table[@class='searchresult']/tr")
        for row in rows[1:]:
            try:
                notice = self.model_factory.create_bill_doc()
                notice.country = "Thailand"

                notice.publication_name = u"Royal Thai Government Gazette (ราชกิจจานุเบกษา)"

                notice.title = self.extract_single('.//td[2]/a/text()', sel=row)

                notice.notice_type = 'Type A'

                notice_date = self.translate_thai_date(
                    self.extract_single('.//td[5]/text()', sel=row)
                )
                notice.publication_date = notice_date
                notice_date = datetime.strptime(notice_date, '%Y-%m-%d')

                notice.publication_issue = 'Volume {}, Part {}'.format(
                    self.thai_digits_to_int(
                        self.extract_single('.//td[3]/text()', sel=row)),
                    self.thai_digits_to_int(
                        self.extract_single('.//td[4]/text()', sel=row))
                )

                notice.document_title = "PDF"

                document_link = self.extract_single('.//td[7]/a/@href', sel=row)
                document_link = document_link.replace('\\', '/')

                # per conway this is to be as precise of a view of the search
                # results as we can make. pdf url is undesired as it does not
                # provide anything new, while the search result page allows
                # "verification."
                notice.source_url = self.build_search_url(notice_date, notice_date)

                download_id, _, doc_ids = self.register_download_and_documents(
                    url=document_link,
                    policy=self.scraper_policy.doc_service,
                    extraction_type=self.extraction_type.tesseract,
                    serve_from_s3=True,
                    content_type="application/pdf",
                    extract_args={
                        'language': 'tha',
                        'pageCount': 25
                    }
                )
                document_id = doc_ids[0]
                notice.document_id = document_id
                notice.download_id = download_id

                if self.validate_doc(notice):
                    self.save_doc(notice)
                else:
                    self.logger.critical(__name__, "schema_failed",
                                         fmt("JsonSchema validation failed for : {}",
                                             json.dumps(notice.to_json())))
            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

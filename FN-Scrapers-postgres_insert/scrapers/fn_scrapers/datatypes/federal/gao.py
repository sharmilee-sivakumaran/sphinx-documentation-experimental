"""
GAO Scraper
"""
import re
from dateutil.parser import parse
import injector
import dateutil.parser
import datetime

from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.scrape.util import JSONEncoderPlus
from fn_scraperutils.events.reporting import EventComponent, ReportingPolicy, Severity

from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scrapers.api.resources import ScraperUtilsScraper
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.utils import JSONEncoderPlus

BASE_URL = u"http://www.gao.gov/browse/date/"
DATE_RANGE_URL = BASE_URL + u"/custom?adv_begin_date={start}&adv_end_date={end}&rows=50&all="


class GAOReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """
    def __init__(self):
        self.gao_list = ReportingPolicy(u"GAO List", Severity.critical)
        self.gao = ReportingPolicy(u"GAO Report", Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)


def _set_event_component(binder):
    binder.bind(EventComponent, u"scraper_gao_reports")


@scraper(handler_modules=[_set_event_component])
@argument(u'--start',
          help=u'Start of scraping date range, in any valid date format')
@argument(u'--end',
          help=u'End of scraping date range, in any valid date format')
@tags(type=u"gao", country_code=u"US", group=u"federal")
class GAOScraper(object):
    """
    GAO Scraper.
    """
    @injector.inject(
        logger=BlockingEventLogger,
        scraper=ScraperUtilsScraper,
        scrape_item_publisher=ScrapeItemPublisher)
    def __init__(self, logger, scraper, scrape_item_publisher):
        self._logger = logger
        self._scraper = scraper
        self._locality = u"us"
        self.policy = GAOReportingPolicy()
        self.scrape_item_publisher = scrape_item_publisher
        self._scraper.send_create_process_event(u"GAO")

    def scrape(self, start=None, end=None):
        """
        Scrape gao documents from every gao report website
        """
        if end:
            end = dateutil.parser.parse(end).date()
        else:
            # Default end date is today.
            end = datetime.date.today()
        if start:
            start = dateutil.parser.parse(start).date()
        else:
            # Default range is 30 days
            start = end - datetime.timedelta(days=30)

        page_link = DATE_RANGE_URL.format(start=start.strftime(u"%m/%d/%Y"),
                                          end=end.strftime(u"%m/%d/%Y"))
        while True:
            list_page = self._scraper.url_to_lxml(page_link, self.policy.gao_list)
            report_list = list_page.xpath(u"//div[@class='listing grayBorderTop']/a")

            for report_ele in report_list:
                report_url = report_ele.get_attrib(u'href')
                report_id = re.findall(ur'/products/(.*)', report_url)[0]
                report_page = self._scraper.url_to_lxml(report_url, self.policy.gao)
                if report_page:
                    # Some links take us to the 'Report Not Found' page,
                    # which includes links to multiple reports
                    not_found = True if report_page.xpath_single(
                        u"head/title[contains(text(), 'Report not Found')]",
                        self.policy.test) else False
                    if not_found:
                        report_links = report_page.xpath(u"//div[@class = 'inner']/a/@href", self.policy.gao)
                        for report_link in report_links:
                            report_id = re.findall(ur'/products/(.*)', report_link)[0]
                            report_page = self._scraper.url_to_lxml(report_link, self.policy.gao)
                            self.scrape_report(report_id, report_page)
                    else:
                        self.scrape_report(report_id, report_page)
                else:
                    self._logger.warning(__name__,
                                 u"scrape_error",
                                 fmt(u"No report found at: {}", report_url))

            next_page = list_page.xpath_single(u"//a[text()='Next >']",
                                               self.policy.test)
            if next_page is None:
                break
            else:
                page_link = next_page.get_attrib(u'href')

    def scrape_report(self, report_id, report_page):
        report = {u'report_id': report_id}

        title_first = report_page.xpath_single(u"//div[@id='summary_head']/h1").text_content()
        title_sec = report_page.xpath_single(u"//div[@id='summary_head']/h2").text_content()
        title = title_first + u" " + title_sec
        report[u'title'] = title

        header_text = report_page.xpath_single(u"//div[@id='summary_head']/h3/span/span").text_content()
        release_date = re.findall(ur'Released:(.*)', header_text)[0]
        release_date = parse(release_date).date()
        report[u'release_date'] = release_date

        hightlight = report_page.xpath_single(u"//div[@id='summary']/div[@class='left_col']").text_content()
        if hightlight:
            report[u'highlights'] = hightlight

        full_report_url = report_page.xpath_single(u"//li[contains(text(), 'Full Report:')]//a")
        if not full_report_url:
            self._logger.info(__name__, fmt(u"No Document Link for {}", title))
            return
        full_report_url = full_report_url.get_attrib('href')
        if u'.htm' in full_report_url or u'.html' in full_report_url:
            extraction_type = self._scraper.extraction_type.html
        else:
            extraction_type = self._scraper.extraction_type.unknown

        download_id, _, doc_ids = \
            self._scraper.register_download_and_documents(full_report_url, self.policy.gao,
                                                          extraction_type,
                                                          True)

        if len(doc_ids) == 0 or doc_ids[0] is None:
            self._logger.warning(__name__,
                                 u"doc_service_error",
                                 fmt(u"Failed to extract text for {}", full_report_url))
            return
        report[u'document_id'] = doc_ids[0]
        report[u'download_id'] = download_id

        self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                     u"gao_reports",
                                                     self._locality,
                                                     report,
                                                     json_encoder=JSONEncoderPlus)

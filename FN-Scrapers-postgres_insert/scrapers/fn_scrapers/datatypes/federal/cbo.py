"""
CBO Scraper
"""
import re
from dateutil.parser import parse
import injector
import datetime

from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.scrape.util import JSONEncoderPlus
from fn_scraperutils.events.reporting import EventComponent, ReportingPolicy, Severity, ScrapeError
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY
from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scrapers.api.resources import ScraperUtilsScraper
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.utils import JSONEncoderPlus

BASE_URL = u"https://www.cbo.gov/cost-estimates"


class CBOReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """
    def __init__(self):
        self.cbo_list = ReportingPolicy(u"CBO List", Severity.critical)
        self.cbo = ReportingPolicy(u"CBO Report", Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)


def _set_event_component(binder):
    binder.bind(EventComponent, u"scraper_cbo_reports")


@scraper(handler_modules=[_set_event_component])
@argument(u"--sessions", u"-s", help=u"Pillar ids of sessions to scrape", nargs=u'+', required=False)
@tags(type=u"cbo", country_code=u"US", group=u"federal")
class CBOScraper(object):
    """
    CBO Scraper.
    """
    @injector.inject(
        logger=BlockingEventLogger,
        scraper=ScraperUtilsScraper,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher)
    def __init__(self, logger, scraper, metadata_client, scrape_item_publisher):
        self._logger = logger
        self._scraper = scraper
        self._locality = u"us"
        self.policy = CBOReportingPolicy()
        self._metadata_client = metadata_client
        self.scrape_item_publisher = scrape_item_publisher
        self._scraper.send_create_process_event(u"CBO")

    def scrape(self, sessions=None):
        """
        Scrape cbo documents from every cbo report website
        """
        session_list = []
        # If we get a list of session ids, we need to grab the data
        # from the Metadata service so we have the session names
        if sessions:
            for session_id in sessions:
                result = self._metadata_client.getSession(priority=0,
                                                          requester=[u"CBO Scraper"],
                                                          locality=self._locality,
                                                          id=session_id)
                if result:
                    session_list.append(result.session)
        # Otherwise, we just grab the active sessions
        else:
            result = self._metadata_client.findCurrentAndFutureSessionsByLocalityAndDate(
                priority=0,
                requester=[u"Legislative Scraper"],
                locality=self._locality,
                date=datetime.date.today().isoformat()
            )
            for session in result:
                if session.id:
                    session_list.append(session)
        if not session_list:
            self._logger.critical(__name__,
                                  u"scraper_failed",
                                  u"No sessions found for CBO scraper")

        list_page = self._scraper.url_to_lxml(BASE_URL, self.policy.cbo_list)

        for session in session_list:
            session_search = "{}-{}".format(session.id[0:4], session.id[4:8])
            try:
                session_item = list_page.xpath_single(
                    '//section[@id="block-congressionalsession"]//li[contains(., "{}")]/a'.format(session_search))
            except ScrapeError:
                self._logger.critical(__name__, u"scraper_failed",
                                      "Could not find information for session {} on CBO site".format(session.id))
                continue

            # This is an internal id not shown which is used to go to the list page
            session_id = session_item.element.get("data-drupal-facet-item-value")

            list_page_url = "https://www.cbo.gov/cost-estimates?f%5B0%5D=congressional_session%3A{}".format(session_id)

            while list_page_url:
                report_list_page = self._scraper.url_to_lxml(list_page_url,
                                                             self.policy.cbo_list,
                                                             retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

                report_list = report_list_page.xpath(u"//div[@class='views-field views-field-title']/h3/a")
                for report_ele in report_list:
                    title = report_ele.text_content()
                    report_url = report_ele.get_attrib(u'href')
                    self.scrape_report(title, report_url, session.id)

                next_page = report_list_page.xpath_single(u"//a[@title='Go to next page']",
                                                          self.policy.test)
                if next_page is None:
                    list_page_url = None
                else:
                    list_page_url = next_page.get_attrib(u'href')

    def scrape_report(self, title, report_url, session):
        report = {u'session': session, u'title': title}
        report_id = re.findall(ur'publication/(.*)', report_url)[0]
        try:
            report[u'report_id'] = int(report_id)
        except ValueError:
            # We have one case where the id we get has a hyphen,
            # but the unhyphenated version seems to an identical report.
            # Because the ids must be ints, we grab the part before the hyphen.
            report[u'report_id'] = int(report_id.split(u"-")[0])

        report_page = self._scraper.url_to_lxml(report_url, self.policy.cbo)

        if not report_page:
            return

        doc_url = report_page.xpath_single(u"//a[text()='View Document']")
        if doc_url:
            doc_url = doc_url.get_attrib(u'href')
        else:
            self._logger.info(__name__, fmt(u"No Document Link for {}", title))
            return

        date = report_page.element.cssselect(u"div.field--name-field-display-date")[0].text_content()
        date = parse(date).date()
        year = date.year

        if year < int(session[:4]) or year > int(session[4:8]):
            self._logger.info(__name__,
                              fmt(u"CBO Report at '{}' does not belong to this session",
                                  report_url))
            return
        report[u'publication_date'] = date

        if doc_url:
            download_id, _, doc_ids = \
                self._scraper.register_download_and_documents(doc_url,
                                                              self.policy.cbo,
                                                              self._scraper.extraction_type.text_pdf,
                                                              True)
            if len(doc_ids) == 0 or doc_ids[0] is None:
                self._logger.warning(__name__,
                                     u"doc_service_error",
                                     fmt(u"Bad Document Link {}", doc_url))
                return

            report[u'document_id'] = doc_ids[0]
            report[u'download_id'] = download_id

        bill_id = re.split(u',', title)[0]
        if not re.match(ur'[HS]\..*\d+', bill_id):
            bill_id = None

        bri_summary = report_page.xpath_single(u"//div[@class='summary']").text_content().strip()
        committee = re.findall(ur'reported by(.*) on', bri_summary)
        if len(committee) == 1:
            committee = committee[0]
            report[u'committee'] = committee.strip()

        if bill_id is None:
            bill_id_group = re.findall(r'[HS]\..*\d+', bri_summary)
            if len(bill_id_group) > 0:
                bill_id = bill_id_group[0]
        if bill_id is not None:
            report[u'citation_bill'] = bill_id
        summary = report_page.xpath_single(u"//div[@id='content-panel']")
        if summary:
            report[u'summary'] = summary.text_content().strip()
        self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                     u"cbo_reports",
                                                     self._locality,
                                                     report,
                                                     json_encoder=JSONEncoderPlus)

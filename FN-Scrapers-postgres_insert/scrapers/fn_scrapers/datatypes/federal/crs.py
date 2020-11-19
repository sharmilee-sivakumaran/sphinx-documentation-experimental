"""
CRS Scraper
"""
import io
import csv
import injector
from datetime import datetime, date, timedelta

from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.scrape.util import JSONEncoderPlus
from fn_scraperutils.events.reporting import EventComponent, ReportingPolicy, Severity
from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from fn_scrapers.api.scraper import scraper, tags, argument
from fn_scrapers.api.resources import ScraperUtilsScraper
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.utils import JSONEncoderPlus


type_dict = {u"metadata": u"metadata",
             u"fulltext": u"fulltext",
             u"title": u"dc_title",
             u"subject": u"dc_subject",
             u"creator": u"dc_creator"
            }


api_base_url = u"https://www.everycrsreport.com/"


class CRSReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """
    def __init__(self):
        self.crs_list = ReportingPolicy(u"CRS List", Severity.critical)
        self.crs = ReportingPolicy(u"CRS Report", Severity.warning)
        self.doc_service = ReportingPolicy(u"Doc Service Call", Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)


def _set_event_component(binder):
    binder.bind(EventComponent, u"scraper_crs_reports")


@scraper(handler_modules=[_set_event_component])
@tags(type=u"crs", country_code=u"US", group=u"federal")
@argument("--scrape-limit-days", help="Limit scrape to reports published in the previous X days")
class CRSScraper(object):
    """
    CRS Scraper.
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
        self.policy = CRSReportingPolicy()
        self._metadata_client = metadata_client
        self.scrape_item_publisher = scrape_item_publisher
        self._scraper.send_create_process_event(u"CRS")

    def scrape(self, scrape_limit_days):
        """
        Scrape crs documents from the csv file downloaded from everycrsreport.com
        """
        if scrape_limit_days:
            limit = date.today() - timedelta(days=int(scrape_limit_days))
        else:
            limit = None

        # Execute an HTTP request to get the CSV listing file.
        resp = self._scraper.http_request(api_base_url + u"reports.csv")
        # Parse it as a CSV file.
        reader = csv.DictReader(io.StringIO(resp.text))

        # Fetch reports.
        for report in reader:
            latest_pub_date = datetime.strptime(report["latestPubDate"], "%Y-%m-%d").date()
            if limit is None or latest_pub_date >= limit:
                self.scrape_report(report, limit)

    def scrape_report(self, report, limit):
        report_info = self._scraper.http_request(api_base_url + report["url"]).json()

        seen_dates = set()

        for version_info in report_info["versions"]:
            date = datetime.strptime(version_info["date"], "%Y-%m-%dT00:00:00").date()

            if limit is not None and date < limit:
                # Skip scraping versions that are too old
                continue

            title = version_info["title"]
            summary = version_info["summary"]

            # I'm not sure if this check is necessary - but the old scraper had it.
            # Basically, we only scrape one version per-date, since that is all
            # that Pillar can handle.
            if date in seen_dates:
                continue
            else:
                seen_dates.add(date)

            self.scrape_report_version(report_info, version_info, title, summary, date)

    def scrape_report_version(self, report_info, version_info, title, summary, date):
        def _get_format(format_type):
            for v in version_info["formats"]:
                if v["format"] == format_type:
                    return v
            return None

        html_version_info = _get_format("HTML")
        pdf_version_info = _get_format("PDF")

        # As of 2018-01-17, of 14,268 CRS reports available on everycrsreport.com,
        # every single one of them has an HTML attachment. 13,902 have PDFs -
        # meaning there are 366 without PDFs.
        if html_version_info is None:
            raise Exception("Couldn't find HTML version for report '{}-{}'".format(
                report_info["number"], version_info["id"]))

        # Its not ideal that we fetch the HTML version every time, regardless of if we
        # have downloaded it before or not. However, there doesn't seem to be a good way around
        # doing this: For the common case of having a PDF file, if we call
        # register_download_and_documents() without an extracted_text, it will do document
        # service PDF extraction - which we don't want.
        version_html = self._scraper.url_to_lxml(
            api_base_url + html_version_info["filename"],
            self.policy.crs,
            encoding=html_version_info["encoding"] if "encoding" in html_version_info else "utf-8")
        if version_html is None:
            self._logger.warning(
                __name__,
                u"crs_report_unavailable",
                fmt(u"Couldn't fetch HTML for CRS Report '{}-{}'", report_info["number"], version_info["id"]))
            return
        version_text = u"".join(version_html.element.xpath("//body")[0].itertext())

        # Best case - we have a PDF and an HTML file. So, we can register
        # the PDF for display purposes, but with the text we extracted
        # from the HTML file.
        if pdf_version_info:
            download_id, _, doc_ids = self._scraper.register_download_and_documents(
                api_base_url + pdf_version_info["filename"],
                self.policy.doc_service,
                self._scraper.extraction_type.text_pdf,
                True,
                extracted_text=version_text)

        # We only have an HTML file - so, we register that with its extracted text.
        else:
            download_id, _, doc_ids = self._scraper.register_download_and_documents(
                api_base_url + html_version_info["filename"],
                self.policy.doc_service,
                self._scraper.extraction_type.html,
                True,
                extracted_text=version_text)

        if len(doc_ids) != 1:
            self._logger.warning(
                __name__,
                u"doc_service_error",
                fmt(u"Could not properly extract text for report '{}-{}'", report_info["number"], version_info["id"]))
            return

        crs_entity = {u'title': title,
                      u'document_id': doc_ids[0],
                      u'authors': [u'Redacted'],
                      u'publication_date': date}
        if summary:
            crs_entity[u'description'] = summary
        self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                     u"crs_reports",
                                                     self._locality,
                                                     crs_entity,
                                                     json_encoder=JSONEncoderPlus)

"""
Federal Committee Transcripts Scraper
"""
import re
from xml.etree import ElementTree
import requests
import dateutil.parser
import injector
import datetime

from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.events.reporting import EventComponent, ReportingPolicy, Severity
from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from fn_scrapers.api.scraper import scraper, tags, argument
from fn_scrapers.api.resources import ScraperUtilsScraper
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.utils import JSONEncoderPlus

COMMITTEE_TRANSCRIPTS_BUCKET_NAME = u"committeetranscripts"

BASE_URL = u"https://www.gpo.gov"

LIST_URL = u"https://www.gpo.gov/fdsys/browse/collection.action?" \
           u"collectionCode=CHRG&browsePath=%s/%s&isCollapsed=false&leafLevelBrowse=false&ycord=0"

MOD_BASE_URL = BASE_URL + u"/fdsys/pkg/%s/mods.xml"

CHAMBER_LIST = {u"HOUSE": u"lower",
                u"JOINT": u"joint",
                u"SENATE": u"upper"}


class CommitteeTranscriptReportingPolicy(object):
    def __init__(self):
        self.transcript_list = ReportingPolicy(u"Committee Transcript List", Severity.critical)
        self.transcript = ReportingPolicy(u"Committee Transcript", Severity.warning)
        self.doc_service = ReportingPolicy(u"Doc Service Call", Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)


def _set_event_component(binder):
    binder.bind(EventComponent, u"scraper_committee_transcripts")


@scraper(handler_modules=[_set_event_component])
@argument(u"--sessions", u"-s", help=u"Pillar ids of sessions to scrape",
          nargs=u'+', required=False)
@tags(type=u"cbo", country_code=u"US", group=u"federal")
class CommitteeTranscriptScraper(object):
    """
    Committee Transcripts Scraper.
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
        self.policy = CommitteeTranscriptReportingPolicy()
        self._metadata_client = metadata_client
        self.scrape_item_publisher = scrape_item_publisher
        self._scraper.send_create_process_event(u"committee_transcript")
        self.local_path = self._scraper.config.scraperutils.tempdir

    @staticmethod
    def get_session_id(session):
        year = int(session[:4])
        session_id = str((year-1985)/2+99)
        return session_id

    def scrape(self, sessions=None):
        """
        Scrape Committee Transcripts
        """
        if not sessions:
            sessions = []
            result = self._metadata_client.findCurrentAndFutureSessionsByLocalityAndDate(
                priority=0,
                requester=[u"Committee Transcript Scraper"],
                locality=self._locality,
                date=datetime.date.today().isoformat()
            )
            for session in result:
                if session.id:
                    sessions.append(session.id)
            if not sessions:
                self._logger.critical(__name__,
                                      u"scraper_failed",
                                      u"No sessions found for CBO scraper")
        for session in sessions:
            for chamber in CHAMBER_LIST:
                chamber_url = LIST_URL % (self.get_session_id(session), chamber)
                doc = self._scraper.url_to_lxml(chamber_url, self.policy.transcript_list)
                for committee_entity in doc.xpath(u"//div[contains(@class, 'level3 browse-leaf-level')]"):
                    committee_name = committee_entity.text_content().strip()
                    committee_url_str = committee_entity.xpath(u".//a")[0].get_attrib(u'onclick')
                    committee_url = BASE_URL + re.findall(ur"goWithVars\(\'(.*?)\'", committee_url_str)[0]
                    # We can only map this committee without "the"
                    # There may be more committees like this, but this is the only one from the old scraper
                    if committee_name == u'Committee on the Budget':
                        committee_name = u'Committee on Budget'
                    committee_doc = self._scraper.url_to_lxml(committee_url,
                                                              self.policy.transcript_list)
                    for hearing_entity in committee_doc.xpath(
                            u"//a[contains(@href, 'search/pagedetails.action?')]"):
                        hearing_url_str = hearing_entity.get_attrib(u'href')
                        self.scrape_hearing(hearing_url_str, committee_name, chamber)

    def scrape_hearing(self, hearing_url, committee_name, chamber):
        output_entity = {}
        hearing_code = re.findall(ur'packageId=(.*?)&', hearing_url)[0]
        mod_url = MOD_BASE_URL % hearing_code
        try:
            mod_page = requests.get(mod_url, timeout=5).text.encode(u'utf-8').strip()
            mod_page = re.sub(' xmlns="[^"]+"', '', mod_page, count=1)
            result_tree = ElementTree.fromstring(mod_page)
        except Exception as err:#pylint:disable=broad-except
            self._logger.warning(__name__,
                                 u"scrape_error",
                                 fmt(u"Could not load hearing page: '{}'",
                                     mod_url),
                                 exc_info=True
                                 )
            return

        title = result_tree.find(u'titleInfo/title').text
        output_entity[u'title'] = title

        date = result_tree.find(u'extension/heldDate').text
        formed_date = dateutil.parser.parse(date).date()
        output_entity[u'hearing_date'] = formed_date

        committee_obj = {u'name': committee_name,
                         u'chamber': CHAMBER_LIST[chamber],
                         u'committee_type': u"committee"}
        output_entity[u'committees'] = [committee_obj]

        subcommittee_entitiy = result_tree.find(u'extension/congCommittee/subCommittee/name')
        if subcommittee_entitiy is not None and subcommittee_entitiy.text is not None:
            subcommittee = subcommittee_entitiy.text
            subcommittee_obj = {u'name': subcommittee,
                                u'committee_type': u"subcommittee",
                                u'chamber': CHAMBER_LIST[chamber]}
            output_entity[u'committees'].append(subcommittee_obj)

        pdf_url = result_tree.findall(u'location/url')[-1].text

        documents, doc_service_ids = self._scraper.handle_file(pdf_url,
                                                               self.policy.doc_service,
                                                               self._scraper.extraction_type.text_pdf,
                                                               True)

        if not doc_service_ids or not doc_service_ids[0]:
            self._logger.warning(__name__,
                                 u"doc_service_error",
                                 fmt(u"Failed to extract text from: '{}'",
                                     pdf_url)
                                 )
            return
        output_entity[u'document_id'] = doc_service_ids[0]


        self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                     u"hearing_transcripts",
                                                     self._locality,
                                                     output_entity,
                                                     json_encoder=JSONEncoderPlus)

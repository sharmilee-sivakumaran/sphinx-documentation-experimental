"""
    NoticeScraper and Notice classes
"""
from __future__ import absolute_import

import urllib2
import logging
import datetime
import enum
import time
import pytz
import injector

from .metadata import get_timezone
from fn_scrapers.api.utils import JSONEncoderPlus

from fn_rabbit.event_publisher import BlockingEventPublisher

from fn_service.server import RequestProcessId

from fn_scrapers.api.scraper import argument
from fn_scrapers.api.resources import (
    ScrapeStartTime,
    BlockingRetryingPublisherManager,
    ScraperArguments,
)
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.common import files, http
from fn_scrapers.api.utils import map_kwargs, Bunch

from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from . import events

logger = logging.getLogger(__name__)


@argument('--start', metavar='mm/dd/yy', type=str, default=None,
          help='start date(Default: 30 days before today)(Format:04/01/16)')
@argument('--end', type=str, metavar='mm/dd/yy',
          default=None, help='end date(Default: today)(Format:04/30/16)')
@argument('--s3-skip-checks', action="store_true",
          help='Whether to skip s3 checks')
class NoticeScraper(object):
    """
    Base class for all regulatory notice scrapers.
    """
    @injector.inject(
        process_id=RequestProcessId,
        session_start_time=ScrapeStartTime,
        publisher_manager=BlockingRetryingPublisherManager,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher,
        http_session=http.Session,
        files_session=files.Session,
        args=ScraperArguments)
    def __init__(
            self,
            locality,
            process_id,
            session_start_time,
            publisher_manager,
            metadata_client,
            scrape_item_publisher,
            http_session,
            files_session,
            args,
            timezone=None):

        self._blocking_event_publisher = BlockingEventPublisher(
            "scraper_state_regs", publisher_manager)

        http_session.set_as_instance()
        files_session.set_as_instance()

        self._locality = locality
        self._process_id = process_id
        self._session_start_time = session_start_time.isoformat()
        timestamp = time.mktime(session_start_time.timetuple())
        self._scrape_session_id = u"notices{}{}".format(locality.upper(), int(timestamp))

        self._scrape_item_publisher = scrape_item_publisher
        self._metadata_client = metadata_client

        self._notices = []
        if timezone:
            self._timezone = pytz.timezone(timezone)
        else:
            self._timezone = get_timezone(metadata_client, locality)

    def scrape(self, start, end, **kwargs):
        """
        Launch a Notice scraper
        """
        events.send_create_process_event(self._blocking_event_publisher, self._process_id, self._locality)

        # Default range for scraping is from 30 days ago until today
        if not start:
            start_date = datetime.date.today()-datetime.timedelta(days=30)
        else:
            try:
                start_date = datetime.datetime.strptime(start, '%x').date()
            except ValueError:
                raise Exception("Unknown start date %s" % start)
        if not end:
            end_date = datetime.date.today()
        else:
            try:
                end_date = datetime.datetime.strptime(end, '%x').date()
            except ValueError:
                raise Exception("Unknown end date %s" % end)

        if start_date > end_date:
            raise Exception("Start date (%s) is after end date (%s)." % (start_date, end_date))

        kwargs.update(start_date=start_date, end_date=end_date)
        map_kwargs(self.do_scrape, kwargs)
        self.sort_and_save()

    def do_scrape(self, start_date, end_date):
        """
        Scrape function for all regulatory notices within a date range.
        """
        raise NotImplementedError("NoticeScrapers must define a scrape method")

    def register_and_extract(self, url, extractor, serve_from_s3, ):
        """
        Given a url, an extraction_type, and an optional parse_function, handle all document service logic and return
        the list of document ids contained in this file and the content of the those documents (for now as plain text).
        :param url: The url we want to extract text from and register with the doc service
        :param extractor: files.extractor
        :return: Two lists, one of ScraperDocument objects, the other of the doc id for each ScraperDocument.
        """
        # We want critical errors sent on failures, but we don't want the scraper to exit on an exception.
        try:
            fil = files.register_download_and_documents(
                url, extractor,serve_from_s3=serve_from_s3)
            return fil.documents, fil.document_ids
        except files.exceptions.FilesException:
            logger.critical(
                "%s Notice Scraper: Document handling failed on '%s'", 
                self._locality, url)
            return [], []

    def save_notice(self, notice):
        # For StateRegs, we always want to make a list of notices,
        # sort it, then send them in chronological order.
        self._notices.append(notice)

    def send_notice(self, notice):
        """Prepare the notice to be sent through Pillar, and then publish it through Rabbit"""
        notice = self.prepare_notice(notice)

        self._scrape_item_publisher.publish_json_item(
            "",
            "state_notices",
            notice['locality'],
            notice,
            json_encoder=JSONEncoderPlus)

        self.send_ok_event(notice["locality"], notice["scraper_notice_id"])

    def prepare_notice(self, notice):
        """
        Prepare notice for being sent through Rabbit
        """
        # Convert datetimes to dates
        dates = ["effective_date", "expiration_date", "publication_date"]
        for date in dates:
            if date in notice:
                if isinstance(notice[date], datetime.datetime):
                    notice[date] = notice[date].date()

        # Remove duplicates from contents
        notice["notice_contents"] = list(set(notice["notice_contents"]))
        # If other is set as the contents alongside other content types, remove other, as it is only needed when
        # we can't figure out the contents of the notice.
        if "other" in notice["notice_contents"] and len(notice["notice_contents"]) > 1:
            notice["notice_contents"].remove("other")

        # Remove empty fields
        return self.remove_empty_fields(notice)

    def remove_empty_fields(self, notice):
        """Recursively remove empty fields from notice"""
        if isinstance(notice, dict):
            return dict((k, self.remove_empty_fields(v)) for k, v in notice.iteritems() if
                        v and self.remove_empty_fields(v))
        elif isinstance(notice, list):
            return [self.remove_empty_fields(v) for v in notice if v and self.remove_empty_fields(v)]
        else:
            return notice

    def sort_and_save(self):
        """Sort list of notices by publication date and save them all from first to last"""
        self._notices.sort(key=lambda x: x["publication_date"])
        for notice in self._notices:
            self.send_notice(notice)

    def send_ok_event(self, locality, obj_id=None, event_keys=None):
        """
        Send OK event when a single object is saved, or when a scrape session completes
        
        TODO: Review this once a more permanent logging utility is in place.
        
        :param locality: Locality of scraper
        :param obj_id: Unique object id. If given, we assume the event is for successfully saving a single object.
        :param event_keys: Additional event keys, such as legislative session for bills.
        """
        event_type = "successful_scrape"
        if not event_keys:
            event_keys = {}
        event_keys["locality"] = locality
        if obj_id:
            event_keys["obj_id"] = obj_id
            message = u"{}: Successfully scraped {}".format(locality, obj_id)
        else:
            message = u"{}: Scrape successfully completed".format(locality)
        for event_key in event_keys:
            if not isinstance(event_keys[event_key], basestring):
                event_keys[event_key] = unicode(event_keys[event_key])
        event = {
            "message": message,
            "event_type": event_type,
            "event_keys": event_keys,
        }
        event["event_keys"] = event_keys
        self._blocking_event_publisher.publish_event("ok", event, self._process_id)

class Notice(dict):
    """
    Class representing a Document object to be sent to Pillar
    """
    def __init__(self, locality, title, publication_date, notice_id, **kwargs):
        super(Notice, self).__init__()

        self['locality'] = locality
        self['title'] = title
        self['scraper_notice_id'] = notice_id
        self['publication_date'] = publication_date

        # notice_ts is a datetime version of the publication date, used for comparing publication dates of notices to
        # construct a timeline of notices in Pillar.
        if isinstance(publication_date, datetime.datetime):
            self['notice_ts'] = publication_date
        else:
            self['notice_ts'] = datetime.datetime.combine(publication_date, datetime.time.min)

        self['regulation'] = None
        self['notice_contents'] = []

        self.update(kwargs)

    # The quote kwarg is different from that in stateregs
    def set_attachment(self, url=None, raw_text=None, text_type=None, document_id=None, quote=True):
        """Set the attachment field for the notice
        NOTE: all arguments are deprecated except for document_id
        url is deprecated in cases where there is a document_id.
        Pillar will always only use the document's external and s3 url.
        """
        if url and quote:
            url = urllib2.quote(url, "://?=&%")
        attachment = dict(url=url, raw_text=raw_text, text_type=text_type, document_id=document_id)
        self['attachment'] = attachment
        # We also need to set the external url to the attachment url, so it can be displayed to the user.
        if not self.get("external_url", None) and url:
            self["external_url"] = url

    def add_agency(self, name=None, short_name=None, external_id=None):
        """Add an agency"""
        agency = {"name": name, "short_name": short_name, "external_id": external_id}
        if 'agencies' not in self:
            self["agencies"] = []
        self['agencies'].append(agency)

    def add_contact(self, contact_type, name=None, role=None, agency=None, address=None, email=None, phone=None):
        """Add a contact"""
        # Contact type is either 'comment' or 'inquiry'
        contact = {"contact_type": contact_type, "name": name, "role": role, "agency": agency,
                   "address": address, "email": email, "phone": phone}
        if 'contacts' not in self:
            self["contacts"] = []
        self['contacts'].append(contact)

    def add_hearing(self, location, hearing_start=None, description=None, timezone=None):
        """Add a hearing"""
        hearing_start_time = {}
        # Added this because WA stateregs don't send hearing start times
        if hearing_start:
            if isinstance(hearing_start, datetime.datetime):
                if not hearing_start.time():
                    hearing_start = hearing_start.date()
                elif timezone:
                    hearing_start = timezone.localize(hearing_start)
                    hearing_start_time["datetime"] = hearing_start
                else:
                    logger.error("No timezone provided for hearing datetime.")
                    return
            if "datetime" not in hearing_start_time:
                hearing_start_time['date'] = hearing_start
        hearing = dict(location=location, hearing_start_time=hearing_start_time, description=description)

        if 'hearings' not in self:
            self["hearings"] = []
        self['hearings'].append(hearing)

    def set_comment_period(self, start_date, end_date):
        """Set comment period for notice"""
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime.datetime):
            end_date = end_date.date()
        comment_period = dict(start_date=start_date, end_date=end_date)
        self["comment_period"] = comment_period

    def set_regulation(self, regulation_type, scraper_regulation_id=None, scraper_non_unique_regulation_id=None,
                       regulation_id=None, title=None, summary=None):
        """
        Set the fields which are relevant to the regulation as a whole. This is important for linking notices.
        :param regulation_type: Regular, emergency, or executive
        :param scraper_regulation_id: A unique identifier for a regulation which will cause all notices with that id
        to be automatically linked together.
        :param scraper_non_unique_regulation_id: A non-unique identifier which will not necessarily link together all
        notices with this id. Instead, it uses timeline logic to link proposals to finals, and not the other way around
        :param regulation_id: A displayable id which will be sent right to the frontend.
        :param title: Regulation title. Also used to do linking as a fallback, using similar timeline logic to reg_id.
        If this isn't set, we use a default title.
        :param summary: A summary of the regulation that is displayed to the user.
        :return:
        """
        # If a scraper_regulation_id is set, but regulation_id isn't, we set it. We assume that the unique identifier is
        # displayable as well.
        if scraper_regulation_id and not regulation_id:
            regulation_id = scraper_regulation_id

        regulation = {"regulation_type": regulation_type, "scraper_regulation_id": scraper_regulation_id,
                      "scraper_non_unique_regulation_id": scraper_non_unique_regulation_id,
                      "regulation_id": regulation_id, "title": title, "summary": summary}

        self["regulation"] = regulation

    def add_contents(self, contents):
        """Add to the list of notice contents"""
        if contents:
            if isinstance(contents, list):
                if isinstance(contents[0], NoticeContentType):
                    contents = [c.name for c in contents]
                self["notice_contents"] += contents
            elif isinstance(contents, NoticeContentType):
                self["notice_contents"].append(contents.name)
            else:
                self["notice_contents"].append(contents)

    def __unicode__(self):
        return "%s %s: %s" % (self['locality'], self['document_type'], self['description'])


class NoticeContentType(enum.Enum):
    """
    Enum of different types of notice contents
    """
    pre_proposal_notice = "pre_proposal_notice"
    proposal_text = "proposal_text"
    proposal_notice = "proposal_notice"
    withdrawn_notice = "withdrawn_notice"
    final_text = "final_text"
    final_notice = "final_notice"
    hearing = "hearing"
    comment_period = "comment_period"
    impact_statement = "impact_statement"
    other = "other"


class RegulationType(enum.Enum):
    """
    Enum of possible regulation types
    """
    regular = "regular"
    emergency = "emergency"
    executive = "executive"

from __future__ import absolute_import
import urllib2
from pytz import timezone
import re
from .event import Scraper, SourcedObject
import injector
import logging

from .metadata import get_metadata

from fn_rabbit.event_publisher import BlockingEventPublisher

from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher

from fn_service.server import RequestProcessId, BlockingEventLogger, fmt, LoggerState

from fn_scrapers.api.scraper import argument
from fn_scrapers.api.resources import (
    ScrapeStartTime,
    BlockingRetryingPublisherManager,
    BlockingRetryPolicy,
    ScraperArguments,
)

from fn_scrapers.common import http, files

from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

logger = logging.getLogger(__name__)


class EventScraper(Scraper):

    scraper_type = 'events'

    @injector.inject(
        process_id=RequestProcessId,
        session_start_time=ScrapeStartTime,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher,
        args=ScraperArguments,
        logger=BlockingEventLogger,
        logger_state=LoggerState,
        http_session=http.Session,
        file_session=files.Session)
    def __init__(
            self,
            locality,
            module_name,
            process_id,
            session_start_time,
            metadata_client,
            scrape_item_publisher,
            args,
            logger,
            logger_state,
            http_session,
            file_session):
        metadata = get_metadata(metadata_client, locality)

        super(EventScraper, self).__init__(
            scrape_item_publisher, session_start_time, logger, logger_state,
            module_name=module_name, metadata=metadata, process_id=process_id)
        http_session.set_as_instance()
        file_session.set_as_instance()

        self._events = []
        self._tz = timezone(metadata['timezone'])

    def scrape(self):
        raise NotImplementedError("EventScrapers must define a scrape method")

    def save_event(self, event):
        self._events.append(event)

    def save_events_calendar(self):
        if self._events:
            calendar = {"leg_events": self._events, "_type": "event"}
            super(EventScraper, self).save_object(calendar)
        else:
            self.warning("No events found")

        # Make sure to empty the events list so it doesn't get passed to the next session scraped
        self._events = []


class Event(SourcedObject):
    def __init__(self, start, description, location, event_type, start_has_time=False, session=None, **kwargs):
        super(Event, self).__init__('event', **kwargs)

        self['start'] = start
        self['description'] = description
        self['sources'] = []
        self['location'] = location
        self['event_type'] = event_type
        self['start_has_time?'] = start_has_time
        if session:
            self['session'] = session

        self.participants = set()
        self.related_bills = set()

    def add_document(self, name, url, **kwargs):
        url = urllib2.quote(url, "://?=&%")
        d = dict(name=name, url=url, **kwargs)
        if 'documents' not in self:
            self['documents'] = []
        self['documents'].append(d)

    def add_related_bill(self, external_id, related_type="consideration", **kwargs):
        # external id can sometimes come in a condensed format like HB123
        # we need to send an external id in 2 parts however
        # most bills fall into this category
        # if there is no space in external_id
        # doesn't add duplicates
        if not external_id:
            return

        # dirty override for scrapers that pass in 'bill' as related type
        if related_type == 'bill':
            related_type = 'consideration'

        external_id = external_id.replace('.', '')
        if " " not in external_id:
            parts = re.match(r'^([A-Z]+)(\d+)$', external_id)
            if parts and len(parts.groups()) >= 2:
                external_id = parts.group(1) + " " + parts.group(2)

        if "related_bills" not in self:
            self["related_bills"] = []

        related_bill = {"external_id": external_id, "type": related_type}
        key = (related_bill['external_id'], related_bill['type'],)

        if key not in self.related_bills:
            self['related_bills'].append(related_bill)
            self.related_bills.add(key)

    def add_participant(self, role, name, **kwargs):
        # doesn't add duplicates
        kwargs.update({'role': role, 'name': name})
        if 'participants' not in self:
            self["participants"] = []

        key = (role, name, kwargs.get('chamber', None))
        if key not in self.participants:
            self['participants'].append(kwargs)
            self.participants.add(key)

    def __unicode__(self):
        return "%s %s" % (self['start'], self['description'])

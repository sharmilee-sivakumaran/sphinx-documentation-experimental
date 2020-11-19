from __future__ import absolute_import
from future.builtins import str as text
from future.utils import python_2_unicode_compatible

import os
import logging
import importlib
import json
import datetime
import sys
import re
import arrow
import uuid
import strict_rfc3339
import traceback
import time
import platform
import lxml.html
from tempfile import NamedTemporaryFile
from urllib2 import quote

from fn_service.server import fmt
from fn_scrapers.common.http import request, request_file
from .utils import remove_empty_fields, JSONEncoderPlus
from fn_rabbit.blocking_rabbit import create_blocking_retrying_publisher_manager
import fn_rabbit.load


class ScrapeError(Exception):
    """
    Base class for scrape errors.
    """
    def __init__(self, msg, orig_exception=None):
        self.msg = msg
        self.orig_exception = orig_exception

    def __str__(self):
        if self.orig_exception:
            return '%s\nOriginal Exception: %s' % (self.msg,
                                                   self.orig_exception)
        else:
            return self.msg


class NoDataForPeriod(ScrapeError):
    """
    Exception to be raised when no data exists for a given period
    """
    def __init__(self, period):
        self.period = period

    def __str__(self):
        return 'No data exists for %s' % self.period


@python_2_unicode_compatible
class InterpolateMessage(object):

    def __init__(self, message, args):
        self.message = message
        self.args = args

    def __str__(self):
        if self.args:
            return text(self.message) % (self.args)
        else:
            return text(self.message)


class Scraper(object):
    """ Base class for all Scrapers

    Provides several useful methods for retrieving URLs and checking
    arguments against metadata.
    """

    latest_only = False
    scraper_type = 'super'

    def __init__(self, scrape_item_publisher, scrape_start_time, logger, logger_state, module_name=__name__, metadata="",
                 scrape_session_id="", process_id="", output_dir=None, strict_validation=None):
        """
        Create a new Scraper instance.

        :param metadata: metadata for this scraper
        :param output_dir: the data directory to use
        :param strict_validation: exit immediately if validation fails
        """
        self.scrape_session_ts = scrape_start_time
        self.scrape_item_publisher = scrape_item_publisher
        self.module_name = module_name

        self.metadata = metadata
        self.session_metadata = None

        self.output_dir = output_dir
        self.object_count = 0

        # validation
        self.strict_validation = strict_validation

        self.follow_robots = False

        # customize logger
        self.logger = logger
        # logging convenience methods
        logger_state.add_event_keys({
            'locality': metadata['abbreviation']
        })

        # Unique id in the format <state><number>
        # Used in scrape session events
        self.scrape_session_id = u'events' + self.metadata['abbreviation'] + str(self.scrape_session_ts)
        # process_id links events together across components
        self.process_id = process_id
        # Counters used for scraper performance report
        self.obj_count = 0
        self.error_count = 0
        self.start_time = time.time()

        if self.output_dir:
            # make output_dir
            os.path.isdir(self.output_dir) or os.path.makedirs(self.output_dir)
        else:
            self.warning("No output dir specified, will write to stdout")

    def log(self, msg, *args):
        self.logger.info(self.module_name, InterpolateMessage(msg, args))

    def debug(self, msg, *args):
        self.logger.debug(self.module_name, InterpolateMessage(msg, args))

    def info(self, msg, *args):
        self.logger.info(self.module_name, InterpolateMessage(msg, args))

    def ok(self, msg, *args, **kwargs):
        self.logger.ok(self.module_name, kwargs.get('ltype', "event_scraper"),
                       InterpolateMessage(msg, args))

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(self.module_name, kwargs.get('ltype', "event_scraper"),
                            InterpolateMessage(msg, args))

    def error(self, msg, *args, **kwargs):
        self.logger.critical(self.module_name, kwargs.get('ltype', "event_scraper"),
                          InterpolateMessage(msg, args))

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(self.module_name, kwargs.get('ltype', "event_scraper"),
                             InterpolateMessage(msg, args))

    def exception(self, msg, *args, **kwargs):
        self.logger.warning(self.module_name, kwargs.get('ltype', "event_scraper"),
                            InterpolateMessage(msg, args))

    def get(self, url, **kwargs):
        resp = request(url, **kwargs)

        def lxmlize():
            return lxml.html.fromstring(resp.text)

        resp.lxml = lxmlize
        return resp

    def post(self, url, **kwargs):
        resp = request(url, method="POST", **kwargs)

        def lxmlize():
            return lxml.html.fromstring(resp.text)

        resp.lxml = lxmlize
        return resp

    def urlretrieve(self, url, **kwargs):
        file_obj = NamedTemporaryFile()
        file_obj, resp = request_file(url, file_obj=file_obj, **kwargs)
        return file_obj.name, resp

    def get_session_metadata(self, session):
        for leg_session in self.metadata.get('legislative_session_containers', []):
            for sessions in leg_session.get('sessions', []):
                if sessions['id'] == session:
                    return sessions

    def _send_to_rabbitmq(self, obj):
        obj_type = obj.pop("_type", None)
        self._send_events(obj)

    def _event_validation_cleanup(self, message):
        return remove_empty_fields(message)

    def all_sessions(self):
        sessions = []
        for t in self.metadata['legislative_session_containers']:
            sessions.extend(t['sessions'])
        return sessions

    def _send_events(self, obj):

        for l in obj['leg_events']:
            if "_type" in l:
                del l["_type"]

            if "participants" in l:
                for participant in l["participants"]:
                    if 'chamber' in participant and participant['chamber'] not in ['upper', 'lower', 'joint']:
                        del participant['chamber']

        obj = self._event_validation_cleanup(obj)

        self.scrape_item_publisher.publish_json_item("leg_events_hub", "leg_events", self.metadata['abbreviation'], obj,
                                                     json_encoder=JSONEncoderPlus)
        self.ok("Sent Leg Event to Pillar")

        self.obj_count += 1

    def save_object(self, obj):
        if "leg_events" in obj:
            for event in obj["leg_events"]:
                self.log('save %s', unicode(event))
        else:
            self.log('save %s', unicode(obj))
        self.object_count += 1

        # For now, locality is not set when an object is initialized, so we do it here.
        locality = getattr(self, 'jurisdiction', None)
        if locality:
            obj['locality'] = locality

        self._send_to_rabbitmq(obj)


class SourcedObject(dict):
    """ Base object used for data storage.

    Base class for :class:`~fnleg.scrape.bills.Bill`,
    :class:`~fnleg.scrape.votes.Vote`.

    SourcedObjects work like a dictionary.  It is possible
    to add extra data beyond the required fields by assigning to the
    `SourcedObject` instance like a dictionary.
    """

    def __init__(self, _type, **kwargs):
        super(SourcedObject, self).__init__()
        self['_type'] = _type
        self['sources'] = []
        self.update(kwargs)
        self.sources = set()

    def add_source(self, url, urlquote=False, **kwargs):
        """
        Add a source URL from which data related to this object was scraped.
        No duplicates
        :param url: the location of the source
        :param urlquote: This is to specify whether or not to quote the URL string. Default is True
        """
        if urlquote:
            source = dict(url=quote(url, "://?=&%"), **kwargs)
        else:
            source = dict(url=url, **kwargs)
        if url not in self.sources:
            self['sources'].append(source)
            self.sources.add(url)


def check_sessions(metadata, sessions):

    """
    DEPRECATED IN PILLAR SCRAPERS
    """
    all_sessions_in_terms = list(reduce(lambda x, y: x + y,
                                 [x['sessions'] for x in metadata['terms']]))
    # copy the list to avoid modifying it
    metadata_session_details = list(metadata.get('_ignored_scraped_sessions',
                                                 []))

    for k, v in metadata['session_details'].iteritems():
        try:
            all_sessions_in_terms.remove(k)
        except ValueError:
            raise ScrapeError('session %s exists in session_details but not '
                              'in a term' % k)

        metadata_session_details.append(v.get('_scraped_name'))

    if not sessions:
        raise ScrapeError('no sessions from session_list()')

    if all_sessions_in_terms:
        raise ScrapeError('no session_details for session(s): %r' %
                          all_sessions_in_terms)

    unaccounted_sessions = []
    for s in sessions:
        if s not in metadata_session_details:
            unaccounted_sessions.append(s)
    if unaccounted_sessions:
        raise ScrapeError('session(s) unaccounted for: %r' %
                          unaccounted_sessions)

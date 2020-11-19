from __future__ import absolute_import

import arrow
import datetime
import json
import logging
import traceback
import time
import injector
import re
import contextlib
import uuid

from fn_scraperutils.events.reporting import EventComponent
from fn_mapper import CommitteeExtractor

from fn_rabbit.event_publisher import BlockingEventPublisher

from fn_service.server import RequestProcessId, RequestId, Reactor, LoggerState, ComponentName
from fn_service.components.logging import RequestEventLogExtra
from fn_service.components.direct import BlockingDirectHandlerCreator, DirectHandler

from fn_scrapers.api.scraper import argument
from fn_scrapers.api.resources import (
    ScraperArguments,
    ScrapeStartTime,
    BlockingRetryingPublisherManager,
    Tags,
    ScraperRequestModule,
)
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher

from fn_scraperutils.scraper import Scraper
from fn_scraperutils.config import Config
from fn_scraperutils.doc_service.doc_service_client import DocServiceClient
from fn_scraperutils.doc_service.transfer_to_s3 import S3Transferer
from fn_scraperutils.request.blocking_client import BlockingClient

from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from .metadata import validate_sessions
from .normalize import normalize_bill_id

logger = logging.getLogger('fn_legislation')


class ExpectedError(Exception):
    """
    ExpectedError is to be used when we are aware of a regular problem with a scraped page (such as a bill missing a
    title) that we choose not to work around (maybe bills without titles are not actually bills but stubs) but do not
    want the scraper to regularly report an error. This error will be logged as either a warning(when found in
    expected_errors) or an error otherwise
    """
    pass


class JSONEncoderPlus(json.JSONEncoder):
    """
    JSONEncoder that encodes datetime objects as Unix timestamps
    """

    def default(self, obj, **kwargs):  # pylint: disable=e0202
        if isinstance(obj, datetime.datetime):
            return str(arrow.get(obj))
        elif isinstance(obj, datetime.date):
            dt_obj = datetime.datetime.combine(obj, datetime.datetime.min.time())
            return dt_obj.isoformat("T") + "Z"
        return super(JSONEncoderPlus, self).default(obj, **kwargs)


def parse_bill_ids(bill_ids_arg):
    if bill_ids_arg is None or all(re.match(r"^[A-Z]+ [\d-]+$", bill_id) for bill_id in bill_ids_arg):
        # bill ids argument is either not provided or a list of bill id strings
        return bill_ids_arg
    else:
        # bill ids argument should be one or multiple string representing dictionaries of
        # bill ids (along with bill info), which will be parsed into lists by argparse that looks like
        # the following:
        # (1) ['{"HB 1": {"url": "www.hb1.com"}, "HB 2": {"url": "www.hb2.com"}}']
        # (2) ['{"HB 1": {"url": "www.hb1.com"}', '{"HB 2": {"url": "www.hb2.com"}}']
        bill_ids_dict = {}
        for bill_id in bill_ids_arg:
            bill_id = json.loads(bill_id)
            bill_ids_dict.update(bill_id)

        return bill_ids_dict


@contextlib.contextmanager
def thread_pool_blocking(reactor, max_threads):
    """
    A simple context manager that allows for starting up a thread pool and then
    making sure it gets shut down again.

    :param reactor: The Twisted Reactor to use
    :param max_threads: The maximum number of threads configure the thread pool with
    """
    from fn_service.thread_pool import BasicThreadPool
    from twisted.internet.threads import blockingCallFromThread
    tp = BasicThreadPool(reactor, min_threads=1, max_threads=max_threads)
    blockingCallFromThread(reactor, tp.start)
    try:
        yield tp
    finally:
        blockingCallFromThread(reactor, tp.async_stop)


class SubrequestModule(injector.Module):
    """
    If we direct scrapers toward workers threads, it means that each bill scraped
    will create a new request context. Doing so automatically creates a new process id
    value for each one - but, we don't want that. So, we override the process id of
    the workers to keep the one from the parent.
    """

    def __init__(self, component_name, process_id, request_id):
        self.component_name = component_name
        self.process_id = process_id
        self.request_id = request_id

    def configure(self, binder):
        binder.bind(ComponentName, to=self.component_name)
        binder.bind(RequestProcessId, to=self.process_id)
        binder.bind(RequestId, to=self.request_id)


def create_subrequest_handler_desc(thread_pool, scraper_cls):
    """
    We need to create a simple wrapper around the scraper class to bridge the
    fn-service and scraper worlds. On the fn-service side, a Handler must be
    configured to run on a particular thread pool and with other such options.
    Scrapers aren't configured with all of those options - so, we define this
    simple wrapper class that defines those options and then forwards any
    parameters to the specified scraper class.

    :param thread_pool:
    :param scraper_cls:
    """
    from fn_service.server import run_in_thread_pool, logging_config, MessageFailedError, watchdog

    @run_in_thread_pool(thread_pool=thread_pool)
    @logging_config(log_start=False, log_end=False, log_error=False)
    @watchdog(max_expected_runtime=None)
    class SubrequestHandler(object):
        @injector.inject(inj=injector.Injector, logger_state=LoggerState)
        def __init__(self, inj, logger_state):
            self.inj = inj
            self.logger_state = logger_state

        def scrape_bill(self, session, bill_id, *args, **kwargs):
            try:
                inst = self.inj.get(scraper_cls)
                with self.logger_state.set_request_contexts({
                    "request": str(uuid.uuid4()),
                    "scraper_external_id": bill_id
                }) as ctx:
                    logger.info("Scraping bill '%s' under request context: request=%s", bill_id, ctx["request"])
                    return inst.scrape_bill_with_error_check(session, bill_id, *args, **kwargs)
            except:
                raise MessageFailedError(traceback.format_exc())

    return DirectHandler(SubrequestHandler, methods=["scrape_bill"])


@argument('-s', '--sessions', nargs='+', required=True,
          help='Internal IDs of sessions to be scraped')
@argument('--s3_skip_checks', action='store_true',
          help='Always skip doc service checks when uploading')
@argument('-b', '--bill_ids', nargs='+',
          help='Bill IDs to be scraped')
@argument('-f', '--filter_bill_ids', nargs='+',
          help='Bill IDs to be scraped filtered from scraper_bill_ids')
@argument('-c', '--concurrency', default=1, type=int,
          help='Number of threads to use while scraping')
@argument('--extraction_flag',
          help='Set a global extraction_params update_flag')
class BillScraper(object):
    """
    Base class for all legislative bill scrapers.
    """
    """
    If a BillScraper has expected errors they are expected to put them here in a list/set. An entry should be
    a tuple of (session, bill, ExpectedError description)
    eg. [("20172018r", "HCR 350", "No title")]  # DI-2181
    """
    expected_errors = []

    @injector.inject(
        process_id=RequestProcessId,
        publisher_manager=BlockingRetryingPublisherManager,
        blocking_client=BlockingClient,
        session_start_time=ScrapeStartTime,
        s3_transferer=S3Transferer,
        scraper_utils_config=Config,
        doc_service_client=DocServiceClient,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher,
        args=ScraperArguments,
        direct_handler_creator=BlockingDirectHandlerCreator,
        reactor=Reactor,
        scraper_tags=Tags,
        log_extra_info=RequestEventLogExtra,
        logger_state=LoggerState)
    def __init__(
            self,
            locality,
            process_id,
            publisher_manager,
            blocking_client,
            session_start_time,
            s3_transferer,
            scraper_utils_config,
            doc_service_client,
            metadata_client,
            scrape_item_publisher,
            args,
            direct_handler_creator,
            reactor,
            scraper_tags,
            log_extra_info,
            logger_state,
            retry_policy=None):
        self.session_start_time_raw = session_start_time
        self.session_start_time = session_start_time.isoformat()
        timestamp = time.mktime(session_start_time.timetuple())
        self.scrape_session_id = u"bills{}{}".format(locality.upper(), int(timestamp))

        self.logger_state = logger_state

        self.process_id = process_id
        self.scraper = Scraper(
            scraper_type=EventComponent.scraper_bills,
            process_id=self.process_id,
            publisher=BlockingEventPublisher(
                EventComponent.scraper_bills.name, publisher_manager),
            ratelimiter_client=blocking_client,
            scrape_start_time=session_start_time,
            retry_policy=retry_policy,
            s3_transferer=s3_transferer,
            config=scraper_utils_config,
            doc_service_client=doc_service_client,
            metadata_client=metadata_client,
            s3_skip_checks=args.s3_skip_checks,
            log_extra_info=log_extra_info,
            update_flag=args.extraction_flag)
        self.locality = locality

        self.scraper.send_create_process_event("bills" + locality.upper())

        self.bill_count = 0

        self.scrape_item_publisher = scrape_item_publisher

        self.args = args

        self.direct_handler_creator = direct_handler_creator

        self.reactor = reactor

        self.scraper_tags = scraper_tags

        # Set up the commitee extractor for this locality
        self.auto_extract_committee = getattr(self, 'auto_extract_committee', True)
        try:
            self.committee_extractor = CommitteeExtractor(locality=locality)
        except ValueError:
            self.committee_extractor = None
            logger.warning(
                u"Could not load committee extractor for locality %s",
                self.locality)

    def scrape(self):
        if ((self.args.bill_ids or self.args.filter_bill_ids)
                and len(self.args.sessions) > 1):
            raise Exception(
                "If bill ids are provided, only one session will be accepted")

        validate_sessions(
            self.scraper.metadata_client,
            self.locality,
            self.args.sessions)

        normalized_ids = {}

        def _process_session_bill_ids(session, session_bill_ids):
            if isinstance(session_bill_ids, list):
                dict_ids = {}
                for bill_id in session_bill_ids:
                    dict_ids.update({bill_id: None})
                session_bill_ids = dict_ids

            normalized_ids[session] = {}
            for bill_id in session_bill_ids:
                try:
                    bill_dict = {normalize_bill_id(bill_id): session_bill_ids[bill_id]}
                    normalized_ids[session].update(bill_dict)
                except AssertionError as e:
                    self.scraper.send_failed_event(self.locality, e, obj_id=bill_id)

        if not self.args.bill_ids:
            for session in self.args.sessions:
                bill_ids = self.scrape_bill_ids(session)
                if self.args.filter_bill_ids:
                    filtered_ids = {}
                    missing_bills = []
                    for bill_id in self.args.filter_bill_ids:
                        if bill_id in bill_ids:
                            filtered_ids[bill_id] = bill_ids[bill_id]
                        else:
                            missing_bills.append(bill_id)
                    if missing_bills:
                        return logging.critical(
                            "Unable to find bills: %s", missing_bills)
                    bill_ids = filtered_ids
                _process_session_bill_ids(session, bill_ids)
        else:
            bill_ids = parse_bill_ids(self.args.bill_ids)
            _process_session_bill_ids(self.args.sessions[0], bill_ids)

        for session in self.args.sessions:
            session_bills = normalized_ids[session]
            if session_bills:
                self.scrape_bills(session, session_bills)

    def scrape_bill_ids(self, session):
        """
        Get all the bills IDs to scrape for a given session.

        :rtype: List of strings
        """
        raise NotImplementedError('Bill Scrapers must define a scrape_bill_ids method')

    def scrape_bill_with_error_check(self, session, bill_id, **kwargs):
        """
        Scrapes bills while checking for any ExpectedError, in which we log a warning for
        :param session:
        :param bill_id:
        :param kwargs:
        :return:
        """
        try:
            self.scrape_bill(session, bill_id, **kwargs)
        except ExpectedError as e:
            if (session, bill_id, e.message) in self.expected_errors:
                logger.warning("Expected Error in {} {}: {}".format(session, bill_id, e))
            else:
                raise

    def scrape_bills(self, session, session_bills):
        """
        Scrape all the bills in the bill_ids list, catching any exceptions thrown by critical errors.
        """
        sorted_bill_ids = sorted(session_bills.keys())
        if self.args.concurrency == 1:
            for bill_id in sorted_bill_ids:
                try:
                    with self.logger_state.set_request_contexts({
                        "request": str(uuid.uuid4()),
                        "scraper_external_id": bill_id
                    }) as ctx:
                        logger.info("Scraping bill '%s' under request context: request=%s", bill_id, ctx["request"])
                        self.scrape_bill_with_error_check(session, bill_id, bill_info=session_bills[bill_id])
                except Exception as e:
                    traceback.print_exc()
                    self.scraper.send_failed_event(self.locality, e, trace=traceback.format_exc(), obj_id=bill_id)
                    continue
        else:
            from fn_service.components.direct import RequestContext
            with thread_pool_blocking(self.reactor, self.args.concurrency) as thread_pool:
                handler_desc = create_subrequest_handler_desc(thread_pool, self.__class__)
                handler = self.direct_handler_creator.create_blocking_direct_handler_factory(handler_desc) \
                    .create_handler()
                results = []

                for bill_id in sorted_bill_ids:
                    result = handler.scrape_bill(RequestContext(
                        args=(session, bill_id),
                        kwargs=dict(bill_info=session_bills[bill_id]),
                        request_modules=[
                            ScraperRequestModule(self.session_start_time_raw, self.scraper_tags),
                            SubrequestModule(
                                self.logger_state.component,
                                self.process_id,
                                self.logger_state.request_id)]))
                    results.append((bill_id, result))

                for bill_id, result in results:
                    try:
                        result.get()
                    except Exception as e:
                        traceback.print_exc()
                        self.scraper.send_failed_event(self.locality, e, trace=traceback.format_exc(), obj_id=bill_id)

    def scrape_bill(self, session, bill_id, **kwargs):
        raise NotImplementedError('Bill Scrapers must define a scrape_bill method')

    def save_bill(self, bill):
        logger.info('save %s', unicode(bill))

        if bill.contains_action_with_unicode_space():
            logger.warning("One or more action_text for {} contains non-standard space(s)".format(bill["id"]))
        bill['locality'] = self.locality
        if self.committee_extractor:
            self.extract_committees(bill)
        self._send_bill(bill)

    def _send_bill(self, bill):
        event_keys = {u"legislative_session": bill["session"]}

        self.scrape_item_publisher.publish_json_item(
            "",
            "bills",
            self.locality,
            bill,
            json_encoder=JSONEncoderPlus,
        )

        self.bill_count += 1
        self.scraper.send_ok_event(self.locality, obj_id=bill["id"], event_keys=event_keys)

    def extract_committees(self, bill):
        if not self.auto_extract_committee:
            return
        for action in bill.get(u"actions", []):
            committees = self.committee_extractor.extract_all(action[u"action"], self.locality, action[u"actor"])
            if committees:
                if not action.get(u"related_entities"):
                    action[u"related_entities"] = []
                for committee in committees:
                    action[u"related_entities"].append({u"name": committee, u"type": u"committee"})

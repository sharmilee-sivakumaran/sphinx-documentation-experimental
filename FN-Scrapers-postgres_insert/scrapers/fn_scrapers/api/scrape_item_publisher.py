from __future__ import absolute_import

from fn_rabbit.async_rabbit import BasicProperties

from fn_service.server import RequestProcessId, LoggerState, per_request
from fn_service.util.request_context_util import build_request_context_rmq_value

from fn_scrapers.api.resources import (
    ScrapeStartTime,
    BlockingRetryingPublisherManager, 
    ScraperArguments,
)

import injector

import json
import threading
import time
import uuid

from .scraper import argument_function


_FILE_LOCK = threading.Lock()


def _scrape_item_publisher_args(parser):
    parser = parser.add_argument_group("Publishing Options")
    parser.add_argument(
        "--save-local", 
        help="Save published messages to a local file. This file will be written in "
             "the fn_rabbit_tool format.",
        metavar="FILE")
    parser.add_argument(
        "--dont-publish", 
        help="Don't publish messages to RabbitMQ.",
        action="store_false",
        dest="publish",
        default=True)


@per_request
@argument_function(_scrape_item_publisher_args)
class ScrapeItemPublisher(object):
    @injector.inject(
        process_id=RequestProcessId,
        scrape_start_time=ScrapeStartTime,
        blocking_retrying_publisher_manager=BlockingRetryingPublisherManager,
        scraper_args=ScraperArguments,
        logger_state=LoggerState)
    def __init__(
            self, 
            process_id, 
            scrape_start_time, 
            blocking_retrying_publisher_manager, 
            scraper_args,
            logger_state=LoggerState):
        if not hasattr(scraper_args, "save_local") or not hasattr(scraper_args, "publish"):
            raise Exception("Can't find command line arguments.")

        self.process_id = process_id
        self.scrape_start_time = scrape_start_time
        self.blocking_retrying_publisher_manager = blocking_retrying_publisher_manager
        self.save_local = scraper_args.save_local
        self.publish = scraper_args.publish
        self.logger_state = logger_state

    def publish_json_item(self, exchange, routing_key, source, json_item, json_encoder=None):
        """
        Send message to rabbitmq queue
        """
        json_message = {
            'document': json_item,
            'process_id': self.process_id,
            'session_start_time': self.scrape_start_time.isoformat(),
            'source': source,
        }

        message = json.dumps(json_message, cls=json_encoder)

        if self.save_local:
            with _FILE_LOCK, open(self.save_local, "ab") as f:
                f.write(b''.join([b"%0.10d:" % len(message), message, b"\n"]))

        if self.publish:
            request_context = self.logger_state.get_request_context()

            properties = BasicProperties(
                delivery_mode=2,  # 2 = persistent
                message_id=bytes(uuid.uuid4()),
                timestamp=int(time.time()),
                content_type="application/json",
                content_encoding="utf-8",  # technically its ASCII, but, ASCII is a subset of UTF-8
                headers={"X-Fn-Request-Context": build_request_context_rmq_value(request_context)},
            )

            self.blocking_retrying_publisher_manager.publish(
                exchange,
                routing_key,
                message,
                properties=properties)
            self.blocking_retrying_publisher_manager.flush()

            # TODO: Do we need to send out OK events?

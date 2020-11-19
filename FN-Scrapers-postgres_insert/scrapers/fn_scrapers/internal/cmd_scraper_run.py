from __future__ import absolute_import

from .config import get_config
from .fn_service_util import FnServiceConfigModule, get_global_setup_config
from .scraper_handler import create_scraper_handler
from .scraper_internal import get_scraper_handler_modules, get_scraper_name, get_tags
from . import log_filter

from fn_scrapers.api.resources import (
    AppModule,
    BlockingRetryingPublisherManager,
    ScraperModule,
    ScraperRequestModule,
    ScraperArguments,
    ScraperUtilsSupportModule,
)

import injector

import pytz

import yaml

import datetime
import io
import logging
import importlib
import os

from twisted.internet import defer, task

from fn_service.server import (
    async_setup,
    global_setup,
    per_app,
    run_in_new_thread,
    MessageFailedError,
    InvalidRequestError,
    BlockingEventLogger,
    RmqConnectionModule,
    RmqConnection,
    DirectEndpointModule,
    DirectHandler,
    RequestContext,
)
from fn_service.components.config import parse_config
from fn_service.components.dispatcher import create_isolated_handler
from fn_service.components.scheduler import PostRunIntervalSchedule, ScheduledTask, SchedulerModule
from fn_service.shutdown_helper import ShutdownHelper


logger = logging.getLogger(__name__)


@per_app
@run_in_new_thread("ping_parent")
class PingParent(object):
    @injector.inject(args=ScraperArguments, log=BlockingEventLogger)
    def __init__(self, args, log):
        self.parent_pipe_fd = args.parent_pipe_fd
        self.log = log

    def ping_parent(self):
        import errno
        try:
            os.write(self.parent_pipe_fd, 'x')
        except OSError as e:
            if e.errno == errno.EPIPE:
                self.log.critical(__name__, "scraper_parent_exited", u"Parent exited. We're going to exit too!")
                self.log.flush()
                os._exit(1)
            raise


@defer.inlineCallbacks
def _run_async(reactor, args, config):
    scraper_class_module_name, scraper_class_name = args.scraper_class
    scraper_class_module = importlib.import_module(scraper_class_module_name)
    scraper_class = getattr(scraper_class_module, scraper_class_name)

    key = injector.Key(get_scraper_name(scraper_class))

    inj_modules = get_scraper_handler_modules(scraper_class)
    inj_modules.append(ScraperModule(get_scraper_name(scraper_class)))

    direct_endpoint_module = DirectEndpointModule(
        async_handler_dict={
            key: DirectHandler(create_isolated_handler(create_scraper_handler(scraper_class), inj_modules))
        }
    )

    monitor_pid_endpoint = []
    if args.parent_pipe_fd is not None:
        monitor_pid_endpoint.append(
            SchedulerModule([
                ScheduledTask(
                    PostRunIntervalSchedule(args.ping_time),
                    PingParent,
                    "ping_parent",
                )
            ])
        )

    if args.working_dir:
        os.chdir(args.working_dir)

    rmq_connection_module = RmqConnectionModule(
        blocking_connections={
            BlockingRetryingPublisherManager: RmqConnection(),
        }
    )

    inj = yield async_setup(
        reactor,
        [AppModule(args), ScraperUtilsSupportModule],
        [direct_endpoint_module, rmq_connection_module] + monitor_pid_endpoint + [FnServiceConfigModule],
        config
    )

    context = RequestContext(request_modules=[ScraperRequestModule(
        datetime.datetime.now(pytz.UTC),
        get_tags(scraper_class))])
    try:
        yield inj.get(key).create_handler().scrape(context)
    except (MessageFailedError, InvalidRequestError):
        logger.exception("Scraper failed")
        # Use exit code 3 because 1 is used for unhandled Exceptions and
        # 2 means a failure to parse the command line arguments.
        import sys
        sys.exit(3)
    except:
        logger.exception("Scraper failed with unexpected Exception")
        raise
    finally:
        yield inj.get(ShutdownHelper).async_shutdown()


def scrape(args):
    if args.parent_pipe_fd:
        log_filter.IS_SCHEDULED_SCRAPER = True

    global_setup(get_global_setup_config())
    config = parse_config(yaml.safe_load(io.BytesIO(get_config("config.yaml"))))
    task.react(_run_async, argv=(args, config))

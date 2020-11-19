from __future__ import absolute_import

import yaml
import io

from .config import get_config
from .fn_service_util import FnServiceConfigModule, get_global_setup_config
from .scheduler import Scheduler
from .serve_until import create_serve_until_task

from fn_scrapers.api.resources import (
    AppModule,
    ScraperUtilsSupportModule,
    BlockingRetryingPublisherManager,
)

from fn_service.server import (
    run_server,
    RmqConnectionModule,
    RmqConnection,
)
from fn_service.components.config import parse_config
from fn_service.components.background import BackgroundTask, BackgroundTaskModule, EarlyCompletionBehavior
from fn_service.components.scheduler import SchedulerModule


def serve(args):
    modules = []

    if args.serve_until:
        modules.append(SchedulerModule([create_serve_until_task(args.serve_until)]))

    modules.append(BackgroundTaskModule([
        BackgroundTask(EarlyCompletionBehavior.EXIT, Scheduler, "schedule")
    ]))

    modules.append(RmqConnectionModule(
        blocking_connections={
            BlockingRetryingPublisherManager: RmqConnection(),
        }
    ))

    modules.append(FnServiceConfigModule)

    run_server(
        [AppModule(args), ScraperUtilsSupportModule],
        modules,
        config=parse_config(yaml.safe_load(io.BytesIO(get_config("config.yaml")))),
        global_setup_config=get_global_setup_config()
    )

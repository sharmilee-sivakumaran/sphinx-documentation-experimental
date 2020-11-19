from __future__ import absolute_import

import os

import injector

from twisted.internet import defer, threads

from fn_service.server import run_async, Reactor, AsyncEventLogger
from fn_service.components.scheduler import PostRunIntervalSchedule, ScheduledTask
from fn_service.shutdown_helper import ShutdownHelper


def _create_serve_until_handler(serve_until_time, serve_until_path):
    @run_async()
    class ServeUntilHandler(object):
        @injector.inject(reactor=Reactor, shutdown_helper=ShutdownHelper, log=AsyncEventLogger)
        def __init__(self, reactor, shutdown_helper, log):
            self.reactor = reactor
            self.shutdown_helper = shutdown_helper
            self.log = log

        @defer.inlineCallbacks
        def async_serve_until(self):
            if self.shutdown_helper.is_shutting_down:
                return
            stat = yield threads.deferToThreadPool(
                self.reactor,
                self.reactor.getThreadPool(),
                os.stat,
                serve_until_path)
            # Format the time into a string for the comparision - this mirrors what the caller has to do.
            if "{}".format(stat.st_mtime) != serve_until_time:
                yield self.log.async_info(__name__, u"Noticed update to file '{}' - stopping".format(serve_until_path))
                self.shutdown_helper.async_shutdown()

    return ServeUntilHandler


def create_serve_until_task(serve_until_arg):
    serve_until_time, serve_until_path = serve_until_arg.split(':', 1)
    if not os.path.exists(serve_until_path):
        raise Exception("--serve-until specified, but path doesn't exist: '{}'".format(serve_until_path))
    return ScheduledTask(
        PostRunIntervalSchedule(60),
        _create_serve_until_handler(serve_until_time, serve_until_path),
        "async_serve_until")

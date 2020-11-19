from __future__ import absolute_import

import attr
import fcntl
import time
import select
import os
import subprocess32
import sys
import errno

from fn_service.server import fmt

from .config import get_config_fd, get_config_names
from .resource_process import ResourceProcess
from .unix_util import CloseFds


ACTION_LOG = "LOG"
ACTION_TERMINATE = "TERMINATE"
ACTION_FAIL = "FAIL"


@attr.s
class WorkerEvent(object):
    occurs_at = attr.ib()
    message = attr.ib()
    action = attr.ib()


class WorkerTerminated(Exception):
    pass


class WorkerFailed(Exception):
    pass


class _EventSet(object):
    def __init__(self, events):
        self.events = events

    def time_until_next(self, now):
        if self.events:
            return min(e.occurs_at - now for e in self.events)
        else:
            return None

    def process_events(self, now, log):
        idx = 0
        while idx < len(self.events):
            event = self.events[idx]
            if event.occurs_at < now:
                log.critical(__name__, "worker_failed", event.message)
                if event.action == ACTION_LOG:
                    pass
                elif event.action == ACTION_TERMINATE:
                    raise WorkerTerminated()
                elif event.action == ACTION_FAIL:
                    raise WorkerFailed()
                else:
                    raise Exception(u"Unknown action: {}".format(event.action))
                del self.events[idx]
            else:
                idx += 1


def _min_non_none(*values):
    return min(x for x in values if x is not None)


def _monitor_worker(log, monitor_period, event_set, r, process, worker_name, ping_func):
    # We need the read half of the pipe to be in non-blocking mode. Otherwise,
    # if we try to read from the pipe and there is no data there, we'll hang
    # forever which defeats the point of trying to check if the child is hung.
    fcntl.fcntl(r, fcntl.F_SETFL, fcntl.fcntl(r, fcntl.F_GETFL) | os.O_NONBLOCK)

    ping_by = time.time() + monitor_period
    while True:
        # Wait for some data to show up, or a timeout.
        # NOTE: We also wait for an "exceptionl condition" on the pipe. Its not
        # clear what this actually means. But, if one does occur and it indicated
        # an error, we should be awoken so that we can notice that error
        # when we attempt to read from the pipe.
        now = time.time()
        timeout = _min_non_none(ping_by - now, event_set.time_until_next(now))
        timeout = max(0, timeout)
        select.select([r], [], [r], timeout)

        now = time.time()

        event_set.process_events(now, log)

        # Read all the data - any data available indicates
        # that a ping was sent
        has_ping = False
        while True:
            try:
                result = os.read(r, 1024)
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    break
                else:
                    log.critical(
                        __name__,
                        "worker_failed",
                        fmt(u"{} failed. Could not read from pipe", worker_name),
                        exc_info=True)
                    raise
            if len(result):
                # If we got some data back, record that we got a ping
                # for this cycle.
                has_ping = True
            else:
                # if we got a read of 0, even after reading other data,
                # it means that the pipe is closed. This should only
                # occur when the worker exits. So, check that the worker
                # exited and log the appropriate message.
                try:
                    process.wait(timeout=5)
                except subprocess32.TimeoutExpired:
                    # The pipe is closed, but, the worker is still running.
                    # This shouldn't happen - the worker must have failed
                    # in some way.
                    log.critical(
                        __name__,
                        "worker_failed",
                        fmt(u"Pipe closed, but {} is still running"), worker_name)
                    raise WorkerFailed()

                if process.returncode == 0:
                    return
                else:
                    raise WorkerFailed()

        if has_ping:
            log.debug(__name__, fmt("Received ping from {}", worker_name))
            ping_by = now + monitor_period

            ping_func()

        if now > ping_by:
            # timeout occured - kill the worker
            log.critical(__name__, "worker_failed", fmt(u"{} is unresponsive. Killing it.", worker_name))
            raise WorkerFailed()


def _pass_configs(closing_fds):
    args = []
    config_fds = []
    for config_name in get_config_names():
        fd = get_config_fd(config_name, cloexec=False)
        closing_fds.add(fd)
        config_fds.append(fd)
        args.extend(["--config-from-fd", "{}:{}".format(fd, config_name)])
    return args, config_fds


def _setup_working_dir(scraper_working_dir):
    if scraper_working_dir:
        import shutil
        if os.path.isdir(scraper_working_dir):
            shutil.rmtree(scraper_working_dir)
        os.makedirs(scraper_working_dir)
        return ["--working-dir", scraper_working_dir]
    else:
        return []


def _report_resources(r):
    return {
        "utime": r.ru_utime,
        "stime": r.ru_stime,
        "maxrss": r.ru_maxrss,
        "ixrss": r.ru_ixrss,
        "idrss": r.ru_idrss,
        "isrss": r.ru_isrss,
        "minflt": r.ru_minflt,
        "majflt": r.ru_majflt,
        "nswap": r.ru_nswap,
        "inblock": r.ru_inblock,
        "oublock": r.ru_oublock,
        "msgsnd": r.ru_msgsnd,
        "msgrcv": r.ru_msgrcv,
        "nsignals": r.ru_nsignals,
        "nvcsw": r.ru_nvcsw,
        "nivcsw": r.ru_nivcsw,
    }


def run_worker(
        log,
        scraper_working_dir,
        scraper_name,
        scraper_args,
        monitor_period,
        ping_time,
        worker_name,
        events,
        ping_func):
    event_set = _EventSet(events)

    # Try to start the child processse the pipe!
    with CloseFds() as closing_fds:
        log.info(__name__, fmt(u"Starting {}".format(worker_name)))

        r, w = os.pipe()
        closing_fds.add_all([r, w])

        config_args, config_fds = _pass_configs(closing_fds)

        working_dir_args = _setup_working_dir(scraper_working_dir)

        # NOTE: We start the worker using its file path, as opposed to -m,
        # since using -m adds the current working directory to sys.modules,
        # and, there is no good reason to do that.
        process = subprocess32.Popen(
            [
                sys.executable, sys.modules['__main__'].__file__,
            ] + config_args + [
                "scraper", "run",
                "--parent-pipe-fd", str(w),
                "--ping-time", str(ping_time),
            ] + working_dir_args + [
                scraper_name,
            ] + scraper_args,
            pass_fds=[w] + config_fds)

        process = ResourceProcess(process)

        log.info(__name__, fmt(u"{} running with pid {}".format(worker_name, process.pid)))

        try:
            # Close the write half of the pipe - we won't be writing to the pipe,
            # just reading.
            closing_fds.remove(w)
            CloseFds([w]).close()

            # Close the read half of the config pipes - we won't read them, the
            # new child will
            closing_fds.remove_all(config_fds)
            CloseFds(config_fds).close()

            _monitor_worker(log, monitor_period, event_set, r, process, worker_name, ping_func)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()

            if process.returncode == 0:
                log.info(
                    __name__,
                    fmt(u"{} completed with code 0.", worker_name, process.resource_usage),
                    extra_info={"resources_used": _report_resources(process.resource_usage)})
            elif process.returncode > 0:
                log.critical(
                    __name__,
                    "worker_failed",
                    fmt(u"{} failed with code: {}.",
                        worker_name,
                        process.returncode,
                        process.resource_usage),
                    extra_info={"resources_used": _report_resources(process.resource_usage)})
            else:
                log.critical(
                    __name__,
                    "worker_failed",
                    fmt(u"{} failed due to signal: {}.",
                        worker_name,
                        -process.returncode,
                        process.resource_usage),
                    extra_info={"resources_used": _report_resources(process.resource_usage)})

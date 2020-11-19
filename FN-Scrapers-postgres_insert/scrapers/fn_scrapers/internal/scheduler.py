from __future__ import absolute_import, division

import attr
import injector
import contextlib
from datetime import datetime, timedelta
import platform
import uuid
import json
import time
import pytz

from fn_service.components.dispatcher import run_in_new_thread
from fn_service.server import fmt, BlockingAppStatus, ServerShutdown, BlockingEventLogger, watchdog

from sqlalchemy.exc import SQLAlchemyError

from fn_scrapers.api.resources import ScraperDbSessionMaker, ScraperArguments

from .schedule import Schedule, PG_NOW
from .scheduler_util import get_schedule_start_times, get_schedule_events, schedule_name
from .run_and_monitor_scraper import run_worker, WorkerFailed, WorkerTerminated


NODE = platform.node()

# PING_TIME is used to tell the worker process how frequently it should
# ping its parent scheduler to inform it that its still alive and well. Whenever the
# worker process pings its parent scheduler, that  schedule also updates the database
# to tell all of the other scheduler processes that that particular scraper is
# still running.
PING_TIME = 60

# KILL_TIME is used to configure how long we should wait between pings before we
# decide that the worker process must be hung or otherwise broken. If this time
# expires without a ping, then, we kill the worker and update the DB to indicate
# that the scraper is no longer running.
KILL_TIME = PING_TIME * 2

# STEAL_TIME defines how long a scheduler will wait without seeing an update in the
# db for a particular scraper before deciding that that scraper and its scheduler process
# must have crashed. When the scheduler decides this, it will start to "steal" the work.
# So, STEAL_TIME defines how long must elapse from the last ping of the DB until some other
# scheduler will attempt to "steal" the work.
STEAL_TIME = PING_TIME * 2

# If the DB is unavailable for a while, that might result in all schedulers failing to update
# the DB for quite a while. When the DB comes back up, all schedulers will look at the DB and
# all work will appear to be stuck and ready to be stolen. In order to handle this case better
# we require that stealing is a two step process - 1) mark stealing as starting and then wait
# a while; 2) if there is no update in that period, complete the steal, otherwise abandon the
# steal. STEAL_DELAY configures this duration.
STEAL_DELAY = PING_TIME * 10


class WorkStolen(Exception):
    pass


def _convert_events(events):
    from . import scheduler_util
    from . import run_and_monitor_scraper

    epoch = datetime(1970, 1, 1, tzinfo=pytz.UTC)

    return [
        run_and_monitor_scraper.WorkerEvent(
            occurs_at=(e.occurs_at - epoch).total_seconds(),
            message=e.message,
            action={
                scheduler_util.ACTION_LOG: run_and_monitor_scraper.ACTION_LOG,
                scheduler_util.ACTION_TERMINATE: run_and_monitor_scraper.ACTION_TERMINATE,
                scheduler_util.ACTION_FAIL: run_and_monitor_scraper.ACTION_FAIL,
            }[e.action]
        )
        for e in events
    ]


@attr.s
class ScheduleWithTimes(object):
    can_start_by = attr.ib()
    should_start_by = attr.ib()
    schedule = attr.ib()

    @staticmethod
    def create(now, schedule):
        can_start_by, should_start_by = get_schedule_start_times(now, schedule)
        return ScheduleWithTimes(can_start_by, should_start_by, schedule)


@run_in_new_thread()
@watchdog(None)
class Scheduler(object):
    @injector.inject(
        bbs=BlockingAppStatus,
        log=BlockingEventLogger,
        session_maker=ScraperDbSessionMaker,
        args=ScraperArguments)
    def __init__(self, bbs, log, session_maker, args):
        self.bbs = bbs
        self.log = log
        self.session_maker = session_maker
        self.args = args

    def _mark_work(self, now, schedule):
        # NOTE: we have to pass "now" into this function explicitly since if we
        # set attributes with PG_NOW, they will be left in an expired state
        # when we commit (regardless of if we set expire_on_commit or not).
        if schedule.owner_tag is not None and schedule.steal_start_at is None:
            # The work is owned, but stealable. So, mark the start of a steal period.
            schedule.steal_start_at = now
            self.log.debug(__name__, "Starting to steal")
            return None
        elif schedule.owner_tag is not None:
            # The work is stealable and its been long enough that we can steal it.
            # Mark it as available. Don't start running it though - we need to
            # run the scheduling algorithms to make sure it can actually start and
            # that there isn't higher priority work to be done.
            schedule.failure_count = schedule.failure_count + 1 if schedule.failure_count else 1
            schedule.last_start_at = schedule.owner_start_at
            schedule.last_end_at = now
            schedule.owner_node = None
            schedule.owner_name = None
            schedule.owner_tag = None
            schedule.owner_start_at = None
            schedule.owner_last_ping_at = None
            schedule.steal_start_at = None
            return None
        else:
            # The work is elligable to run - so, run it!
            tag = uuid.uuid4()
            schedule.owner_node = NODE
            schedule.owner_name = self.args.scheduler_name
            schedule.owner_tag = tag
            schedule.owner_start_at = now
            schedule.owner_last_ping_at = now
            schedule.steal_start_at = None
            schedule.run_immediately = None
            self.log.debug(__name__, "Marked row as ready to start")
            return tag

    def _ping_work(self, tag, schedule):
        try:
            with contextlib.closing(self.session_maker()) as session:
                s = session\
                    .query(Schedule)\
                    .filter(Schedule.scraper_name == schedule.scraper_name)\
                    .with_for_update()\
                    .one_or_none()
                if s is None:
                    # This is weird - stuff shouldn't generally get deleted
                    # If this occurs, we exit since we can't ping the db anymore
                    self.log.critical(__name__, "work_stolen", "Schedule Deleted. Killing scraper")
                    raise WorkStolen()
                elif s.owner_tag == tag:
                    s.owner_last_ping_at = PG_NOW
                    s.steal_start_at = None
                    session.commit()
                    if s.kill_immediately:
                        self.log.warning(__name__, "killing_scraper", fmt("Killing {}", schedule.scraper_name))
                        s.kill_immediately = False
                        session.commit()
                        raise WorkerFailed()
                else:
                    self.log.critical(__name__, "work_stolen", "Work stolen. Killing scraper")
                    raise WorkStolen()
        except SQLAlchemyError:
            self.log.critical(__name__, "work_update_failed", "Failed to ping work", exc_info=True)
            # We don't retry here - we'll retry the next time the worker pings us

    def _complete_work(self, tag, schedule):
        # Yay - worker finished. We retry in a loop until we can update the
        # db.
        while True:
            try:
                with contextlib.closing(self.session_maker()) as session:
                    now = session.query(PG_NOW).scalar()

                    s = session\
                        .query(Schedule)\
                        .filter(Schedule.scraper_name == schedule.scraper_name)\
                        .with_for_update()\
                        .one_or_none()
                    if s is None:
                        # Unusual - but not much to do about it
                        self.log.debug(__name__, "Couldn't get schedule row")
                        break
                    elif s.owner_tag == tag:
                        s.last_good_start_at = s.owner_start_at
                        s.last_good_end_at = now
                        s.last_start_at = s.owner_start_at
                        s.last_end_at = now
                        s.failure_count = 0
                        if s.average_good_duration is None:
                            s.average_good_duration = now - s.owner_start_at
                        else:
                            s.average_good_duration = timedelta(
                                seconds=0.37 * (now - s.owner_start_at).total_seconds() +
                                (1 - 0.37) * s.average_good_duration.total_seconds())

                        if s.run_immediately is not None and s.run_immediately <= s.owner_start_at:
                            # If run_immediately is set, we want to unset it since we completed
                            # a run. However, we only want to do this if it was set BEFORE
                            # we started running. If it was set afterwards, the user may be indicating
                            # that they want to kick off an immediate 2nd run.
                            # NOTE: The run_immediately flag used to cause a scraper to run as frequently
                            # as possible once it was set until it completed a run - ignoring backoff for
                            # failures. That didn't seem to be the behavior that was really desired, however.
                            # So, we changed it to get unset as soon as the work is marked as running - ie,
                            # it only causes a single execution to happen before it normally would. Anyway,
                            # for the time being, we left the type of this column as a datetime. It might
                            # make sense to change its type in the future.
                            s.run_immediately = None

                        s.owner_node = None
                        s.owner_name = None
                        s.owner_tag = None
                        s.owner_start_at = None
                        s.owner_last_ping_at = None
                        s.steal_start_at = None

                        session.commit()

                        self.log.debug(__name__, "Marked work as completed")

                        return
                    else:
                        # Unusual - but not much to do about it
                        self.log.debug(__name__, "Schedule seems to have been stolen")
                        break
            except SQLAlchemyError:
                self.log.critical(__name__, "work_update_failed", "Failed to complete work. Will retry.", exc_info=True)
                time.sleep(5)

    def _fail_work(self, tag, schedule, increment_failures):
        while True:
            try:
                with contextlib.closing(self.session_maker()) as session:
                    s = session\
                        .query(Schedule)\
                        .filter(Schedule.scraper_name == schedule.scraper_name)\
                        .with_for_update()\
                        .one_or_none()
                    if s is None:
                        # Unusual - but not much to do about it
                        self.log.debug(__name__, "Couldn't get schedule row")
                        break
                    elif s.owner_tag == tag:
                        if increment_failures:
                            s.failure_count = s.failure_count + 1 if s.failure_count else 1
                        s.last_start_at = s.owner_start_at
                        s.last_end_at = PG_NOW

                        s.owner_node = None
                        s.owner_name = None
                        s.owner_tag = None
                        s.owner_start_at = None
                        s.owner_last_ping_at = None
                        s.steal_start_at = None

                        session.commit()
                        
                        self.log.debug(__name__, "Marked work as failed")
                        return
                    else:
                        # Unusual - but not much to do about it
                        self.log.debug(__name__, "Schedule seems to have been stolen")
                        break
            except SQLAlchemyError:
                self.log.critical(__name__, "work_update_failed", "Failed to complete work. Will retry.", exc_info=True)
                time.sleep(5)

    def _do_acquire_work(self):
        sleep_time = 0
        while True:
            # Sleep until the next time we should check for work to run
            self.bbs.sleep_until_shutdown(sleep_time)

            # Go get all the schedules to examine
            with contextlib.closing(self.session_maker(expire_on_commit=False)) as session:
                schedules = session.query(Schedule).with_for_update().all()

                now = session.query(PG_NOW).scalar()

                # Items that haven't been pinged since this time are elligable to be run.
                stealable_after = now - timedelta(seconds=STEAL_TIME)
                steal_complete_after = now - timedelta(seconds=STEAL_DELAY)

                def is_eligible(s):
                    return NODE not in s.exclude_nodes and\
                        (s.enabled or s.run_immediately is not None) and\
                        (s.owner_tag is None or s.owner_last_ping_at <= stealable_after) and\
                        (s.steal_start_at is None or s.steal_start_at <= steal_complete_after)

                # Filter out schedules we aren't eligable to run
                eligible_schedules = [s for s in schedules if is_eligible(s)]

                # Find the work most deserving of being run and try to mark it as running by us
                if not eligible_schedules:
                    self.log.debug(__name__, "No schedules eligible to run.")
                    sleep_time = 60
                else:
                    # Calculate the next start times for all eligible schedules.
                    schedules_with_times = [ScheduleWithTimes.create(now, s) for s in eligible_schedules]

                    # Sort eligible schedules by their can_start_by values
                    by_availability = sorted(
                        schedules_with_times,
                        key=lambda x: (x.can_start_by, x.schedule.scraper_name))

                    next_available = by_availability[0]

                    if next_available.can_start_by > now:
                        self.log.debug(
                            __name__,
                            fmt(u"{} is next to run, but in the future", next_available.schedule.scraper_name))
                        sleep_time = min((next_available.can_start_by - now).total_seconds(), 60)
                    else:
                        # Sort runnable schedules by their should_start_by values
                        by_need = sorted(
                            (s for s in schedules_with_times if s.can_start_by <= now),
                            key=lambda x: (x.should_start_by, x.can_start_by, x.schedule.scraper_name))

                        schedule = by_need[0].schedule

                        self.log.debug(__name__, fmt(u"Attempting run {}", schedule.scraper_name))

                        tag = self._mark_work(now, schedule)
                        session.commit()
                        if tag is not None:
                            return now, tag, schedule
                        else:
                            sleep_time = 0

    def _acquire_work(self):
        while True:
            try:
                return self._do_acquire_work()
            except SQLAlchemyError:
                self.log.critical(__name__, "mark_work", "Failed to mark work", exc_info=True)

    def _invoke_scraper(self, now, tag, schedule):
        events = _convert_events(get_schedule_events(now, schedule))

        run_worker(
            self.log,
            self.args.scraper_working_dir,
            schedule.scraper_name,
            json.loads(schedule.scraper_args) if schedule.scraper_args else [],
            KILL_TIME,
            PING_TIME,
            schedule_name(schedule),
            events,
            ping_func=lambda: self._ping_work(tag, schedule))

    def schedule(self):
        while True:
            try:
                now, tag, schedule = self._acquire_work()
                try:
                    self._invoke_scraper(now, tag, schedule)
                    self._complete_work(tag, schedule)
                except (WorkerFailed, WorkStolen):
                    self._fail_work(tag, schedule, increment_failures=True)
                except WorkerTerminated:
                    self._fail_work(tag, schedule, increment_failures=False)
            except ServerShutdown:
                return

from __future__ import absolute_import

import attr
import croniter
from datetime import datetime, timedelta
import json
import pytz
from .duration_format import format_duration
from .period_set import PeriodSet, TimePeriod


CONDITION_OK = "OK"
CONDITION_WARNING = "WARNING"
CONDITION_ERROR = "ERROR"

ACTION_LOG = "LOG"
ACTION_TERMINATE = "TERMINATE"
ACTION_FAIL = "FAIL"


@attr.s
class ScheduleEvent(object):
    occurs_at = attr.ib()
    message = attr.ib()
    action = attr.ib()


def is_schedule_running(schedule):
    return schedule.owner_tag is not None


def has_schedule_completed_before(schedule):
    return schedule.last_good_end_at is not None


def has_schedule_attempted_before(schedule):
    return schedule.last_end_at is not None


def schedule_name(schedule):
    return schedule.scraper_name


def _create_blackout_period_set(tz, blackout_periods):
    def _parse_time(t):
        try:
            return datetime.strptime(t, "%H:%M:%S").time()
        except ValueError:
            return datetime.strptime(t, "%H:%M:%S.%f").time()

    # It seems like this will sometimes come back as a list, mostly
    # if its empty. So, if its already a list, use that. Otherwise,
    # parse it as JSON.
    if blackout_periods is None:
        blackout_periods = []
    elif isinstance(blackout_periods, list):
        blackout_periods = blackout_periods
    else:
        blackout_periods = json.loads(blackout_periods)

    return PeriodSet(
        [TimePeriod(_parse_time(spec["start"]), _parse_time(spec["end"])) for spec in blackout_periods],
        pytz.timezone(tz))


def _get_schedule_start_times_periodic(schedule):
    if has_schedule_completed_before(schedule):
        can_start_by = schedule.last_good_end_at + schedule.cooldown_duration
        must_end_by = schedule.last_good_start_at + schedule.scheduling_period
        should_start_by = must_end_by - schedule.average_good_duration
    else:
        can_start_by = schedule.created_at
        should_start_by = schedule.created_at

    return can_start_by, should_start_by


def _next_cron_time(schedule):
    tz = pytz.timezone(schedule.tz)

    if has_schedule_completed_before(schedule):
        ref_time = schedule.last_good_start_at
    else:
        ref_time = schedule.created_at
    ref_time = ref_time.astimezone(tz)

    c = croniter.croniter(schedule.cron_schedule, ref_time)
    next_time_local = c.get_next(datetime)
    next_time_utc = next_time_local.astimezone(pytz.UTC)

    return next_time_utc


def _get_schedule_start_times_cron(schedule):
    can_start_by = _next_cron_time(schedule)

    if has_schedule_completed_before(schedule):
        must_end_by = can_start_by + schedule.cron_max_schedule_duration
        should_start_by = must_end_by - schedule.average_good_duration
    else:
        should_start_by = can_start_by

    return can_start_by, should_start_by


def _backoff_until(schedule):
    if schedule.failure_count == 0:
        raise Exception("Schedule has not failed")
    elif schedule.failure_count == 1:
        return schedule.last_end_at + timedelta(minutes=5)
    elif schedule.failure_count == 2:
        return schedule.last_end_at + timedelta(hours=1)
    else:
        return schedule.last_end_at + timedelta(hours=4)


def get_schedule_start_times(now, schedule):
    """
    For a given schedule, get the earlier time that we can run it next,
    can_start_by, and the latest that we should start it by, should_start_by.
    can_start_by effectively works as a flag - if its less than NOW, we can
    run the schedule, if its greater, we can't. should_start_by works as a priority -
    the farther in the past it is, the higher the priority.
    """
    if schedule.cron_schedule is None:
        can_start_by, should_start_by = _get_schedule_start_times_periodic(schedule)
    else:
        can_start_by, should_start_by = _get_schedule_start_times_cron(schedule)

    # Don't permit should_start_by to be before the last time we completed.
    # If this were allowed, it could result in schedules that ran more recently
    # getting prioritized unfairly over schedules that ran less recently.
    if has_schedule_completed_before(schedule):
        should_start_by = max(schedule.last_good_end_at, should_start_by)

    # If we have failed, apply a backoff before we try again
    if schedule.failure_count > 0:
        can_start_by = max(can_start_by, _backoff_until(schedule))

    # If the run_immediately flag is set, allow the schedule to start right now
    # regardless of when it would normally be scheduled (except for blackout restrictions)
    if schedule.run_immediately is not None:
        can_start_by = min(can_start_by, schedule.run_immediately)

    # Account for blackout periods
    if schedule.blackout_periods is not None:
        blackout_periods = _create_blackout_period_set(schedule.tz, schedule.blackout_periods)
        if blackout_periods.is_in_period(now):
            # If we're currently in the middle of a blackout period, make sure that
            # can_start_by falls after that period has ended (can_start_by might be
            # set to a value earlier than when the period started - so we need to
            # make sure that we don't kick off work we'll need to immediately kill).
            can_start_by = max(can_start_by, blackout_periods.next_period_end(now))
        if blackout_periods.is_in_period(can_start_by):
            # If can_start_by falls in the middle of a blackout period, adjust it
            # so that can_start_by is set to when the period ends
            can_start_by = max(can_start_by, blackout_periods.next_period_end(can_start_by))

    return can_start_by, should_start_by


def get_schedule_events(now, schedule):
    """
    get_schedule_events is called immediately after we decide to start running
    a schedule. It returns a list of events that should be triggered while
    that schedule is running, if it runs for long enough. Those events can
    cause the schedule to be killed with an error, killed without an error,
    and/or for messages to be logged.

    NOTE: The values passed to this function must be the same as the ones
    that were passed to get_schedule_start_times() when we decided to run the
    schedule, otherwise the events may be incorrect.
    """
    events = []

    if schedule.blackout_periods is not None:
        blackout_periods = _create_blackout_period_set(schedule.tz, schedule.blackout_periods)
        next_blackout_start = blackout_periods.next_period_start(now)
        # Entering a blackout period and having to kill the scraper isn't an error by
        # itself. Its only an error if that causes us to fall behind schedule - but we
        # can't know if that is the case here.
        events.append(ScheduleEvent(
            occurs_at=next_blackout_start,
            message=u"WARNING: {} entered a blackout period".format(schedule_name(schedule)),
            action=ACTION_TERMINATE,
        ))

    if schedule.max_allowed_duration is not None:
        events.append(ScheduleEvent(
            occurs_at=now + schedule.max_allowed_duration,
            message=u"ERROR: {} has been running for more than max_allowed_duration of {}".format(
                schedule_name(schedule),
                format_duration(schedule.max_allowed_duration)
            ),
            action=ACTION_FAIL,
        ))

    if schedule.max_expected_duration is not None:
        events.append(ScheduleEvent(
            occurs_at=now + schedule.max_expected_duration,
            message=u"WARNING: {} has been running for more than max_expected_duration of {}".format(
                schedule_name(schedule),
                format_duration(schedule.max_expected_duration)
            ),
            action=ACTION_LOG,
        ))

    return events


def get_schedule_condition(now, tz_name, datetime_format, schedule):
    """
    Return the condition of the schedule - a code value: CONDITION_OK,
    CONDITION_WARNING, or CONDITION_ERROR, and a descriptive string explaining
    why the code value was chosen.
    """
    tz = pytz.timezone(tz_name)

    condition = [CONDITION_OK, []]

    def _format_dt(dt):
        return dt.astimezone(tz).strftime(datetime_format)

    # This is just a helper function that updates the condition variables
    def _condition(new_cond, description):
        if new_cond == CONDITION_ERROR:
            condition[0] = CONDITION_ERROR
        elif new_cond == CONDITION_WARNING and condition[0] == CONDITION_OK:
            condition[0] = CONDITION_WARNING
        condition[1].append(description)

    if has_schedule_completed_before(schedule):
        if schedule.cron_schedule is None:
            required_end = schedule.last_good_start_at + schedule.scheduling_period

            if now > required_end:
                _condition(
                    CONDITION_ERROR,
                    u"ERROR: Behind. Should have finished by: {}".format(_format_dt(required_end)))
            else:
                if is_schedule_running(schedule):
                    # Schedule is running: We can estimate when it will finish
                    expected_end = schedule.owner_start_at + schedule.average_good_duration
                else:
                    # Schedule isn't running: We can estimate the earliest it could finish
                    # if started now.
                    expected_end = now + schedule.average_good_duration

                if expected_end > required_end:
                    _condition(
                        CONDITION_WARNING,
                        u"WARNING: Unlikely to finish by: {}".format(_format_dt(required_end)))
        else:
            required_end = _next_cron_time(schedule) + schedule.cron_max_schedule_duration

            if now > required_end:
                _condition(
                    CONDITION_ERROR,
                    u"ERROR: Behind. Should have finished by: {}".format(_format_dt(required_end)))
            else:
                if is_schedule_running(schedule):
                    # Schedule is running: We can estimate when it will finish
                    expected_end = schedule.owner_start_at + schedule.average_good_duration
                else:
                    # Schedule isn't running: We can estimate the earliest it could finish
                    # if started now.
                    expected_end = now + schedule.average_good_duration

                if expected_end > required_end:
                    _condition(
                        CONDITION_WARNING,
                        u"WARNING: Unlikely to finish by: {}".format(_format_dt(required_end)))
    else:
        if schedule.created_at < now - timedelta(hours=24):
            _condition(CONDITION_ERROR, u"ERROR: Hasn't ever completed")

    if schedule.failure_count > 2:
        # Failures aren't an error - its only when we fall behind schedule
        # that we have an error.
        # If we've only failed once or twice, don't report a warning since that
        # could be overly chatty. Its not that failures should be ignored, but,
        # transient failures don't rise to the level of needing to get immediate
        # notifications if they succeed after a 2nd or 3rd attempt.
        _condition(CONDITION_WARNING, u"WARNING: {} consecutive failure(s)".format(schedule.failure_count))

    return condition[0], condition[1]

from __future__ import absolute_import

import attr
import collections
from datetime import datetime, timedelta


@attr.s(slots=True)
class TimePeriod(object):
    # start and end are time objects
    start = attr.ib()
    end = attr.ib()


class InvalidPeriods(Exception):
    pass


TIME_MIN = 0
TIME_MAX = 24 * 60 * 60


@attr.s(slots=True)
class _Period(object):
    # start and end are integers between TIME_MIN and TIME_MAX, inclusive
    start = attr.ib()
    end = attr.ib()


def _split_periods(periods):
    # If a period crosses over midnight, split it into
    # two period - neither of which crosses over midnight
    out_periods = []
    for p in periods:
        if p.end > p.start:
            # Doesn't cross over midnight
            out_periods.append(p)
        elif p.end == TIME_MIN:
            # A range like: 23:00:00 - 00:00:00
            # We handle this by inserting a Period that looks
            # like: 23:00:00 - 24:00:00 which results in
            # start still being less than end.
            out_periods.append(_Period(p.start, TIME_MAX))
        else:
            # A range that crosses over midnight, such as:
            # 23:00:00 - 01:00:00. We represent this as
            # two periods, 23:00:00 - 24:00:00 and 00:00:00 - 01:00:00.
            out_periods.append(_Period(p.start, TIME_MAX))
            out_periods.append(_Period(TIME_MIN, p.end))
    return out_periods


def _fuse_periods(periods):
    # Take any periods that overlap and combine them into a
    # smaller set of sorted, non-overlapping periods
    periods = collections.deque(sorted(periods, key=lambda x: x.start))
    out_periods = []
    while periods:
        # Get the next period
        p = periods.popleft()

        # Try to figure out if the period overlaps with any other
        # periods. The periods are already sorted by start time.
        while periods:
            # 1. If two periods share the same start time, then, they must
            # overlap
            # 2. If the end time of one period occurs after the start
            # time of the next period, they must overlap
            # If either case is true, fuse them into a single period
            if p.start == periods[0].start or p.end >= periods[0].start:
                p = _Period(p.start, max(p.end, periods[0].end))
                periods.popleft()
            else:
                break
        out_periods.append(p)
    return out_periods


def _normalize_periods(periods):
    periods = _split_periods(periods)
    periods = _fuse_periods(periods)
    if len(periods) == 1 and periods[0].start == TIME_MIN and periods[0].end == TIME_MAX:
        raise InvalidPeriods(u"Periods must not cover the entire day")
    return periods


def _convert_to_periods(time_periods):
    def _to_time_secs(t):
        return t.hour * 60 * 60 + t.minute * 60 + t.second + t.microsecond / 1000000.0

    def _create_period(start, end):
        if start == end:
            raise InvalidPeriods(u"Start and end times are equal: {}".format(start))
        return _Period(start, end)

    return [_create_period(_to_time_secs(tp.start), _to_time_secs(tp.end)) for tp in time_periods]


def _day_seconds(dt):
    return (dt.replace(year=1970, month=1, day=1, tzinfo=None) - datetime(1970, 1, 1)).total_seconds()


def _time_to_next_start(day_seconds, periods):
    for p in periods:
        if p.start > day_seconds:
            return p.start - day_seconds
    return periods[0].start + TIME_MAX - day_seconds


def _time_to_next_end(day_seconds, periods):
    for p in periods:
        if p.end > day_seconds:
            if p.end == TIME_MAX and periods[0].start == TIME_MIN:
                # If p.end == TIME_MAX, that means we may have stumbled upon a
                # period that got split over midnight. If that's the case, we need
                # to see if there is an overlapping period. And, if so, we use
                # that period's end time
                return periods[0].end - day_seconds
            else:
                return p.end - day_seconds
    return periods[0].end + TIME_MAX - day_seconds


class PeriodSet(object):
    def __init__(self, set_spec, tz):
        self.periods = _normalize_periods(_convert_to_periods(set_spec))
        self.tz = tz

    def is_in_period(self, dt):
        if dt.tzinfo is None:
            raise Exception("Invalid naive datetime")
        if not self.periods:
            return False

        local_dt = dt.astimezone(self.tz)
        day_seconds = _day_seconds(local_dt)
        for period in self.periods:
            if period.start <= day_seconds < period.end:
                return True
        return False

    def has_periods(self):
        return bool(self.periods)

    def next_period_start(self, dt):
        if dt.tzinfo is None:
            raise Exception("Invalid naive datetime")
        if not self.periods:
            raise Exception("No periods defined")

        local_dt = dt.astimezone(self.tz)
        day_seconds = _day_seconds(local_dt)
        time_to_next_start = _time_to_next_start(day_seconds, self.periods)
        next_period_start_local = self.tz.normalize(local_dt + timedelta(seconds=time_to_next_start))
        return next_period_start_local.astimezone(dt.tzinfo)

    def next_period_end(self, dt):
        if dt.tzinfo is None:
            raise Exception("Invalid naive datetime")
        if not self.periods:
            raise Exception("No periods defined")

        local_dt = dt.astimezone(self.tz)
        day_seconds = _day_seconds(local_dt)
        time_to_next_end = _time_to_next_end(day_seconds, self.periods)
        next_period_end_local = self.tz.normalize(local_dt + timedelta(seconds=time_to_next_end))
        return next_period_end_local.astimezone(dt.tzinfo)

'''
common.time - various time functions
'''

import datetime as dt
import time

from six import string_types
import pytz


def utcnow():
    ''' Returns a timezone-aware datetime '''
    return dt.datetime.utcnow().replace(tzinfo=pytz.utc)

def sleep(when, tz=None, now_func=None, sleep_func=None):
    '''
    Pauses execution if in any blackout period. Designed for time-based (HH:MM)
    blackouts only. The `when` argument is a list of pairs of times, either
    datetime.datetime's, datetime.time's, or strings in `'HH:MM'` format.

    Times are expected to be in UTC timezone and 24H format.

    sleep([
        ('3:25', '4:22'),
        ('23:55', '0:05')
    ], 'US/Eastern')

    Args:
        when: list of two-tuple time ranges. A time is expected to be a
            datetime.datetime object (tz-aware or naive), datetime.time object,
            or time string (HH:MM - 24H format).
        tz: Timezone to use, defaults to UTC. Expected to be a pytz.timezone
            object or a string representing a valid pytz timezone.

    Returns:
        boolean if sleep occurred.

    Raises:
        ValueError: A time or timezone argument was invalid.
    '''

    tz = tz or pytz.utc
    if isinstance(tz, string_types):
        tz = pytz.timezone(tz)
    if not isinstance(tz, dt.tzinfo):
        raise ValueError("Invalid timezone object: " + str(tz))

    now_func = now_func or utcnow
    sleep_func = sleep_func or time.sleep

    waited = False
    for start, end in when:
        start = _minutes_since_midnight(start, tz, now_func)
        end = _minutes_since_midnight(end, tz, now_func)
        if start > end:
            end += 1440 # 24 * 60

        now = _minutes_since_midnight(now_func(), tz, now_func)
        while start <= now <= end or start <= (now + 1440) <= end:
            waited = True
            sleep_func(60)
            now = _minutes_since_midnight(now_func(), tz, now_func)
    return waited


def _minutes_since_midnight(t, tz, now_func=None):
    '''
    Converts times of various types (dt.dt, dt.time, string) to integer minutes.

    NOTE: DST naive.
    '''
    now_func = now_func or utcnow

    if isinstance(t, string_types):
        t = dt.time(*[int(n) for n in t.split(':')])
    if isinstance(t, dt.time):
        t = now_func().replace(hour=t.hour, minute=t.minute, tzinfo=None)
    if isinstance(t, dt.datetime):
        if t.tzinfo is None:
            t = tz.localize(t)
        t = t.astimezone(pytz.utc)
        return t.hour * 60 + t.minute
    raise ValueError("Invalid time object - " + str(t))
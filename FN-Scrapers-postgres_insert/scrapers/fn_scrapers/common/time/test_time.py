from fn_scrapers.common.time import sleep

from datetime import datetime as dt, timedelta as td, time

import pytz

class Timer(object):
    ''' Used for now/sleep state injection '''
    def __init__(self, start):
        self.start = start
        self.now_dt = self.start
        self.max_now = self.now_dt + td(hours=1)

    def now(self):
        if self.now_dt > self.max_now:
            raise ValueError("Exceeded max")
        return self.now_dt

    def sleep(self, seconds=60):
        self.now_dt += td(seconds=seconds)


def test_sleep():
    timer = Timer(dt(2018, 5, 2, 2, 25))
    blackout_periods = [('2:27', '2:33')]
    sleep(blackout_periods, now_func=timer.now, sleep_func=timer.sleep)
    assert timer.now_dt == timer.start
    timer.sleep(120)
    sleep(blackout_periods, now_func=timer.now, sleep_func=timer.sleep)
    assert timer.now_dt.hour == 2 and timer.now_dt.minute == 34

def test_sleep_rollover():
    timer = Timer(dt(2018, 5, 2, 23, 50))
    blackout_periods = [('23:50', '0:10')]
    sleep(blackout_periods, now_func=timer.now, sleep_func=timer.sleep)
    assert timer.now_dt == dt(2018, 5, 3, 0, 11)

def test_sleep_with_time():
    timer = Timer(pytz.utc.localize(dt(2018, 5, 2, 23, 50)))
    tz = pytz.timezone('US/Eastern')
    blackout_periods = [(time(19, 50), time(20, 10))]
    sleep(blackout_periods, tz=tz, now_func=timer.now, sleep_func=timer.sleep)
    assert timer.now_dt.replace(tzinfo=None) == dt(2018, 5, 3, 0, 11)

def test_sleep_with_timezone():
    timer = Timer(pytz.utc.localize(dt(2018, 5, 2, 23, 52)))
    tz = pytz.timezone('US/Eastern')
    blackout_periods = [("19:50", "20:10")]
    sleep(blackout_periods, tz=tz, now_func=timer.now, sleep_func=timer.sleep)
    assert timer.now_dt.replace(tzinfo=None) == dt(2018, 5, 3, 0, 11)

def _test_current():
    '''
    NOTE: not intended for server running (may block up to (1) minute).
    NOTE: assumes US/Eastern TZ - will not work if not US/Eastern.
    '''
    from sys import platform
    assert platform == 'darwin'
    h = dt.now().hour
    m = dt.now().minute
    print h, m
    assert sleep([
        (time(h, m), time(h, m))
    ], tz=pytz.timezone('US/Eastern'))

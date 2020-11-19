from __future__ import absolute_import

import os

import lxml.html
from pytz import timezone

from .ky_events import KYEventScraper

RESOURCES = os.path.join(os.path.dirname(__file__), 'resources')


def test_20170706():
    '''Ensure that we receive 6 events. '''
    with open(os.path.join(RESOURCES, 'events_20170706.html')) as file_pointer:
        page = lxml.html.fromstring(file_pointer.read())
    events = list(KYEventScraper.scrape_page('http://foo.com', page, timezone('MST')))
    assert len(events) == 6


def test_20180108():
    '''Ensure that we receive 6 events. '''
    with open(os.path.join(RESOURCES, 'events_20180108.html')) as file_pointer:
        page = lxml.html.fromstring(file_pointer.read())
    events = list(KYEventScraper.scrape_page('http://foo.com', page, timezone('MST')))
    assert len(events) == 2
    assert events[0]['related_bills'][0]['external_id'] == 'SB 70'

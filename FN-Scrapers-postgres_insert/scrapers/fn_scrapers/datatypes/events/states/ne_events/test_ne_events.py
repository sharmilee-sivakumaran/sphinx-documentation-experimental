from __future__ import absolute_import
from fn_scrapers.datatypes.events.states.ne_events.ne_events import NEEventScraper
import lxml.html
import arrow
import time
from datetime import datetime
from pytz import timezone

import os

RESOURCES = os.path.join(os.path.dirname(__file__), 'resources')

meta = {
    'legislative_session_containers': [
        {
            'end_year': 2018,
            'start_year': 2017,
            'id': u'20172018',
            'sessions': [
                {
                    'external_id': u'105',
                    'type': 'regular',
                    'id': u'20172018r',
                    'subsessions': [
                        {
                            'start_date': u'2017-01-04',
                            'end_date': u'2018-04-18'
                        }
                    ],
                    'name': u'2017-2018 Regular Session (105th)'
                }
            ]
        },
    ],
    'name': u'Nebraska',
    'legislature_type': 'unicameral',
    'abbreviation': u'ne',
    'timezone': u'US/Central',
    'legislature_name': u'Nebraska Legislature',
    'legislature_url': u'http://nebraskalegislature.gov/'
}

tz = timezone(meta['timezone'])


def test_split_comm():
    with open(os.path.join(RESOURCES, 'events_02:22:2018.html')) as file:
        page = lxml.html.fromstring(file.read())
        results = list(NEEventScraper.get_Events(page, tz))
        assert len(results) == 5
        event = results[0]
        assert event['location'] == "Room 1507"
        obj = datetime.strptime("02-22-2018 1:30 PM", "%m-%d-%Y %I:%M %p")
        assert event['start'] == timezone(meta['timezone']).localize(obj)
        assert [b['external_id'] for b in event['related_bills']] ==\
            ['LB 817', 'LB 839', 'LB 1129', 'LB 1027']

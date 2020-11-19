from datetime import datetime, date
import json
from os import path
from pprint import pprint
import pytz
from lxml import html

from fn_scrapers.datatypes.bills.common import Bill

RESOURCES = path.join(path.dirname(path.abspath(__file__)), 'resources')

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise ValueError()

from .nh_notices import NHNoticeScraper

NHNoticeScraper.nh_timezone = pytz.timezone("America/New_York")
NHNoticeScraper.nh_locality = 'nh'

def test_notice_2017_17():
    with open(path.join(RESOURCES, 'notice_2012-17.html')) as fp:
        notice_page = html.fromstring(fp.read())
    notices = list(NHNoticeScraper.scrape_notice(notice_page))

    print json.dumps(notices, indent=2, default=json_serializer)
    assert len(notices) == 2
    assert notices[0]['title'] == 'Proposed Rulemaking'
    assert notices[1]['title'] == 'Final Rulemaking'
    assert all(
        notice['regulation']['title'] == 'Various Carnival-Amusement Rules.'
        for notice in notices)

def test_document_notices_feb_16_12():
    from .resources.register_feb_16_12 import CONTENT 
    docs = NHNoticeScraper.entities_to_reports(CONTENT.entities)
    pprint([(key, val.page_num) for key, val in docs.items()])
    notices = ['2012-%s' % i for i in range(16, 21)] + ['2012-2']
    assert all(notice in docs for notice in notices)

def test_document_notices_mar_01_18():
    from .resources.register_mar_01_18 import CONTENT 
    docs = NHNoticeScraper.entities_to_reports(CONTENT.entities)
    pprint([(key, val.page_num) for key, val in docs.items()])
    notices = ['2018-%s' % i for i in range(30, 37)]
    assert all(notice in docs for notice in notices)

def test_document_notices_jan_11_18():
    from .resources.register_jan_11_18 import CONTENT 
    docs = NHNoticeScraper.entities_to_reports(CONTENT.entities)
    pprint([(key, val.page_num) for key, val in docs.items()])
    notices = ['2018-%s' % i for i in range(1, 4)] + ['2017-29']
    assert all(notice in docs for notice in notices)

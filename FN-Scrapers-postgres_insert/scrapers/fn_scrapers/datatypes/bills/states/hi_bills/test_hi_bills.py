'''
HI unit tests
'''
from __future__ import absolute_import
from lxml import html
import os.path as path
import logging

from fn_scrapers.datatypes.bills.common import BillReportingPolicy as BRP
from fn_scraperutils.scrape.element_wrapper import ElementWrapper
from fn_scraperutils.scraper import Scraper
from fn_scraperutils.config import parse_config
from .hi_bills import HIBillScraper

"""
The get_scraper function requires a scraperutils-config file in the system in order to create a Scraper object.
That scraper object is only needed to create an ElementWrapper object from the HTML test file that is in resources,
so there are three options to handle that, wither manually create an ElementWrapper obejct from the HTML file (check
previous version of this code) OR use mock to pass in a fake config file OR simply create a config dictionary and
pass it in to the Scraper arguments
"""


def get_scraper():
    class InternalPublisher(object):
        def publish_event(self, severity, event, process_id):
            level = logging.getLevelName(severity.upper())
            msg = event.pop('message')
            logging.log(level, msg, process_id=process_id, extra=event)
    config = {'thrift': {"doc_service_host": "hostwiththemost:9090", "data_access_host": 'hostwiththeleast:0'}}
    return Scraper('test_scraper', '1234', publisher=InternalPublisher(), ratelimiter_client='bar',
                   config=parse_config(config))


def get_bill_object(resource='hi_2018_HB1294.html', bill_id='HB 1294', session='20172018r',
                    chamber='lower', bill_type="bill"):
    scraper = get_scraper()
    with get_recource(resource) as html_file:
        content = html_file.read()
    bill_page = scraper.wrap_html('http://foo', content, BRP.bill)
    bill = HIBillScraper.create_bill(
        bill_page, bill_id, session, chamber, bill_type)
    return bill, bill_page


def get_recource(resource, option='r'):
    name = path.join(path.dirname(path.abspath(__file__)), 'resources', resource)
    return open(name, option)


def test_bill_object_creation():
    bill, bill_page = get_bill_object()
    assert bill['title'] == 'RELATING TO ENERGY EFFICIENCY.'.title()
    assert bill['summary'] == (
        'Requires the Department of Business, Economic Development, and Tourism '
        'to establish a task force to make recommendations on building and energy '
        'code and standards for commercial buildings.  (HB1294 HD2)')


def test_bill_sponsors():
    bill, bill_page = get_bill_object()
    HIBillScraper.scrape_sponsors(bill, bill_page)
    assert len(bill['sponsors']) == 5
    assert all(s['type'] == 'primary' for s in bill['sponsors'])


def test_bill_actions():
    bill, bill_page = get_bill_object()
    HIBillScraper.scrape_actions(bill, bill_page)
    assert len(bill['actions']) == 12

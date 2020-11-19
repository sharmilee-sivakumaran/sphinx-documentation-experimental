

from os import path
import pprint

from lxml import etree

from .uk_notice_scraper import UKNoticeScraper

CWD = path.join(path.dirname(path.abspath(__file__)), 'test_uk_notice_scraper')

def test_parse_feed():
    with open(path.join(CWD, 'ukdsi.data.feed.xml'), 'r') as fp:
        xml = etree.fromstring(fp.read())
    url, docs = UKNoticeScraper.parse_feed(xml)
    assert url == 'http://www.legislation.gov.uk/ukdsi/data.feed?page=2'
    assert len(docs) == 20
    akn, dt = docs[0]
    assert akn == 'http://www.legislation.gov.uk/ukdsi/2018/9780111168523/data.akn'
    assert dt.strftime('%Y%m%d') == '20180417'

def test_parse_akn_ukdsi():
    with open(path.join(CWD, 'ukdsi.data.akn.xml'), 'r') as fp:
        xml = etree.fromstring(fp.read())
    records = [rec for rec, _ in UKNoticeScraper.parse_akn(xml)]
    pprint.pprint(records)
    assert len(records) == 3
    assert all(rec['notice_id'] == 'ISBN 978-0-11-116852-3' for rec in records)
    assert all(
        rec['notice_type'] == 'UK Draft Statutory Instrument' for rec in records)
    assert any(rec['publication_date'] == '2018-04-16' for rec in records)
    assert any(rec['publication_date'] == '2018-04-13' for rec in records)
    assert len(set(rec['document_title'] for rec in records)) == 3

def test_parse_akn_uksi():
    with open(path.join(CWD, 'uksi.data.akn.xml'), 'r') as fp:
        xml = etree.fromstring(fp.read())
    records = [rec for rec, _ in UKNoticeScraper.parse_akn(xml)]
    pprint.pprint(records)
    assert len(records) == 2
    assert all(
        rec['notice_type'] == 'UK Statutory Instrument' for rec in records)
    assert all(rec['notice_id'] == '2018 No. 593' for rec in records)
    assert any(rec['publication_date'] == '2018-05-14' for rec in records)
    assert any(rec['publication_date'] == '2018-05-15' for rec in records)

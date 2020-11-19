from __future__ import absolute_import
import os
from fn_scrapers.datatypes.events.states.ar_events.ar_events import AREventScraper
from lxml.html import html5parser
from lxml.html.html5parser import HTMLParser


RESOURCES = os.path.join(os.path.dirname(__file__), 'resources')
html_parser = HTMLParser(namespaceHTMLElements=False)

metadata = {
    'legislative_session_containers': [],
    'name': u'Arkansas',
    'legislature_type': 'bicameral',
    'abbreviation': u'ar',
    'timezone': u'US/Central',
    'legislature_name': u'Arkansas General Assembly',
    'legislature_url': u'http://www.arkleg.state.ar.us/'
}

def test_02142018():
    """
    Test that the number of events is correct
    """
    import pdb
    pdb.set_trace()
    scraper = AREventScraper()
    scraper.metadata = metadata
    with open(os.path.join(RESOURCES, 'events_02-14-2018.html')) as file:
        page = html5parser.fromstring(file.read(), guess_charset=True, parser=html_parser)
    comm = list(scraper.scrape_events(page, 'foo', download_files=False))
    assert len(comm) == 15


def test_02142018_each_event():
    """
    Validate each event against schema
    """
    scraper = AREventScraper(metadata=metadata)
    with open(os.path.join(RESOURCES, 'events_02-14-2018.html')) as file:
        page = html5parser.fromstring(file.read(), guess_charset=True, parser=html_parser)
    for comm in scraper.scrape_events(page, 'foo', download_files=False):
        assert 'sources' in comm
        assert comm['sources']
        for source in comm['sources']:
            assert 'url' in source
            assert source['url']
        assert 'description' in comm
        assert 'start' in comm
        assert 'start_has_time?' in comm
        assert 'location' in comm
        assert 'event_type' in comm
        if 'chamber' in comm:
            assert comm['chamber'] in {'upper', 'lower', 'joint'}
        if 'participants' in comm:
            assert comm['participants']
            for part in comm['participants']:
                assert 'name' in part
                assert 'role' in part
                assert part['role'] in {'host', 'chair', 'participant'}
                if 'chamber' in part:
                    assert part['chamber'] in {'upper', 'lower', 'joint'}
        if 'related_bills' in comm:
            assert comm['related_bills']
            for bill in comm['related_bills']:
                assert 'external_id' in bill
                assert 'type' in bill
                assert bill['type'] in {'amendment', 'companion', 'other', 'consideration'}


def test_agenda_1():
    """
    Test for valid bill ID extraction from PDF
    """
    scraper = AREventScraper(metadata=metadata)
    fname = os.path.join(RESOURCES, '281.PDF')
    assert 'HB1069' == next(scraper.get_bill_ids(fname))


def test_agenda_2():
    """
    Test for valid bill ID extraction from PDF
    """
    scraper = AREventScraper(metadata=metadata)
    fname = os.path.join(RESOURCES, '301.PDF')
    assert 'HB 1001' == next(scraper.get_bill_ids(fname))


def test_agenda_3():
    """
    Test for valid bill ID extraction from PDF
    """
    scraper = AREventScraper(metadata=metadata)
    fname = os.path.join(RESOURCES, '284.PDF')
    assert next(scraper.get_bill_ids(fname), None) is None


def test_agenda_4():
    """
    Test for valid bill ID extraction from PDF
    """
    scraper = AREventScraper(metadata=metadata)
    fname = os.path.join(RESOURCES, '295.PDF')
    assert 'HB1073' == next(scraper.get_bill_ids(fname), None)


def test_02152018():
    """
    Test that the number of events is correct
    """
    scraper = AREventScraper(metadata=metadata)
    with open(os.path.join(RESOURCES, 'events_02-15-2018.html')) as file:
        page = html5parser.fromstring(file.read(), guess_charset=True, parser=html_parser)
    comm = list(scraper.scrape_events(page, 'foo', download_files=False))
    assert len(comm) == 12


def test_02152018_each_event():
    """
    Validate each event against schema
    """
    scraper = AREventScraper(metadata)
    with open(os.path.join(RESOURCES, 'events_02-15-2018.html')) as file:
        page = html5parser.fromstring(file.read(), guess_charset=True, parser=html_parser)
    for comm in scraper.scrape_events(page, 'foo', download_files=False):
        assert 'sources' in comm
        assert comm['sources']
        for source in comm['sources']:
            assert 'url' in source
            assert source['url']
        assert 'description' in comm
        assert 'start' in comm
        assert 'start_has_time?' in comm
        assert 'location' in comm
        assert 'event_type' in comm
        if 'chamber' in comm:
            assert comm['chamber'] in {'upper', 'lower', 'joint'}
        if 'participants' in comm:
            assert comm['participants']
            for part in comm['participants']:
                assert 'name' in part
                assert 'role' in part
                assert part['role'] in {'host', 'chair', 'participant'}
                if 'chamber' in part:
                    assert part['chamber'] in {'upper', 'lower', 'joint'}
        if 'related_bills' in comm:
            assert comm['related_bills']
            for bill in comm['related_bills']:
                assert 'external_id' in bill
                assert 'type' in bill
                assert bill['type'] in {'amendment', 'companion', 'other', 'consideration'}

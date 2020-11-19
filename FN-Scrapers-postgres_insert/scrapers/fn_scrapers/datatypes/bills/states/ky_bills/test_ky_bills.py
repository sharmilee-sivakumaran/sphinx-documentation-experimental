from __future__ import absolute_import
import os
import codecs
from datetime import datetime as dt
import pprint

from lxml import html
from fn_scraperutils.scrape.element_wrapper import ElementWrapper

from fn_scrapers.datatypes.bills.states.ky_bills.ky_bills import KYBillScraper

RESOURCES = os.path.join(os.path.dirname(__file__), 'resources')

pretty = pprint.PrettyPrinter(indent=4)


def test_votes_SB5():
    with codecs.open(os.path.join(RESOURCES, 'SB5.txt'), encoding='utf-8') as file:
        vote_text = file.read()
    votes = [vote for vote in KYBillScraper.parse_votes(vote_text, 'foo') if vote]

    assert len(votes) == 4
    assert votes[0]['chamber'] == 'upper'
    assert votes[0]['motion'] == 'RSN# 487'
    assert votes[0]['date'] == dt(month=3, day=1, year=2018, hour=17, minute=21, second=52)
    assert votes[0]['passed']
    assert votes[0]['other_count'] == 2
    assert votes[0]['other_count'] == len(votes[0]['other_votes'])
    assert votes[0]['yes_count'] == len(votes[0]['yes_votes'])
    assert votes[0]['no_count'] == len(votes[0]['no_votes'])
    assert votes[1]['chamber'] == 'lower'
    assert votes[1]['motion'] == 'RCS# 364'
    assert votes[1]['date'] == dt(month=3, day=20, year=2018, hour=14, minute=48, second=36)
    assert votes[1]['other_count'] == 13
    assert votes[1]['other_count'] == len(votes[1]['other_votes'])
    assert votes[1]['yes_count'] == len(votes[1]['yes_votes'])
    assert votes[1]['no_count'] == 0
    assert votes[2]['other_count'] == len(votes[2]['other_votes'])
    assert votes[2]['yes_count'] == len(votes[2]['yes_votes'])
    assert votes[2]['no_count'] == 0
    assert votes[3]['other_count'] == len(votes[3]['other_votes'])
    assert votes[3]['yes_count'] == len(votes[3]['yes_votes'])
    assert votes[3]['no_count'] == 0


def test_votes_SB112():
    with codecs.open(os.path.join(RESOURCES, 'SB112.txt'), encoding='utf-8') as file:
        vote_text = file.read()
    votes = [vote for vote in KYBillScraper.parse_votes(vote_text, 'foo') if vote]

    assert len(votes) == 3
    assert votes[0]['chamber'] == 'upper'
    assert votes[0]['motion'] == 'RSN# 464'
    assert votes[0]['date'] == dt(month=2, day=26, year=2018, hour=17, minute=55, second=20)

    yes_votes = {
        'Alvarado',
        'Givens',
        'Parrett',
        'Stivers',
        'Bowen',
        'Harris',
        'Raque Adams',
        'Thayer',
        'Buford',
        'Higdon',
        'Ridley',
        'Turner',
        'Carpenter',
        'Hornback',
        'Robinson',
        'Webb',
        'Carroll D',
        'Humphries',
        'Schickel',
        'West',
        'Carroll J',
        'Kerr',
        'Schroder',
        'Westerfield',
        'Embry',
        'McDaniel',
        'Seum',
        'Wilson',
        'Girdler',
        'Meredith',
        'Smith',
        'Wise'
    }
    no_votes = {
        'Clark',
        'Harper Angel',
        'McGarvey'
    }
    other_votes = {
        'Jones',
        'Neal',
        'Thomas'
    }
    assert votes[0]['passed']
    assert votes[0]['other_count'] == 3
    assert votes[0]['other_count'] == len(votes[0]['other_votes'])
    assert votes[0]['yes_count'] == len(votes[0]['yes_votes'])
    assert votes[0]['no_count'] == len(votes[0]['no_votes'])
    assert set(votes[0]['no_votes']) == no_votes
    assert set(votes[0]['yes_votes']) == yes_votes
    assert set(votes[0]['other_votes']) == other_votes


def test_votes_HB200():
    with codecs.open(os.path.join(RESOURCES, 'HB200.txt'), encoding='utf-8') as file:
        vote_text = file.read()
    votes = [vote for vote in KYBillScraper.parse_votes(vote_text, 'foo') if vote]
    pretty.pprint(votes)

    assert len(votes) == 7

def test_parse_actions():
    with open(os.path.join(RESOURCES, 'Bill_HB463.html')) as fp:
        page = html.fromstring(fp.read())
    ew = ElementWrapper(page, None, None, None)
    content = ew.text_content()
    actions = list(KYBillScraper.parse_actions(content))
    pprint.pprint(actions)
    assert any('received in House' == action[1] for action in actions)

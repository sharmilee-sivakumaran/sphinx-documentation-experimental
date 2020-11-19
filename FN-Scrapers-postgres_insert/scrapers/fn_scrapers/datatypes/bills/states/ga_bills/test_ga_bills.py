import datetime
from itertools import permutations
import json
from os import path
from fn_scrapers.datatypes.bills.common import Bill

RESOURCES = path.join(path.dirname(path.abspath(__file__)), 'resources')
from .ga_bills import GABillScraper

def _load_sessions(session_list=None):
    if not session_list:
        with open(path.join(RESOURCES, 'sessions.json')) as fp:
            session_list = json.load(fp)
    GABillScraper.set_sessions(session_list)

def _get_bill(bill_doc):
    _load_sessions()
    return Bill('20172018r', 'lower', 'HB 87', 'title', 'bill')

def _string_dicts(dicts):
    sorted([json.dumps(d, sort_keys=True) for d in dicts])

def test_sessions():
    '''Test basic session loading. '''
    _load_sessions()
    session = GABillScraper.get_session('20172018r')
    assert session['Library'] == 'http://www.legis.ga.gov/Legislation/20172018/'

def test_special_sessions():
    '''Test special session numbering. '''
    session_list = [
        {
            "Description": "2002 1st Special Session",
            "Library": "http://www.legis.ga.gov/Legislation/2002EX1/",
            "Id": "20012002ss3"
        },
        {
            "Description": "2002 2nd Special Session",
            "Library": "http://www.legis.ga.gov/Legislation/2002EX2/",
            "Id": "20012002ss4"
        },
        {
            "Description": "2001 1st Special Session",
            "Library": "http://www.legis.ga.gov/Legislation/2001EX1/",
            "Id": "20012002ss1"
        },
        {
            "Description": "2001 2nd Special Session",
            "Library": "http://www.legis.ga.gov/Legislation/2001EX2/",
            "Id": "20012002ss2"
        },
    ]
    for sessions in permutations(session_list):
        for i in range(4):
            test_session = "20012002ss" + str(i+1)
            _load_sessions(sessions)
            session = GABillScraper.get_session(test_session)
            assert session['Id'] == test_session

def test_build_bill():
    '''Test build_bill with data from pillar. '''
    _load_sessions()
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    bill = GABillScraper.build_bill('20172018r', bill_doc)
    assert bill['id'] == 'HB 87'
    assert bill['session'] == '20172018r'
    assert bill['chamber'] == 'lower'
    assert bill['title'] == (
        'Corporations, partnerships, and associations; multiple-year registrat'
        'ions for certain types of business organizations; provide')
    assert bill['type'] == 'bill'
    assert bill['sources'][0] == {
        'source_type': 'default', 
        'url': 'http://www.legis.ga.gov/legislation/en-US/display/20172018/HB/87'
    }

def test_sponsors():
    sponsors = [
        {'type': 'primary', 'name': 'Raffensperger, Brad'},
        {'type': 'primary', 'name': 'Williamson, Bruce'},
        {'type': 'primary', 'name': 'Welch, Andrew'},
        {'type': 'primary', 'name': 'Golick, Rich'},
        {'type': 'primary', 'name': 'Petrea, Jesse'},
        {'type': 'primary', 'name': 'Kirby, Tom'},
        {'type': 'primary', 'name': 'Tillery, Blake'},
    ]
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    bill = _get_bill(bill_doc)
    GABillScraper.scrape_sponsors(bill, bill_doc)
    s = lambda d: json.dumps(d, sort_keys=True)
    assert _string_dicts(bill['sponsors']) == _string_dicts(sponsors)

def test_actions():
    actions = [
        [u'Effective Date', u'executive', '2017-07-01'],
        [u'Act 47', u'executive', '2017-05-01'],
        [u'House Date Signed by Governor', u'executive', '2017-05-01'],
        [u'House Sent to Governor', u'executive', '2017-04-05'],
        [u'House Agreed Senate Amend or Sub', u'lower', '2017-03-30'],
        [u'Senate Passed/Adopted By Substitute', u'upper', '2017-03-28'],
        [u'Senate Third Read', u'upper', '2017-03-28'],
        [u'Senate Read Second Time', u'upper', '2017-03-16'],
        [u'Senate Committee Favorably Reported  By Substitute', u'upper', '2017-03-15'],
        [u'Senate Read and Referred', u'upper', '2017-03-01'],
        [u'House Passed/Adopted', u'lower', '2017-02-28'],
        [u'House Third Readers', u'lower', '2017-02-28'],
        [u'House Committee Favorably Reported', u'lower', '2017-02-23'],
        [u'House Second Readers', u'lower', '2017-01-25'],
        [u'House First Readers', u'lower', '2017-01-24'],
        [u'House Hopper', u'lower', '2017-01-23'],
    ]
    for i, action in enumerate(actions):
        actions[i] = dict(zip(('action', 'actor', 'date'), action))
        actions[i]['date'] = datetime.datetime.strptime(
            actions[i]['date'], '%Y-%m-%d').date()
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    bill = _get_bill(bill_doc)
    GABillScraper.scrape_actions(bill, bill_doc)
    for i, action in enumerate(actions):
        for key in action:
            assert action[key] == bill['actions'][i][key]

def test_scrape_summary():
    summary = (
        'A BILL to be entitled an Act to amend Title 14 of the Official Code '
        'of Georgia Annotated, relating to corporations, partnerships, and '
        'associations, so as to provide for multiple-year registrations for '
        'certain types of business organizations; to provide for the adoption '
        'of rules and regulations by the Secretary of State as necessary to '
        'implement such a multiple-year registration process; to provide for '
        'related matters; to repeal conflicting laws; and for other purposes.'
    )
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    bill = _get_bill(bill_doc)
    GABillScraper.scrape_summary(bill, bill_doc)
    assert bill['summary'] == summary

def test_scrape_vote_14945():
    with open(path.join(RESOURCES, 'members.json')) as fp:
        GABillScraper.members = {m['Id']: m for m in json.load(fp)}
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    with open(path.join(RESOURCES, 'vote_14945.json')) as fp:
        rollcall = json.load(fp)
    vote = GABillScraper.scrape_vote(bill_doc['Votes'][0], rollcall)
    assert vote['motion'] == 'House Vote #141 (PASSAGE)'
    assert vote['chamber'] == 'lower'
    assert vote['date'].strftime('%Y-%m-%d') == '2017-02-28'
    assert vote['passed'] == True
    assert vote['yes_count'] == 167
    assert vote['no_count'] == 3
    assert vote['other_count'] == 10

    assert len(vote['yes_votes']) == 167
    assert 'Amy Carter' in vote['yes_votes']
    assert 'Doreen Carter' in vote['yes_votes']
    assert 'Park Cannon' in vote['no_votes']
    assert 'David Ralston' in vote['other_votes'] # no vote
    assert 'David Casas' in vote['other_votes'] # excused


def test_action_committees():
    with open(path.join(RESOURCES, 'bill_25_hb_87.json')) as fp:
        bill_doc = json.load(fp)
    bill = _get_bill(bill_doc)
    GABillScraper.scrape_actions(bill, bill_doc)
    senate_committee = 'Economic Development and Tourism'
    house_committee = 'Small Business Development'
    for action in bill['actions']:
        if action['action'].startswith("House Committee"):
            assert action['related_entities'][0]['name'] == house_committee
            house_committee = None
        if action['action'].startswith("Senate Committee"):
            assert action['related_entities'][0]['name'] == senate_committee
            senate_committee = None
    assert not house_committee and not senate_committee

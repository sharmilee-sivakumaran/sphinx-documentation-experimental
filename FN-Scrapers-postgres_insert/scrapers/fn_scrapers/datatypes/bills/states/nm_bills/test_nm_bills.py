import json
from .nm_bills import NMBillScraper

def test_vote_s0091():
    from resource.vote_test_SM0091SVOTE_3 import CONTENT
    scraper_doc = NMBillScraper.vote_parser(CONTENT.entities, {
        "motion": 'title text here',
        "vote_date": "02/07/18",
        "vote_chamber": "upper"
    })
    vote = scraper_doc[0].additional_data.get('vote')
    for vot in ('yes', 'no', 'other'):
        assert vote[vot+'_count'] == len(vote.get(vot+'_votes', []))
        for name in vote.get(vot+'_votes', []):
            assert not name.startswith('TOTAL')
    assert vote['yes_count'] == 43

def test_vote_hb00983s():
    from resource.vote_test_HB0083SVOTE_3 import CONTENT
    scraper_doc = NMBillScraper.vote_parser(CONTENT.entities, {
        "motion": 'title text here',
        "vote_date": "",
        "vote_chamber": "upper"
    })
    vote = scraper_doc[0].additional_data.get('vote')
    for vot in ('yes', 'no', 'other'):
        assert vote[vot+'_count'] == len(vote.get(vot+'_votes', []))
        for name in vote.get(vot+'_votes', []):
            assert not name.startswith('TOTAL')
    assert vote['yes_count'] == 38
    assert vote['other_count'] == 3

def test_vote_hb00983h():
    from resource.vote_test_HB0083HVOTE_3 import CONTENT
    scraper_doc = NMBillScraper.vote_parser(CONTENT.entities, {
        "motion": 'title text here',
        "vote_date": "",
        "vote_chamber": "lower"
    })
    vote = scraper_doc[0].additional_data.get('vote')
    print json.dumps(vote, indent=2)
    for vot in ('yes', 'no', 'other'):
        assert vote[vot+'_count'] == len(vote.get(vot+'_votes', []))
        for name in vote.get(vot+'_votes', []):
            assert not name.startswith('TOTAL')
    assert vote['yes_count'] == 65
    assert vote['other_count'] == 5

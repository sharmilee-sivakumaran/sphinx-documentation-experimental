from __future__ import absolute_import
import os

from lxml import html
from fn_scraperutils.scrape.element_wrapper import ElementWrapper

from .ma_bills import MABillScraper

TEST_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_data')

def test_vote_summary_parse_H2424():
    summary = MABillScraper.vote_summary_parse(
        "Prevailed\nYea - 37 Nay - 0\nAbsent/Not Voting - 1	")
    assert summary == {"yeas": 37, "nays": 0, "other": 1}

def test_vote_summary_parse_H1100():
    summary = MABillScraper.vote_summary_parse(
        "Yea - 36 Nay - 0\nAbsent/Not Voting - 1	")
    assert summary == {"yeas": 36, "nays": 0, "other": 1}



# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import os
from lxml import html

from .thailand_reg_gazette_scraper import (
    THAILANDregulationnoticescraper as Scraper)
CWD = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources')


def test_dates():
    with open(os.path.join(CWD, 'search_results.html')) as fp:
        content = fp.read()
    page = html.fromstring(content)
    
    for date in page.xpath('.//td[5]/text()'):
        assert Scraper.translate_thai_date(date)

def test_thailand_digits():
    assert Scraper.thai_digits_to_int('๑๓๕') == 135
    assert Scraper.thai_digits_to_int('๓๖ ก') == 36

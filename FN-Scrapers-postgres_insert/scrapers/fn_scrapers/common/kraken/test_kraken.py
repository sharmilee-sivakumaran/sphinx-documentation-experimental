from __future__ import absolute_import

import json
from pprint import pprint
from os import path

from .kraken import CWD, krakenize, Feed

def test_canada_input():
    with open(path.join(CWD, 'test_data', 'canada_input.json')) as fp:
        scraper_doc = json.load(fp)
    feed = Feed('international_reg_notices', 'canada')
    kraken_doc = krakenize(scraper_doc, feed)
    assert kraken_doc['documentId'] == scraper_doc['document_id']
    meta = json.loads(kraken_doc['documentMetadata'])
    assert meta['departments'] == [scraper_doc['departments'][0]['department_name']]
 
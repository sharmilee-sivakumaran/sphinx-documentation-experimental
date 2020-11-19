from __future__ import absolute_import

from fn_scrapers.datatypes.events.common.utils import remove_empty_fields


def test_empty_fields():
    bad_dict = {
        'a': 'd',
        'b': 'c',
        'e': [],
        'f': '',
        'g': {
            'x': 's',
            'y': [],
            'u': ''
        },
        'h': [
            {
            },
            {
                'x': ''
            },
            {
                'x': 'sds'
            }
        ],
        'i': None,
        'j': {}
    }
    good_dict = {
        'a': 'd',
        'b': 'c',
        'g': {
            'x': 's',
        },
        'h': [
            {
                'x': 'sds'
            }
        ]
    }
    assert good_dict == remove_empty_fields(bad_dict)

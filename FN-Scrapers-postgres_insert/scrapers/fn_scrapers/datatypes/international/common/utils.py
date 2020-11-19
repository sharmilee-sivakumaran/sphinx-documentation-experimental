# -*- coding: utf-8 -*-
import requests
import datetime
import dateparser.date_parser
from babel import languages as blang
import pycountry
from w3lib.encoding import *
from w3lib.html import *
from w3lib.http import *
from w3lib.url import *
from collections import namedtuple
try:
    import simplejson as json
except ImportError:
    import json

# class for utilities required for data handling

def get_default_http_headers():
    headers = requests.utils.default_headers()
    headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
        'Connection': 'keep-alive'
    })
    return headers


def parse_date_as_str(text, strftime_format="%Y-%m-%d", languages=None):
    if text and isinstance(text, six.string_types):
        date_parsed = dateparser.parse(text, languages=languages)
        if date_parsed:
            return date_parsed.strftime(strftime_format)

def two_digit_year_to_four_digit_year(year_int):
    from datetime import date
    current_year_without_century = date.today().year % 100
    if year_int <= current_year_without_century:
        return 2000 + year_int
    elif year_int < 100:
        return 1900 + year_int
    else:
        return year_int


def get_official_language_list(country="US"):
    return blang.get_official_languages(country)

def get_country_info(country="US"):
    return pycountry.countries.lookup(country)


def to_unicode(text, encoding=None, errors='strict'):
    """Return the unicode representation of a bytes object `text`. If `text`
    is already an unicode object, return it as-is."""
    if isinstance(text, six.text_type):
        return text
    if not isinstance(text, (bytes, six.text_type)):
        raise TypeError('to_unicode must receive a bytes, str or unicode '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)


def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of `text`. If `text`
    is already a bytes object, return it as-is."""
    if isinstance(text, bytes):
        return text
    if not isinstance(text, six.string_types):
        raise TypeError('to_bytes must receive a unicode, str or bytes '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)

def unicode_str(text, encoding=None, errors='strict'):
    """ Return str representation of `text`
    (bytes in Python 2.x and unicode in Python 3.x). """

    if six.PY2:
        return to_bytes(text, encoding, errors)
    else:
        return to_unicode(text, encoding, errors)


def urljoin(base_url, url, allow_fragments=None):
    return six.moves.urllib.parse.urljoin(base_url, url, allow_fragments)



def dict_to_namedtuple(name, dictionary):
    for key, value in dictionary.iteritems():
        if isinstance(value, dict):
            dictionary[key] = dict_to_namedtuple(key, value)
    return namedtuple(name, dictionary.keys())(**dictionary)


def dict_from_class(cls):
    return dict(
        (key, value)
        for (key, value) in cls.__dict__.items()
        if key not in set(cls.__dict__.keys())
    )


class JSONEncoderPlus(json.JSONEncoder):
    """
    JSONEncoder that encodes datetime objects as Unix timestamps
    """

    def default(self, obj, **kwargs):

        if isinstance(obj, datetime.datetime):
            return obj.isoformat("T") + "Z"
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif hasattr(obj, "to_json"):
            return obj.to_json()
        elif hasattr(obj, "for_json"):
            return obj.for_json()
        return super(JSONEncoderPlus, self).default(obj, **kwargs)



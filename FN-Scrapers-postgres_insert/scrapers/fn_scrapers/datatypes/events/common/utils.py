from __future__ import absolute_import
import json
import arrow
import datetime
import time
import pytz
import logging
from lxml import html
from lxml.html import html5parser
from lxml.html.html5parser import HTMLParser

from fn_scrapers.common.http import request, request_file

logger = logging.getLogger(__name__)

def remove_empty_fields(_dict):
    if isinstance(_dict, dict):
        return dict((k, remove_empty_fields(v)) for k, v in _dict.iteritems() if
                    v and remove_empty_fields(v))
    elif isinstance(_dict, list):
        return [remove_empty_fields(v) for v in _dict if v and remove_empty_fields(v)]
    else:
        return _dict

def iter_months(start, end):
    """
    Iterate over the months between start and end, inclusively.
    """
    if start > end:
        raise Exception("'start' must not be after 'end'")
    start = start.replace(day=1)
    end = end.replace(day=1)
    i = start
    while i <= end:
        yield i
        if i.month < 12:
            i = i.replace(month=i.month + 1)
        else:
            i = i.replace(year=i.year + 1, month=1)

def get_page(url, html_parser='html',**kwargs):
    resp = request(url, **kwargs)
    resp._content = resp._content.replace('&nbsp;', ' ')
    if html_parser == 'html':
        lxml_obj = html.fromstring(resp.text)
        lxml_obj.make_links_absolute(url)
    elif html_parser == 'html5':
        lxml_obj = html5parser.fromstring(resp.text, parser=HTMLParser(namespaceHTMLElements=False))

    return lxml_obj

def get_today_as_timezone(tz):
    today = datetime.datetime.utcnow()
    today = today.replace(tzinfo=pytz.UTC)
    return today.astimezone(tz).date()

# Used to parse date-only strings such as event dates.
def parse_str_to_date(date):
    dateformats = ["%A, %B %d, %Y", "%A, %b %d, %Y", "%a, %B %d, %Y",
                   "%a, %b %d, %Y", "%B %d, %Y", "%b %d, %Y", "%x"]
    date_obj = None
    for f in dateformats:
        try:
            date_obj = datetime.datetime.strptime(date, f)
            break
        except:
            pass
    else:
        logger.error("Cannot determine date format of - {}".format(date))
    return date_obj


class JSONEncoderPlus(json.JSONEncoder):
    """
    JSONEncoder that encodes datetime objects as Unix timestamps and mongo
    ObjectIds as strings.
    """
    def default(self, obj, **kwargs):
        if isinstance(obj, datetime.datetime):
            return str(arrow.get(time.mktime(obj.utctimetuple())))
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        return super(JSONEncoderPlus, self).default(obj, **kwargs)


from __future__ import absolute_import, division

import datetime

import pytz

from .status import FieldList
from ..tableformat.format_csv import FormatCsvBuilder
from ..tag_util import AllTags


def print_csv_status(out, table, tz):
    def _format_timedelta(td):
        return td.total_seconds()

    def _format_datetime(dt):
        return dt.astimezone(pytz.timezone(tz)).isoformat()

    format_csv = FormatCsvBuilder() \
        .add_formatter(datetime.timedelta, _format_timedelta) \
        .add_formatter(datetime.datetime, _format_datetime) \
        .add_formatter(FieldList, lambda val: u"|".join(val.fields)) \
        .add_formatter(AllTags, lambda x: u"|".join(tn + u"=" + tv for tn in x.tags for tv in x.tags[tn])) \
        .add_formatter(list, lambda x: u"|".join(x)) \
        .build()

    out.write(format_csv(table).encode('utf-8'))


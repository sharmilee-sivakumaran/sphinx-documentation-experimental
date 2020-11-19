from __future__ import absolute_import, division

import datetime

import pytz

from .status import FieldList
from ..tableformat.format_json import FormatJsonBuilder
from ..tag_util import AllTags


def print_json_status(out, table, tz):
    def _format_timedelta(td):
        return td.total_seconds()

    def _format_datetime(dt):
        return dt.astimezone(pytz.timezone(tz)).isoformat()

    format_json = FormatJsonBuilder() \
        .add_formatter(datetime.timedelta, _format_timedelta) \
        .add_formatter(datetime.datetime, _format_datetime) \
        .add_formatter(FieldList, lambda val: val.fields) \
        .add_formatter(AllTags, lambda x: {tn: [tv for tv in x.tags[tn]] for tn in x.tags}) \
        .add_formatter(list, lambda x: x) \
        .build()

    out.write(format_json(table).encode("utf-8"))


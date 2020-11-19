from __future__ import absolute_import, division

import datetime

import pytz

from .status import FieldList
from ..duration_format import format_duration
from ..tableformat.format_text_table import FormatTextTableBuilder
from ..tag_util import AllTags


def print_text_table_status(out, color, table, tz, datetime_format):
    now = datetime.datetime.now(pytz.UTC)

    def _format_datetime(dt):
        return dt.astimezone(pytz.timezone(tz)).strftime(datetime_format)

    def _format_next_scrape_at(dt):
        if dt is None:
            return u"-"
        elif dt < now:
            return u"ASAP"
        else:
            return _format_datetime(dt)

    builder = FormatTextTableBuilder() \
        .add_formatter(datetime.timedelta, format_duration) \
        .add_formatter(datetime.datetime, _format_datetime) \
        .add_formatter(FieldList, lambda val: u", ".join(val.fields)) \
        .add_formatter(AllTags, lambda x: u", ".join(sorted(tn + u"=" + tv for tn in x.tags for tv in x.tags[tn]))) \
        .add_formatter(list, lambda x: u"\n".join(x)) \
        .add_column_formatter("next_scrape_at", _format_next_scrape_at)

    if color is None:
        builder.with_color_if_tty(out)
    else:
        builder.with_color(color)

    format_text_table = builder.build()

    out.write(format_text_table(table).encode('utf-8'))


from __future__ import absolute_import, division

from future.utils import PY3

import json

from ._formatter_common import FormatterCache


_DEFAULT_FORMATTERS = {
    int: lambda x: x,
    float: lambda x: x,
    type(None): lambda _: None,
    bool: lambda x: x,
}
if PY3:
    _DEFAULT_FORMATTERS[str] = lambda x: x
else:
    _DEFAULT_FORMATTERS[long] = lambda x: x
    _DEFAULT_FORMATTERS[unicode] = lambda x: x


class FormatJsonBuilder(object):
    def __init__(self):
        self._use_default_formatters = True
        self._formatters = {}
        self._column_formatters = {}

    def with_default_formatters(self, val):
        self._use_default_formatters = val
        return self

    def add_formatter(self, column_type, formatter):
        self._formatters[column_type] = formatter
        return self

    def add_formatters(self, formatters):
        self._formatters.update(formatters)
        return self

    def add_column_formatter(self, column_name, formatter):
        self._column_formatters[column_name] = formatter
        return self

    def add_column_formatters(self, column_formatters):
        self._column_formatters.update(column_formatters)
        return self

    def build(self):
        formatters = _DEFAULT_FORMATTERS.copy() if self._use_default_formatters else {}
        formatters.update(self._formatters)

        def format_json(table):
            return _format_json(
                table,
                formatters,
                self._column_formatters,
            )

        return format_json


def _format_json(table, formatters, column_formatters):
    format_cache = FormatterCache(formatters, column_formatters)

    val = json.dumps(dict(
            rows=[
                {c.name: format_cache.format_cell(c, row.cells_dict[c.name]) for c in table.columns}
                for row in table.rows
            ]
        ),
        sort_keys=True,
        indent=4,
        separators=(',', ': '),
        ensure_ascii=True)

    # We want to remain consistent - the output of this function should be unicode.
    # Also, its maximally friendly to escape non-ASCII characters, so, doing that is
    # nice too. But, with Python2, if you escape non-ASCII characters (ensure_ascii=True),
    # that also means that the result is a str (ie: bytes) instance. So, we convert that
    # to a unicode. With Python3, the result is always a str (ie: unicode in Py2 parlance).
    if not PY3:
        return val.decode('ascii')
    else:
        return val

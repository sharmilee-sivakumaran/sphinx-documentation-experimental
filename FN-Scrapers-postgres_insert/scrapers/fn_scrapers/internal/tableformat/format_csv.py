from __future__ import absolute_import

from future.utils import PY3
from future.builtins import str as text

import csv
import io

from ._formatter_common import FormatterCache


_DEFAULT_FORMATTERS = {
    int: lambda x: text(int),
    float: lambda x: text(int),
    type(None): lambda _: None,
    bool: lambda x: text(x),
}
if PY3:
    _DEFAULT_FORMATTERS[str] = lambda x: x
else:
    _DEFAULT_FORMATTERS[long] = lambda x: text(x)
    _DEFAULT_FORMATTERS[unicode] = lambda x: x


class FormatCsvBuilder(object):
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

        def format_csv(table):
            return _format_csv(
                table,
                formatters,
                self._column_formatters,
            )

        return format_csv


def _format_csv(table, formatters, column_formatters):
    format_cache = FormatterCache(formatters, column_formatters)

    # So, this is ugly: Python3 wants write CSVs in text mode, which makes sense.
    # In Python2, however, the CSV writer doesn't understand unicode at all and
    # just wants to write out bytes. So, we have to handle writing a bit differently
    # on Python2 vs Python3. But, the end result of the function is always a proper
    # string (ie: str on Python3 and unicode on Python2)

    if PY3:
        def _encode(x): return x
        out = io.StringIO()
    else:
        def _encode(x):
            if hasattr(x, 'encode'):
                return x.encode('utf-8')
            else:
                return x
        out = io.BytesIO()

    writer = csv.writer(out)

    # headers
    writer.writerow([_encode(c.name) for c in table.columns])

    # rows
    for row in table.rows:
        writer.writerow([_encode(format_cache.format_cell(c, row.cells_dict[c.name])) for c in table.columns])

    if PY3:
        return out.getvalue()
    else:
        return out.getvalue().decode('utf-8')


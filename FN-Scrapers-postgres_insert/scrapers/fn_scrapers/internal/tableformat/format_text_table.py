from __future__ import absolute_import, division

from future.utils import PY3
from future.builtins import range, str as text
from StringIO import StringIO

import colors

from ._formatter_common import FormatterCache


_DEFAULT_FORMATTERS = {
    int: lambda x: u"{:,}".format(x),
    float: lambda x: u"{:,}".format(x),
    type(None): lambda _: u"-",
    bool: lambda x: text(x),
}
if PY3:
    _DEFAULT_FORMATTERS[str] = lambda x: x
else:
    _DEFAULT_FORMATTERS[long] = lambda x: u"{:,}".format(x)
    _DEFAULT_FORMATTERS[unicode] = lambda x: x


_DEFAULT_ALIGNMENTS = {
    int: 1,
    float: 1,
}
if not PY3:
    _DEFAULT_ALIGNMENTS[long] = 1


LEFT = -1
CENTER = 0
RIGHT = 1


class FormatTextTableBuilder(object):
    def __init__(self):
        self._color = False

        self._translations = {}

        self._use_default_formatters = True
        self._formatters = {}
        self._column_formatters = {}

        self._use_default_alignments = True
        self._alignments = {}
        self._column_alignments = {}

    def with_color(self, color):
        self._color = color
        return self

    def with_color_if_tty(self, out):
        if hasattr(out, "isatty"):
            self._color = out.isatty()
        else:
            self._color = False
        return self

    def add_translation(self, column_name, translation):
        self._translations[column_name] = translation
        return self

    def add_translations(self, translations):
        self._translations.update(translations)
        return self

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

    def with_default_alignments(self, val):
        self._use_default_alignments = val
        return self

    def add_alignment(self, column_type, alignment):
        self._alignments[column_type] = alignment
        return self

    def add_alignments(self, alignments):
        self._alignments.update(alignments)
        return self

    def add_column_alignment(self, column_name, alignment):
        self._column_alignments[column_name] = alignment
        return self

    def add_column_alignments(self, column_alignments):
        self._column_alignments.update(column_alignments)
        return self

    def build(self):
        formatters = _DEFAULT_FORMATTERS.copy() if self._use_default_formatters else {}
        formatters.update(self._formatters)

        alignments = _DEFAULT_ALIGNMENTS.copy() if self._use_default_alignments else {}
        alignments.update(self._alignments)

        def format_text_table(table):
            return _format_text_table(
                table,
                self._color,
                self._translations,
                formatters,
                self._column_formatters,
                alignments,
                self._column_alignments,
            )

        return format_text_table


def _format_text_table(
        table,
        include_color=False,
        translations=None,
        formatters=None,
        column_formatters=None,
        alignments=None,
        column_alignments=None):
    format_cache = FormatterCache(formatters, column_formatters)

    def _column_name(column):
        if column.name in translations:
            return translations[column.name]
        else:
            return column.name

    def _alignment(column):
        if column.name in column_alignments:
            return column_alignments[column.name]
        elif column.column_type in alignments:
            return alignments[column.column_type]
        else:
            return -1

    def _max_column_width(c):
        return max(len(l) for r in table.rows for l in format_cache.format_cell_lines(c, r.cells_dict[c.name]))

    def _row_lines(r):
        return max(len(format_cache.format_cell_lines(c, r.cells_dict[c.name])) for c in table.columns)

    if table.rows:
        column_widths = {
            column.name: _max_column_width(column)
            for column in table.columns
        }
    else:
        column_widths = {column.name: 0 for column in table.columns}
    for column in table.columns:
        if len(_column_name(column)) > column_widths[column.name]:
            column_widths[column.name] = len(_column_name(column))

    out = StringIO()

    def _write_sep(c1, c2):
        out.write(c1)
        for column in table.columns:
            for _ in range(column_widths[column.name] + 2):
                out.write(c2)
            out.write(c1)
        out.write(u"\n")

    def _write_value(column, color_val, width):
        if _alignment(column) == -1:
            pre_space = 0
            post_space = column_widths[column.name] - width
        elif _alignment(column) == 1:
            pre_space = column_widths[column.name] - width
            post_space = 0
        else:
            pre_space = (column_widths[column.name] - width) // 2
            post_space = (column_widths[column.name] - width) // 2
            if (column_widths[column.name] - width) % 2 == 1:
                post_space += 1

        out.write(u" " * (pre_space + 1))
        out.write(color_val)
        out.write(u" " * (post_space + 1))

    def _write_headers():
        out.write(u"|")
        for column in table.columns:
            column_name = _column_name(column)
            _write_value(column, column_name, len(column_name))
            out.write(u"|")
        out.write(u"\n")

    def _write_row(row, line_num):
        row_tags = {t for c in row.cells for t in c.tags} | row.tags
        if "bad" in row_tags:
            color_func = colors.red
        elif "warning" in row_tags:
            color_func = colors.yellow
        elif "good" in row_tags:
            color_func = colors.green
        else:
            def color_func(x): return x

        out.write(u"|")
        for column in table.columns:
            cell = row.cells_dict[column.name]
            val_lines = format_cache.format_cell_lines(column, cell)
            if line_num < len(val_lines):
                val = val_lines[line_num]
            else:
                val = u""
            width = len(val)

            color_val = val
            if include_color:
                color_val = color_func(color_val)
                if cell.tags:
                    color_val = colors.bold(color_val)

            _write_value(column, color_val, width)

            out.write(u"|")
        out.write(u"\n")

    _write_sep(u"+", u"-")

    _write_headers()

    _write_sep(u"+", u"=")

    if table.rows:
        for row in table.rows:
            for line_num in range(0, _row_lines(row)):
                _write_row(row, line_num)

        _write_sep(u"+", u"-")

    return out.getvalue()

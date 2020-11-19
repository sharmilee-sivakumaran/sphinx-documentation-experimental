from __future__ import absolute_import


class FormatterCache(object):
    def __init__(self, formatters, column_formatters):
        self._formatters = formatters
        self._column_formatters = column_formatters
        self._format_cache = {}
        self._format_line_cache = {}

    def format_cell(self, column, cell):
        if id(cell) not in self._format_cache:
            if cell.value is not None:
                column_type = column.column_type
            else:
                column_type = type(None)

            if column.name in self._column_formatters:
                formatter = self._column_formatters[column.name]
            elif column_type in self._formatters:
                formatter = self._formatters[column_type]
            else:
                raise Exception(u"No formatter for type {}".format(column.column_type))

            self._format_cache[id(cell)] = formatter(cell.value)
        return self._format_cache[id(cell)]

    def format_cell_lines(self, column, cell):
        if id(cell) not in self._format_line_cache:
            val = self.format_cell(column, cell).splitlines()
            if not val:
                val = ['']
            self._format_line_cache[id(cell)] = val
        return self._format_line_cache[id(cell)]

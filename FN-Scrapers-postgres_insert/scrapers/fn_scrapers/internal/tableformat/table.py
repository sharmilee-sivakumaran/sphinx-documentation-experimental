from __future__ import absolute_import

from future.utils import iterkeys, python_2_unicode_compatible
from future.builtins import str as text

import attr


_SENTINAL = object()


def memoize(func):
    def _memoize(self):
        if not hasattr(self, '__memoized_values'):
            self.__memoized_values = {}

        if func.__name__ not in self.__memoized_values:
            try:
                self.__memoized_values[func.__name__] = _SENTINAL
                self.__memoized_values[func.__name__] = func(self)
            except:
                del self.__memoized_values[func.__name__]
                raise
        else:
            if self.__memoized_values[func.__name__] is _SENTINAL:
                raise Exception(u"Encountered recursive call while calculating a value. A value cannot "
                                u"depend on itself.")
        return self.__memoized_values[func.__name__]

    return _memoize


def _tag_to_list(tags):
    if tags is None:
        return []
    elif isinstance(tags, (str, text)):
        return [tags]
    else:
        return tags


@attr.s
class ColumnDef(object):
    name = attr.ib()
    value_func = attr.ib()
    column_type = attr.ib()
    tag_func = attr.ib()


@python_2_unicode_compatible
class CellValue(object):
    def __init__(self, row, data, column_def):
        self._row = row
        self._data = data
        self._column_def = column_def

    @property
    def name(self):
        return self._column_def.name

    @property
    @memoize
    def value(self):
        val = self._column_def.value_func(self._row)
        if self._column_def.column_type is not None:
            if not isinstance(val, self._column_def.column_type):
                raise Exception(
                    u"Returned invalid value for cell in column '{name}'. The returned "
                    u"value, '{val}', has type '{val_type}', but '{column_type}' was expected.".format(
                        name=self._column_def.name,
                        val=val,
                        val_type=type(val),
                        column_type=self._column_def.column_type))
        return val

    @property
    @memoize
    def tags(self):
        if self._column_def.tag_func:
            return {t for t in _tag_to_list(self._column_def.tag_func(self._row, self.value))}
        else:
            return set()

    def __str__(self):
        if self.tags:
            tags_str = u" ({})".format(u", ".join(t for t in self.tags))
        else:
            tags_str = u""
        return u"{}: {}{}".format(self._column_def.name, self.value, tags_str)


@python_2_unicode_compatible
class RowValue(object):
    def __init__(self, data, columns_defs, row_tag_funcs):
        self._data = data
        self._column_defs = columns_defs
        self._row_tag_funcs = row_tag_funcs

    @property
    def data(self):
        return self._data

    @property
    @memoize
    def cells(self):
        return [CellValue(self, self._data, cd) for cd in self._column_defs]

    @property
    @memoize
    def cells_dict(self):
        return {c.name: c for c in self.cells}

    @property
    @memoize
    def tags(self):
        return {t for rtf in self._row_tag_funcs for t in _tag_to_list(rtf(self))}

    def __str__(self):
        if self.tags:
            tags_str = u" [{}]".format(u", ".join(t for t in self.tags))
        else:
            tags_str = u""
        cells_str = u", ".join(text() for c in self.cells)
        return cells_str + tags_str


@python_2_unicode_compatible
class Column(object):
    def __init__(self, name, row_values, column_type):
        self._name = name
        self._row_values = row_values
        self._column_type = column_type

    @property
    def name(self):
        return self._name

    @property
    @memoize
    def column_type(self):
        if self._column_type is not None:
            return self._column_type
        type_ = None
        for r in self._row_values:
            if r.cells_dict[self._name].value is None:
                continue
            if type_ is None:
                type_ = type(r.cells_dict[self._name].value)
            else:
                type2 = type(r.cells_dict[self._name].value)
                if issubclass(type_, type2):
                    type_ = type2
                elif issubclass(type2, type_):
                    pass
                else:
                    raise Exception(u"Conflicting types for column {}: {} and {}".format(self._name, type_, type2))
        return type_

    def __str__(self):
        return u"{} ({})".format(self.name, self.column_type)


@python_2_unicode_compatible
class Table(object):
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns

    @property
    def rows(self):
        return self._rows

    @property
    def columns(self):
        return self._columns

    def __str__(self):
        columns_str = u" | ".join(text(c) for c in self.columns)
        rows_str = u"\n".join(text(r) for r in self.rows)
        return columns_str + u"\n" + rows_str


ASCENDING = 1
DESCENDING = -1


class TableBuilderBuilder(object):
    def __init__(self):
        self._column_names = set()
        self._column_defs = []
        self._row_tag_funcs = []
        self._fields = None
        self._sorts = []
        self._include_funcs = []

    def add_column(self, name, value_func, column_type=None, tag_func=None):
        if name in self._column_names:
            raise Exception(u"Multiple columns with name: {}".format(name))
        self._column_names.add(name)
        self._column_defs.append(ColumnDef(name, value_func, column_type, tag_func))
        return self

    def add_row_tag_func(self, row_tag_func):
        self._row_tag_funcs.append(row_tag_func)
        return self

    def with_fields(self, fields):
        self._fields = fields

    def add_sort(self, column_name, direction):
        self._sorts.append((column_name, direction))

    def add_include_func(self, include_func):
        self._include_funcs.append(include_func)

    def add_include_funcs(self, include_funcs):
        self._include_funcs.extend(include_funcs)

    def build(self):
        if self._fields is None:
            fields = [cd.name for cd in self._column_defs]
        else:
            fields = self._fields

        def build_table(row_data):
            return _build_table(
                row_data,
                self._column_defs,
                self._row_tag_funcs,
                fields,
                self._sorts,
                self._include_funcs
            )

        return build_table


def _build_table(row_data, column_defs, row_tag_funcs, fields, sorts, include_funcs):
    rows = [
        RowValue(data, column_defs, row_tag_funcs)
        for data in row_data
    ]

    if include_funcs:
        rows = [r for r in rows if all(f(r) for f in include_funcs)]

    all_columns = {cd.name: Column(cd.name, rows, cd.column_type) for cd in column_defs}

    columns = [all_columns[f] for f in fields]

    sorts = list(sorts)
    while sorts:
        column_name, direction = sorts.pop()
        rev = direction == DESCENDING
        rows.sort(key=lambda r: r.cells_dict[column_name].value, reverse=rev)

    return Table(rows, columns)


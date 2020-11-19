'''
JSON Formatter

There's some weird stuff going on here, mainly the JsonList and JsonMap
classes. These are equivalent to lists and dictionaries but were needed
to support the funky outputting rules I desired for this application,
primarily:
 
 - Dictionary keys are sorted by added order for JsonMap.
 - Output needs to be tabbed for visual scanning, but the data values should
   data values should not be tabbed for easier reading.

Despite these two rules, the results are still valid json and can be
piped into the jq library for more complex analysis.
'''

from __future__ import absolute_import, print_function

from collections import OrderedDict
import datetime
import json

from sql_comp.sql import RecordOnlyIn, Pair, json_diff
from sql_comp.formatters.base import Formatter


class JsonFormatterValue(object):
    '''Custom formatting for JsonFormatter'''
    terms = '()'

    def print(self, indent=None):
        '''Print the item'''
        is_root = indent is None
        if is_root:
            indent = 3
            print('  ' * (indent - 1), end='')
        print(self.terms[0])
        self.print_items(indent)
        print('{}{}'.format('  '*(indent-1), self.terms[1]), end='')
        if is_root:
            print('')

    def print_items(self, indent):
        pass


class JsonList(JsonFormatterValue):
    '''
    Allows formatter to distinguish between from formatter lists and data lists. Yeah, I know
    the name is dumb.
    '''
    terms = '[]'

    def __init__(self, *args):
        self.lis = list(args)

    def append(self, item):
        self.lis.append(item)

    def print_items(self, indent):
        tab = '  ' * indent
        init = True
        for val in self.lis:
            if not init:
                print(',')
            init = False
            print(tab, end='')
            if isinstance(val, JsonFormatterValue):
                val.print(indent=indent+1)
            else:
                print(json.dumps(val), end='')
        print('')


class JsonMap(JsonFormatterValue):
    terms = '{}'
    def __init__(self, *items):
        self.dic = OrderedDict(items)

    def print_items(self, indent):
        tab = '  ' * indent
        init = True
        for key, val in self.dic.items():
            if not init:
                print(',')
            init = False
            if isinstance(val, JsonFormatterValue):
                print('{}"{}": '.format(tab, key), end='')
                val.print(indent=indent+1)
            else:
                print('{}"{}": {}'.format(tab, key, json.dumps(val)), end='')
        print('')



class JsonFormatter(Formatter):
    def header(self):
        print('{')
        for key, val in self._header_fields():
            print('  "{}": {},'.format(key, json.dumps(val)))
        print('  "report": [')
    
    def footer(self):
        print("  ],")
        print('  "total": {},'.format(self.total))
        print('  "differences": {}'.format(self.different))
        print("}")

    def report(self, result):
        if isinstance(result, RecordOnlyIn):
            return JsonMap(
                ('name', result.name),
                ('type', 'only_in'),
                ('only_in', result.side)
            ).print()
        if isinstance(result, Pair):
            diffs = JsonList()
            for path, left, right in json_diff(result.left, result.right,
                                               comparator=self.comparator):
                if self.comparator.filter_path(path):
                    continue
                if not left:
                    diffs.append(JsonMap(
                        ('op', 'add'),
                        ('path', path),
                        ('value', right)
                    ))
                elif not right:
                    diffs.append(JsonMap(
                        ('op', 'del'),
                        ('path', path),
                        ('value', left)
                    ))
                else:
                    diffs.append(JsonMap(
                        ('op', 'mod'),
                        ('path', path),
                        ('value', JsonMap(
                            ('left', left),
                            ('right', right)
                        ))
                    ))

            if not diffs and self.args.show_same:
                return JsonMap(
                    ('name', result.name),
                    ('type', 'same'),
                ).print()

            JsonMap(
                ('name', result.name),
                ('type', 'diff'),
                ('diffs', diffs)
            ).print()

    @classmethod
    def print_map(cls, entry, indent=None):
        entry.print(indent)

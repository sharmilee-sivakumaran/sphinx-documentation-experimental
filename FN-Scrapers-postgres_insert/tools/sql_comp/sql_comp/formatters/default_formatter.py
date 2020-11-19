from __future__ import absolute_import, print_function

from datetime import datetime
import json
import sys

from sql_comp.sql import json_diff, RecordOnlyIn, Pair, json_dumps
from sql_comp.formatters.base import Formatter

class DefaultFormatter(Formatter):
    '''
    The default formatter. Outputs a human-readable report.
    '''
    def __init__(self, *args, **kwargs):
        super(DefaultFormatter, self).__init__(*args, **kwargs)
        self.last_header = None

    def header(self):
        for key, val in self._header_fields():
            print("{}: {}".format(key.upper(), val))
        sys.stdout.flush()

    def footer(self):
        self.page_break("{} out of {} Match".format(self.total - self.different, self.total))

    def report(self, result):
        if isinstance(result, RecordOnlyIn):
            self.page_break('{} Only In {}'.format(
                result.name, result.side.title()))
        elif isinstance(result, Pair):
            shown_header = result.name == self.last_header
            for path, left, right in json_diff(result.left, result.right,
                                               comparator=self.comparator):
                if self.comparator.filter_path(path):
                    continue
                if not shown_header:
                    shown_header = True
                    self.last_header = result.name
                    self.page_break(result.name)
                if not left:
                    print("ADD {}".format(path))
                    print('  ' + json_dumps(right))
                elif not right:
                    print("DEL {}".format(path))
                    print('    ' + json_dumps(left))
                else:
                    print("MOD {}".format(path))
                    print('    L: ' + json_dumps(left))
                    print('    R: ' + json_dumps(right))
                sys.stdout.flush()

            if not shown_header and self.args.show_same:
                self.page_break('{} Okay'.format(result.name))

    def page_break(self, msg):
        print('\n=== {} {}'.format(msg, '='*(80-5-len(msg))))

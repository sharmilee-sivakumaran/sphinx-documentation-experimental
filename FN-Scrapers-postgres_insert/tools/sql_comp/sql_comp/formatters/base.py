'''
Base formatter.
'''

from collections import OrderedDict
from datetime import datetime
import sys

from sql_comp.sql import Pair, json_diff, RecordOnlyIn

class Formatter(object):
    def __init__(self, comparator, session, args, config):
        self.comparator = comparator
        self.session = session
        self.args = args
        self.config = config

        self.total = 0
        self.different = 0
        self.current = None
    
    def header(self):
        '''Displays any opening info. '''

    def footer(self):
        '''Displays and closing info. '''

    def iterate(self, generator):
        '''Receive an iterator and generate the body of the report. '''
        self.total = 0
        self.different = 0
        for item in generator:
            self.total += 1
            if item.name != self.current and isinstance(item, Pair):
                self.current = item.name
                if any(json_diff(item.left, item.right, comparator=self.comparator)):
                    self.different += 1
            elif isinstance(item, RecordOnlyIn):
                self.different += 1
            self.report(item)
        
    def report(self, item):
        '''Handle an individual item (either an instance of ExistsOnlyIn or Pair). '''

    def _header_fields(self):
        '''Provides a useful default list of fields for formatter headers. '''
        return [
            ("time", "{:%Y-%m-%d %H:%M:%S} UTC".format(datetime.utcnow())),
            ("command", ' '.join([
                '"{}"'.format(cmd) if any(c in cmd for c in (' ', '$(')) else cmd
                for cmd in sys.argv[1:]
            ])),
            ("left", "{username}@{host}".format(**self.session.left_creds)),
            ("right", "{username}@{host}".format(**self.session.right_creds))
        ]

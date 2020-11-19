from datetime import datetime, date
import json

from .sql import Session, QueryOptions, json_serialize
from .formatters import get_formatters

class SqlComp(object):
    '''The root SQL Comparison object. '''
    def __init__(self, args, config, env_type=None):
        self.args = args
        self.config = config
        self.credentials = self.get_envs(env_type)
        self.json_filters = (
            '.id', '_id', '.created_at', '.updated_at', '.last_scraped_at',
            '.version'
        )
        self._filters_ready = False

        self.trimmings = {}
        self.custom_compares = {}

    def run(self):
        '''Run the comparator'''
        raise NotImplementedError()

    def _circ(self, record):
        '''circular reference handler. '''
        if record.__class__ in self.trimmings:
            return '<{} {}>'.format(
                record.__class__.__name__, 
                self.trimmings[record.__class__](record)
            )
        return '<{}>'.format(record.__class__.__name__)

    @classmethod
    def parser(cls, config, parser):
        envs = list(config['envs'])
        parser.add_argument('--left', choices=envs, default=None,
                            help='The first environment to compare to')
        parser.add_argument('--right', choices=envs, default=None,
                            help='The second environment to compare to')

        parser.add_argument('--logging', type=str, default='warning',
                            choices=['debug', 'info', 'warning', 'critical'],
                            help='Set up logging level')
        parser.add_argument('--left_only', help='Query only the left database',
                            action='store_true')
        parser.add_argument('--right_only', help='Query only the right database',
                            action='store_true')
        parser.add_argument('--clear_ignored', action='store_true',
                            help='Clear default list of ignored fields.')
        parser.add_argument('--ignore', nargs='*',
                            help='List of field patterns that are not checked for change.')
        parser.add_argument('--show_same', action='store_true',
                            help='Show recoreds that match as well as mismatches.')
        parser.add_argument('--format', default='default', choices=get_formatters().keys(),
                            help='Output format')

    def node_filter(self, obj):
        '''Filters ORM objects while crawling. '''
        return False

    def get_envs(self, env_type=None):
        '''Return a pair of environments to use. '''
        left = None
        right = None
        envs = self.config['envs']
        if self.args.left:
            left = envs[self.args.left]
        if self.args.right:
            right = envs[self.args.right]
        if env_type:
            for env in envs:
                if envs[env].get('type') == env_type:
                    if not left:
                        left = envs[env]
                    elif not right:
                        right = envs[env]
        if not left or not right:
            raise ValueError("Unable to determine comparison environments")
        return left, right


    def diff(self, obj, ident, filters):
        '''
        Main diff entry point. Designed to handle a basic run of the program. Should probably
        be split up a bit or moved to `run()` so inherited comparators can just call
        `super(..., self).run()`.
        '''
        with Session(*self.credentials) as session:
            options = QueryOptions(
                circ=self._circ, trimmings=self.trimmings, filters=filters,
                sort=ident, field_filter=self.json_filters,
                node_filter=self.node_filter)
            if self.args.left_only or self.args.right_only:
                return self.single_side_report(
                    session, 
                    session.left_session if self.args.left_only else session.right_session,
                    obj, options
                )
            self.diff_report(session, session.query(
                obj, ident, options, node_filter=self.node_filter))

    def single_side_report(self, session, side_sess, obj, options):
        '''
        Run the query against a single side (session) and print the raw results. Useful for
        troubleshooting unexpected diffs.
        '''
        for result in session.yield_rows(side_sess, obj, options, node_filter=self.node_filter):
            try:
                print(json.dumps(result, sort_keys=True, indent=2, default=json_serialize))
            except IOError: 
                # piping to less will sometimes close stdout prematurely
                pass

    def get_json_filters(self):
        '''Lazy load jsonpath filters from given arguments. '''
        if self._filters_ready:
            return self.json_filters

        filters = list(self.json_filters)
        if self.args.clear_ignored:
            filters = list()
        if self.args.ignore:
            filters.extend(self.args.ignore)
        self.json_filters = filters
        return filters



    def filter_path(self, path):
        '''Whether to filter a jsonpath. True: Filter. False: Report. '''
        for entry in self.get_json_filters():
            if entry.startswith('~') and entry[1:] in path:
                return True
            if path.endswith(entry):
                return True
        return False

    def diff_report(self, session, generator):
        formatter = self.args.format(self, session, self.args, self.config)
        formatter.header()
        formatter.iterate(generator)
        formatter.footer()

    @classmethod
    def trim(cls, *keys):
        '''
        Shortcut method to set up trimmings. Pass it one or more keys (or a
        single string with white-space separated keys) and it returns a
        function to return a dict with those keys given an SqlAlchemy 
        record.
        '''
        if len(keys) == 1 and ' ' in keys[0]:
            keys = keys[0].strip().split()
        def _inner(record):
            return {key: getattr(record, key) for key in keys}
        return _inner

    @classmethod
    def set_compare(cls, left, right, path):
        '''
        Does a recursively deep-translation to a set, converting lists into
        tuples, dictionaries into tuple (key, val) pairs, and sets into sorted
        tuples. Then checks the difference between the two sets.
        '''
        def _tupify(obj):
            if isinstance(obj, dict):
                return tuple([
                    (key, _tupify(obj[key])) for key in sorted(obj.keys())
                ])
            if isinstance(obj, list):
                return tuple([_tupify(val) for val in obj])
            if isinstance(obj, set):
                return tuple([_tupify(val) for val in sorted(obj)])
            return obj

        if left is None and right is None:
            raise StopIteration()
        if left is None or right is None:
            yield path, left, right
            raise StopIteration()

        path += '[]'
        assert isinstance(left, list)
        assert isinstance(right, list)

        tuples = {}

        sets = [set(), set()]

        for i, lis in enumerate((left, right)):
            for val in lis:
                tup = _tupify(val)
                tuples[tup] = val
                sets[i].add(tup)

        for tup in sets[0] - sets[1]:
            yield path, tuples[tup], None
        for tup in sets[1] - sets[0]:
            yield path, None, tuples[tup]

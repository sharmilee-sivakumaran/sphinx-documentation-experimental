

from collections import namedtuple, OrderedDict
from datetime import date, datetime, time
import json
import logging
import re

import sqlalchemy
from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy.engine import create_engine
from sqlalchemy.dialects import postgresql
import jsonpath_ng

LOGGER = logging.getLogger(__name__)

Pair = namedtuple('Pair', 'name left right')

class RecordOnlyIn(object):
    '''Record only in left or right. '''
    def __init__(self, side, name, record=None):
        self.name = str(name)
        self.side = side
        self.record = record

    def __str__(self):
        return u"<OnlyIn{} {}>".format(self.side.title(), self.name)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if not isinstance(other, RecordOnlyIn):
            return False
        return self.name == other.name and self.side == other.side
    
class OnlyInRight(RecordOnlyIn):
    '''Only in right exception. '''
    def __init__(self, name, record=None):
        super(OnlyInRight, self).__init__('right', name, record)
    
class OnlyInLeft(RecordOnlyIn):
    '''Only in left exception. '''
    def __init__(self, name, record=None):
        super(OnlyInLeft, self).__init__('left', name, record)

class QueryOptions(object):
    '''Common query options collection. '''
    def __init__(self, filters=None, limit=None, sort=None, asc=True, ids=None,
                 trimmings=None, circ=None, field_filter=None, node_filter=None):
        self.filters = filters
        self.limit = limit
        self.sort = sort
        self.asc = asc
        self.trimmings = trimmings or {}
        self.ids = ids
        self.circ = circ
        self.field_filter = field_filter or (lambda x: False)
        self.node_filter = node_filter or (lambda x: False)

class Session(object):
    CONN_STRING = ("postgresql+psycopg2://{username}:{password}@{host}:{port}/"
                   "{database}")

    FILTER_METHODS = {
        '==': '__eq__',
        '=': '__eq__',
        '!=': '__ne__',
        '<': '__lt__',
        '>': '__gt__',
        '<=': '__le__',
        '>=': '__ge__',
        ' in ': 'in_'
    }

    FILTER_PATTERN = re.compile(
        r'^(?P<field>.*?)\s*(?P<op>' + '|'.join(FILTER_METHODS)+r')\s*(?P<val>.*)$')

    def __init__(self, left_creds, right_creds, comparator=None):
        self.left_creds = left_creds
        self.right_creds = right_creds
        self.left_engine = self._create_engine(**left_creds)
        self.left_session = sqlalchemy.orm.sessionmaker(bind=self.left_engine)()
        self.right_engine = self._create_engine(**right_creds)
        self.right_session = sqlalchemy.orm.sessionmaker(bind=self.right_engine)()

        self.comparator = comparator

    def close(self):
        self.left_session.close()
        # self.left_engine.close()
        self.right_session.close()
        # self.right_engine.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @classmethod
    def _create_engine(cls, **creds):
        if 'port' not in creds:
            creds['port'] = 5432
        return create_engine(cls.CONN_STRING.format(**creds))

    @classmethod
    def _next_or_none(cls, iterator):
        '''
        Returns the next item from an iterator, or None on StopIteration.
        '''
        try:
            return next(iterator)
        except StopIteration:
            return None

    def query(self, model, ident, options=None, node_filter=None):
        '''
        Yields a matched diff of a record.
        '''
        left_iter = self.yield_rows(self.left_session, model, options, node_filter)
        right_iter = self.yield_rows(self.right_session, model, options, node_filter)
        for result in self.iter_pair(left_iter, right_iter, ident):
            yield result

    @classmethod
    def get_ident(self, record, identity):
        if not record:
            return None
        if isinstance(identity, basestring):
            return record[identity]
        return tuple(record[part] for part in identity)


    @classmethod
    def iter_pair(cls, left_iter, right_iter, ident):
        '''
        Accepts a pair of dictionary generators (left and right) and an identity
        key, generates a series of Pair, OnlyInLeft, or OnlyInRight objects.
        '''
        left_stack = OrderedDict()
        right_stack = OrderedDict()
        while True:
            left = cls._next_or_none(left_iter)
            right = cls._next_or_none(right_iter)

            left_id = cls.get_ident(left, ident)
            right_id = cls.get_ident(right, ident)

            LOGGER.debug("buffer got: %s, %s", left and left_id, right and right_id)

            if right:
                right_stack[right_id] = right
            if left:
                left_stack[left_id] = left
            LOGGER.debug("Left stack: [%s]", ', '.join(str(key) for key in left_stack.keys()))
            LOGGER.debug("Right stack: [%s]", ', '.join(str(key) for key in right_stack.keys()))

            if left and left_id in right_stack:
                key = left_id
            elif right and right_id in left_stack:
                key = right_id
            elif not left or not right:
                for key in left_stack:
                    yield OnlyInLeft(key, left_stack[key])
                    del left_stack[key]
                for key in right_stack:
                    yield OnlyInRight(key, right_stack[key])
                    del right_stack[key]
                while True:
                    right = cls._next_or_none(right_iter)
                    if not right:
                        break
                    yield OnlyInRight(right_id, right)
                while True:
                    left = cls._next_or_none(left_iter)
                    if not left:
                        break
                    yield OnlyInLeft(left_id, left)
                raise StopIteration()
            else:
                continue

            left_keys = list(left_stack.keys())
            right_keys = list(right_stack.keys())

            left_index = left_keys.index(key)
            right_index = right_keys.index(key)

            for i in range(left_index):
                missing_key = left_keys[i]
                yield OnlyInLeft(missing_key, left_stack[missing_key])
                del left_stack[missing_key]

            for i in range(right_index):
                missing_key = right_keys[i]
                yield OnlyInRight(missing_key, right_stack[missing_key])
                del right_stack[missing_key]

            yield Pair(key, left_stack[key], right_stack[key])
            del right_stack[key]
            del left_stack[key]

    @classmethod
    def yield_rows(cls, session, model, options, node_filter):
        '''Creates a query model and iterates across it.'''
        start = 0
        query = session.query(model)
        filters = []
        if options.ids:
            filters.append(model.id.in_(options.ids))
        if options.filters:
            for clause in options.filters:
                match = cls.FILTER_PATTERN.match(clause)
                assert match, "Invalid filter '{}'".format(clause)
                match = match.groupdict()
                assert '.' not in match['field'], (
                    'Invalid filter, subfields not supported ("{}")'
                    ).format(match['field'])
                field = getattr(model, match['field'])
                op = getattr(field, cls.FILTER_METHODS[match['op']])
                filters.append(op.__call__(json.loads(match['val'])))
        if filters:
            query = query.filter(*filters)
        sorts = []
        if options.sort:
            sort_list = options.sort
            if isinstance(sort_list, basestring):
                sort_list = [sort_list]
            for sort_field in sort_list:
                sort_field = sort_field.strip()
                sort = getattr(model, sort_field)
                if not options.asc:
                    sort = sqlalchemy.desc(sort)
                sorts.append(sort)

        id_sort = model.id
        if not options.asc:
            id_sort = sqlalchemy.desc(id_sort)
        sorts.append(id_sort)

        query = query.order_by(*sorts)
        LOGGER.debug(
            'Query: \n%s',
            str(query.statement.compile(dialect=postgresql.dialect()))
        )
        while True:
            stop = start + 100
            if options.limit:
                stop = min(stop, options.limit)
            try:
                rows = query.slice(start, stop).all()
                if not rows:
                    raise StopIteration
                for row in rows:
                    yield cls.record_to_dict(row, options=options)
            except sqlalchemy.exc.OperationalError as exception:
                LOGGER.info('SQL Operational Error - ' + exception.message)
                session.rollback()
                continue
            start = stop

    @classmethod
    def record_to_dict(cls, record, options, error=None, found=None, _cnt=0):
        '''Converts an sqlalchemy record to a json-like object.

        The `error` parameter is designed to work around unhandled cases such as
        recursion or unloaded entities. If set to None (default), then any problem
        value will also be set to None. However, if set to a callable (function,
        lambda, method, etc) it will pass the error message as a single parameter
        to the callable.

        :param record: The SQLAlchemy ORM Record
        :param error: Error handler.
        :param found: set used for internal recursion tracking/prevention.
        '''
        def _error(msg):
            '''Error routing function'''
            if error and hasattr(error, '__call__'):
                return error(msg)
            return None

        if found is None:
            found = set()
        if record not in found:
            found.add(record)
        else:
            if not options.circ:
                return _error("{}@{}".format(record.__class__.__name__, record.id))
            options.circ(record)

        if _cnt > 20:
            raise MaxRecursionDepthException(record.__class__.__name__)
        
        if record.__class__ in options.trimmings and _cnt:
            output = options.trimmings[record.__class__](record)
            return output
        mapper = sqlalchemy.orm.class_mapper(record.__class__)
        output = {}
        for column in mapper.columns:
            value = getattr(record, column.key)
            if isinstance(value, datetime):
                output[column.key] = value.isoformat().replace('+00:00', '')+'Z'
            elif isinstance(value, date):
                output[column.key] = value.isoformat()
            else:
                output[column.key] = value
        for name, relation in mapper.relationships.items():
            try:
                related_obj = getattr(record, name)
                if related_obj is not None:
                    if relation.uselist:
                        children = []
                        for child in related_obj:
                            if child in found or options.node_filter(child):
                                continue
                            children.append(cls.record_to_dict(
                                child, error=error, found=found,
                                options=options, _cnt=_cnt+1))
                        output[name] = children
                    else:
                        if relation in found or options.node_filter(relation):
                            continue
                        output[name] = cls.record_to_dict(
                            related_obj, error=error, found=found,
                            options=options, _cnt=_cnt+1)
            except sqlalchemy.exc.InvalidRequestError as exc:
                output[name] = _error(exc.message)
            except MaxRecursionDepthException as exc:
                raise MaxRecursionDepthException(
                    '{}.{}'.format(record.__class__.__name__, exc.message)
                )
        return output

class MaxRecursionDepthException(Exception):
    '''Exceeded limit. '''


def json_dumps(obj, **kwargs):
    '''Runs json.dumps with default arguments'''
    kwargs['default'] = kwargs.get('default', json_serialize)
    kwargs['sort_keys'] = kwargs.get('sort_keys', True)
    return json.dumps(obj, **kwargs)


def json_serialize(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError ("Type %s not serializable" % type(obj))


def json_diff(left, right, comparator=None):
    jsonpath_filter = getattr(comparator, 'filter_path', lambda x: False)
    # sets = getattr(comparator, 'sets', [])

    custom_compares = comparator.custom_compares if comparator else {}

    addresses = {}
    for path in custom_compares:
        for doc in left, right:
            for struct in jsonpath_ng.parse(path).find(doc):
                if not isinstance(struct.value, (list, dict)):
                    raise ValueError(
                        "Custom Compares can only be done on collections: {}".format(struct))
                addresses[id(struct.value)] = path
    def _diff(a, b, path):
        if id(a) in addresses or id(b) in addresses:
            custom_key = addresses.get(id(a)) or addresses.get(id(b))
            for diff in custom_compares[custom_key].__call__(a, b, path):
                yield diff
        elif isinstance(a, dict):
            assert isinstance(b, dict)
            for key in sorted(set(a.keys() + b.keys())):
                item_path = '{}.{}'.format(path, key)
                if key not in a:
                    yield item_path, None, b[key]
                    continue
                if key not in b:
                    yield item_path, a[key], None
                    continue
                for diff in _diff(a[key], b[key], item_path):
                    yield diff
        elif isinstance(a, list):
            assert isinstance(b, list)
            length = min(len(a), len(b))
            length_diff = len(a) - len(b)
            for i in range(length):
                item_path = '{}[{}]'.format(path, i)
                for diff in _diff(a[i], b[i], item_path):
                    yield diff
            if length_diff == 0:
                pass
            elif length_diff > 0:
                for i in range(length, length + length_diff):
                    item_path = '{}[{}]'.format(path, i)
                    yield item_path, a[i], None
            elif length_diff < 0:
                for i in range(length, length - length_diff):
                    item_path = '{}[{}]'.format(path, i)
                    yield item_path, None, b[i]
        elif a != b:
            yield path, a, b
    for path, a, b in _diff(left, right, '$'):
        if not jsonpath_filter(path):
            yield path, a, b


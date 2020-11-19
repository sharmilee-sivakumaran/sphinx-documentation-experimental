from __future__ import absolute_import, print_function

from pprint import pprint

from .sql import Session, OnlyInLeft, OnlyInRight, Pair, json_diff
from .sql_comp import SqlComp

def test_iter_pair():
    def _gen(num=3):
        left, right = [], []
        for i in range(num):
            left.append({'key': i})
            right.append({'key': i})
        return left, right

    def _pair(a, b):
        result = list(Session.iter_pair(iter(a), iter(b), 'key'))
        for i in range(len(result)):
            if isinstance(result[i], Pair):
                result[i] = result[i].name
        return result

    result = _pair(*_gen())
    assert result == [0, 1, 2]

    left, right = _gen()
    left.insert(1, {'key': 'a'})
    assert _pair(left, right) == [0, OnlyInLeft('a'), 1, 2]

    left, right = _gen()
    left.insert(0, {'key': 'a'})
    assert _pair(left, right) == [OnlyInLeft('a'), 0, 1, 2]

    left, right = _gen()
    left.insert(3, {'key': 'a'})
    assert _pair(left, right) == [0, 1, 2, OnlyInLeft('a')]

    left, right = _gen()
    left.insert(1, {'key': 'a'})
    right.insert(1, {'key': 'z'})
    assert _pair(left, right) == [0, OnlyInLeft('a'), OnlyInRight('z'), 1, 2]

    left, right = _gen()
    left.insert(1, {'key': 'a'})
    left.insert(2, {'key': 'b'})
    assert _pair(left, right) == [0, OnlyInLeft('a'), OnlyInLeft('b'), 1, 2]

    left, right = _gen()
    left.insert(1, {'key': 'a'})
    left.insert(2, {'key': 'b'})
    right.insert(1, {'key': 'z'})
    right.insert(3, {'key': 'y'})
    assert _pair(left, right) == [
        0, OnlyInLeft('a'), OnlyInLeft('b'), OnlyInRight('z'),
        1, OnlyInRight('y'), 2]

def test_json_diff():
    a = {
        "sponsor_legislators": [
            "Alan T. Powell", 
            "Michael Thomas Gravley", 
            "James Allen Collins", 
            "Heath Nicholas Clark", 
            "David T. Clark", 
            "Vernon Jones"
        ], 
    }
    b = { 
        "sponsor_legislators": [
            "Alan T. Powell", 
            "Michael Thomas Gravley", 
            "James Allen Collins", 
            "Heath Nicholas Clark", 
            "David T. Clark", 
            "Vernon Jones", 
            "Allen Milne Peake"
        ], 
    }
    result = list(json_diff(a, b))
    pprint(result)
    assert result == [('$.sponsor_legislators[6]', None, 'Allen Milne Peake')]


def test_set_compare():
    result = list(SqlComp.set_compare(
        [1, 2, 3, 4],
        [3, 2, 1, 5],
        'foo'
    ))
    assert result == [
        ('foo[]', 4, None),
        ('foo[]', None, 5),
    ]

def test_set_compare_dict():
    result = list(SqlComp.set_compare(
        [{'foo': 1, 'bar': 2}, {'foo': 2, 'bar': 2}],
        [{'foo': 1, 'bar': 2}],
        'foo'
    ))
    assert result == [('foo[]', {'foo': 2, 'bar': 2}, None)]

def test_set_compare_list():
    result = list(SqlComp.set_compare(
        [['a', 'b', 'c'], ['b', 'c', 'a']],
        [['b', 'c', 'a'], ['c', 'a', 'b'], ['a', 'b', 'c']],
        'foo'
    ))
    assert result == [('foo[]', None, ['c', 'a', 'b'])]

def test_set_compare_complex():
    result = list(SqlComp.set_compare(
        [{'foo': list('abc'), 'bar': list('def')}],
        [{'foo': 1, 'bar': 2}],
        'foo'
    ))
    assert result == [
        ('foo[]', {'foo': list('abc'), 'bar': list('def')}, None),
        ('foo[]', None, {'foo': 1, 'bar': 2})
    ]
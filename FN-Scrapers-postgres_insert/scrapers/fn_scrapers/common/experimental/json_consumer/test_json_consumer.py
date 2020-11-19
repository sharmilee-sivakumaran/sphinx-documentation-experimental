'''JSON Consumer Tests'''

import os
import sys

folder = os.path.abspath(__file__)
while not folder.endswith("fn_scrapers"):
    folder = os.path.dirname(folder)
sys.path.append(folder)

from json_consumer import JSONObject, JSONConsumerException

def test_json_consumer():
    
    id_schema = JSONObject({
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {
                "path": "$.docket.id",
                "type": "integer"
            }
        }
    })

    id_document = {
        "docket": {
            "id": 1
        }
    }

    id_response = {
        "id": 1
    }

    assert id_response == id_schema.consume(id_document)

    id_document['ignore_me'] = 'this should be dropped'

    assert id_response == id_schema.consume(id_document)

def test_json_lists():
    schema = JSONObject({
        'type': 'object',
        'required': ['list'],
        'properties': {
            'list': {
                "path": "list[*]",
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["a", "b"]
                },
                "minItems": 1
            }
        }
    })

    document = {"list": ["a", "b"]}
    expected = {"list": ["a", "b"]}
    assert expected == schema.consume(document)

    document = {"list": ["a", "c"]}
    expected = {"list": ["a"]}
    assert expected == schema.consume(document)

    document = {"list": ["c"]}
    try:
        schema.consume(document)
    except JSONConsumerException:
        pass
    else:
        assert False


def test_nested():
    schema = JSONObject({
        'type': 'object',
        'properties': {
            'a': {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ['b'],
                    "properties": {
                        "b": {
                            "path": "b",
                            "type": "integer"
                        },
                        "d": {
                            "path": "c",
                            "type": "string",
                            "enum": ["foo", "baz"]
                        }
                    }
                },
                "minItems": 1
            }
        }
    })

    response = schema.consume({'a': [
        {'b': 1, 'c': 'foo'},   # valid
        {'b': 2, 'c': 1},       # partially valid - string
        {'_b': 3, 'c': 'bar'},  # invalid - missing req'd
        {'b': 4, 'c': 'baz'},   # valid
        {'b': 5, 'c': 'lorem'}, # partially valid - enum
    ]})


    assert len(response['a']) == 4
    assert response['a'][0] == {'b': 1, 'd': 'foo'}
    assert response['a'][1] == {'b': 2}
    assert response['a'][2] == {'b': 4, 'd': 'baz'}
    assert response['a'][3] == {'b': 5}

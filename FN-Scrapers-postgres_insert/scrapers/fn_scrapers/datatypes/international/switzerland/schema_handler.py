"""
Scrapy Item

See documentation in docs/topics/item.rst
"""

from pprint import pformat
from collections import MutableMapping
from jsonschema import Draft4Validator, RefResolver
from abc import ABCMeta
import six
import os
import json
import re

from ..common import make_json_serializable


def dict_from_class(cls):
    return dict(
        (key, value)
        for (key, value) in cls.iteritems()
    )


def to_unicode(text, encoding=None, errors='strict'):
    """Return the unicode representation of a bytes object `text`. If `text`
    is already an unicode object, return it as-is."""
    if isinstance(text, six.text_type):
        return text
    if not isinstance(text, (bytes, six.text_type)):
        raise TypeError('to_unicode must receive a bytes, str or unicode '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)


class BaseItem(object):
    """Base class for all scraped items."""
    pass


class Field(dict):
    """Container of field metadata"""


class ItemMeta(ABCMeta):
    def __new__(mcs, class_name, bases, attrs):
        classcell = attrs.pop('__classcell__', None)
        new_bases = tuple(base._class for base in bases if hasattr(base, '_class'))
        _class = super(ItemMeta, mcs).__new__(mcs, 'x_' + class_name, new_bases, attrs)

        fields = getattr(_class, 'fields', {})
        new_attrs = {}
        for n in dir(_class):
            v = getattr(_class, n)
            if isinstance(v, Field):
                fields[n] = v
            elif n in attrs:
                new_attrs[n] = attrs[n]

        new_attrs['fields'] = fields
        new_attrs['_class'] = _class
        if classcell is not None:
            new_attrs['__classcell__'] = classcell
        return super(ItemMeta, mcs).__new__(mcs, class_name, bases, new_attrs)


class DictItem(MutableMapping, BaseItem):
    fields = {}

    def __init__(self, *args, **kwargs):
        self._values = {}
        if args or kwargs:  # avoid creating dict for most common case
            for k, v in six.iteritems(dict(*args, **kwargs)):
                self[k] = v

    def __getitem__(self, key):
        return self._values[key]

    def __setitem__(self, key, value):
        if key in self.fields:
            self._values[key] = value
        else:
            raise KeyError("%s does not support field: %s" %
                           (self.__class__.__name__, key))

    def __delitem__(self, key):
        del self._values[key]

    def __getattr__(self, name):
        if name in self.fields:
            raise AttributeError("Use item[%r] to get field value" % name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name.startswith('_'):
            raise AttributeError("Use item[%r] = %r to set field value" %
                                 (name, value))
        super(DictItem, self).__setattr__(name, value)

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    __hash__ = BaseItem.__hash__

    def keys(self):
        return self._values.keys()

    def __repr__(self):
        return pformat(dict(self))

    def copy(self):
        return self.__class__(self)


@six.add_metaclass(ItemMeta)
class Item(DictItem):
    pass


class JsonSchemaMeta(ABCMeta):
    def __new__(mcs, class_name, bases, attrs):
        cls = super(JsonSchemaMeta, mcs).__new__(mcs, class_name, bases, attrs)
        fields = {}
        schema = attrs.get('jsonschema')
        schema_path = attrs.get('schema_path')

        if not schema:
            raise ValueError('{} must contain "jsonschema" attribute'
                             .format(cls.__name__))
        if schema_path:
            resolver = RefResolver('file://' + schema_path + '/', schema)
            cls.validator = Draft4Validator(schema, resolver=resolver)
        else:
            cls.validator = Draft4Validator(schema)
        cls.validator.check_schema(schema)
        for k in schema['properties']:
            fields[k] = Field()
        cls.fields = cls.fields.copy()
        cls.fields.update(fields)
        return cls


@six.add_metaclass(JsonSchemaMeta)
class JsonSchemaItem(DictItem):
    schema_path = os.getcwd()
    jsonschema = {
        "properties": {}
    }


class DocumentItem(JsonSchemaItem):
    schema_dir = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'schemas'
    schema_file_path = schema_dir + os.sep + 'doc_schema.json'
    with open(schema_file_path) as fp:
        json_schema_data = fp.read().decode("utf-8")
    schema_path = schema_dir
    jsonschema = json.loads(json_schema_data)

    def validate(self):
        required_re = re.compile("'(.+?)' is a required property")
        '''
        err_msg = best_match(self.validator.iter_errors(self._values))
        if err_msg:
            raise Exception(u'schema validation failed: \n {}'.format(err_msg.message))
        else:
            return
        '''
        errors = list(self.validator.iter_errors(dict(self)))
        paths_messages = []
        for error in errors:
            absolute_path = list(error.absolute_path)
            # error path is not available when required field is not filled
            # so we parse error message. Nasty.
            required_match = required_re.search(error.message)
            if required_match:
                absolute_path.append(required_match.group(1))

            path = '.'.join(map(str,absolute_path))
            paths_messages.append((path, error.message))
        if errors:
            error_msg = ''
            for path, message in paths_messages:
                error_msg += u'{}: {}\n'.format(path, message)
            raise Exception(u'schema validation failed: \n {}'.format(error_msg))


class DocumentAttachment(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    jsonschema_text = '''{
        "type": "object",
        "properties": {
          "document_type": {
            "type": "string"
          },
          "document_id": {
            "type": "integer",
            "description": "ID of the document, from the document service. May include HTML. may be long"
          },
          "download_id": {
            "type": "integer",
            "description": "download_id, from the document service"
          }
        }
      }'''
    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_document_type(self, document_type):
        self[u'document_type'] = document_type

    def add_document_id(self, document_id):
        self[u'document_id'] = document_id

    def add_download_id(self, download_id):
        self[u'download_id'] = download_id

    def to_json(self):
        return self._values


class DocumentAuthor(DocumentItem):

    jsonschema_text = '''{
      "type": "object",
      "description": "information about the primary author",
      "additionalProperties": false,
      "properties": {
        "author_type": {
          "type": "string"
        },
        "author_id": {
          "type": "integer"
        },
        "author_name": {
          "type": "string"
        }
      }
    }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_author_type(self, author_type):
        self[u'author_type'] = author_type

    def add_author_id(self, author_id):
        self[u'author_id'] = author_id

    def add_author_name(self, author_name):
        self[u'author_name'] = author_name

    def to_json(self):
        return self._values


class SWITZERLANDBill(DocumentItem):
    """
    Object representing an Switzerland Bill document scraped from Switzerland bills website.
    """
    schema_dir = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'schemas'
    schema_file_path = schema_dir + os.sep + 'doc_schema.json'
    with open(schema_file_path) as fp:
        json_schema_data = fp.read().decode("utf-8")
    schema_path = schema_dir
    jsonschema = json.loads(json_schema_data)

    def add_bill_id(self, bill_id):
        self[u'bill_id'] = bill_id

    def add_bill_short_id(self, bill_short_id):
        self[u'bill_short_id'] = bill_short_id

    def add_source_language(self, source_language):
        self[u'source_language'] = source_language

    def add_bill_type(self, bill_type):
        self[u'bill_type'] = bill_type

    def add_title(self, title):
        self[u'title'] = title

    def add_introduction_date(self, introduction_date):
        self[u'introduction_date'] = introduction_date

    def add_introduction_council(self, introduction_council):
        self[u'introduction_council'] = introduction_council

    def add_session(self, session):
        self[u'session'] = session

    def add_current_status(self, current_status):
        self[u'current_status'] = current_status

    def add_source_url(self, source_url):
        self[u'source_url'] = source_url

    def add_last_updated_date(self, last_updated_date):
        self[u'last_updated_date'] = last_updated_date

    def add_attachment_by_obj(self, attachment):
        if u'documents' not in self:
            self[u'documents'] = []
            if isinstance(attachment, DocumentAttachment):
                self[u'documents'].append(attachment.to_json())
            else:
                raise ValueError("DocumentAttachment class argument is required")
        else:
            if self[u'documents'] and isinstance(self[u'documents'], list):
                if isinstance(attachment, DocumentAttachment):
                    self[u'documents'].append(attachment.to_json())
                else:
                    raise ValueError("DocumentAttachment class argument is required")

    def add_author_by_obj(self, author):
        if u'author' not in self:
            if isinstance(author, DocumentAuthor):
                self[u'author'] = author.to_json()
            else:
                raise ValueError("DocumentAuthor class argument is required")
        else:
            if self[u'author'] and isinstance(author, DocumentAuthor):
                    self[u'author'] = author.to_json()
            else:
                raise ValueError("DocumentAuthor class argument is required")

    def to_json(self):
        return self._values

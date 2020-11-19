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
    schema_file_path = schema_dir + os.sep + 'eu_legislative_procedure.json'
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


class Documentkey_players(DocumentItem):
    """
    Object representing an stages of document scraped from Website.
    """
    jsonschema_text = '''{
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "committee_name": {
                "type": "string"
              },
              "committee_type": {
                "type": "string",
                "values": [
                  "Primary",
                  "Opinion"
                ]
              },
              "rapporteur": {
                "type": "string",
                "description": "re-order & fix casing from LASTNAME Firstname to Firstname Lastname. Skip shadow rapporteurs"
              }
            }
          }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_committee_name(self, committee_name):
        self[u'committee_name'] = committee_name

    def add_committee_type(self, committee_type):
        self[u'committee_type'] = committee_type

    def add_rapporteur(self, rapporteur):
        self[u'rapporteur'] = rapporteur

    def to_json(self):
        return self._values


class Documentlegislative_observatory(DocumentItem):
    """
    Object representing an stages of document scraped from Website.
    """
    jsonschema_text = '''{
      "type": "object",
      "properties": {
        "key_players": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "committee_name": {
                "type": "string"
              },
              "committee_type": {
                "type": "string",
                "values": [
                  "Primary",
                  "Opinion"
                ]
              },
              "rapporteur": {
                "type": "string",
                "description": "re-order & fix casing from LASTNAME Firstname to Firstname Lastname. Skip shadow rapporteurs"
              }
            }
          }
        },
        "council_configuration": {
          "type": "string"
        },
        "meeting_date": {
          "type": "string",
          "format": "date",
          "description": "use standard ISO (YYYY-MM-DD) format"
        },
        "source_url": {
          "type": "string",
          "format": "uri",
          "description": "for the legislative observatory, specifically"
        }
      }
    }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_council_configuration(self, council_configuration):
        self[u'council_configuration'] = council_configuration

    def add_meeting_date(self, meeting_date):
        self[u'meeting_date'] = meeting_date

    def add_source_url(self, source_url):
        self[u'source_url'] = source_url

    def add_key_players_by_obj(self, key_players):
        if u'key_players' not in self:
            self[u'key_players'] = []
            if isinstance(key_players, Documentkey_players):
                self[u'key_players'].append(key_players.to_json())
            else:
                raise ValueError("Documentkey_players class argument is required")
        else:
            if self[u'key_players']:
                if isinstance(key_players, Documentkey_players):
                    self[u'key_players'].append(key_players.to_json())
                else:
                    raise ValueError("Documentkey_players class argument is required")

    def to_json(self):
        return self._values


class Documentadoption_by_commission(DocumentItem):
    """
    Object representing an stages of document scraped from Website.
    """
    jsonschema_text = '''{
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "adoption_date": {
          "type": "string",
          "format": "date"
        },
        "leading_person": {
          "type": "string",
          "description": "capital-case last name"
        },
        "leading_service": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        },
        "addressee_for_formal_act": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        },
        "addressee_for_information": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        },
        "addressee_for_mandatory_consultation": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        },
        "addressee_for_optional_consultation": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        },
        "celexes": {
          "type": "array",
          "uniqueItems": true,
          "items": {
            "type": "string"
          }
        }
      }
    }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_adoption_date(self, adoption_date):
        self[u'adoption_date'] = adoption_date

    def add_leading_person(self, leading_person):
        self[u'leading_person'] = leading_person

    def add_leading_service(self, leading_service):
        self[u'leading_service'] = leading_service

    def add_addressee_for_formal_act(self, addressee_for_formal_act):
        self[u'addressee_for_formal_act'] = addressee_for_formal_act

    def add_addressee_for_information(self, addressee_for_information):
        self[u'addressee_for_information'] = addressee_for_information

    def add_addressee_for_mandatory_consultation(self, addressee_for_mandatory_consultation):
        self[u'addressee_for_mandatory_consultation'] = addressee_for_mandatory_consultation

    def add_addressee_for_optional_consultation(self, addressee_for_optional_consultation):
        self[u'addressee_for_optional_consultation'] = addressee_for_optional_consultation

    def add_celexes(self, celexes):
        self[u'celexes'] = celexes

    def to_json(self):
        return self._values


class DocumentStages(DocumentItem):
    """
    Object representing an stages of document scraped from Website.
    """
    jsonschema_text = '''{
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "date": {
            "type": "string",
            "format": "date"
          },
          "title": {
            "type": "string"
          },
          "chamber": {
            "type": "string",
            "values": [
              "European Parliament",
              "European Council",
              "European Commission",
              "Economic and Social Committee",
              "European Committee of the Regions"
            ]
          }
        }
      }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_date(self, date):
        self[u'date'] = date

    def add_chamber(self, chamber):
        self[u'chamber'] = chamber

    def add_title(self, title):
        self[u'title'] = title

    def to_json(self):
        return self._values


class Documentprocedure(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    schema_dir = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'schemas'
    schema_file_path = schema_dir + os.sep + 'eu_legislative_procedure.json'
    with open(schema_file_path) as fp:
        json_schema_data = fp.read().decode("utf-8")
    schema_path = schema_dir
    jsonschema = json.loads(json_schema_data)

    def add_procedure_code(self, procedure_code):
        self[u'procedure_code'] = procedure_code

    def add_title(self, title):
        self[u'title'] = title

    def add_procedure_type(self, procedure_type):
        self[u'procedure_type'] = procedure_type

    def add_lp_file_type(self, lp_file_type):
        self[u'lp_file_type'] = lp_file_type

    def add_adoption_by_commission_by_obj(self, adoption_by_commission):
        if u'adoption_by_commission' not in self:
            if isinstance(adoption_by_commission, Documentadoption_by_commission):
                self[u'adoption_by_commission'] = adoption_by_commission.to_json()
            else:
                raise ValueError("Documentadoption_by_commission class argument is required")
        else:
            if self[u'adoption_by_commission'] and isinstance(adoption_by_commission, Documentadoption_by_commission):
                    self[u'adoption_by_commission'] = adoption_by_commission.to_json()
            else:
                raise ValueError("Documentadoption_by_commission class argument is required")

    def add_stages_by_obj(self, stages):
        if u'stages' not in self:
            self[u'stages'] = []
            if isinstance(stages, DocumentStages):
                self[u'stages'].append(stages.to_json())
            else:
                raise ValueError("DocumentStages class argument is required")
        else:
            if self[u'stages']:
                if isinstance(stages, DocumentStages):
                    self[u'stages'].append(stages.to_json())
                    self[u'stages'] = map(dict, set(tuple(x.items()) for x in self[u'stages']))
                else:
                    raise ValueError("DocumentStages class argument is required")

    def add_source_url(self, source_url):
        self[u'source_url'] = source_url

    def add_adopted_act_celex(self, adopted_act_celex):
        self[u'adopted_act_celex'] = adopted_act_celex

    def add_legislative_observatory_by_obj(self, legislative_observatory):
        if u'legislative_observatory' not in self:
            if isinstance(legislative_observatory, Documentlegislative_observatory):
                self[u'legislative_observatory'] = legislative_observatory.to_json()
            else:
                raise ValueError("Documentlegislative_observatory class argument is required")
        else:
            if self[u'legislative_observatory'] and isinstance(legislative_observatory, Documentlegislative_observatory):
                    self[u'legislative_observatory'] = legislative_observatory.to_json()
            else:
                raise ValueError("Documentlegislative_observatory class argument is required")

    def to_json(self):
        return self._values

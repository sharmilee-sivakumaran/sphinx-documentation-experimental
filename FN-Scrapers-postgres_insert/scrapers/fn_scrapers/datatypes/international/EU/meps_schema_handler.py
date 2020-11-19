"""
Scrapy Item

See documentation in docs/topics/item.rst
"""

from pprint import pformat
from collections import MutableMapping, MutableSequence
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


class ListItem(MutableSequence, BaseItem):
    def __init__(self, *args, **kwargs):
        """Initialize the class"""
        self._values = list()
        super(ListItem, self).__init__()
        if args or kwargs:  # avoid creating dict for most common case
            self._values = list(*args, **kwargs)
        else:
            self._values = list()

    def __repr__(self):
        return "<{0} {1}>".format(self.__class__.__name__, self._values)

    def __len__(self):
        """List length"""
        return len(self._values)

    def __getitem__(self, ii):
        """Get a list item"""
        return self._values[ii]

    def __delitem__(self, ii):
        """Delete an item"""
        del self._values[ii]

    def __setitem__(self, ii, val):
        # optional: self._acl_check(val)
        self._values[ii] = val

    def __str__(self):
        return str(self._values)

    def insert(self, ii, val):
        # optional: self._acl_check(val)
        self._values.insert(ii, val)

    def append(self, val):
        self.insert(len(self._values), val)


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
    schema_file_path = schema_dir + os.sep + 'eu_meps.json'
    with open(schema_file_path) as fp:
        json_schema_data = fp.read().decode("utf-8")
    schema_path = schema_dir
    jsonschema = json.loads(json_schema_data)
    jsonschema = {
        "properties": {}
    }

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

            path = '.'.join(map(str, absolute_path))
            paths_messages.append((path, error.message))
        if errors:
            error_msg = ''
            for path, message in paths_messages:
                error_msg += u'{}: {}\n'.format(path, message)
            raise Exception(u'schema validation failed: \n {}'.format(error_msg))


class Documentaddress(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    jsonschema_text = '''{
            "type": "object",
            "required": [
              "street",
              "postal_code",
              "city"
            ],
            "properties": {
              "building": {
                "type": "string"
              },
              "office": {
                "type": "string"
              },
              "street":{
                "type": "string"
              },
              "postal_code": {
                "type": "string"
              },
              "city": {
                "type": "string"
              }
            }
          }'''
    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_building(self, building):
        self[u'building'] = building

    def add_office(self, office):
        self[u'office'] = office

    def add_street(self, street):
        self[u'street'] = street

    def add_postal_code(self, postal_code):
        self[u'postal_code'] = postal_code

    def add_city(self, city):
        self[u'city'] = city

    def to_json(self):
        return self._values


class Documentphone_information(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    jsonschema_text = '''{
              "type": "object",
              "additionalProperties": false,
              "required": [
                "phone_type",
                "phone_number"
              ],
              "properties": {
                "phone_type": {
                  "type": "string",
                  "values": [
                    "phone",
                    "fax"
                  ]
                },
                "phone_number": {
                  "type": "string"
                }
              }
            }'''
    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_phone_type(self, phone_type):
        self[u'phone_type'] = phone_type

    def add_phone_number(self, phone_number):
        self[u'phone_number'] = phone_number

    def to_json(self):
        return self._values


class DocumentCDM(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    jsonschema_text = '''{
        "type": "object",
        "additionalProperties": false,
        "required": [
          "comm_del_name",
          "comm_del_role"
        ],
        "properties": {
          "comm_del_name": {
            "type": "string",
            "description": "name of the committee/delegation"
          },
          "comm_del_role": {
            "type": "string",
            "values": [
              "Member",
              "Substitute",
              "Vice-Chair",
              "Chair"
            ]
          }
        }
      }'''
    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_comm_del_name(self, comm_del_name):
        self[u'comm_del_name'] = comm_del_name

    def add_comm_del_role(self, comm_del_role):
        self[u'comm_del_role'] = comm_del_role

    def to_json(self):
        return self._values


class Documentaddresses(DocumentItem):
    """
    Object representing an attachment of document scraped from Website.
    """
    jsonschema_text = '''{
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "address_type": {
            "type": "string"
          },
          "address": {
            "type": "object",
            "required": [
              "street",
              "postal_code",
              "city"
            ],
            "properties": {
              "building": {
                "type": "string"
              },
              "office": {
                "type": "string"
              },
              "street":{
                "type": "string"
              },
              "postal_code": {
                "type": "string"
              },
              "city": {
                "type": "string"
              }
            }
          },
          "phone_information": {
            "type": "array",
            "minItems": 1,
            "items": {
              "type": "object",
              "additionalProperties": false,
              "required": [
                "phone_type",
                "phone_number"
              ],
              "properties": {
                "phone_type": {
                  "type": "string",
                  "values": [
                    "phone",
                    "fax"
                  ]
                },
                "phone_number": {
                  "type": "string"
                }
              }
            }
          }
        }
      }'''
    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_address_type(self, address_type):
        self[u'address_type'] = address_type

    def add_phone_information_by_obj(self, phone_information):
        if u'phone_information' not in self:
            self[u'phone_information'] = []
            if isinstance(phone_information, Documentphone_information):
                self[u'phone_information'].append(phone_information.to_json())
            else:
                raise ValueError("Documentphone_information class argument is required")
        else:
            if self[u'phone_information'] and isinstance(self[u'phone_information'], list):
                if isinstance(phone_information, Documentphone_information):
                    self[u'phone_information'].append(phone_information.to_json())
                else:
                    raise ValueError("Documentphone_information class argument is required")

    def add_address_by_obj(self, address):
        if u'address' not in self:
            if isinstance(address, Documentaddress):
                self[u'address'] = address.to_json()
            else:
                raise ValueError("Documentaddress class argument is required")
        else:
            if self[u'address'] and isinstance(address, Documentaddress):
                self[u'address'] = address.to_json()
            else:
                raise ValueError("Documentaddress class argument is required")

    def to_json(self):
        return self._values


class EU_MEPs(DocumentItem):
    jsonschema_text = '''{
    "type": "object",
    "additionalProperties": false,
    "required": [
      "last_name",
      "country"
    ],
    "properties": {
      "first_name": {
        "type": "string"
      },
      "last_name": {
        "type": "string",
        "description": "Convert from ALL CAPS to Standard Casing"
      },
      "country": {
        "type": "string"
      },
      "date_of_birth": {
        "type": "string",
        "format": "date"
      },
      "place_of_birth": {
        "type": "string"
      },
      "photograph_download_id": {
        "type": "integer",
        "description": "download_id for the photograph"
      },
      "eu_party": {
        "type": "string"
      },
      "eu_party_role": {
        "type": "string"
      },
      "national_party": {
        "type": "string"
      },
      "committee_delegation_memberships": {
        "type": "array",
        "minItems": 1,
        "items": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "comm_del_name",
            "comm_del_role"
          ],
          "properties": {
            "comm_del_name": {
              "type": "string",
              "description": "name of the committee/delegation"
            },
            "comm_del_role": {
              "type": "string",
              "values": [
                "Member",
                "Substitute",
                "Vice-Chair",
                "Chair"
              ]
            }
          }
        }
      },
      "contact_email": {
        "type": "string"
      },
      "contact_website": {
        "type": "string"
      },
      "contact_facebook": {
        "type": "string"
      },
      "contact_twitter": {
        "type": "string"
      },
      "addresses": {
        "type": "array",
        "minItems": 1,
        "items": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "address_type": {
              "type": "string"
            },
            "address": {
              "type": "object",
              "required": [
                "street",
                "city"
              ],
              "properties": {
                "building": {
                  "type": "string"
                },
                "office": {
                  "type": "string"
                },
                "street": {
                  "type": "string"
                },
                "postal_code": {
                  "type": "string"
                },
                "city": {
                  "type": "string"
                }
              }
            },
            "phone_information": {
              "type": "array",
              "minItems": 1,
              "items": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                  "phone_type",
                  "phone_number"
                ],
                "properties": {
                  "phone_type": {
                    "type": "string",
                    "values": [
                      "phone",
                      "fax"
                    ]
                  },
                  "phone_number": {
                    "type": "string"
                  }
                }
              }
            }
          }
        }
      },
      "source_url": {
        "type": "string"
      }
    }
  }'''

    jsonschema = json.loads(jsonschema_text.decode('utf-8'))

    def add_first_name(self, first_name):
        self[u'first_name'] = first_name

    def add_last_name(self, last_name):
        self[u'last_name'] = last_name

    def add_country(self, country):
        self[u'country'] = country

    def add_date_of_birth(self, date_of_birth):
        self[u'date_of_birth'] = date_of_birth

    def add_place_of_birth(self, place_of_birth):
        self[u'place_of_birth'] = place_of_birth

    def add_photograph_download_id(self, photograph_download_id):
        self[u'photograph_download_id'] = photograph_download_id

    def add_eu_party(self, eu_party):
        self[u'eu_party'] = eu_party

    def add_eu_party_role(self, eu_party_role):
        self[u'eu_party_role'] = eu_party_role

    def add_national_party(self, national_party):
        self[u'national_party'] = national_party

    def add_contact_email(self, contact_email):
        self[u'contact_email'] = contact_email

    def add_contact_website(self, contact_website):
        self[u'contact_website'] = contact_website

    def add_contact_facebook(self, contact_facebook):
        self[u'contact_facebook'] = contact_facebook

    def add_contact_twitter(self, contact_twitter):
        self[u'contact_twitter'] = contact_twitter

    def add_source_url(self, source_url):
        self[u'source_url'] = source_url

    def add_cdm_by_obj(self, cdm):
        if u'committee_delegation_memberships' not in self:
            self[u'committee_delegation_memberships'] = []
            if isinstance(cdm, DocumentCDM):
                self[u'committee_delegation_memberships'].append(cdm.to_json())
            else:
                raise ValueError("DocumentCDM class argument is required")
        else:
            if self[u'committee_delegation_memberships'] and isinstance(self[u'committee_delegation_memberships'],
                                                                        list):
                if isinstance(cdm, DocumentCDM):
                    self[u'committee_delegation_memberships'].append(cdm.to_json())
                else:
                    raise ValueError("DocumentCDM class argument is required")

    def add_addresses_by_obj(self, addresses):
        if u'addresses' not in self:
            self[u'addresses'] = []
            if isinstance(addresses, Documentaddresses):
                self[u'addresses'].append(addresses.to_json())
            else:
                raise ValueError("Documentaddresses class argument is required")
        else:
            if self[u'addresses'] and isinstance(self[u'addresses'], list):
                if isinstance(addresses, Documentaddresses):
                    self[u'addresses'].append(addresses.to_json())
                else:
                    raise ValueError("Documentaddresses class argument is required")

    def to_json(self):
        return self._values


class EU_MEPs_main(ListItem):
    def __init__(self):
        self._values = list()

    def add_mep_by_obj(self, mep):
        if isinstance(mep, EU_MEPs):
            if isinstance(self._values, dict) and len(self._values) == 0:
                self._values.append(mep.to_json())
            else:
                index = len(self._values)
                if index > 0:
                    index = index - 1
                print index
                self._values.append(mep.to_json())
        else:
            raise ValueError("EU_MEPs class argument is required")

    def to_json(self):
        return self._values

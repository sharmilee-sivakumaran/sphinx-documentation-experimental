'''JSONConsumer class.'''

from copy import deepcopy
import re
import logging
import jsonpath_ng

class JSONBase(object):
    '''Base path that object, list, and value inherit from.'''
    ignored_properties = ['$schema', 'title', 'description']

    def __init__(self, schema, path=None, refs=None, root=None):
        self._logger = logging.getLogger(__name__)
        self.root = root or self
        for key in self.ignored_properties:
            if key in schema:
                del schema[key]
        if 'type' not in schema:
            raise SchemaError("Missing required 'type' field.")
        self._datatype = schema.pop('type')

        self.path = path
        if path is None:
            self.path = "$"

        self.refs = refs or {}

        self._validators = {}

    def consume(self, content, path=None):
        '''Validate a JSON Object and returns the resulting object.'''
        if path is None:
            path = []
        value = self._consume_value(content, path)
        self._validate(value, path)
        return value

    def _make_schema(self, schema, path):
        '''Returns an object of type JSONValue, JSONList, or JSONObject'''
        if 'type' not in schema:
            if "$ref" in schema:
                if schema['$ref'] not in self.refs:
                    print self.refs
                    raise ValueError("Unrecognized reference: " + schema['$ref'])
                schema = self.refs[schema['$ref']]
            else:
                raise SchemaError("Missing type in '{}'".format(path))
        datatype = schema['type']
        if datatype in ['array', 'list']:
            return JSONList(schema, path, self.refs, self.root)
        elif datatype == 'object':
            return JSONObject(schema, path, self.refs, self.root)
        elif datatype in ['string', 'int', 'integer', 'number', 'boolean']:
            return JSONValue(schema, path, self.refs, self.root)
        else:
            raise SchemaError("Unrecognized datatype '{}'".format(datatype))

    def _consume_value(self, value, path):
        '''Override in object/list/value classes'''
        raise NotImplementedError

    def _validate(self, value, path):
        """Inspect schema for validators, and if set, try to locate and run
        them."""
        for key in self._validators:
            validator = '_validate__'+key.lower()
            if hasattr(self, validator):
                validator = getattr(self, validator)
                validator(path, self._validators[key], value)
            else:
                raise SchemaError(
                    "{}: Unrecognized validator '{}'".format(
                        self._path(path), key))

    def _path(self, path):
        '''Converts a path array into a string.'''
        return "[{}]".format("][".join(map(str, path)))

class JSONObject(JSONBase):
    """Implements a JSON Object definition."""
    def __init__(self, schema=None, path=None, refs=None, root=None):
        schema = deepcopy(schema)
        super(JSONObject, self).__init__(schema, path, refs, root)
        self._fields = {}
        self._add_fields(schema.pop('properties'))
        self._validators = schema

    def _add_fields(self, fields):
        '''Iterates across a dictionary schema, adding fields.'''
        for key in fields:
            schema = fields[key]
            if 'path' not in schema:
                schema['path'] = "$."+key
                if schema.get('type') in ('array', 'list'):
                    schema['path'] += '[*]'
            path = schema.pop('path')
            schema = self._make_schema(schema, self.path+"."+key)
            self._fields[key] = JSONField(path, schema)

    def _consume_value(self, value, path):
        '''Validate a JSON Object and returns the resulting object.'''
        final_content = {}
        for name in self._fields:
            field = self._fields[name]
            jsonvalue = field.jsonpath.find(value)
            if jsonvalue:
                try:
                    if isinstance(field.schema, JSONList):
                        final_content[name] = field.consume(
                            [j.value for j in jsonvalue], path+[name]
                        )
                    else:
                        final_content[name] = field.consume(
                            jsonvalue[0].value, path+[name]
                        )
                except ValidationError as exception:
                    self._logger.debug(exception)
        return final_content

    def _validate__required(self, path, names, value):
        """Checks for required fields in object."""
        missing = list([name for name in names if name not in value])
        if missing:
            raise ValidationError(
                "{}: Object missing required properties: '{}'.".format(
                    self._path(path), "', '".join(missing)))

    def _validate__additionalproperties(self, path, names, value):
        '''Not implemented.'''
        pass

class JSONField(object):
    """Name/value pair for use in objects."""
    def __init__(self, path, schema):
        self.path = path
        self.schema = schema
        self.jsonpath = jsonpath_ng.parse(path)

    def consume(self, value, path):
        """Passses a value to a property and returns the result"""
        return self.schema.consume(value, path)

class JSONValue(JSONBase):
    """Represents a json value (number, string, etc)."""
    types = {
        'string': basestring,
        'int': int,
        'integer': (int, long),
        'number': (float, int),
        'boolean': bool,
        'array': list,
        'object': dict
    }

    str_formats = {
        'date': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        'date-time': re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+|)(?:\+00:00)?Z$')
    }

    def __init__(self, schema, path=None, refs=None, root=None):
        schema = deepcopy(schema)
        super(JSONValue, self).__init__(schema, path, refs, root)
        self._validators = schema
        if self._datatype not in self.types:
            raise SchemaError("Unrecognized type '{}'".format(
                self._datatype))

    def _consume_value(self, value, path):
        """Verify that the value is indeed the correct type."""
        if not isinstance(value, self.types[self._datatype]):
            raise ValidationError("{}: Received type {}, expected type {}".format(
                self._path(path), value.__class__.__name__,
                self._datatype
            ))
        return value

    def _validate__minlength(self, path, length, value):
        '''Ensures the value is a string and has the minimum length.'''
        if not isinstance(value, basestring):
            raise ValidationError("{}: Expected string instance.".format(
                self._path(path)
            ))
        if len(value) >= length:
            return
        raise ValidationError(
            "{}: List does not meet minimum length of '{}'.".format(
                self._path(path), length))

    def _validate__format(self, path, str_format, value):
        '''Look up a format and match it accordingly.'''
        if not isinstance(value, basestring):
            raise ValidationError(
                "[{}]: Expected string instance.".format(']['.join(path)))
        if str_format not in self.str_formats:
            raise ValidationError("[{}]: Unrecognized string format '{}'".format(
                ']['.join(path), str_format))
        if self.str_formats[str_format].match(value) is None:
            raise ValidationError("{}: String did not match format '{}'".format(
                self._path(path), str_format))

    def _validate__values(self, path, values, value):
        self._validate__enum(path, values, value)

    def _validate__enum(self, path, enum, value):
        '''Ensures a value exists in an enumeration list.'''
        if value in enum:
            return
        raise ValidationError("{}: Value not found in enum: '{}'.".format(
            self._path(path), value))


class JSONList(JSONBase):
    """Implements a json list container"""
    def __init__(self, schema, path, refs, root=None):
        schema = deepcopy(schema)
        super(JSONList, self).__init__(schema, path, refs, root)
        self.schema = self._make_schema(schema.pop('items'), self.path + '[]')
        self._validators = schema

    def _consume_value(self, value, path):
        """Iterates over a given list, and applies a schema to each item."""
        temp_list = []
        if not isinstance(value, list):
            import pprint
            print pprint.pprint(value)
            raise ValidationError(
                "{}: Expected list instance.".format(self._path(path)))
        for i, item in enumerate(value):
            try:
                temp_list.append(self.schema.consume(item, path+[i]))
            except ValidationError as exception:
                self._logger.debug(exception)
        return temp_list

    def _validate__minitems(self, path, length, value):
        '''Min length for lists/arrays.'''
        if not isinstance(value, list):
            raise ValidationError(
                "{}: Expected list instance.".format(self._path(path)))
        if len(value) >= length:
            return
        raise ValidationError(
            "{}: List does not meet minimum length of '{}'.".format(
                self._path(path), length))

class JSONConsumerException(Exception):
    '''Base JSONObject Exception.'''
    pass

class SchemaError(JSONConsumerException):
    '''Exception parsing the schema.'''
    pass

class ValidationError(JSONConsumerException):
    '''Exception validating the document.'''
    pass

from __future__ import absolute_import, print_function

import imp
from os import listdir, path
from sql_comp.formatters.base import Formatter 

_FORMATTERS = {}

def get_formatters():
    '''Returns a dictionary of name: formatter-class. '''
    global _FORMATTERS
    cwd = path.dirname(path.abspath(__file__))

    if _FORMATTERS:
        return _FORMATTERS
    for filename in listdir(cwd):
        fullpath = path.join(cwd, filename)
        module = None
        if not path.isfile(fullpath) or not filename.endswith('_formatter.py'):
            continue
        name = filename.replace('_formatter.py', '')
        module = imp.load_source(filename[:-3], fullpath)

        for val in module.__dict__.values():
            if not hasattr(val, '__bases__'):
                continue
            if Formatter not in val.__bases__:
                continue
            _FORMATTERS[name] = val
            break
        else:
            raise ValueError("Invalid Formatter: " + fullpath)

    return _FORMATTERS

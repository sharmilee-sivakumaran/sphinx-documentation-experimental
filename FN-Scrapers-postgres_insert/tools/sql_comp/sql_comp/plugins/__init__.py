from __future__ import absolute_import, print_function

import imp
from os import listdir, path

def get_plugins():
    cwd = path.dirname(path.abspath(__file__))

    plugins = []
    for filename in listdir(cwd):
        fullpath = path.join(cwd, filename)
        module = None
        if path.isdir(fullpath):
            fullpath = path.join(fullpath, '__init__.py')
            if not path.isfile(fullpath):
                continue
            module = filename
        elif filename.endswith('.py') and not filename.startswith('__'):
            module = filename[:-3]
        else:
            continue
        plugins.append(imp.load_source(module, fullpath))
    return plugins

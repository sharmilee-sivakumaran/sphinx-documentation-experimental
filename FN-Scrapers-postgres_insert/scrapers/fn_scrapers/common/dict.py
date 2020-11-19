from collections import Mapping
from copy import deepcopy

def dict_deep_merge(dic, other):
    '''
    Recursively iterates across two dictinoaries, updating the first with
    values from the second.
    '''
    for key, val in other.iteritems():
        if key in dic and isinstance(dic[key], dict) and isinstance(val, Mapping):
            dict_deep_merge(dic[key], other[key])
        else:
            dic[key] = deepcopy(other[key])

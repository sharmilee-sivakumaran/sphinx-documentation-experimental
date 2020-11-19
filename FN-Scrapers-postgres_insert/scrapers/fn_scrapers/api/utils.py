from __future__ import absolute_import

import arrow
import json
import datetime

import inspect


class JSONEncoderPlus(json.JSONEncoder):
    """
    JSONEncoder that encodes datetime objects as Unix timestamps
    """
    def default(self, obj, **kwargs):
        if isinstance(obj, datetime.datetime):
            return str(arrow.get(obj))
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        return super(JSONEncoderPlus, self).default(obj, **kwargs)


class Bunch(object):
  def __init__(self, adict):
    self.__dict__.update(adict)


def map_kwargs(func, scraper_args, **kwargs):
    '''
    Maps injected keyword arguments to accepted arguments, supporting the
    double splat operator (**kwargs).

    Arguments:
        func: Function to call.
        scraper_args: Argument collection to be passed. Can be either a
            Namespace object or a dictionary.
        **kwargs: Additional arguments that will be merged with scraper_args.
    Returns:
        Value from calling `func`.
    '''
    if isinstance(scraper_args, dict):
        scraper_args = Bunch(scraper_args)

    scraper_args.__dict__.update(**kwargs)

    args, varargs, keywords, _ = inspect.getargspec(func)
    if varargs:
        raise ValueError(
            "Posional-expansion argument ({}) is not accepted ({}.{})".format(
                varargs, inspect.getmodule(func).__name__, func.__name__
            ))
    if keywords:
        func_args = vars(scraper_args)
    else:
        func_args = {}
        for name in args:
            if name in ['self', 'cls'] or not hasattr(scraper_args, name):
                continue
            func_args[name] = getattr(scraper_args, name)
    return func(**func_args)


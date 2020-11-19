from __future__ import absolute_import

from collections import defaultdict
from future.utils import PY2, text_type


IS_SCRAPER_PROPERTY = "__fn_scrapers_is_scraper"
SCRAPER_NAME_PROPERTY = "__fn_scrapers_scraper_name"
ARGUMENT_FUNCS_PROPERTY = "__fn_scrapers_argument_groups"
HANDLER_MODULES_PROPERTY = "__fn_scrapers_handler_modules"
TAGS_PROPERTY = "__fn_scrapers_tags"

CLASS_ARGUMENTS = defaultdict(list)

def configure_class_as_scraper(klass, name=None, handler_modules=None):
    if isinstance(name, text_type):
        raise Exception("Scraper names must be unicode objects")
    if name is None:
        name = getattr(klass, SCRAPER_NAME_PROPERTY, text_type(klass.__name__))

    setattr(klass, IS_SCRAPER_PROPERTY, True)
    setattr(klass, SCRAPER_NAME_PROPERTY, name)

    if not hasattr(klass, HANDLER_MODULES_PROPERTY):
        setattr(klass, HANDLER_MODULES_PROPERTY, [])
    if handler_modules:
        getattr(klass, HANDLER_MODULES_PROPERTY).extend(handler_modules)

    if not hasattr(klass, TAGS_PROPERTY):
        setattr(klass, TAGS_PROPERTY, {})

    return klass


def add_argument(klass, args, kwargs):
    return add_argument_function(klass, lambda p: p.add_argument(*args, **kwargs))


def add_argument_function(klass, func):
    CLASS_ARGUMENTS[klass].append(func)
    return klass

def is_scraper(klass):
    return getattr(klass, IS_SCRAPER_PROPERTY, False)


def get_scraper_name(klass):
    return getattr(klass, SCRAPER_NAME_PROPERTY)


def get_scraper_handler_modules(klass):
    return getattr(klass, HANDLER_MODULES_PROPERTY)


def get_argument_funcs(klass):
    argument_funcs = []
    klasses = [klass]
    for klass in klasses:
        klasses.extend(klass.__bases__)
        argument_funcs.extend(CLASS_ARGUMENTS[klass])
    return argument_funcs


def add_tags(klass, tags):
    if not hasattr(klass, TAGS_PROPERTY):
        setattr(klass, TAGS_PROPERTY, {})
    for tag_name in tags:
        if PY2:
            simple_tag = isinstance(tags[tag_name], (str, unicode))
        else:
            simple_tag = isinstance(tags[tag_name], str)

        if text_type(tag_name) not in getattr(klass, TAGS_PROPERTY):
            getattr(klass, TAGS_PROPERTY)[tag_name] = set()

        if simple_tag:
            getattr(klass, TAGS_PROPERTY)[text_type(tag_name)].add(text_type(tags[tag_name]))
        else:
            getattr(klass, TAGS_PROPERTY)[text_type(tag_name)].update(text_type(tv) for tv in tags[tag_name])

    return klass


def get_tags(klass):
    return getattr(klass, TAGS_PROPERTY)


def mark_scraper_as_broken(klass, reason):
    def _broken():
        raise Exception(u"{} is broken and cannot be used: {}".format(get_scraper_name(BrokenScraper), reason))

    class BrokenScraper(object):
        def __init__(self):
            _broken()

        def __getattr__(self, item):
            _broken()

    # Copy over the properties - the name needs to be handled a little specially to
    # make sure that our BrokenScraper class will keep the same scraper name as
    # klass.
    if hasattr(klass, IS_SCRAPER_PROPERTY):
        setattr(BrokenScraper, IS_SCRAPER_PROPERTY, getattr(klass, IS_SCRAPER_PROPERTY))
    if hasattr(klass, SCRAPER_NAME_PROPERTY):
        setattr(BrokenScraper, SCRAPER_NAME_PROPERTY, getattr(klass, SCRAPER_NAME_PROPERTY))
    else:
        setattr(BrokenScraper, SCRAPER_NAME_PROPERTY, klass.__name__)
    if hasattr(klass, HANDLER_MODULES_PROPERTY):
        setattr(BrokenScraper, HANDLER_MODULES_PROPERTY, getattr(klass, HANDLER_MODULES_PROPERTY))
    if hasattr(klass, TAGS_PROPERTY):
        setattr(BrokenScraper, TAGS_PROPERTY, getattr(klass, TAGS_PROPERTY))

    return BrokenScraper

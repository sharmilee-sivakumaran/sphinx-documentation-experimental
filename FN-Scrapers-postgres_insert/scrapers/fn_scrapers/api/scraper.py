from __future__ import absolute_import


def argument(*args, **kwargs):
    from fn_scrapers.internal.scraper_internal import add_argument
    return lambda klass: add_argument(klass, args, kwargs)


def argument_function(argument_function):
    from fn_scrapers.internal.scraper_internal import add_argument_function
    return lambda klass: add_argument_function(klass, argument_function)


def scraper(name=None, handler_modules=None):
    from fn_scrapers.internal.scraper_internal import configure_class_as_scraper
    return lambda klass: configure_class_as_scraper(klass, name, handler_modules)


def tags(**tags):
    from fn_scrapers.internal.scraper_internal import add_tags
    return lambda klass: add_tags(klass, tags)


def broken(reason):
    from fn_scrapers.internal.scraper_internal import mark_scraper_as_broken
    return lambda klass: mark_scraper_as_broken(klass, reason)

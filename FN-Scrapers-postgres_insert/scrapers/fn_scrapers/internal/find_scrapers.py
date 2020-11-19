from __future__ import absolute_import

import threading
from .scraper_internal import get_scraper_name


class ScraperNotFound(Exception):
    pass


def _find_scrapers_in_mod(mod):
    from .scraper_internal import is_scraper
    import pkgutil
    import importlib
    import inspect

    scrapers = {
        k[1]: (mod.__name__, k[0])
        for k in inspect.getmembers(mod, inspect.isclass)
        if is_scraper(k[1])}

    if hasattr(mod, "__path__"):
        for _, module_name, _ in pkgutil.iter_modules(mod.__path__):
            if module_name == "tests":
                continue
            child_mod = importlib.import_module(mod.__name__ + "." + module_name)
            scrapers.update(_find_scrapers_in_mod(child_mod))

    return scrapers


def _find_scrapers():
    from .scraper_internal import get_scraper_name
    import fn_scrapers.datatypes

    scrapers = _find_scrapers_in_mod(fn_scrapers.datatypes)

    scraper_names = set()
    for scraper_class in scrapers:
        name = get_scraper_name(scraper_class)
        if name.lower() in scraper_names:
            raise Exception(u"Duplicate scrapers with name: {}".format(name.lower()))
        scraper_names.add(name.lower())

    return scrapers


_SCRAPER_IMPORT_NAME_CACHE = None  # dict mapping scraper classes to their import names
_SCRAPER_CLASS_CACHE = None  # List of all scraper classes, in sorted order
_SCRAPER_CLASS_BY_NAME_CACHE = None  # dict mapping scraper names to scraper classes
_IS_SETUP = False
_SCRAPER_CACHE_LOCK = threading.Lock()


def _init_caches():
    global _SCRAPER_CLASS_CACHE
    global _SCRAPER_CLASS_BY_NAME_CACHE
    global _SCRAPER_IMPORT_NAME_CACHE
    global _IS_SETUP
    with _SCRAPER_CACHE_LOCK:
        if not _IS_SETUP:
            _SCRAPER_IMPORT_NAME_CACHE = _find_scrapers()

            _SCRAPER_CLASS_CACHE = sorted(_SCRAPER_IMPORT_NAME_CACHE.keys(), key=lambda x: get_scraper_name(x))

            _SCRAPER_CLASS_BY_NAME_CACHE = {get_scraper_name(klass): klass for klass in _SCRAPER_CLASS_CACHE}

            _IS_SETUP = True


def get_scraper_classes():
    _init_caches()
    return _SCRAPER_CLASS_CACHE


def get_scraper_class_by_name(scraper_name):
    _init_caches()
    try:
        return _SCRAPER_CLASS_BY_NAME_CACHE[scraper_name]
    except KeyError:
        raise ScraperNotFound(scraper_name)


def get_import_name_for_scraper(scraper_class):
    _init_caches()
    return _SCRAPER_IMPORT_NAME_CACHE[scraper_class]

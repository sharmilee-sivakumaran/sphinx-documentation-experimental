from __future__ import absolute_import

from future.utils import PY2, python_2_unicode_compatible
from future.builtins import str as text

import pycountry

from .scraper_internal import get_tags


@python_2_unicode_compatible
class TagValues(object):
    def __init__(self, tag_values):
        self.tag_values = tag_values
        self._lower_tag_values = None

    def __get_lower_tag_values(self):
        if self._lower_tag_values is None:
            self._lower_tag_values = {tv.lower() for tv in self.tag_values}
        return self._lower_tag_values

    def __eq__(self, other):
        return other.lower() in self.__get_lower_tag_values()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __contains__(self, item):
        return any(item.lower() in tv for tv in self.__get_lower_tag_values())

    if PY2:
        def __nonzero__(self):
            return len(self.tag_values) > 0
    else:
        def __bool__(self):
            return len(self.tag_values) > 0

    def __cmp__(self, other):
        if not isinstance(other, TagValues):
            raise ValueError("Invalid comparison type")
        return cmp(text(self), text(other))

    def __str__(self):
        return u", ".join(sorted(self.__get_lower_tag_values()))


@python_2_unicode_compatible
class AllTags(object):
    def __init__(self, tags):
        self.tags = tags
        self._lower_tags = None

    def __getattr__(self, tag_name):
        return TagValues(self.tags.get(tag_name, set()))

    def __get_lower_tags(self):
        if self._lower_tags is None:
            self._lower_tags = {tn.lower(): {tv.lower() for tv in self.tags[tn]} for tn in self.tags}
        return self._lower_tags

    def __cmp__(self, other):
        if not isinstance(other, AllTags):
            raise ValueError("Invalid comparison type")
        return cmp(text(self), text(other))

    def __str__(self):
        return u", ".join(
            u"{}={}".format(tn, tv)
            for tn in sorted(self.__get_lower_tags())
            for tv in sorted(self.__get_lower_tags()[tn]))


def get_all_tags(klass):
    tags = dict(get_tags(klass))
    if tags.get("country_code"):
        tags["country"] = {pycountry.countries.get(alpha_2=cc).name for cc in get_tags(klass)["country_code"]}
    if tags.get("subdivision_code"):
        tags["subdivision"] = {pycountry.subdivisions.get(code=c).name for c in get_tags(klass)["subdivision_code"]}
    return AllTags(tags)

from __future__ import absolute_import

from datetime import timedelta
import numbers
import re


def parse_duration(duration_val):
    if isinstance(duration_val, numbers.Real):
        return timedelta(seconds=duration_val)

    m = re.match(r"^\s*(\d+)\s*$", duration_val)
    if m:
        return timedelta(seconds=int(m.group(1)))

    m = re.match(r"^\s*(?:(\d+)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?\s*$", duration_val)
    if m and (m.group(1) or m.group(2) or m.group(3) or m.group(4)):
        return timedelta(
            days=int(m.group(1)) if m.group(1) else 0,
            hours=int(m.group(2)) if m.group(2) else 0,
            minutes=int(m.group(3)) if m.group(3) else 0,
            seconds=int(m.group(4)) if m.group(4) else 0)

    raise Exception(u"Invalid duration: '{}'. Please specify a number of seconds or the"
                    u"duration using the format: '0d 0h 0m 0s' where at least one of"
                    u"those number is not 0.".format(duration_val))


def format_duration(td):
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return u"{}s".format(total_seconds)
    days = total_seconds // 86400
    hours = (total_seconds - days * 86400) // 3600
    minutes = (total_seconds - days * 86400 - hours * 3600) // 60
    seconds = total_seconds - days * 86400 - hours * 3600 - minutes * 60
    if days > 0:
        return u"{}d {}h {}m {}s".format(days, hours, minutes, seconds)
    elif hours > 0:
        return u"{}h {}m {}s".format(hours, minutes, seconds)
    elif minutes > 0:
        return u"{}m {}s".format(minutes, seconds)
    else:
        return u"{}s".format(total_seconds)

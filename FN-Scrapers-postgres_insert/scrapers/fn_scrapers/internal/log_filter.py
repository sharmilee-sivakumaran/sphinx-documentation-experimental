from __future__ import absolute_import

import logging
import six


IS_SCHEDULED_SCRAPER = False


class ScheduledScraperFilter(logging.Filter):
    """
    This is a simple filter that kicks in only if we're
    running as a scheduler scraper. If we're running as
    a scheduled scraper, we will then filter out any log
    messages that are at at least the configured level.

    NOTE: the IS_SCHEDULER_SCRAPER global must be configured
    _before_ the SchedulerScraperFilter is instantiated!
    """
    def __init__(self, level):
        if isinstance(level, six.string_types):
            levelno = logging.getLevelName(level)
        else:
            levelno = level

        self.levelno = levelno if IS_SCHEDULED_SCRAPER else None

    def filter(self, record):
        if self.levelno is None:
            return True
        elif record.levelno >= self.levelno:
            return True
        else:
            return False

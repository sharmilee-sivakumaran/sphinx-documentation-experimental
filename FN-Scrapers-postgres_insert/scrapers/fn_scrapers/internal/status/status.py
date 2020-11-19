from __future__ import absolute_import

from future.utils import PY2

import attr

from ..find_scrapers import ScraperNotFound, get_scraper_class_by_name
from ..scheduler_util import (
    CONDITION_OK,
    CONDITION_WARNING,
    CONDITION_ERROR,
    has_schedule_attempted_before,
    has_schedule_completed_before,
    is_schedule_running,
    get_schedule_start_times,
    get_schedule_condition,
)
from ..tableformat.table import TableBuilderBuilder, ASCENDING
from ..tag_util import get_all_tags


@attr.s
class FieldList(object):
    fields = attr.ib()

    if PY2:
        def __nonzero__(self):
            return bool(self.fields)
    else:
        def __bool__(self):
            return bool(self.fields)


def create_status_table_builder(now, tz_name, datetime_format, fields, include_funcs):
    tbb = TableBuilderBuilder()
    tbb.add_column("name", lambda row: row.data.scraper_name)
    tbb.add_column("enabled", lambda row: row.data.enabled)
    tbb.add_column("run_immediately", lambda row: row.data.run_immediately is not None)
    tbb.add_column("running", lambda row: row.data.owner_tag is not None)
    tbb.add_column("started_at", lambda row: row.data.owner_start_at)
    tbb.add_column("owner_node", lambda row: row.data.owner_node)
    tbb.add_column("owner_name", lambda row: row.data.owner_name)
    tbb.add_column("last_scrape_at", lambda row: row.data.last_good_start_at)
    tbb.add_column("last_scrape_end_at", lambda row: row.data.last_good_end_at)
    tbb.add_column("last_attempt_at", lambda row: row.data.last_start_at)
    tbb.add_column("last_attempt_end_at", lambda row: row.data.last_end_at)
    tbb.add_column("max_expected_duration", lambda row: row.data.max_expected_duration)

    def _last_duration_value(row):
        if has_schedule_completed_before(row.data):
            return row.data.last_good_end_at - row.data.last_good_start_at

    tbb.add_column("last_duration", _last_duration_value)

    def _latency_value(row):
        if has_schedule_completed_before(row.data):
            return now - row.data.last_good_start_at

    tbb.add_column("latency", _latency_value)

    def _average_duration_value(row):
        if has_schedule_completed_before(row.data):
            return row.data.average_good_duration

    tbb.add_column("avg_duration", _average_duration_value)

    def _last_attempt_duration_value(row):
        if has_schedule_attempted_before(row.data):
            return row.data.last_end_at - row.data.last_start_at

    tbb.add_column("last_attempt_duration", _last_attempt_duration_value)

    def _failures_value(row):
        return row.data.failure_count

    def _failures_tags(row, value):
        if value > 0:
            return "warning"

    tbb.add_column("failures", _failures_value, tag_func=_failures_tags)

    def _expected_complete_by_value(row):
        if is_schedule_running(row.data) and has_schedule_completed_before(row.data):
            return row.data.owner_start_at + row.data.average_good_duration

    tbb.add_column("expected_complete_by", _expected_complete_by_value)

    def _next_scrape_at_value(row):
        if not is_schedule_running(row.data):
            return get_schedule_start_times(now, row.data)[0]

    tbb.add_column("next_scrape_at", _next_scrape_at_value)

    def _next_scrape_by_value(row):
        if not is_schedule_running(row.data):
            return get_schedule_start_times(now, row.data)[1]

    tbb.add_column("next_scrape_by", _next_scrape_by_value)

    def _current_duration_value(row):
        if is_schedule_running(row.data):
            return now - row.data.owner_start_at

    tbb.add_column("current_duration", _current_duration_value)

    def _status_value(row):
        return {
            CONDITION_OK: u"OK",
            CONDITION_WARNING: u"WARNING",
            CONDITION_ERROR: u"ERROR",
        }[get_schedule_condition(now, tz_name, datetime_format, row.data)[0]]

    def _status_tags(row, value):
        return {
            u"OK": None,
            u"WARNING": "warning",
            u"ERROR": "bad",
        }[value]

    tbb.add_column("status", _status_value, tag_func=_status_tags)

    def _condition_value(row):
        return get_schedule_condition(now, tz_name, datetime_format, row.data)[1]

    tbb.add_column("condition", _condition_value)

    def _active_value(row):
        return row.cells_dict["enabled"].value or \
            row.cells_dict["running"].value or \
            row.cells_dict["run_immediately"].value

    tbb.add_column("active", _active_value)

    def _tags_value(row):
        try:
            scraper_class = get_scraper_class_by_name(row.data.scraper_name)
            return get_all_tags(scraper_class)
        except ScraperNotFound:
            return None

    tbb.add_column("tags", _tags_value)

    if fields:
        tbb.with_fields(fields)

    tbb.add_include_funcs(include_funcs)

    tbb.add_sort("name", ASCENDING)

    return tbb.build()

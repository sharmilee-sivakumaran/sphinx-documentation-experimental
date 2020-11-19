from __future__ import absolute_import, division

import contextlib
import os
import sys
import io
import yaml

import pytz

from sqlalchemy.orm import sessionmaker

from fn_service.components.config import parse_config
from fn_service.util.postgres import create_pg_engine

from .config import get_config
from .schedule import PG_NOW, Schedule
from .status.status import create_status_table_builder
from .status.text_table_status import print_text_table_status
from .status.csv_status import print_csv_status
from .status.json_status import print_json_status
from .tableformat.cli_utils import get_fields, build_filter_include_func, build_eval_include_func


def status(args):
    default_fields = [
        "name",
        "status",
        "failures",
        "latency",
        "avg_duration",
        "current_duration",
        "expected_complete_by",
        "next_scrape_at",
    ]

    # Validate and parse the 'color' param
    if args.color == "auto":
        color = None
    elif args.color == "always":
        color = True
    elif args.color == "never":
        color = False
    else:
        raise Exception(u"Unexpected value for color param: {}".format(args.color))

    if args.tz:
        tz = args.tz
    else:
        if os.environ.get("FN_SCRAPERS_TZ") and os.environ["FN_SCRAPERS_TZ"] in pytz.all_timezones_set:
            tz = os.environ["FN_SCRAPERS_TZ"]
        elif os.environ.get("TZ") and os.environ["TZ"] in pytz.all_timezones_set:
            tz = os.environ["TZ"]
        else:
            tz = "UTC"

    if args.datetime_format:
        datetime_format = args.datetime_format
    elif os.environ.get("FN_SCRAPERS_DATETIME_FORMAT"):
        datetime_format = os.environ.get("FN_SCRAPERS_DATETIME_FORMAT")
    else:
        datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"  # ISO-8601 format

    config = parse_config(yaml.safe_load(io.BytesIO(get_config("config.yaml"))))
    pg_engine = create_pg_engine(config.app["global"]["scraper_db"])
    Session = sessionmaker(bind=pg_engine)

    with contextlib.closing(Session(expire_on_commit=False)) as session:
        now = session.query(PG_NOW).scalar()
        schedules = session.query(Schedule).all()

    include_funcs = []
    if args.filter:
        include_funcs.append(build_filter_include_func(["name"], args.filter))
    if args.eval:
        include_funcs.append(build_eval_include_func(args.eval))

    def _active_include_func(row):
        if args.include == "active":
            return row.cells_dict["active"].value
        elif args.include == "inactive":
            return not row.cells_dict["active"].value
        elif args.include == "all":
            return True
        else:
            raise Exception(u"Invalid include value: {}".format(args.include))
    include_funcs.append(_active_include_func)

    table_builder = create_status_table_builder(
        now,
        tz,
        datetime_format,
        get_fields(default_fields, args.fields),
        include_funcs)

    table = table_builder(schedules)

    if not args.no_output:
        if args.format == "table":
            print_text_table_status(sys.stdout, color, table, tz, datetime_format)
        elif args.format == "csv":
            print_csv_status(sys.stdout, table, tz)
        elif args.format == "json":
            print_json_status(sys.stdout, table, tz)
        else:
            raise Exception(u"Unknown format: {}".format(args.format))

    for row in table.rows:
        if "bad" in row.tags or "bad" in {t for cell in row.cells for t in cell.tags}:
            sys.exit(3)

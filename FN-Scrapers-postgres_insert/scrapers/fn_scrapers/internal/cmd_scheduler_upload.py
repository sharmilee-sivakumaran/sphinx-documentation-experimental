from __future__ import absolute_import

import contextlib
from datetime import timedelta
import yaml
import json
import sys
import io

from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

from fn_service.components.config import parse_config
from fn_service.util.postgres import create_pg_engine, update_list, EntityManager, DeleteAction

from .duration_format import format_duration, parse_duration
from .find_scrapers import ScraperNotFound, get_scraper_class_by_name
from .schedule import Schedule
from .config import get_config


def _sync_db(dry_run, no_output, schedule_names, schedule_file, on_modified_obj, on_deleted_obj):
    schedule_names = set(schedule_names)
    wildcard = u"*" in schedule_names

    config = parse_config(yaml.safe_load(io.BytesIO(get_config("config.yaml"))))
    pg_engine = create_pg_engine(config.app["global"]["scraper_db"])
    Session = sessionmaker(bind=pg_engine)

    if schedule_file:
        with open(schedule_file) as f:
            config_schedules = yaml.safe_load(f)
    else:
        config_schedules = yaml.safe_load(io.BytesIO(get_config("schedules.yaml")))

    schedules_in_file = {item["scraper_name"] for item in config_schedules}

    # If the user specifies a schedule that we can't find in the schedule file,
    # produce a warning
    if not no_output and not wildcard:
        for sn in schedule_names:
            if sn not in schedules_in_file:
                print u"NOTE: no schedule named '{}' found in schedules.yaml. If you aren't " \
                      u"intending to delete a schedule from the database, this may indicate " \
                      u"a typo.".format(sn)

    # If the user hasn't asked us to look at all the available schedules in the file,
    # limit the schedules to just those that the use specified.
    if not wildcard:
        config_schedules = [item for item in config_schedules if item["scraper_name"] in schedule_names]

    for config_schedule in config_schedules:
        try:
            get_scraper_class_by_name(config_schedule["scraper_name"])
        except ScraperNotFound:
            print u"A scraper with name '{}' appears in the schedules.yaml file provided. " \
                  u"However, there is no scraper with that name.".format(config_schedule["scraper_name"])
            sys.exit(4)

    changes = [False]

    with contextlib.closing(Session()) as session:
        if wildcard:
            schedules = session.query(Schedule).with_for_update().all()
        else:
            schedules = session.query(Schedule).filter(Schedule.scraper_name.in_(schedule_names)).with_for_update().all()

        def _update(obj, item, context):
            obj.scraper_name = item["scraper_name"]
            obj.scraper_args = json.dumps(item["scraper_args"]) if "scraper_args" in item else []
            obj.exclude_nodes = json.dumps(item["exclude_nodes"]) if "exclude_nodes" in item else []
            obj.blackout_periods = json.dumps(item["blackout_periods"]) if "blackout_periods" in item else None
            obj.tz = item["tz"] if "tz" in item else "UTC"
            obj.max_expected_duration = parse_duration(item["max_expected_duration"])
            if "max_allowed_duration" in item:
                obj.max_allowed_duration = parse_duration(item["max_allowed_duration"])
            else:
                obj.max_allowed_duration = None

            if "cron_schedule" not in item:
                obj.scheduling_period = parse_duration(item["scheduling_period"])
                obj.cooldown_duration = parse_duration(item["cooldown_duration"]) if "cooldown_duration" in item \
                    else timedelta()
                obj.cron_schedule = None
                obj.cron_max_schedule_duration = None
            else:
                obj.scheduling_period = None
                obj.cooldown_duration = None
                obj.cron_schedule = item["cron_schedule"]
                obj.cron_max_schedule_duration = parse_duration(item["cron_max_schedule_duration"])

            obj.enabled = item["enabled"] if "enabled" in item else True

            if any(bool(attr.history.added) or bool(attr.history.deleted) for attr in inspect(obj).attrs):
                changes[0] = True
                on_modified_obj(context.is_new, obj)

        def _delete_func(obj):
            changes[0] = True
            on_deleted_obj(obj)
            return DeleteAction.delete

        update_list(
            entity_manager=EntityManager(session, None),
            existing_db_objects=schedules,
            new_items=config_schedules,
            db_obj_key_func=lambda x: x.scraper_name,
            item_key_func=lambda x: x["scraper_name"],
            db_obj_type=Schedule,
            set_db_obj_fields_func=_update,
            delete_db_obj_func=_delete_func)

        if not dry_run:
            session.commit()

    if not no_output and not changes[0]:
        print u"No changes"

    return changes[0]


def _print_change(is_new, obj):
    def _format(x):
        if isinstance(x, timedelta):
            return format_duration(x)
        else:
            return x

    if is_new:
        print u"Created {}".format(obj.scraper_name)
        # We only care about printing out non-None values here. If a non-None value is set,
        # it will be put in a list, and that list will evaluate true, even if there is a Falsy
        # value in it.
        modified_strs = [
            u"{}: {}".format(attr_name, _format(attr.history.added[0]))
            for attr_name, attr in inspect(obj).attrs.items()
            if bool(attr.history.added)
        ]
    else:
        print u"Modified {}:".format(obj.scraper_name)
        # If a field was modified, it will have both an added and a delete
        # value. And we want to print out both. Even if the values are Falsy,
        # this works because SqlAlchemy puts them inside lists.
        modified_strs = [
            u"{}: {} -> {}".format(attr_name, _format(attr.history.deleted[0]), _format(attr.history.added[0]))
            for attr_name, attr in inspect(obj).attrs.items()
            if bool(attr.history.added) and bool(attr.history.deleted)
        ]
    print u"\t" + u"\n\t".join(modified_strs)
    print u""


def _print_deleted(obj):
    print u"Deleted: {}".format(obj.scraper_name)


def _silent(*_):
    pass


def upload_schedules(args):
    if u"*" in args.schedule_names and not args.yolo:
        print "You must specify --yolo if you wish to upload all schedules."
        sys.exit(2)
    if args.no_output:
        print_change, print_deleted = _silent, _silent
    else:
        print_change, print_deleted = _print_change, _print_deleted
    _sync_db(False, args.no_output, args.schedule_names, args.schedule_file, print_change, print_deleted)


def diff_schedules(args):
    if args.no_output:
        print_change, print_deleted = _silent, _silent
    else:
        print_change, print_deleted = _print_change, _print_deleted
    if _sync_db(True, args.no_output, args.schedule_names, args.schedule_file, print_change, print_deleted):
        sys.exit(3)

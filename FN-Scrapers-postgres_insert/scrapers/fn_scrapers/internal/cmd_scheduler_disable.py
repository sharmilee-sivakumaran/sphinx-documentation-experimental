from __future__ import absolute_import, print_function

import contextlib
import sys
import io
import yaml

from sqlalchemy.orm import sessionmaker

from fn_service.components.config import parse_config
from fn_service.util.postgres import create_pg_engine

from .config import get_config
from .schedule import Schedule, PG_NOW
from .scheduler_util import is_schedule_running, schedule_name


def disable_scraper(args):
    config = parse_config(yaml.safe_load(io.BytesIO(get_config("config.yaml"))))
    pg_engine = create_pg_engine(config.app["global"]["scraper_db"])
    Session = sessionmaker(bind=pg_engine)

    with contextlib.closing(Session()) as session:
        schedule = session.query(Schedule).filter(Schedule.scraper_name == args.scraper_name).one_or_none()
        if schedule is None:
            print(u"Failed to find scraper schedule '{}'".format(args.scraper_name))
            sys.exit(3)
        if schedule.enabled:
            schedule.enabled = False
            print("Diabled {}".format(args.scraper_name))
            print("To enable use shceduler upload {}".format(args.scraper_name))
        else:
            print("{} already disabled".format(args.scraper_name))
        session.commit()

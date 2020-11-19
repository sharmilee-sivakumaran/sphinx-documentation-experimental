from __future__ import absolute_import

from sqlalchemy import func, Column, String, BigInteger, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID, JSONB, INTERVAL
from sqlalchemy.ext.declarative import declarative_base


PG_NOW = func.NOW()

BASE = declarative_base()


class Schedule(BASE):
    __tablename__ = "schedules"
    __table_args__ = (
        {'schema': 'fnscrapers'},
    )

    id = Column(BigInteger, primary_key=True)

    scraper_name = Column(String, nullable=False, unique=True)
    scraper_args = Column(JSONB, nullable=False)

    # The nodes that this scraper should never run on (JSON array of strings)
    exclude_nodes = Column(JSONB, nullable=False)

    # The times during which the scraper must not run
    # (JSON array of maps, each of which looks like
    # {"start": "hh:mm:ss", "end": "hh:mm:ss"})
    blackout_periods = Column(JSONB, nullable=True)

    # Set the timezone to use for the blackout_periods
    # and, if it is a cron scheduler, the cron schedule.
    tz = Column(String, nullable=False)

    # Maximum amount of time that the scraper is expected to run for.
    # After this amount of time, if the scraper is still running, it will
    # be considered to be in an error state.
    max_expected_duration = Column(INTERVAL, nullable=False)

    # Maximum amount of time that the scraper is allowed to run for.
    # After this amount of time, if the scraper is still running, it will
    # be killed.
    max_allowed_duration = Column(INTERVAL, nullable=True)

    # The amount of the time that the scraper runs on average when
    # it has a good run.
    average_good_duration = Column(INTERVAL, nullable=True)

    # For periodic schedules only:
    # During this period of time we should always have at least one
    # full scrape run.
    scheduling_period = Column(INTERVAL, nullable=True)

    # For periodic schedules only:
    # The amount of time that we should wait after a scraper
    # completes before starting it again
    cooldown_duration = Column(INTERVAL, nullable=True)

    # For cron schedule only:
    # The schedule when the scraper should kick off,
    # in cron format. If set, this makes the schedule
    # a "cron-style" schedule. If unset, it is a
    # "period-style" schedule.
    cron_schedule = Column(String, nullable=True)

    # For cron schedule only:
    # The amount of time after it is scheduled to start
    # that its ok for the scraper to go before completing.
    cron_max_schedule_duration = Column(INTERVAL, nullable=True)

    # If non-NULL this scraper should run as soon as its able to, otherwise use
    # normal scheduling. This is set to the current time when it is set. The
    # time value is only used when we are deciding to clear this flag after
    # a completed scrape.
    run_immediately = Column(DateTime(True))

    # True if this scraper should run, False otherwise
    enabled = Column(Boolean, nullable=False)

    # If this is set, then the scraper is marked for sudden death
    # This will be considered as a failure, as it normally is when a
    # scraper is killed using kill -9
    kill_immediately = Column(Boolean, default=False, nullable=False)

    # Information about the last complete run (this is only updated once
    # a run completes, not while it is running). If failure_count is 0,
    # it means that the last run completed successfully. Anything else indicates the
    # number of times that the scraper has failed consecutively.
    # last_good_start_at and last_good_end_at are only updated at the conclusion
    # of a successful run.
    # last_start_at and last_end_at are updated when a run ends,
    # regardless of if it completed successfully or with an error.
    last_good_start_at = Column(DateTime(True))
    last_good_end_at = Column(DateTime(True))
    last_start_at = Column(DateTime(True))
    last_end_at = Column(DateTime(True))
    failure_count = Column(BigInteger)

    # If the scraper is currently running, these columns identify the scraper
    # that is running it.
    owner_node = Column(String)
    owner_name = Column(String)
    owner_tag = Column(UUID(as_uuid=True))
    owner_start_at = Column(DateTime(True))
    owner_last_ping_at = Column(DateTime(True))

    # If this is non-NULL, it means that someone is trying
    # to steal the task. If this goes too long without getting
    # set back to NULL, it means that its ok to steal it.
    steal_start_at = Column(DateTime(True))

    created_at = Column(DateTime(True), default=PG_NOW, nullable=False)
    updated_at = Column(DateTime(True), default=PG_NOW, onupdate=PG_NOW, nullable=False)

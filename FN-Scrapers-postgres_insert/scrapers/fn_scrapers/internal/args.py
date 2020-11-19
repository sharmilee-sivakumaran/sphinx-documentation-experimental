from __future__ import absolute_import

from .command_replay import CommandReplay, replay
from .find_scrapers import get_scraper_classes, get_import_name_for_scraper

import argparse
import argcomplete
import sqlite3
import sys
import cPickle
from cStringIO import StringIO
import os
import os.path
import itertools
import textwrap

import fn_scrapers


def _configure_parser(parser):
    from .scraper_internal import get_scraper_name, get_argument_funcs
    import injector
    from .magic_dependency_finder import find_dependencies
    from fn_scrapers.api.resources import AppModule, ScraperModule

    scraper_classes = get_scraper_classes()

    parser.add_argument("--lock-fd", action="append", help=argparse.SUPPRESS)
    parser.add_argument("--config-from-fd", action="append", help=argparse.SUPPRESS)

    groups_subparsers = parser.add_subparsers(title="Commands")

    p = groups_subparsers.add_parser("scheduler", help="Scheduler commands")
    command_subparser = p.add_subparsers()

    p = command_subparser.add_parser("serve", help="Run the scraper server")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_serve", "serve"))
    p.add_argument("scheduler_name", help="The name the scheduler should use - must be unique per server")
    p.add_argument("--serve-until", help=argparse.SUPPRESS)
    p.add_argument("--scraper-working-dir", help=argparse.SUPPRESS)

    p = command_subparser.add_parser("upload", help="Upload schedules into the database from schedules.yaml")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_upload", "upload_schedules"))
    p.add_argument(
        "schedule_names",
        metavar="schedule-names",
        nargs="+",
        help="The name of the schedule. Use '*' to mean all schedules")
    p.add_argument("--schedule", "-s", dest="schedule_file", help="The schedule file to upload")
    p.add_argument("--yolo", action="store_true", help="If you specify a schedule_name of '*' this is required.")
    p.add_argument("--no-output", action="store_true", help="Suppress output")

    p = command_subparser.add_parser("diff", help="Compare the schedules in the database with schedules.yaml")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_upload", "diff_schedules"))
    p.add_argument(
        "schedule_names",
        metavar="schedule-names",
        nargs="*",
        default="*",
        help="The name of the schedule. Use '*' to mean all schedules")
    p.add_argument("--schedule", "-s", dest="schedule_file", help="The schedule file")
    p.add_argument("--no-output", action="store_true", help="Suppress output - just return an exit code")

    p = command_subparser.add_parser("schedule", help="Schedule a scraper to run immediately")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_schedule", "schedule_scrape"))
    p.add_argument("scraper_name", help="The scraper to schedule to run immediately")
    p.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Whether to schedule the scraper to run immediately even if it is already running")
    p.add_argument(
        "--unschedule",
        dest="unschedule",
        action="store_true",
        help="Whether to remove the run_immediately flag")

    status_description = """
        Available fields:
            About the schedule:
            name                  - The schedule name
            active                - If the schedule is running or will run in the future
            enabled               - If the schedule is enabled
            running               - If the schedule is currently running
            run_immediately       - If the schedule is tagged to run immediately
            next_scrape_at        - The time the schedule is expected to start next
            max_expected_duration - The maximum amount of time that the scraper is expected to run for
            tags                  - The tags associated with the scraper

            About errors and warnings:
            failures              - The number of consecutive failures
            status                - The status of the schedule: ERROR, WARNING, or OK
            condition             - A description of why the status is what it is

            About the currently running scrape (if there is one):
            started_at            - The time the currently running scrape started
            current_duration      - The time that the current scrape has been running for
            expected_completion   - The expected time that the current scrape will complete
            owner_node            - The node running the current scrape
            owner_name            - The name of the scheduler running the current scrape

            About previous scrapes:
            latency               - The time since the start of the last successful scrape
            last_scrape_at        - The time that the last successful scrape started
            last_scrape_end_at    - The time that the last successful scrape ended
            last_duration         - The time that the last successful scrape took
            avg_duration          - The average time that successful scrapes take
            last_attempt_at       - The time that the last scrape started (it may have failed)
            last_attempt_end_at   - The time that the last scrape ended (it may have failed)
            last_attempt_duration - The time that the last scrape took (it may have failed)

            all                   - Include all fields

        If a field is prefixed with a +, it will be appended to the list of fields to display.
        If it is prefixed with a -, it will be removed from the list. (NOTE: If specified with
        a -, it must be specified like: "-f=-running" ie, with an = sign between the argument and
        value.
        """

    p = command_subparser.add_parser("kill", help="Kill a running scraper immediately")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_kill", "kill_scraper"))
    p.add_argument("scraper_name", help="The scraper to kill immediately")

    p = command_subparser.add_parser("disable", help="Disable schedule for a scraper that is not running")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_disable", "disable_scraper"))
    p.add_argument("scraper_name", help="The scraper to diable from scheduling")

    p = command_subparser.add_parser(
        "status",
        help="Look up status of all schedules in the database",
        description=textwrap.dedent(status_description),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.set_defaults(command=("fn_scrapers.internal.cmd_scheduler_status", "status"))
    p.add_argument("--tz", help="Timezone to use")
    p.add_argument("--color", default="auto", choices=["never", "always", "auto"], help="Whether to color the output")
    p.add_argument(
        "--include",
        default="active",
        choices=["active", "inactive", "all"],
        help="Whether to include inactive schedules")
    p.add_argument("--filter", help="Filter schedules to include")
    p.add_argument("--eval", help="Select schedules to include by evaluating a Python expression")
    p.add_argument("--no-output", action="store_true", help="Don't output text - just set exit status code")
    p.add_argument("--format", choices=["table", "csv", "json"], default="table", help="The format to output")
    p.add_argument(
        "--field", "-f",
        action="append",
        dest="fields",
        metavar="FIELDS",
        help="Fields to include")
    p.add_argument(
        "--datetime-format",
        help="The format to be used for displaying datetime values. Only applicable to the table format type.")

    p = groups_subparsers.add_parser("scraper", help="Scraper commands")
    command_subparser = p.add_subparsers()

    p = command_subparser.add_parser("list", help="List available scrapers")
    p.set_defaults(command=("fn_scrapers.internal.cmd_scraper_list", "list_scrapers"))
    p.add_argument("--filter", help="Filter scrapers to include")
    p.add_argument("--eval", help="Select scrapers to include by evaluating a Python expression")
    p.add_argument(
        "--field", "-f",
        action="append",
        dest="fields",
        help="Fields to include")

    p = command_subparser.add_parser("run", help="Run a scraper")
    p.add_argument("--parent-pipe-fd", type=int, help=argparse.SUPPRESS)
    p.add_argument("--ping-time", type=int, help=argparse.SUPPRESS)
    p.add_argument("--working-dir", help=argparse.SUPPRESS)

    scraper_subparser = p.add_subparsers(title="Scrapers")

    for scraper_class in scraper_classes:
        name = get_scraper_name(scraper_class)
        sub_parser = scraper_subparser.add_parser(
            name,
            help=u"Run {}".format(name),
        )
        sub_parser.set_defaults(
            command=("fn_scrapers.internal.cmd_scraper_run", "scrape"),
            scraper_class=get_import_name_for_scraper(scraper_class),
        )
        inj = injector.Injector([AppModule(argparse.Namespace()), ScraperModule(get_scraper_name(scraper_class))])
        for dependency in find_dependencies(inj, scraper_class):
            for arg_func in get_argument_funcs(dependency):
                arg_func(sub_parser)

    return parser


def _build_argument_parser(real_parser):
    command_replay = CommandReplay(real_parser)
    parser = command_replay.proxy
    _configure_parser(parser)
    return command_replay


def _pickle(replay_commands):
    io = StringIO()

    pickler = cPickle.Pickler(io)

    def _persistent_id(obj):
        if obj is argparse.SUPPRESS:
            return "SUPPRESS"
        else:
            return None
    
    pickler.persistent_id = _persistent_id

    pickler.dump(replay_commands)

    return io.getvalue()


def _unpickle(pickled_replay_commands):
    unpickler = cPickle.Unpickler(StringIO(pickled_replay_commands))

    def _persistent_load(persid):
        if persid == "SUPPRESS":
            return argparse.SUPPRESS
        else:
            raise cPickle.UnpicklingError('Invalid persistent id')

    unpickler.persistent_load = _persistent_load

    return unpickler.load()


def _load_argument_parser():
    parser = argparse.ArgumentParser(prog="python -m fn_scrapers")

    if "FN_SCRAPERS_DISABLE_CACHE" not in os.environ:
        con = sqlite3.connect(
            os.path.join(os.path.expanduser("~"), ".fn_scraper_cache.db"),
            isolation_level="IMMEDIATE",
            timeout=3)
        con.row_factory = sqlite3.Row
        try:
            # Setup the DB
            con.execute("""
                    CREATE TABLE IF NOT EXISTS scraper_cache_v1 (
                        root_module_dir TEXT NOT NULL,
                        python_hex_version INTEGER NOT NULL,
                        last_modified_at REAL NOT NULL,
                        cached_argument_parser BLOB NOT NULL
                    )
                """)
            con.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS scraper_cache_v1_idx
                    ON scraper_cache_v1 (root_module_dir, python_hex_version)
                """)

            with con:
                cache_row = con.execute("""
                        SELECT rowid, last_modified_at, cached_argument_parser
                        FROM scraper_cache_v1
                        WHERE root_module_dir = ? AND python_hex_version = ?
                    """,
                    (fn_scrapers.__path__[0], sys.hexversion)).fetchone()

                last_modified_at = max(
                    os.stat(os.path.join(dirpath, p)).st_mtime
                    for dirpath, dirs, files in os.walk(fn_scrapers.__path__[0])
                    for p in itertools.chain((f for f in files if f.endswith(".py")), dirs)
                )

                if cache_row and last_modified_at > cache_row["last_modified_at"]:
                    con.execute("DELETE FROM scraper_cache_v1 WHERE rowid = ?", (cache_row["rowid"],))
                    cache_row = None

                if cache_row:
                    replay(_unpickle(bytes(cache_row["cached_argument_parser"])), parser)
                    return parser
                else:
                    command_replay = _build_argument_parser(parser)
                    con.execute("""
                        INSERT INTO scraper_cache_v1 VALUES (?, ?, ?, ?) 
                        """,
                        (
                            fn_scrapers.__path__[0].decode('utf-8'),
                            sys.hexversion,
                            last_modified_at,
                            buffer(_pickle(command_replay.commands)),
                        ))
                    return parser
        except sqlite3.OperationalError:
            # If we got an error trying to work with the DB cache, bail out on that
            # and just re-create a parser from scratch - its better to be a little
            # slower than to fail.
            pass
        finally:
            con.close()

    _configure_parser(parser)
    return parser


def parse_args():
    parser = _load_argument_parser()
    argcomplete.autocomplete(parser)
    return parser.parse_args()

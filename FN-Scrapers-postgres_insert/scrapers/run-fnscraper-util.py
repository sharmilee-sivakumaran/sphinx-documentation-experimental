#!/usr/bin/env python

import argparse
from contextlib import contextmanager
import errno
import fcntl
import os
import shutil
import subprocess
import sys
import time


def set_cloexec(fd, set_flag):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    if set_flag:
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    else:
        fcntl.fcntl(fd, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)


@contextmanager
def lock_file(file_name, mode, cloexec=True):
    fd = os.open(file_name, os.O_RDONLY)
    try:
        if cloexec:
            set_cloexec(fd, True)
        fcntl.flock(fd, mode)
        yield fd
    finally:
        os.close(fd)


def makedirs(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def touch(path):
    with open(path, 'a'):
        os.utime(path, None)


def get_default_source():
    # We don't technically need an absolute, canonical path here.
    # However, os.path.dirname("blah") returns "" which will
    # confuse pip.
    return os.path.realpath(os.path.dirname(sys.argv[0]))


def get_default_fnscraper_home():
    # Get an absolute, canonical path - this way we can pass
    # around paths that are independent of the current working
    # directory.
    return os.path.realpath(os.path.join(os.path.dirname(sys.argv[0]), ".."))


def cmd_install(args):
    # Create directories
    makedirs(os.path.join(args.dest, "bin"))
    makedirs(os.path.join(args.dest, "etc"))
    makedirs(os.path.join(args.dest, "lock"))
    makedirs(os.path.join(args.dest, "venvs"))
    makedirs(os.path.join(args.dest, "working"))

    # Create the main lock file
    touch(os.path.join(args.dest, "lock", "main"))

    # Grab the main lock - prevents anyone else from modifying the environment
    with lock_file(os.path.join(args.dest, "lock", "main"), fcntl.LOCK_EX):
        # Build the virtualenv
        virtualenv_name = str(int(time.time()))
        try:
            subprocess.check_call(["virtualenv", os.path.join(args.dest, "venvs", virtualenv_name)])
            # The pip script has issues # with paths that exceed ~128 bytes on Linux
            # so, we need to use short-ish paths
            subprocess.check_call([
                    os.path.join(args.dest, "venvs", virtualenv_name, "bin", "pip"),
                    "install",
                    "--upgrade",
                    "pip", "wheel", "setuptools",
                ])
            env = os.environ.copy()
            env.update(dict(PKG_VERSION="git"))
            subprocess.check_call([
                    os.path.join(args.dest, "venvs", virtualenv_name, "bin", "pip"),
                    "install",
                    "Cython==0.27.3",
                ],
                env=env)
            subprocess.check_call([
                    os.path.join(args.dest, "venvs", virtualenv_name, "bin", "pip"),
                    "install",
                    "-r",
                    os.path.join(args.source, "requirements.txt"),
                ],
                env=env)
            subprocess.check_call([
                    os.path.join(args.dest, "venvs", virtualenv_name, "bin", "pip"),
                    "install",
                    "-r",
                    os.path.join(args.source, "requirements_mssql.txt"),
                ],
                env=env)
            subprocess.check_call([
                    os.path.join(args.dest, "venvs", virtualenv_name, "bin", "pip"),
                    "install",
                    args.source,
                ])
        except:
            shutil.rmtree(os.path.join(args.dest, "venvs", virtualenv_name), ignore_errors=True)
            raise

        # Update the "current" symlink to point to the virtualenv we want to be active
        if os.path.islink(os.path.join(args.dest, "venvs", "current")):
            os.unlink(os.path.join(args.dest, "venvs", "current"))
        os.symlink(virtualenv_name, os.path.join(args.dest, "venvs", "current"))

        # Delete old configs and then copy over any configuration files that have been specified
        # NOTE: Its important that we unlike the files first - this is because scrapers may have
        # open file descriptors pointing at the existing versions. If we unlink the files, those programs
        # will see a snapshot of the file at the time it was unlinked - which is what we want.
        # If we modified them in place, the programs might see our updates - which we don't
        # want.
        for fn in os.listdir(os.path.join(args.dest, "etc")):
            if fn == "last-update":
                continue
            os.unlink(os.path.join(args.dest, "etc", fn))
        for fn in args.configs:
            with open(fn, "r") as fs:
                with open(os.path.join(args.dest, "etc", os.path.basename(fn)), "w+") as fd:
                    fd.write(fs.read())

        # Copy over the util programs
        shutil.copy2(
            os.path.join(args.source, "run-fnscraper-util.py"),
            os.path.join(args.dest, "bin", "run-fnscraper-util.py"))
        shutil.copy2(
            os.path.join(args.source, "fnscraper"),
            os.path.join(args.dest, "bin", "fnscraper"))

        # Remove old virtual environments and working directories
        do_gc(args.dest)

        # Update the last-update file
        with open(os.path.join(args.dest, "etc", "last-update"), "w+") as f:
            f.write(time.strftime("%x %X %z\n").encode("utf-8"))


def gc_dir(dir, exclude=None):
    # NOTE: Only call this function with the main lock already held!
    for dirname in os.listdir(dir):
        if exclude and dirname in exclude:
            continue
        try:
            with lock_file(os.path.join(dir, dirname), fcntl.LOCK_EX | fcntl.LOCK_NB):
                shutil.rmtree(os.path.join(dir, dirname))
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise


def do_gc(fnscraperhome):
    # NOTE: Only call this function with the main lock already held!
    current_venv = os.readlink(os.path.join(fnscraperhome, "venvs", "current"))
    gc_dir(os.path.join(fnscraperhome, "venvs"), {"current", current_venv})
    gc_dir(os.path.join(fnscraperhome, "working"))


def cmd_gc(args):
    # Grab the main lock - prevents anyone else from modifying the environment
    with lock_file(os.path.join(args.fnscraperhome, "lock", "main"), fcntl.LOCK_EX):
        do_gc(args.fnscraperhome)


def run_fnscraper(fnscraperhome, argv, extra_locks=None, extra_env=None):
    # NOTE: Only call this function with the main lock already held!

    # Use a fresh environment so that it won't inherit stuff from the
    # caller - such as being in a virtual environment.
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    }
    if "TZ" in os.environ:
        env["TZ"] = os.environ["TZ"]
    for env_name in os.environ:
        if env_name.startswith("FN_SCRAPERS_"):
            env[env_name] = os.environ[env_name]
    if extra_env:
        env.update(extra_env)

    # Find and then lock the virtualenv
    venv_dir = os.path.realpath(os.path.join(fnscraperhome, "venvs", "current"))
    with lock_file(venv_dir, fcntl.LOCK_SH, cloexec=False) as venv_lock_fd:

        # Find and pass config files
        config_args = []
        for fn in os.listdir(os.path.join(fnscraperhome, "etc")):
            if fn in {"last-update"}:
                continue

            # NOTE: we never update these files in place - (see the install
            # command above). So, even if these files get updated before they are
            # read, we'll still see the version of the file at the time
            # that we did this open.
            fd = os.open(os.path.join(fnscraperhome, "etc", fn), os.O_RDONLY)
            config_args.extend([
                "--config-from-fd",
                "{}:{}".format(fd, fn)
            ])

        # Construct the list of locks to pass along
        lock_args = ["--lock-fd", str(venv_lock_fd)]
        for l in extra_locks or []:
            lock_args.extend(["--lock-fd", str(l)])

        python = os.path.join(venv_dir, "bin", "python")
        os.execve(
            python,
            [
                python,
                # We run it this way, because, if we run it like python -m fn_scrapers,
                # the current working directory gets pre-pended to sys.path - which could
                # result in unexpected stuff getting loaded.
                os.path.join(venv_dir, "lib", "python2.7", "site-packages", "fn_scrapers"),
            ] + lock_args + config_args + argv,
            env,
        )


def cmd_fnscraper(argv):
    # Find the home and then lock it from modifications
    fnscraperhome = get_default_fnscraper_home()
    with lock_file(os.path.join(fnscraperhome, "lock", "main"), fcntl.LOCK_SH):
        return run_fnscraper(fnscraperhome, argv)


def cmd_scheduler_serve(args):
    # Find the home and then lock it from modifications
    fnscraperhome = get_default_fnscraper_home()
    with lock_file(os.path.join(fnscraperhome, "lock", "main"), fcntl.LOCK_SH):

        # Create working directories for the scheduler and scrapers
        working_dir = os.path.join(fnscraperhome, "working", args.scheduler_name)
        if not os.path.isdir(working_dir):
            os.mkdir(working_dir)

        # Pass in LOCK_NB - if the working directory is already locked, we should
        # exit with an error as opposed to waiting forever (which Supervisor might
        # interpret as everything being OK).
        with lock_file(working_dir, fcntl.LOCK_EX | fcntl.LOCK_NB, cloexec=False) as cwd_lock_fd:
            # Clear out any old files. We can't use shutil.rmtree since we want to
            # retain the directory itself (since we just threw a lock on it)
            for dirpath, dirnames, filenames in os.walk(working_dir, topdown=False):
                for fn in filenames:
                    os.unlink(os.path.join(dirpath, fn))
                for dn in dirnames:
                    os.rmdir(os.path.join(dirpath, dn))

            # NOTE: We don't create this directory because the scheduler will do that
            scraper_working_dir = os.path.join(working_dir, "scraper_cwd")

            serve_until_file = os.path.join(fnscraperhome, "etc", "last-update")
            serve_until_info = "{}:{}".format(os.stat(serve_until_file).st_mtime, serve_until_file)

            os.chdir(working_dir)

            run_fnscraper(
                fnscraperhome,
                [
                    "scheduler", "serve",
                    "--scraper-working-dir", scraper_working_dir,
                    "--serve-until", serve_until_info,
                    args.scheduler_name
                ],
                extra_locks=[cwd_lock_fd],
                extra_env={"FN_SCRAPERS_DISABLE_CACHE": "True"})


def main():
    def _add_install_args(p):
        p.set_defaults(command=cmd_install)
        p.add_argument("--source", default=get_default_source(), help="The location of the FN-Scraper sources")
        p.add_argument("--dest", default="/opt/fnscrapers/", help="Where to install to")
        p.add_argument("--configs", nargs="+", default=[], help="List of configuration files to install")

    def _add_gc_args(p):
        p.set_defaults(command=cmd_gc)
        p.add_argument(
            "--fnscraperhome",
            default=get_default_fnscraper_home(),
            help="The installed home directory")

    def _add_scheduler_serve_args(p):
        p.set_defaults(command=cmd_scheduler_serve)
        p.add_argument(
            "scheduler_name",
            metavar="scheduler-name",
            help="The scheduler name")

    # Special case - if the first command line argument is the string "fnscraper",
    # it means that we are being asked to run the fnscraper CLI. In that case,
    # just take the remaining arguments and pass them along - don't try to
    # parse them since they are intended for fnscraper itself.
    if len(sys.argv) >= 2 and sys.argv[1] == "fnscraper":
        cmd_fnscraper(sys.argv[2:])
        raise Exception("cmd_fnscraper should not return")

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    _add_install_args(subparsers.add_parser("install"))
    _add_gc_args(subparsers.add_parser("gc"))
    _add_scheduler_serve_args(subparsers.add_parser("scheduler-serve"))

    # This is a dummy subparser - it will never get called because if a user
    # passes this option, we skip argument parsing. However, we add it in so
    # that it shows up in the help message.
    subparsers.add_parser("fnscraper")

    args = parser.parse_args()
    args.command(args)


if __name__ == "__main__":
    main()

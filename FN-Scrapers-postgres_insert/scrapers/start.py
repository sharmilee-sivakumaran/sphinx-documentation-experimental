#!/usr/bin/env python2

import errno
import json
import os
import shutil
import subprocess
import sys


if hasattr(os, 'sync'):
    sync = os.sync
else:
    import ctypes
    libc = ctypes.CDLL("libc.so.6")

    def sync():
        libc.sync()


def mkdirp(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise


name = sys.argv[1]

git_dir = os.path.join(os.getcwd(), "FN-Scrapers", ".git")
base_dir = os.path.join(os.getcwd(), "scrapers", name)
work_dir = os.path.join(base_dir, "work")
src_dir = os.path.join(base_dir, "src")
scraper_src_dir = os.path.join(src_dir, "scrapers")
virtualenv_dir = os.path.join(base_dir, "venv")

config_path = os.path.join(os.getcwd(), "CONFIG.json")

pip_path = os.path.join(virtualenv_dir, "bin", "pip")
python_path = os.path.join(virtualenv_dir, "bin", "python")

requirements_path = os.path.join(scraper_src_dir, "requirements.txt")
saved_requirements_path = os.path.join(base_dir, "saved-requirements.txt")

# Open up the config file and load its contents - this tells
# us the git commit we want to check out as well as the configuration
# files we want to write into the working directory for the scraper.
with open(config_path, "r") as f:
    config = json.load(f)
    config_time = os.fstat(f.fileno()).st_mtime

config_branch = config["branch"]

# Create the base directory
mkdirp(base_dir)

# Get rid of the existing src directory - we'll check out a fresh
# copy of the code
if not os.path.exists(src_dir):
    subprocess.check_call(["git", "clone", "git@github.com:FiscalNote/FN-Scrapers.git", src_dir])
subprocess.check_call(["git", "-C", src_dir, "reset", "-q", "--hard"])
subprocess.check_call(["git", "-C", src_dir, "clean", "-f", "-q", "-x"])
subprocess.check_call(["git", "-C", src_dir, "remote", "update"])
subprocess.check_call(["git", "-C", src_dir, "fetch", "--tags"])
subprocess.check_call(["git", "-C", src_dir, "checkout", config_branch])
if subprocess.call(["git", "-C", src_dir, "symbolic-ref", "--short", "-q", "HEAD"]) == 0:
    # If we're on a branch, do a pull
    subprocess.check_call(["git", "-C", src_dir, "pull", "--ff-only"])

# Create the virtual environment

# Step 1: If we we're missing either the environment of the saved requirements file
# that was used to create it, delete both so that we end up in a clean state.
if not os.path.exists(saved_requirements_path) or not os.path.exists(virtualenv_dir):
    if os.path.exists(saved_requirements_path):
        os.unlink(saved_requirements_path)
    if os.path.exists(virtualenv_dir):
        shutil.rmtree(virtualenv_dir)

# Step 2: If there is a saved requirements file and it doesn't match up with the
# current requirements, delete the virtualenv so we can re-create it
if os.path.exists(saved_requirements_path):
    with open(saved_requirements_path, "r") as sr, open(requirements_path, "r") as r:
        if sr.read() != r.read():
            if os.path.exists(virtualenv_dir):
                shutil.rmtree(virtualenv_dir)

# Step 3: If there isn't a virtualenv at this point, we know we need to create it - either
# because it didn't exist already or, it did, but was outdated, so we deleted it.
if not os.path.exists(virtualenv_dir):
    subprocess.check_call(["virtualenv", virtualenv_dir])
    subprocess.check_call([pip_path, "install", "--upgrade", "pip"])
    subprocess.check_call([pip_path, "install", "--upgrade", "setuptools"])
    subprocess.check_call(
        [pip_path, "install", "--no-deps", "-r", requirements_path],
        env=dict(os.environ, PKG_VERSION="git"),
    )
    # Do a sync here to make sure that the full virtual environment is
    # written to disk. If the system crashes after this point, at least we won't
    # come back up with a corrupted virtual environment.
    sync()
    with open(saved_requirements_path, "w+") as sr, open(requirements_path, "r") as r:
        sr.write(r.read())
subprocess.check_call([pip_path, "install", "--no-deps", "-e", scraper_src_dir])

# If the working directory exists, delete it. Then, create it and populate it
# with files from the config
if os.path.exists(work_dir):
    shutil.rmtree(work_dir)
mkdirp(work_dir)
for filename in config.get("files", {}):
    with open(os.path.join(work_dir, filename), "w+") as f:
        f.write(config["files"][filename].encode("utf-8"))

# Finally, kick off the scraper scheduler!
os.chdir(work_dir)
os.execl(python_path, python_path, "-m", "fn_scrapers", "scheduler", "serve", "--serve-until", "{}:{}".format(config_time, config_path), name)


# Deployment and Execution

## Goals

1. Allow multiple running FN-Scrapers instances to share
a virtual environment. Creating virtual environments tends
to be pretty expensive, so, sharing is important if we want
to be able to comfortably run many instances on a single
server.

2. Support the "long scraper" use case. Basically, some scrapers
can take many hours to execute. However, we don't want that
to prevent us from being able to deploy updates. So, deploying
a new scraper must not interrupt existing scrapers. Since
scrapers must share virtual environments, this also means
that there must be a way for different scrapers to use
different virtual environments.

3. Support virtual environment garbage collection. Due to the
above, we must support having multiple virtual environments
on the system at once. However, we don't want that to grow
without bound. So, there must be a mechanism to delete
old virtual environments once they are no longer needed.

4. Unfortunately, virtual environments only work from
absolute paths. So, its not possible to create copies
of a single virtual environment for use by multiple scrapers -
each scraper must use the virtual environment from the
path it was originally created at.

5. We only care if this works on 64-bit Linux. Supporting
Windows / MacOS would be nice, but, not needed. Development
on those platforms must work, but the full installation
described need not (and probably doesn't).

## Design

### Runtime Directories

`/opt/fnscrapers/` - The root directory where scrapers are
deployed.

`/opt/fnscrapers/bin/` - The directory where the `fnscraper`
and `fnscheduler` helper programs are stored.

`/opt/fnscrapers/etc/` - The directory where scraper
configuration files are stored.

`/opt/fnscrapers/lock/` - The directory where scraper locks are
stored.

`/opt/fnscrapers/venvs/` - The directory where all of the
virtual environments are stored.

`/opt/fnscrapers/working/SCHEDULER_NAME/` - The working directory
for a scheduler.

`/opt/fnscrapers/working/SCHEDULER_NAME/scraper_cwd/` - The working directory
for a scraper being run by a scheduler.

### Locking

We rely on Linux file locking to support the sharing and
garbage collection of virtual environments. Specifically,
we make use fo the `flock()` function and file-descriptor
inheritance.

Some examples:

Acquire a lock:
```python
import os
import fcntl

fd = os.open("some-file-or-directory", os.O_RDONLY)

# Acquire a shared lock on the file or directory
# This blocks if another process has an exclusive lock
# already. If we also pass the option fcntl.LOCK_NB, instead
# of blocking, this will immediately fail if we can't get the 
# lock.
fcntl.flock(fd, fcntl.LOCK_SH)

# Upgrade that lock to an exclusive lock
# This blocks if another process has an exclusive or shared
# lock already
fcntl.flock(fd, fcntl.LOCK_EX)

# Unlock it. If no other program has a copy of fd,
# closing fd or exiting would also unlock it.
fcntl.flock(fd, fcntl.LOCK_UN)
```

File descriptor inheritance:
```python
import os
import fcntl

# Acquire a lock
fd = os.open("some-file-or-directory", os.O_RDONLY)
fcntl.flock(fd, fcntl.LOCK_EX)

# Exec another program. That program inherits fd, and,
# thus inherits the lock. We need to pass the fd to the
# new program so it knows which file descriptor has the lock.
os.execl("my-prog.py", "my-prog.py", str(fd))
```

In my-prog.py:
```python
import sys
import fcntl

# Get the fd that contains the lock
lock_fd = int(sys.argv[0])

# Set the CLOEXEC flag on the file descriptor. This ensures
# that if we create any child programs, they won't get a copy
# of the file descriptor. This is used to ensure that they
# won't be able to release the lock, even if they wanted to.
flags = fcntl.fcntl(fd, fcntl.F_GETFD)
fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
```

Anyway, this shows basically how we can use file locking to
support multi-process Reader/Writer locks.

### Implementation

#### fnscraper User

Traditionally, we've run everything as the deploy user.
Unfortunately, that user is effectivley root. So,
we create a new user, fnscraper, that is an un-privaleged
system user.

All installed files are owned by this user, but are left readable by anyone
on the system (but not writable). The scheduler is configured to run as this
user. Other CLI commands, however, run as the user that initiated the
command. As such, outside of installation, there is never any reason
to sudo to the fnscraper user.

#### run-fnscraper-util.py

run-fnscraper-util.py is a utility script that provides most of the
functionality described in the rest of this document. Significantly - it runs
using the system python and has no depdendencies. Additionally, it can detect
the name that was used to call it, and alter its behavior according. So, if
it is called as "run-fnscraper-util.py run" it knows to run a CLI command.
However, we can also create a symlink to it named "fnscraper". When called
using this name, it knows to run a CLI command without additional arguments.
We make use of this so that we can install just the single file, which can
define some utility functions, and still implement all of the tools we need.

#### Installation ("run-fnscraper-util.py install")

Installation is performed by running the `run-fnscraper-util.py install`
program as the fnscraper user.

1. If the directories don't exist, they are created.

2. It grabs the lock `/opt/fnscrapers/lock/main` in
exclusive mode. This prevents any other scrapers from starting.

3. Using the checked in requirements.txt file, it creates the virtual
environment. It is named according to the current timestamp and placed into
the directory `/opt/fnscrapers/venvs/`. As the FN-Scrapers code is also
installed into the virtual environment, we always create a fresh one for each
installation. A symlink is created at `/opt/fnscrapers/venvs/current` which
points to the currently active virtualenvironment.

4. Configuration files are deposited into
`/opt/fnscrapers/etc/`. Old config files are removed. There is one
special config files:

    * `/opt/fnscrapers/etc/last-update` - This is a special
    config file that is touched to update its last modified at timestamp.
    This is used to indicate to the schedulers that the configuration has
    been updated and that they should restart to pickup the new config.

5. We run a GC of virtual environments and scheduler working directories.

#### GC ("run-fnscraper-util.py gc" or "run-fnscraper-gc")

1. For each virtual environment in `/opt/fnscrapers/venvs/` that is _not_ the
current one, we attempt to grab an exclusive lock on the directory. If we
succeed, we know that no one else is using it, so, we delete it. If we don't
succeed, we leave it alone. We do the 

2. We do the same thing for all directories in `/opt/fnscrapers/working/` to
clean up working directories created for schedulers that are not currently
being used.

#### CLI Usage ("run-fnscraper-util.py run" or "fnscraper" helper)

The fnscraper helper program is intended to be the primary
user interface to the FN-Scrapers CLI. Its job is to 
start up the CLI using the correct virtual environment and
configuration files.

1. Acquire a shared lock on `/opt/fnscrapers/lock/main`.
This will prevent any configuration updates while we are
setting up. This lock is marked with FD_CLOEXEC so that it will
be released once the CLI starts up.

2. We setup a clean set of of environment variables. We do this
to make sure that environment variables set by the user won't
impact how the CLI runs - spcecifically having a virtual environment
activated could cause issues.

3. The current virtual environment to use is found by reading
`/opt/fnscrapers/venvs/current`. We acquire a shared lock
on this directory and do not mark it with FD_CLOEXEC to allow
it to be inherited by the CLI.

4. We open all files in `/opt/fnscrapers/etc/` (except for "last-update") and
pass file descriptors to these files to the main program. We do this so that
we can be sure that the main program will see the right versions of these
files when it starts, even if another install operation runs first.

5. We `exec` the python program in the chosen virtual environment
and run the `fn_scrapers` module, passing along all extra arguments.
We pass along two special arguments:

    * `--lock-fd` - This is a lock that we are passing to the CLI.
    The CLI just marks this with FD_CLOEXEC after starting up but
    leaves it open to keep the lock alive.

    * `--config-from-fd` - This specifies a file descriptor to use
    to load a config file from. For example, it might be:
    "7:config.yaml" - which means to load "config.yaml" by reading
    file descriptor 7. The config files are all fairly small, so,
    it is a minor optimization, but do avoid reading any of the
    file descriptors unless we actually need the content. So, there
    is very little cost in passing file descriptors for all config
    files.

#### Scheduler ("run-fnscraper-util.py scheduler-serve" or "run-fnscraper-scheduler-serve")

This helper program is used to start up a new
scheduler instance. It's very similar to the `fnscraper` program,
but with a few key differences to allow for multiple schedulers to
run at once.

It works by:

1. `fnscraper` only accepts a single argument, the name of the
scraper being started. We'll call that "SCRAPER_NAME" from
here on out.

2. Create a working directory in `/opt/fnscrapers/working/SCRAPER_NAME`.
Once created, the directory is immediately locked in exclusive mode (which
prevents multiple schedulers from attempting to share a working directory
which won't ever work well). Next, we clear out everything in the
directory, so we can start fresh.

3. We change the working directory to 
`/opt/fnscrapers/working/SCRAPER_NAME`.

4. We start program using exec just as for the fnscraper helper program. The only
differences are:

    * `--scraper-working-dir`: This option is passed to tell the
    scheduler where to run its workers. The scheduler assumes that it
    has complete control over this directory and will delete and
    re-create it before running each worker. The path passed is:
    `/opt/fnscrapers/working/SCRAPER_NAME/scraper_cwd`.

    * `--serve-until FILE_NAME:LAST_UPDATE_TIME` - This tells the 
    scheduler about a file that it should monitor for changes, and,
    the last time that that file was modified. If the scheduler notices
    that file was changed, it knows to restart itself to pick up
    the latest configuration. If the scheduler is busy running a
    scraper, however, it won't do this restart until after the
    scraper has finished.

    * We pass an additional lock file descriptor - one for the
    scheduler working directory to protect it from garbage collection.

    * We set an additional environment variable: FN_SCRAPERS_DISABLE_CACHE - 
    this disables the scraper cache. The cache isn't all that useful for
    the scheduler case, and, it requires writing to the user's home
    directory - and the fnscrapers user may not have a home directory.

5. Eventually, the scheduler will want to start a scraper. It will do
this by createing the appropriate directory and starting the scraper there.
One tricky part is passing the configuration files to the scraper - it
does this by establishing pipes - the scheduler process will then write
the config file contents to this pipe while the scraper will inherit the
read end of the pipe and read the contents from there. In this way,
we can be sure that the configs passed to the scraper are identical
to those used by the scheduler.

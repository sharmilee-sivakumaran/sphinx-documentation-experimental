# Architecture

# Description of the main modules

Basic description of various files and directories:

These files make up the public API:
* `api/resources.py` - This file defines the resources available by to individual scrapers. These resource an be accessed via the Injector.
* `api/scrape_item_publisher.py` - This file defines a utility component that sends messages to RMQ. The difference between this implementation and the one we have in fn-scraperutils is that this one ties into FN-Scrapers - specifically that it provides a mechanism to receive command line arguments from the scraper.
* `api/scraper.py` - This file defines the main FN-Scraper API. There isn't much to this API - its just a mechanism for a class to declare that it is a scrape and to declare the command line arguments that it would like to receive.
* `api/utils.py` - This file contains utility functions that are generally useful for scrapers - right now, just the `JSONEncoderPlus` class.

These files are the actual scrapers:
* `datatypes/international/` - These are where all the international scrapers live. The `common` sub-directory contains the common code that the scrapers written by the ODA team use. The `base_scraper.py` file in that directory contains the `BaseScraper` class which uses the Injector framework to hook up FN-ScraperUtils with the FN-Scrapers code. This is a good example for how the scrapers written here use FN-ScraperUtils as their main framework - Fn-Scrapers functions more as a runtime environment than a framework.
* `datatypes/example/` - This includes a very simple example scraper.
* `datatypes/social/` - This includes a WIP port of the Twitter scraper.

These files contain the internal code that implements the FN-Scrapers runtime:
* `internal/main.py` - this is the entry-point to FN-Scrapers. Its called from `__main__.py` in the root of the fn_scrapers pacakge. Basically all it does is call the args module (below) to build up the `ArgumentParser` and then based on the return value of that, invoke the appropriate module to handle whatever command the user selected.
* `internal/args.py` - This module is responsible for building up the `ArgumentParser` to be used to parse command line arguments. The main complication of this module is that the available Scrapers and their command line arguments aren't centrally registered anywhere. Instead, we have to scan the modules under `datatypes/` looking for classes annotated with `@scraper()`. This takes a while on MacOS, although its quite speedy on Linux. Anyway, due to being slow on MacOS, this module implements a simple cache of for the constructed `ArgumentParser` in a Sqlite database. Its an open question as to whether or not this is worth the complexity, but, it seems to work fine for now.
* `internal/command_replay.py` - Unfortunately, `ArgumentParser` instances can't be pickled. This module provides a workaround: When constructing an `ArgumentParser` for the first time, you wrap the `ArgumentParser` with a proxy from this module. Then, you build the `ArgumentParser`. This module intercepts every call being used to build the `ArgumentParser`. Its not possible to pickle an `ArgumentParser`. However, it is possible to pickle the complete list of commands that were used to construct an `ArgumentParser` and then to replay those commands to create a new `ArgumentParser`. As above, its unclear if its worth the complexity just to optimize on MacOS. On the other hand, again, this seems to work fine for now.
* `internal/magic_dependency_finder.py` - So, a scraper can depend on other classes, and those dependencies are expressed via the Injector framework. The issue is if one of those dependencies wants to accept a command line parameter - how does it do that? The original solution was for each scraper to explicitly state which dependencies its using - but, the problem with that is that if a scraper has transitive dependencies, that becomes confusing. It also becomes pretty painful if we want to add new command line arguments for a class that previously didn't have any. The solution - the Injector annotations already define a dependency tree for each scraper. So, we can walk that dependency tree without instantiating any objects to find which classes are required for any given scraper. Given that knowledge, we can look at the `@argument` annotations on all of those dependencies and use that to build the `ArgumentParser`. This is a bit magical since it has to dig into the Injector internals a bit - but, I think its enough of a usability win that its worth it.

These files contain the code the implement the various commands supported by FN-Scrapers: "scrape", "serve", "update-schedule", "schedule-scrape", and "status":
* `internal/cmd_scraper_run.py` - This contains the code for setting up a run of a single scraper.
* `internal/cmd_scraper_list.py` - This contains the code for listing all of the available scrapers.
* `internal/cmd_scheduler_serve.py` - This contains the code for running the scheduler.
* `internal/cmd_scheduler_update.py` - This contains the code for updating the schedules in the DB.
* `internal/cmd_scheduler_schedule.py` - This contains the code for marking a schedule in the DB as needing to run immediately instead of at its normally scheduled time.
* `internal/cmd_scheduler_status.py` - This contains the code for dumping the schedule table in the DB into a human readable output giving the state of all the scrapers. One day, this might be replaced with a nicer web-based interface.

* `internal/run_and_monitor_scraper.py` - This contains code for starting up a scraper child process of a scheduler, running it, monitoring it, and killing it if it appears to be misbehaving. Some of the code is kinda low-level - using UNIX functionality directly - its done this way because there aren't higher level interfaces to use. The main UNIX-ism is that we open a UNIX pipe from the child to the parent scheduler instance. We then expect that the child will write single byte into that pipe every 30 seconds. If we don't get such a ping, we assume that the child is hung and kill it.
* `internal/schedule.py` - This defines the SqlAlchemy DB model object.
* `internal/scheduler.py` - This defines the main scheduler logic. This contains the code to look for which scraper needs to run next, mark that scraper as running, and then use `run_and_monitor_scraper` to actually run it.
* `internal/scheduler_util.py` - This just contains the utility function to take a scraper schedule in the DB and to figure out when it wants to run next. This is used both by the scheduler to figure out which scraper should be run next and by the status code to print out the expected runtime of the scrapers.

* `internal/scraper_handler.py` - This is a bridge from fn-service land into the scrapers - it just defines an fn-service handler that know how to run a scraper.
* `internal/scraper_internal.py` - This defines functions used by `api/scraper.py` that set values. These functions are meant to only be used by fn-scrapers and not by the scraper themselves.
* `internal/serve_until.py` - This defines a simple fn-service scheduled handler that periodically checks for if a given file has been updated. If it has, its a sign to the scheduler that its time for it to exit and restart itself with a new configuration (such as after a deployment when it might need to update its dependencies).
* `internal/find_scrapers` - This contains the code to load all of the modules in the fn_scrapers.datatypes module and submodules looking for scraper classes.
* `internal/tag_util.py` - This contains the code for working with scraper tags for the "scheduler list" and "scraper status" commands.
* `internal/tableformat/` - This defines a simple library that can handle formatting scraper status information into various output formats - an ASCII table, CSV output, or JSON.
* `internal/status/` - These are the support routines or the `cmd_status.py` modules.

# Getting Started

## Commands

* `scraper run` - This runs a scraper. It takes as arguments
the named of the scraper to run (by default, its class name)
and any other arguments defined by the scraper.
* `scraper list` - This lists all of the available scrapers.
It supports various options for filtering scrapers as well.
* `scheduler status` - This lists the status of all defined
schedules. It also supports various filtering options
and a few different output formats.
* `scheduler update` - This updates the schedules
in the database from the schedules.yaml file.
* `scheduler schedule` - This takes a schedule name as an
argument and then marks that schedule to be executed as
soon as possibly, regardless of when it is currently
scheduled to be run.
* `scheduler serve` - This kicks off the scheduler in its "server"
mode. In this mode, it waits for a schedule to be ready to
run, attempts to reserve it, and then kicks off the
appropriate scraper using the `scraper run` command.

## Creating a scraper

In order to create a new scraper, define a new class somewhere in the
fn_scrapers.datatypes module (or a child module). It doesn't matter where -
put it in some place that makes sense. The class should look like:

```python
from fn_scrapers.api.scraper import scraper

@scraper()
class TestScraper(object):
    def scrape(self):
        print "I'm a scraper!"
```

This defines a new scraper named `TestScraper` that just prints
out a simple message when it is run (for example, by
`python -m fn_scrapers scraper run TestScraper`).

There are two key things to note:
1. The class is annotated with the `@scraper()` annotation. What
this tells FN-Scraper is that the class is a Scraper class and it
will be made available to run along with all of the other
scrapers.
2. It has a single method, `scrape()` - this is the "scrape"
method. It is invoked when the scraper is executed. When it
returns, the scraper has completed.

### Arguments

Of course, `TestScraper` doesn't do much of anything useful. One
thing that many scrapers need to do is to get some arguments
specifying what they should scrape, such as the time period
or session to scrape over. That can be done with the
`@argument()` annotation like the following:

```python
from fn_scrapers.api.scraper import scraper, argument

@scraper()
@argument("--message", help="The message to print out")
class TestScraper(object):
    def scrape(self, message):
        print message
```

The `@argument()` annotation defines command line arguments
and the parameters that it takes are identical to those
of `ArgumentParser.add_argument()` - so, anything that you can
do with that function, you can do with this annotation. You can
then get access to the supplied argument by naming a parameter
of the `scrape()` method the same as the command line argument
that you want to get access to, in this case `message`.

For more complicated scenarios, there is also a
`@arguments_from_function()` annotation  - instead of taking an
argument specification directly, this annotation expects a
function. That function will then be called with an
`ArgumentParser` instance and the function can do whatever
it wants to setup the arguments for the scraper. Argument created
this way can still be accessed in the same manner as before.

Try getting a list of all the available arguments:

```
python -m fn_scrapers scraper run TestScraper --help
```

Now, try kicking off the updated scraper:

```
python -m fn_scrapers scraper run TestScraper --message "Hi"
```

## Tags

Tags allow for categorizing a scraper so that it is easy to
refer to as part of a group. You can set a tag on a scraper
using the `@tags()` annotation, like:

```python
from fn_scrapers.api.scraper import scraper, argument, tags

@scraper()
@argument("--message", help="The message to print out")
@tags(group="example", type="bills", country_code="FR")
class TestScraper(object):
    def scrape(self, message):
        print message
```

This defines a scraper that is an example scraper, scrapes
bills, and those bills are scraped from France.

Tags don't modify the actual operation of a scraper, but, they
do make it easier to find scrapers using the `scraper list`
command. The scraper above, for example, could be found using
the command:

```
python -m fn_scrapers scrapers list --eval "country == 'France'"
```

(FN-Scrapers is able to convert the country code, "FR", into
the propper name for the country, France)

## Dependencies

So far, the scraper we've created don't do much of anything. Let's update
our scraper to take its input argument and publish it as a message.

One way to do that would be to create a RabbitMQ connection in the
`__init__()` function, save it, and then use it as necesssary in
the `scrape()` function to send messages, and then shut it down before
the scraper exits.

With FN-Service, the prefered way to accomplish this, however, is to
ask FN-Service for an instance of the `ScrapeItemPublisher` class, like
the following: 

```python
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
import injector

@scraper()
@argument("--message", help="The message to print out")
@tags(group="example", type="bills", country_code="FR")
class TestScraper(object):
    @injector.inject(scrape_item_publisher=ScrapeItemPublisher)
    def __init__(self, scrape_item_publisher):
        self.scrape_item_publisher = scrape_item_publisher

    def scrape(self, message):
        self.scrape_item_publisher.publish_json_item(
            "",  # The exchange - "" means default
            "france_bills",  # The routing key - ie "queue"
            "fr",  # source value - 'fr' for France
            {"bill_title": message}  # Data to publish
        )
```

The `@injector.inject()` annotation is the way that the scraper
declares to FN-Scrapers that it requires the functionality
provided by `ScrapeItemPublisher`. Then, when the `TestScraper`
class is instantiated, it will be provided with an instance of
that class that it can make use of while scraping.

One benefit of using `ScrapeItemPublisher`, is that it supports a few useful
options for controlling how, or if, messages are actually published to
RabbitMQ.

Try getting a updated list of all the available arguments:

```
python -m fn_scrapers scraper run TestScraper --help
```

You should see two new arguments pop up: `--dont-publish` and
`--save-local <FILE>`. The first of these allow the user telling the
`ScrapeItemPublisher` instance that it should not publish to RabbitMQ.
The second allows telling the instance that it should save any messages
sent via this class to the named file.

How did FN-Scrapers figure out that those options should be avilable when
they weren't previously available for our new scraper?
While trying to figure out the arguments for our scraper, `TestScraper`, it
noticed that it depended on the `ScrapeItemPublisher`. Then, it then looked at
the `ScrapeItemPublisher` class. Doing that, it noticed that that class was
annotated with the `@arguments_from_function()` annotation from
earlier. Knowing that, it called the specified function to create those
arguments when setting up the scraper.

`ScrapeItemPublisher` is just one of the resources available. Eventually all
the resources that are provided will be documented in the
AVAILABLE_RESOURCES.md file in this directory. However, until that is completed,
you can review the `fn_scrapers/api/resources.py` module to see most of the
provided resources. You can also review the existing scrapers to see more
complex usage scenarios, such as the international base class:
`fn_scrapers.datatypes.international.common.base_scraper.ScraperBase`. Additionaly,
any resource provided by FN-Service is also available for use with a scraper.

Try kicking off our improved scraper:

```
python -m fn_scrapers scraper run TestScraper --message "Hi" --dont-publish --save-local test.q
```

And then check out the result in the test.q file.

## Scheduling

The schedule that the scraper should be run on is defined in the file
schedules.yaml. A schedule for our scraper might looke like:

```yaml
- scraper_name: TestScraper
  scraper_args: ["--message", "Test Message!"]
  cron_schedule: "* * * * *"
  max_expected_duration: 2d # 2 days
  cooldown_duration: 4h # 4 Hours
  enabled: true
```

* `scraper_name` - Is the name of the scraper, which, is by default, the
  name of the class.
* `scraper_args` - These are the command line arguments to pass to the scraper
  as a list of strings.
* `cron_schedule` - This defines when the scraper wants to start, in cron format.
  If you set this to `* * * * *`, this means run every minute. For scrapers that
  can run at anytime, this is probably what you want; if the scraper should only
  run at certain times, this setting can be used to configure that.
* `max_expected_duration`: The maximum amount of time that the scraper is expected to
  run for. After this time, an error will be reported. The scraper will continue
  to run until it completes or is forcefully stopped, however.
* `cooldown_duration`: This is the amount of time that should elapse between when
  a scraper completes and when it should run again.

So, in short: The scraper, `TestScraper`, will be run every 4 hours, with the
command line arguments `"--message 'Test Message!'"` and an error will be
reported if it ever takes more than 2 days to run.

Once defined, the schedule can be updated in the database using the
`scheduler update` command.
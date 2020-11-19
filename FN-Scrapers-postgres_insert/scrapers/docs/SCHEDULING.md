# Deadline Greedy Scheduling Proposal

## Terms:

**scraper** - The scraper is the actual code that runs to scrape a website.

**schedule** - A schedule is the configuration that we assign to a scraper
that determines when the scraper should be run.

**scheduler** - The scheduler is the component that examines the list of
schedules and then picks one to run next.

## High Level Ideas

There are two mains parts to this design:

1. For every schedule, we define constraints for how the schedule should
be run. Given those constraints, at any time, it is possible to ask what
the condition of the schedule is: `OK`, `WARNING`, or `ERROR`. Additionally,
for an `OK` schedule, we know that if it is not run by a certain time, it
will enter the `ERROR` state - we use the defined constraints to figure out
when that time is.

2. When the scheduler needs to pick a schedule to run, it examines all of
the available schedules, looking at the one that will enter an `ERROR`
state soonest if it is not run. It then picks that schedule and runs is.

## Goals

1. Provide a mechanism to determine if all the scrapers in the system are
running as they should.

2. Define a scheduler that can do a reasonable job of picking schedules
to run, even as different schedules have different constraints. A basic
assumption here is that we have many more scrapers in our system than
we want to run at once so we are often in a situation where we must pick
from multiple candidates to run next.

3. Minimize manual configuration as much as possible.

4. Take basic steps to recover from errors (ie: basic retrying).

5. If a scraper does enter an `ERROR` state or a number of scrapers
fall behind their schedule, the system should recover gracefuly and quickly
once the problem is fixed or more capacity is added.

6. If a server fails, the system should recover quickly as well, without
human interaction required.

7. Never run more than a single copy of a particular schedule at a time.

This design proposal should not be looked at as the end goal, but a proposal
for a starting point. It includes notes for some additional features that we
probably want to look at in the future, but are explicitly not included in
this proposal.

## Constraints: All About Start Time

Let's say that we ran a scraper at 1pm and it finished at 2pm. Later, a user
comes to us and asks if a bill posted at 1:30pm is on our system. Given what
we know, we can't answer. However, lets say that same user asked us about a
bill that was posted at 12:30pm - in that case, we can say that the bill
should be in our system.

**The point**: Once a schedule finishes, we know that everything that happened
before the schedule _started_ running must have been processed while the
schedule was running. Things that happened while the schedule was in the
middle of running, may or may not have been processed. However, those things
definitally will be processed when the schedule is run again.

So, what this mean is that the main metric that we care about when designing
constraints that define a schedules operation is: When was the last time that
the scraper started and subsequently successfully completed.

## Schedule Types

I propose that we have two different types of schedules, which I'll call
**periodic** and **cron**.

**periodic**: These are schedules were we define how stale its ok for
the data to be, but not a specific time that the schedule should be run. The
staleness value is determined as the time ellapsed since the schedule last
started and subsequently completed. We call this value, `scheduling_period`.

For example, if we have a schedule defined with a `scheduling_period` of 24
hours, it means that the scheduler should make sure to run that schedule
frequently enough that at any given time you can look back less than 24 hours
and find an instance of the scraper starting and completing.

**cron**: These schedules have a cron-style schedule associated
with them. For example, `0 0 * * *` would specify that the scraper should run
at midnight. Since the time that the schedule starts doesn't mean much until
it completes, we define a parameter, `cron_max_schedule_duration`. This
parameter speifies how long after a scheduled start time that the schedule
must complete.

For example, if we have a schedule defined as `0 0 * * *` with a
`cron_max_schedule_duration` of 6 hours, what we're telling the scheduler is
that it should attempt to start the schedule at midnight. If it can't, thats
fine - but, it must start the schedule in time for it to complete by 6am. If
by 6am it still hasn't finished, then, the schedule is in an `ERROR` state.

# Details

## Parameters and stats

A schedule record in the database stores a variety of different values. Among
those are record indicating parameters of the schedule and those recording
the past history of the schedule.

`last_good_start_at` & `last_good_end_at` - These record the last time that
the scraper started and finished without failing. These may be `NULL` if the
scraper has never succeeded.

`last_start_at` & `last_end_at` - These record the last time that the scraper
started and finished either successfully or with an error. These may be
`NULL` if the scraper has never run.

`failure_count` - This records the number of consecutive failures of the
scraper. Once the scraper succeeds, this is set back to 0. This is `NULL` if
the scraper has never run.

`average_good_duration` - This records the average runtime of the scraper
using an [Exponential Weighted Moving
Average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average).
Basically, whenever the scraper completes without an error. We calculate how
long it took. Then, we combine it with the previous average with:

`average_good_duration = α * last_duration + (1 - α) * average_good_duration`

We need to pick a value for the term α - the proposal is to pick the term
such that 75% of the weight of the average is equal to the last 3 runtimes:
approximately 0.37.

This value may be `NULL` if the scraper has never completed without an error.

`max_expected_duration` - The maximum amount of time that the scraper is
expected to run. If it runs longer than this time, an error is logged.
The failure count is not incremented, however.

`max_allowed_duration` - The maximum amount of time that the scraper is
allowed to run for. If it runs longer than this, the scraper is terminated
and an error is logged. The failure count is also incremented.

`blackout_periods` - A JSON array of time values that indicate when a scraper
is explicitly not allowed to run during. If a scraper is found to be running
during this time period, an error is logged and the scraper is terminated.
The failure count is also incremented. This may be `NULL`.

`owner_started_at` - If the scraper is running, this is set to the time that
it started at. It is `NULL` if the scraper is not running.

`owner_node`, `owner_name`, & `owner_tag` - If a scraper is running, these
are equal to the name of the server that is running the scraper, a particular
instance name on that server, a UUID generated for the particular run. These
are `NULL` if the scraper is not running.

`owner_last_ping_at` - Every 30 seconds, while the scraper is running, the
scheduler will update this value with the current time. If its beeing more
than 30 seconds since this field was updated, other schedulers may determine
that the scheduler that was running the scraper has failed. This is `NULL` if
the scraper is not running.

`steal_start_at` - If a scheduler fails to update `owner_last_ping_at` for
more than 30 seconds, other schedulers will consider the scraper to have
failed. They set this value to the current time to indicate that they want to
"steal" the work. If this value remains non-`NULL` for long enough after that,
the work is "stolen" and can be run by another scheduler. This is `NULL` if
the scraper is not running or the scraper is running normally.

`dont_run_on_nodes` - The list of servers that the particular schedule must
not run on - this is useful if a particular server has been blocked.

`tz` - The timezone to use when looking at the blackout periods and
`cron_schedule` values. Must be a valid pytz timezone.

### Periodic Schedule parameters

`scheduling_period` - This contains time period during which a scraper must
run completely. For example, this may be 24 hours to indicate that during any
24 hour period, the scraper should both start and complete at least once.

`cooldown_duration` - This indicates the amount of time after the last
scraper run that we should wait before we start the scraper again.

### Cron Schedule Parameters

`cron_schedule` - This contains a cron-style specification for when the
scraper should run. "0 0 * * *" means to run at midnight, for example.

`cron_max_schedule_duration` - This indicates the maximum amount of time that
a schedule is allowed to run for after its scheduled start time.

## Schedule condition algorithm

The function that determines the condition of a particular schedule is
called `get_schedule_condition()`. It returns `OK`, `WARNING`, or `ERROR`.

`OK` - This means that everything is fine.

`WARNING` - This means that something has gone wrong, but that the
constraints defined on the schedule have not yet been violated.

`ERROR` - This means that we've violated the constraints - the result may
be that we're serving stale data to our clients.

The function works by:

1. If we determine that the schedule constraints are currently being
violated, we return `ERROR`.

2. If the schedule is currently running, but, based on its average 
runtime it is likely to violate the constraints before it completes,
we return `WARNING`. Likewise, if the schedule is not currently running,
but, even if we started it immediately, it is likely to violate its
constraints before it complete, we return `WARNING`.

3. If the schedule failed the last time it ran, we return `WARNING`.

4. Otherwise, we return `OK`.

Determining if the constraints are, or are likely, to be violated
works slightly differently depending on the type of schedule. The basic
algorithms are the same - we just use different values to calculate them.

For periodic style schedules:

The schedule has violated its constraints if:

`NOW - last_good_start_at > scheduling_period`

If the schedule _is_ currently running, it is likely to violate its constraints
before it completes if:

`owner_start_at + average_good_duration >
last_good_start_at - scheduling_period`.

If the schedule is _not_ currently running, it is likely to violate its
constraints before it completes if:

`NOW + average_good_duration > last_good_start_at + scheduling_period`.

For cron style schdules:

The schedule has violated its constraints if:

After the last run, we are supposed to run again at some time. If that
run either isn't running or is running but hasn't yet completed:

`NOW > next_cron_time(last_good_start_at) + cron_max_schedule_duration`

NOTE: `next_cron_time()` is a function that, given a time, returns the
next cron scheduled time. So, if the cron schedule is `0 */4 * * *` and
`last_good_start_at` is `"03:00:00"`, it will return `"04:00:00"` of that
day.

If the schedule _is_ currently running, it is likely to violate its constraints
before it completes if:

`owner_start_at + average_good_duration >
next_cron_time(last_good_start_at) + cron_max_schedule_duration`

If the schedule is _not_ currently running, it is likely to violate its
constraints before it completes if:

`NOW + average_good_duration >
next_cron_time(last_good_start_at) + cron_max_schedule_duration`

## Scheduling algorithm

A main element of the scheduling algorithm is a function named
`get_schedule_start_times()`. This function, takes two parameters, the
current time and a schedule, and produces 2 output values:

1. `can_start_by` - The time that the schedule is next elligable to be run.

2. `should_start_by` - The time that the schedule should start running by so
that it is expected to finish in time to avoid violating its constraints.

The scheduling algorithm is:

1. Fetch a list of all schedules from the DB.

2. Eliminate all schedules that are currently running or that are defined not
to run on the scheduler's node.

3. If not schedules remain, sleep for 6 seconds and then start over.

3. Of the schedules that remain, calculate `can_start_by` and `should_start_by`
using `get_schedule_start_times()`.

4. Pick the schedule with the lowest `should_start_by` value that also has a
`can_start_by` value in the past and start running it. If no schedules have a
`can_start_by` in the past, sleep until the time until the lowest
`can_start_by` value or 60 seconds, whichever is less, and then start over.

The gist of the algorithm is - look at all the schedules that are ready to be
run, and then run the one that is the nearest to generating an `ERROR` if it
isn't run.

## get_schedule_start_times()

The `get_schedule_start_times()` algorithm is defined slightly differently
for period and cron style schedules.

For periodic style schedules:

1. `can_start_by`: `last_good_end_at + cooldown_duration`.

2. `should_start_by`: `last_good_start_at + scheduling_period -
average_good_duration`.

For cron style schedules

1. `can_start_by`: `next_cron_time(last_good_start_at)`.

2. `should_start_by`: `can_start_by + cron_max_schedule_duration -
average_good_duration`

## Algorithm To Ensure Only 1 Instance is Running at a Time

After a scheduler decides to run a particular scraper, it will ping the
database at regular intervals to let other scrapers know that it is still
running, updating `owner_last_ping_at` each time. If it goes long enough
without pinging the database in this way, other schedulers will attempt to
"steal" the work. They do this by setting the `steal_start_at` field to the
current timestamp. Once this occurs, the originally scheduler is given a
period of additional time to ping the database again to indicate that it is
still running. If it does, it will clear the `steal_start_at` value. The
purpose of this value is to handle cases where the database becomes
unavailable - we want to avoid a case where the database fails and all
schedulers think that all other schedulers have failed and everyone steals
everyone else's work. This field ensure that after a database failure, every
running scheduler gets a period of time to ping the database again to
indicate that they are still alive.

If the period of time after `steal_start_at` ellapses and the field has not
been cleared, then other schedulers will consider the scraper to be up for
grabs. If a scheduler decides to run the scraper, it will reset the `owner_*`
fields and start running it.

Whenever a scheduler pings the database, it will check the `owner_tag` value.
If this value is not equal to what it set when it began running the scraper,
it will assume that the schedule was "stolen" and terminate it.

## Retrying

Retrying is used to allow the scraper to recover from transient errors
without human intervention. The idea is that if a scraper has failed, it will
be retried automatically - however, it will not be retried so aggressively as
to negatively impact other, non-failing scrapers.

The idea is to apply a simple backoff to the `can_start_by` time. To that
end, if `failure_count` is greater than 0, after `can_start_by` is
calculated, we then calculate `retry_at` depending on the value of
`failure_count`:

* 1 - `retry_at = last_end_time + 5 minutes`

* 2 - `retry_at = last_end_time + 1 hour`

* 3+ - `retry_at = last_end_time + 4 hours`

And then modify `can_start_by` as follows:

* `can_start_by` = `max(can_start_by, retry_at)`

## Blackout Periods

Some scrapers must not run during certain times. These times are called
blackout periods. If any scraper is found to be running during its blackout
period, it is immediately terminated and an error is logged. The blackout
periods are specified as a list of start/end time pairs - such as `[{"start":
"00:00:00", "end": "01:00:00"}, {"start": ["12:00:00", "end": "14:00:00"}]`
which indicates that this scraper must not run between midnight and 1am or
noon and 2pm.

When `get_schedule_start_times()` is called - it will take the blackout
periods into account. `can_start_by` falls into the middle of a blackout
period, it will be moved to the end of that blackout period.

`should_start_by` is NOT adjusted. Even if there was a blackout period
during that time, its still when the scraper should have started. Leaving
it unadjusted will result in the schedule being moved to a top priority
spot once the blackout period ends.

## Don't Run on Nodes

Some scrapers may not be allowed to run on certain servers. Those servers are
specified as `don_run_on_nodes`. This is just a list of server names that
shouldn't run the given scraper. When making scheduling decisions, a
scheduler on that server will never choose to run that scraper.

## Greedy

A key part of this design is that it is "greedy" in that it only takes into
account the current state of the schedules when making a decision. In an
ideal world, the scheduler would look ahead a little bit to make a decision.
However, its not clear how to design such an algorithm that isn't
tremendously complex, both code-wise and in terms of big-O. Practically, what
this means is:

1. The scheduler can't decide to _not_ run a scraper that is elligable in
order to reserve capacity to one that will soon become elligable. For
example, lets say we have a scraper that must run once a week and that runs
for 12 hours that is ready to run after having completed a run less than a
day ago. And, we also have a scraper that must run once an hour and only for
1 minute. And, also lets assume that we only have a single unit of capacity
that is likely to be available for the next hour because all of the other
schedulers are busy running scrapers that won't complete for a while. In this
case, the optimal decision would be to not run the long running scraper until
more capacity is available and to instead prioritize the short running
scraper. However, actually detecting these cases is very, very hard since it
requires schedulers to try to reason about what other schedulers are doing.

2. The scheduler only looks at the current node. If there are two schedulers
that are available to run scrapers, but one of those schedulers is on a
server that isn't allowed to run one of those scapers, in an ideal world, the
scheduler that can run either scraper would smartly pick to run the scraper
that would then allow the other scheduler to run the other one. But, actually
implementing this logic seems pretty tricky.

## Future directions

1. Not all scrapers require the same amount of resources - for example, a
scraper that requires headless Chrome is much heavier than one that doesn't.
It probably makes sense to consider the idea of creating "pools" of
schedulers dedicated to particular sets of scrapers that are either more
resource intense or higher priority than others. But, its not clear that we
want to do that right now.

2. This proposal doesn't include a mechanism to handle errors that occur to
only a subset of scraped items - just scrapers that fail outright. It likely
makes sense to build a mechanism to respond to a scraper that fails to scrape
some percentage of its items. But, its not quite clear what that mechanism
would look like right now.

3. `average_good_duration` is calculated as a moving average of scraper
times. As a result, there is a chance that a few slow runs skew this average
excessively. It may make sense to add an override for this value so that we
can set better values for scrapers most likely to have this problem. But, its
not clear what the best way to do that is.

4. Currently, its only possible to define a single schedule for a scraper. It
probably makes sense to add the ability to define multiple schedules for a
single scraper at some point in the future. The proposed mechanism to do this
is to define a `configuration` parameter on the schedule. So, for the
schedule named "ExampleScraper", there could be a configuration named "" -
the default configuration - and another named "test_configuration". The
schedules would then be refered to as "ExampleScraper" and
"ExampleScraper@test_configuration".

5. It would be handy if `get_schedule_condition()` also returned a reason
code. But, that can wait until later to be implemented.

6. Ideally, the scheduler wouldn't start a scraper if it was likely to fail
to complete before running into a blackout period. Its not clear that it
makes sense to implement this logic right now since it can be complex. For
example, we would need a way to handle the case where our estimate of a
scrapers runtime was way off and that such an estimate resulted in us
concluding that a given scraper could never be run - ie, the estimate for the
scraper runtime is 24 hours. Again, this seems too complicated to try to
implement now. The result is that the blackout periods are really only useful
for cron style schedules.

7. It probably makese sense to tweak α once we have a better idea of
how well it is performing.

8. It _may_ make sense to store an estimate of the standard deviation
of the average_duration value. However, before we do that, we should validate
that the scraper duration are actually normally distributed.

9. When retrying, it may make sense to take into account how long that last
schedule that failed took. If the last schedule failed after 5 seconds, it
probably doesn't hurt to retry it immediately. However, if the last scrape
failed after 12 hours, an immediate retry may have a larger impact on the
system. Striking the right balance here is hard, however, and probably makes
sense to consider later.

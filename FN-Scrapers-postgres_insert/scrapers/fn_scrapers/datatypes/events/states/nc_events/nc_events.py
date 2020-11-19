from __future__ import absolute_import

import re
import logging
from datetime import datetime, time
import pytz

from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.http import request_html5
from fn_scrapers.common.xpath_shortcuts import one, one_or_none


logger = logging.getLogger(__name__)


base_url = "https://www.ncleg.net/LegislativeCalendar/"


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NC")
class NCEventScraper(EventScraper):
    jurisdiction = 'nc'

    def __init__(self, *args, **kwargs):
        super(NCEventScraper, self).__init__('nc', __name__, **kwargs)

    def scrape(self):
        page = request_html5(base_url)

        # All of the events are in a table in the middle of the page. Within that table,
        # rows either contain a date - in which case they have a single cell that spans all
        # three columns OR they have details of a hearing - in which case there are 3 cells.
        # When there is a date, the date applies to all following events, until there is another
        # date. So, we scan the rows of the table - if its a date, we save it; if its an event,
        # we scrape that event with the last date that we saw.

        event_table = one("//table//div[preceding-sibling::div[@id='title']]//table", page)
        event_rows = event_table.xpath("tbody/tr")

        event_date = None

        for event_row in event_rows:
            cells = event_row.xpath("td")
            if len(cells) == 1:
                event_date = datetime.strptime(u"".join(cells[0].itertext()).strip(), "%a, %B %d, %Y").date()
            else:
                if not event_date:
                    # If we get to a row that we think is an event row, but, we haven't seen a date
                    # row yet, the page may have changed its formatting. We bail out because we can't
                    # be sure where the date is anymore - if we continued, we'd likely fail, but,
                    # if we didn't fail, we can't have confidence that we'll scrape good data.
                    raise Exception("No date found for events - the page has likely changed. Cannot scrape.")
                self.scrape_event_row(event_date, event_row)

        self.save_events_calendar()

    def scrape_event_row(self, event_date, event_row):
        # There are 3 cells - the first has the time of the event (which may not be a valid
        # time - eg "15 MINTUES AFTER SESSION"); the second has details on the event - the
        # committee, the chamber, and related bills; the third has location information.
        time_cell, event_cell, location_cell = event_row.xpath("td")

        # The time string is in the format: "08:01 a.m." or "1:15 p.m.". However,
        # strptime's %p doesn't like the periods - so, use this regex to remove
        # them. (We make the periods optional in the regex - if they go away, we
        # can still process the time just find; also, we set IGNORECASE because
        # times with all caps AM and PM have been seen along with lower case am and pm)
        time_string = re.sub(u"\s+", u" ", u"".join(time_cell.itertext()).strip())
        m = re.match(r"^(\d+):(\d+)\s+([ap])\.?m\.?$", time_string, re.UNICODE | re.IGNORECASE)
        if m:
            try:
                event_time = datetime.strptime(
                    "{}:{} {}M".format(m.group(1), m.group(2), m.group(3)),
                    "%I:%M %p").time()
            except ValueError:
                event_time = None
        else:
            event_time = None

        # in the cell that contains details of the event, there will be 1 or 2 divs.
        # The first one contains the name of the sponsoring entity (ie, a committee)
        # and the chamber; if the 2nd div exists, it contains related bills.
        committee_name_div = one("div[1]", event_cell)
        event_bills_div = one_or_none("div[2]", event_cell)

        # The committee name is in the first div along with the chamber.
        # We don't want the chamber, however. Helpfully, the chamber is
        # inside of a span element - so, we just select all text not in a span
        # element and join it. The name may also have the string "-- UPDATED" at
        # the end - in that case, remove it.
        m = re.search(
            r"^\s*(.+?)(?:\s*--\s*(UPDATED|CANCELLED|Press Conference))?\s*$",
            u"".join(committee_name_div.xpath(".//text()[not(ancestor::span)]")),
            re.UNICODE)
        if not m:
            logger.critical("Couldn't find committee", event_type="no_committee")
            return
        if m.group(2) in ("CANCELLED", "Press Conference"):
            # Skip cancelled events and press conferences
            return
        committee_name = m.group(1)

        # If there is a span element, it should contain the name of the chamber
        senate_or_house_span = one_or_none("span", committee_name_div)
        if senate_or_house_span is not None:
            senate_or_house_text = u"".join(senate_or_house_span.itertext()).strip()
            if senate_or_house_text == "(Senate)":
                chamber = "upper"
            elif senate_or_house_text == "(House)":
                chamber = "lower"
            else:
                logger.critical("Found unexpected chamber: " + senate_or_house_text, event_type="no_chamber")
                chamber = None
        else:
            chamber = None

        # If we have a 2nd div in the event details cell, it contains a list of
        # related bills. Helpfully, these ids are all inside of a elements. So,
        # we can just extract those.
        if event_bills_div is not None:
            related_bills = []
            for related_bill_link in event_bills_div.xpath("a"):
                related_bill_id = u"".join(related_bill_link.itertext()).strip()
                related_bills.append(related_bill_id)
        else:
            related_bills = []

        # The location is in the final cell - we just get its text
        location = u"".join(location_cell.itertext()).strip()

        # There may or may not be a time - regardless, we need to construct
        # a datetime value to send with the event. This code is a little werid,
        # but, its what downstream expects.
        if event_time is not None:
            formed_date = datetime.combine(event_date, event_time)
            # timezone handling for events is a bit odd - they must be sent over
            # in the UTC timezone
            formed_date = self._tz.localize(formed_date).astimezone(pytz.UTC)
        else:
            formed_date = datetime.combine(event_date, time())

        event = Event(
            formed_date,
            "%s Hearing" % committee_name,
            location,
            'committee_hearing',
            start_has_time=bool(event_time))
        if chamber is not None:
            event["chamber"] = chamber

        event.add_source(base_url)

        event.add_participant('host', committee_name, chamber=chamber)

        for bill_id in related_bills:
            event.add_related_bill(bill_id)

        self.save_event(event)

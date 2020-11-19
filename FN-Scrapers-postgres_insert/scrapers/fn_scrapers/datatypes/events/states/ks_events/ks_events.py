from __future__ import absolute_import
import re
import datetime
import pytz
from dateutil.parser import parse
from collections import defaultdict
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

senate_url = "http://kslegislature.org/li/events/senate/current/"
house_url = "http://kslegislature.org/li/events/house/current/"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-KS")
class KSEventScraper(EventScraper):
    jurisdiction = 'ks'

    def __init__(self, *args, **kwargs):
        super(KSEventScraper, self).__init__('ks', __name__, **kwargs)

    def get_today_as_timezone(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz).date()

    def scrape(self):
        meeting_dict = defaultdict(list)
        for chamber in ["upper", "lower"]:
            seen_descriptions = {}
            happenings_url = senate_url if chamber == "upper" else house_url
            page = self.lxmlize(happenings_url)
            session_day = page.xpath("//h3[contains(text(),'Session Day - ')]/text()")
            if not session_day:
                session_day = page.xpath("//select[@name='days']/option[1]/@value")
                if not session_day:
                    self.error("Cannot scrape events. Unknown session day at {}".format(happenings_url))
                    return
                else:
                    session_day = int(session_day[0]) - 1
            else:
                session_day = int(session_day[0].replace('Session Day - ', ''))

            for i in range(1, session_day+1):
                day_url = happenings_url + "?days={}".format(i)
                try:
                    day_page = self.lxmlize(day_url)
                except:
                    self.warning("Missing page at {}".format(day_url))
                    continue

                hearings = day_page.xpath("//li[@class='module-item special-event' and ./p[contains(text(), 'Hearing')]]")
                for hearing in hearings:
                    description = hearing.xpath("./p[@class='module-title']/text()")[0].strip()
                    if "canceled" in description.lower() or 'cancelled' in description.lower():
                        continue
                    event_fields = re.match(r"Hearing:?\s*(.+?)(Room \S+)", description)
                    if not event_fields:
                        continue
                    when = parse(event_fields.group(1), fuzzy=True)
                    when = self._tz.localize(when)
                    if when.date() < self.get_today_as_timezone():
                        continue
                    where = event_fields.group(2)
                    event = Event(when, description, where, 'committee_markup', start_has_time=True, chamber=chamber)
                    event.add_source(day_url)
                    # Add related bills
                    related_bills = hearing.xpath("./p[contains(text(), 'Links')]/a")
                    for related_bill in related_bills:
                        bill_id = related_bill.text
                        event.add_related_bill(bill_id, 'consideration')
                        description = bill_id + ' ' + description
                    event['description'] = description

                    # Kind of hacky, but gets rid of duplicates, which are definitely a mistake on KS's part
                    if description not in seen_descriptions:
                        seen_descriptions[description] = day_url
                        if (description, when, where) not in meeting_dict:
                            meeting_dict[(description, when, where)] = event
                            self.save_event(event)
                    else:
                        self.warning("Duplicate events found: '{}' on '{}' and '{}'".
                                     format(description, day_url, seen_descriptions[description]))

        self.save_events_calendar()

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

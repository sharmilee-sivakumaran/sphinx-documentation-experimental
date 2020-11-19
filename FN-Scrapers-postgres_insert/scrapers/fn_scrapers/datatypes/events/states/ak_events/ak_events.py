from __future__ import absolute_import
import re
import pytz
import datetime
import lxml.html
from itertools import islice
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-AK")
class AKEventScraper(EventScraper):
    jurisdiction = 'ak'

    """
    Scraper workflow

    1. The scraper goes to the URL:
       http://www.akleg.gov/basis/Meeting/Index?mode=results&type=All&startDate=2/21/2018&endDate=04/22/2018&chamber=H

       to scrape House level events. We only look for events 2 month ahead from the current date
    2. We are only interested in the table element in that page.
    3. The events are scattered in the table element in rows, i.e, each events commitee, location
       related bills etc information is separated into sibling tr elements
    4. Each event is grouped between the following element:
        //td[@colspan="8"]/parent::tr
    5. So if the indices of two separators are 0 and 5, then the indices of trs we're interested in are 1-4
    6. We extract the committee, date time, location and related bills from the rows
       iteratively and save each event.
    7. Finally save the events calendar
    """

    def __init__(self, *args, **kwargs):
        super(AKEventScraper, self).__init__('ak', __name__, **kwargs)

    chamber_dict = {
        'H': 'lower',
        'S': 'upper'
    }

    def scrape(self):
        for chamber in self.chamber_dict:
            self.scrape_chamber_event(chamber)
        self.save_events_calendar()

    def scrape_chamber_event(self, chamber):
        url = "http://www.akleg.gov/basis/Meeting/Index?mode=results&type=All&" +\
              "startDate={}&endDate={}&chamber={}"
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        today = today.astimezone(self._tz)
        months = datetime.timedelta(days=60)
        future = today + months

        page = lxml.html.fromstring(self.get(url.format(
                today.strftime('%m/%d/%Y'),
                future.strftime('%m/%d/%Y'),
                chamber)
            ).text)

        table = page.find('table')
        trs = iter(map(table.index, table.xpath('//td[@colspan="8"]/parent::tr')))
        first_ind = next(trs, None)
        next_ind = next(trs, None)
        table_len = len(table)

        date_time = None
        location = None
        committee = None
        com_chamber = None
        canceled = False
        event = None
        for i, tr in enumerate(table.iterchildren('tr')):
            """
            The if conditions below ensure that we only consider the tr elements
            that are between either between two separator tr elements or the after the last
            separator element.

            The first condition skips all trs that occure before the known separator index
            The website structure has atleast one separator

            The second condition gets all the event tr elements because next_ind can be None
            if the page only contains one event

            If the page has one event and no separator then the loop will go through all the
            trs and get all the trs and tries to find the event information
            """
            if first_ind is not None and i < first_ind:
                continue
            if (first_ind or 0) < i < (next_ind or table_len):
                tds = tr.xpath('td')
                row_text = tr.text_content().strip()
                if i == first_ind + 1:
                    # Committee info
                    if not re.search(r'other', tds[1].text_content(), flags=re.I):
                        committee = tds[0].text_content().strip()
                        committee = re.sub(r'\((H|S)\)', '', committee)
                        if re.search(r'joint', tds[1].text_content(), flags=re.I):
                            com_chamber = 'joint'
                        else:
                            com_chamber = self.chamber_dict[chamber]
                        if re.search(r'subcommittee|special', tds[1].text_content(), flags=re.I):
                            committee += ' ' + tds[1].text_content().strip().upper()
                            committee = committee.strip('*')
                    else:
                        canceled = True
                elif i == first_ind + 2:
                    # Date time and location
                    if not canceled:
                        # No event has been encountered yet that doesn't have a valid time attached to it
                        date_time = parse(tds[0].text_content().strip())
                        if date_time.month < today.month:
                            date_time.year += 1
                        date_time = self._tz.localize(date_time)
                        location = tds[1].text_content().strip()
                        event = Event(date_time, "%s HEARING" % committee, location,
                                      'committee_hearing', start_has_time=True,
                                      chamber=self.chamber_dict[chamber])
                        event.add_source(url.format(
                                today.strftime('%m/%d/%Y'),
                                future.strftime('%m/%d/%Y'),
                                chamber
                            ))
                        event.add_participant('host', committee, chamber=com_chamber)
                else:
                    # Related bills if available
                    if not canceled:
                        if re.search(r'meeting\s+?cancel{1,2}ed', row_text, flags=re.I | re.U):
                            canceled = True
                            event = None
                        try:
                            bill = re.search(r'[SH](?:C|J)?[BR]\s+\d+', row_text).group(0)
                            event.add_related_bill(bill)
                        except AttributeError as e:
                            pass
                        if re.search(r'Bill\s*?Hearing\s*?Cancel{1,2}ed', row_text, flags=re.I | re.U):
                            del event['related_bills'][-1]

            if next_ind and i == next_ind:
                # Save event
                first_ind = next_ind
                next_ind = next(trs, None)
                if not canceled:
                    self.save_event(event)
                    event = None
                else:
                    canceled = False

        if event:
            self.save_event(event)

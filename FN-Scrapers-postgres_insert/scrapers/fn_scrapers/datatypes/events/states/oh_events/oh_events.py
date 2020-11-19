from __future__ import absolute_import
import re
import pytz
import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.extraction.textpdf import get_pdf_text

chamber_event_data = {
    'lower': {
        'url': "http://www.ohiohouse.gov/Assets/CommitteeSchedule/calendar.pdf",
        'date_ptrn': (r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
                      r"(\w+) (\d+), (\d+)"),
        'date_ptrn_2': (r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
                        r"(\w+) (\d+)(?:th|rd|nd|st), (\d+)"),

        # Time and/or 'following session/committee', location, Chair: name
        # Regex carefully crafted to account for varying time formats, and to
        # discard text related to the time, while accounting for complex
        # location strings.
        'meeting_ptrn': r"((\d{1,2}:\d{2}) ?([ap]\.?m\.?)?)?,?(?: .*following .*"\
        r"(?:session|committee))?, (.+), Chairs?:.*$",

        # Bill ID is followed by the sponsor names (may have an initial),
        # and often a description of the event (e.g. 2nd Hearing, ...).
        'bill_ptrn': r"^([HS]\.?[JCBRM]\.?[JCBRM]?\.? \d+) [A-Za-z/-]+(, [A-Za-z]\.)?(, (.+))?$",

        # Extended bill descriptions will be indented (leading whitespace), and
        # have more words than just a page number.  Admit blank lines, too.
        'desc_ptrn': r"^(\s+\S*[A-Za-z]+\S*|$)"
    },
    'upper': {
        'url': "http://www.ohiosenate.gov/",
        'date_ptrn': (r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
                      "(\w+) (\d+)"),
        'date_ptrn_2': (r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
                        r"(\w+) (\d+)(?:th|rd|nd|st)"),

        # Committee, time, location
        'meeting_ptrn': r"(.+), ((\d{1,2}:\d{2}) ?([AP]\.?M\.?)?)?,?(?: .*following .*(?:session|committee))?, (.+)$",

        # Bill ID is followed by the sponsor names (may have an initial),
        # and often a description of the event (e.g. 2nd Hearing, ...).
        'bill_ptrn': r"^([HS]\.?[JCBRM]\.?[JCBRM]?\.? \d+) [A-Za-z/-]+(, [A-Za-z]\.)?(, (.+))?$",

        # Bill descriptions have leading whitespace and are not appointments.
        'desc_ptrn': r"^\s+\S*[A-Za-z]+\S* (?!Appointments:)"
    }
}

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-OH")
class OHEventScraper(EventScraper):
    jurisdiction = 'oh'

    def __init__(self, *args, **kwargs):
        super(OHEventScraper, self).__init__('oh', __name__, **kwargs)

    def get_today_as_timezone(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz)

    def _create_meeting_info(self, chamber, committee, date, time_raw, mrdm, location, event_list):
        """
        Use the passed properties to format and save an event with source and participant.
        """

        # If no time (e.g. 'Immediately following session'), use 12:00am
        time = None
        if time_raw:  # meeting_match.group(2):
            has_time = True
            time_raw = time_raw.strip()
        else:
            time_raw = '12:00'
            has_time = False

        # If no meridiem for some reason, assume 'am'
        if mrdm:  # meeting_match.group(3):
            # strptime wants meridiem to have no periods
            mrdm = mrdm.strip().replace('.', '')
        else:
            mrdm = 'am'

        # Form the full time
        full_time = datetime.datetime.strptime("%s %s" % (time_raw, mrdm),
                                               "%I:%M %p").time()
        if date:  # Hopefully this is always true at this point
            # Form the full date + time for the meeting
            meeting_datetime = datetime.datetime.combine(date, full_time)
            meeting_datetime = self._tz.localize(meeting_datetime)
            location = location.strip()
            if meeting_datetime.date() < self.get_today_as_timezone().date():
                return None

            event_key = ("Committee Hearing: {}".format(committee), meeting_datetime, location)
            if event_key in event_list:
                event = event_list[event_key]
            else:
                # Save this committee meeting's information
                event = Event(meeting_datetime, "Committee Hearing: {}".format(committee),
                              location, 'committee_markup', start_has_time=has_time, chamber=chamber)

                event.add_source(chamber_event_data[chamber]['url'])
                event.add_participant('host', committee, chamber=chamber)
            return event
        else:
            self.warning(("No date found for %s committee event "
                          "(at %s), skipping") % (committee, full_time))
            return None

    def _scrape_long_bill_desc(self, lines, line_idx, bill_ptrn, desc_ptrn):
        """
        Build up an event/bill description by reading in consecutive lines until the next bill or higher level
        (committee, date, page end, etc) is reached.  Return the complete description and the line number of the
        following line.
        """
        event_desc = ''
        line_idx += 1
        event_line = lines[line_idx].strip()
        # Stop when we get to the next bill or other text that doesn't seem to be the current description.
        while not bill_ptrn.search(event_line) and desc_ptrn.search(lines[line_idx]):
            event_desc += event_line + (' ' if event_line else '')
            line_idx += 1
            event_line = lines[line_idx].strip()
        # Back up in case the reached line is a new bill.
        line_idx -= 1
        return (event_desc.strip(), line_idx)

    def scrape_committee_hearings(self, chamber):
        """
        Scrape this week's committee hearing events for senate or house chamber, from the corresponding pdf.
        """
        # Make sure this is the latest session, since we can only get data
        # for the current _week_

        page_url = chamber_event_data[chamber]['url']
        if chamber == 'upper':
            senate_page = self.get(page_url).lxml()
            schedule_a = senate_page.cssselect('div.todayInTheSenateCommitteeSchedule a')
            if schedule_a:
                page_url = schedule_a[0].get('href')
            else:
                self.error("Unable to get Senate schedule PDF")
        data = get_pdf_text(page_url)
        date = event = None
        date_ptrn = re.compile(chamber_event_data[chamber]['date_ptrn'], re.I)
        date_ptrn_2 = re.compile(chamber_event_data[chamber]['date_ptrn_2'], re.I)
        meeting_ptrn = re.compile(chamber_event_data[chamber]['meeting_ptrn'], re.I)
        bill_ptrn = re.compile(chamber_event_data[chamber]['bill_ptrn'], re.I)
        desc_ptrn = re.compile(chamber_event_data[chamber]['desc_ptrn'], re.I)
        lines = data.split("\n")
        line_idx = 0

        # Scan through the lines of the pdf, but we need the ability to ingest
        # more than one line per loop for readability/simplicity.  Attempt to
        # identify each line as a date, time and location, or bill header.
        event_list = {}
        while line_idx < len(lines):
            line = lines[line_idx].strip()
            date_match = date_ptrn.search(line)
            # A.) Dates will apply to every following event until the next date is reached
            if not date_match:
                date_match = date_ptrn_2.search(line)
            if date_match:
                if chamber == 'lower':
                    date_string = date_match.group(0)
                    date_string = re.sub(r"(?<!Augu)(th|rd|nd|st), ", ", ", date_string)
                    date = datetime.datetime.strptime(date_string,
                                                      "%A, %B %d, %Y")
                else:  # 'upper'
                    # Assume the weekly schedule being scraped is for the current year.
                    date_string = date_match.group(0)
                    date_string = re.sub(r"(?<!Augu)(th|rd|nd|st), ", ", ", date_string)
                    date = datetime.datetime.strptime(date_string + ', ' + str(datetime.date.today().year),
                                                      "%A, %B %d, %Y")

            meeting_match = meeting_ptrn.search(line)
            if meeting_match:
                if chamber == 'lower':
                    # B.) Time and location appears below each committee line - we now have enough info to create a
                    # committee meeting event. Committees are related to every following bill until the next
                    # time/location is reached.
                    # group 1: full time, 2: clock, 3: meridiem, 4: location
                    event = self._create_meeting_info(chamber, lines[line_idx-1].strip(), date, meeting_match.group(2),
                                                      meeting_match.group(3), meeting_match.group(4), event_list)
                else:  # 'upper'
                    # B.) Committe, time and location appears on a line - we now have enough info to create a
                    # committee meeting event. Committees are related to every following bill until the next committee
                    # is reached.
                    # group 1: committee, 2: full time, 3: clock, 4: meridiem, 5: location
                    event = self._create_meeting_info(chamber, meeting_match.group(1), date, meeting_match.group(3),
                                                      meeting_match.group(4), meeting_match.group(5), event_list)
                if not event:
                    line_idx += 1
                    continue

                # C.) Inner loop: find all bills related to and under the
                # committee that was just found.
                while line_idx + 1 < len(lines):
                    line_idx += 1
                    line = lines[line_idx].strip()
                    # Break to outer loop if we reach a new date or time/location.
                    if meeting_ptrn.search(line) or date_ptrn.search(line):
                        # Process this line on the next outer iteration.
                        line_idx -= 1
                        break
                    bill_match = bill_ptrn.search(line)
                    if bill_match:
                        # group 1: bill ID, group 4: event description
                        bill_id = bill_match.group(1).strip()
                        # Expand abbreviations, then discard any leading comma
                        if bill_match.group(4):
                            event_desc = bill_match.group(4).strip()
                            if chamber == 'lower':
                                event_desc = event_desc.replace(
                                    ' *PV', ', Possible vote').replace(
                                    ' *PA', ', Possible amendments').replace(
                                    ' *PS', ', Possible substitute')
                            else:  # 'upper'
                                event_desc = event_desc.replace('*', ', Possible vote')

                            event_desc = re.sub('^, ', '', event_desc)
                        else:
                            # If there is no event description on the bill's line, use the following lines instead
                            # (which is most often the bill title/description)
                            event_desc, line_idx = self._scrape_long_bill_desc(lines, line_idx, bill_ptrn, desc_ptrn)

                        # Finally add this bill to the committee meeting event
                        event.add_related_bill(bill_id, type='bill')

                # Save the committee meeting event with all its related bills.
                event_key = (event['description'], event['start'], event['location'])
                event_list[event_key] = event

            # D.) All other lines will be ignored or are difficult to identify
            # and will be processed in a later iteration (e.g. committee name).
            line_idx += 1

        for event_key in event_list:
            self.save_event(event_list[event_key])

    def scrape(self):
        """
        Scrape all known events for this state.
        """
        for chamber in ["upper", "lower"]:
            self.scrape_committee_hearings(chamber)

        self.save_events_calendar()

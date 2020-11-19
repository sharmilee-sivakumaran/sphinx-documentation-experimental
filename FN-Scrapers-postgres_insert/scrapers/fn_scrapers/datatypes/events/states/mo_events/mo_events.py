from __future__ import absolute_import
import re
import pytz
from datetime import datetime, timedelta
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from lxml.html import html5parser
from lxml.html.html5parser import HTMLParser

"""
MO has the following types of bills in the DB
HRM123
SRM 1231
HB1231
SB12312
SM1231
HJR1313
HCR 1313
SR 313
SJR 1231
HCB132
HRB 123
SRB 123
HR 123
SCR 1231

The following RegEx captures all of them so that we don't send invalid bill IDs to pillar
"""
bill_re = re.compile(r"([HS](?:R(?!R)|J(?=R)|C(?!M))?[MBR]\s*\d+)")

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-MO")
class MOEventScraper(EventScraper):
    jurisdiction = 'mo'
    scraped_events = {}

    def __init__(self, *args, **kwargs):
        super(MOEventScraper, self).__init__('mo', __name__, **kwargs)

    def scrape(self):
        # Missouri has different hearings for each chamber
        self.scrape_lower()

        self.scrape_upper()

        for event in self.scraped_events.values():
            self.save_event(event)

        self.save_events_calendar()

    def get_today_as_timezone(self):
        today = datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz)

    def scrape_upper(self):
        label_re = re.compile(r'^(Committee|Date|Time|Room|(?:[HS]C?S )?[HS][CJRBM]{1,2}):?(.*)')
        bill_re = re.compile(r'^(?:[HS]C?S )?([HS][CJRBM]{1,2} \d+)')
        cancel_re = re.compile(r'cancell?ed', re.I)
        committee_re = re.compile(r'^(.+?),?')
        joint_committee_re = re.compile(r'^Joint Committee on (.+?),?')

        chamber = 'upper'
        source_url = 'http://www.senate.mo.gov/hearingsschedule/hrings.htm'
        source_page = self.get(source_url)
        source_page = source_page.lxml()

        # Basically, the page has a bunch of sibling elements, each element
        # containing information about an event. However, each element only
        # contains a piece of information about an event, such as location or
        # time. Thus, we must iterate through all elements and parse them
        # sequentially. We do this by getting all the elements, looking at their
        # text content and splitting up the lines found. We then iterate through
        # these lines, parsing each one and creating events in the process. If
        # there's a better way to do this, please implement it.

        # The array with all lines
        lines = []
        # The elements
        raw_content = source_page.xpath('//div[@id="content"]/div[@class="tg-container"]')[0].getchildren()
        # Iterate through elements, extracting text content by line and pushing these lines into LINES
        for content_element in raw_content:
            if content_element.tag == 'table':
                td = content_element.xpath('.//td')
                for child in td:
                    new_lines = child.text_content().strip().split('\n')
                    lines.extend(new_lines)
            elif content_element.tag == 'a':
                lines.append(None)

        # Add a final None line because None signals that we save data
        lines.append(None)

        date = None
        time = None
        location = None
        committee = None
        related_bills = []
        other_info = None
        skip = False
        has_time = False

        for idx, line in enumerate(lines):
            # None signals that the next line contains info about a new
            # event, so save the current event data
            if line is None:
                if date is not None and location is not None and committee is not None:
                    if has_time:
                        date = self._tz.localize(date)
                    if not location:
                        location = "N/A"
                    # Save last event
                    event = Event(date, "Committee Hearing: {}".format(committee), location,
                                  'committee_markup', start_has_time=has_time, chamber=committee_chamber)


                    for bill_id, bill_description in related_bills:
                        event.add_related_bill(bill_id, type='bill', description=bill_description)

                    # Get/set source
                    event.add_source(source_url)

                    # Get/set participants
                    event.add_participant('host', committee, chamber=committee_chamber)
                    self.save_event(event)

                date = None
                location = None
                committee = None
                committee_chamber = None
                related_bills = []
                other_info = None
                skip = False
                has_time = False
                continue

            elif skip:
                continue
            elif cancel_re.search(line):
                skip = True
                continue

            # Parse line, which could contain info such as committee name, date,
            # time, room, or a related bill. First break the line into parts, a
            # label and the value, e.g. label is "Committee", value is
            # "Appropriations".
            line_parts = label_re.search(line)
            if line_parts:
                key = line_parts.group(1)
                value = line_parts.group(2).strip()

                # Add committee
                if key == 'Committee':
                    # First check if committee is joint committee
                    temp = joint_committee_re.search(value)
                    if temp:
                        committee_chamber = 'joint'
                        committee = value
                    else:
                        committee_chamber = chamber
                        committee = value

                # Set DATE if none, otherwise, assume that DATE was set with
                # just a valid time, so update that DATE with the actual
                # date value
                elif key == 'Date':
                    value = value.replace(" - Adjourned", "")
                    if date is None:
                        date = parse(value + " " + str(datetime.today().year))
                    else:
                        temp = parse(value + " " + str(datetime.today().year))
                        date = temp.replace(hour=date.hour, minute=date.minute)

                # Update DATE with time if DATE is not none, otherwise, set
                # DATE as a datetime value with only a valid time
                elif key == 'Time':
                    parsed_time = self.parse_time(value)

                    # Could not parse time value probably because it's relative,
                    # like "Upon adjournment"
                    if parsed_time is None:
                        continue

                    has_time = True
                    if date is not None:
                        date = date.replace(hour=parsed_time.hour, minute=parsed_time.minute)
                    else:
                        date = parsed_time

                # Add location
                elif key == 'Room':
                    location = value

                # Add related bill
                else:
                    # Get bill description, which is in the next line
                    bill_description = lines[idx + 1]
                    try:
                        # Get bill id
                        bill_id = bill_re.search(line).group(1)
                        
                        # Push tuple with bill id and description to RELATED_BILLS
                        related_bills.append((bill_id, bill_description))
                    except AttributeError:
                        try:
                            group_bill_re = re.compile(r'^(?:[HS]C?S )?([HS][CJRBM]{1,2})s (\d+) & (\d+)')
                            bill_type = group_bill_re.search(line).group(1)
                            bill_num_1 = group_bill_re.search(line).group(2)
                            bill_num_2 = group_bill_re.search(line).group(3)
                            related_bills.append(("%s %s" % (bill_type, bill_num_1)))
                            related_bills.append(("%s %s" % (bill_type, bill_num_2)))
                        except:
                            self.warning("Bad bill line")
                            continue

        return

    def scrape_lower(self):
        source_url = 'http://www.house.mo.gov/AllHearings.aspx'

        """
        The new website has a structured calendar type structure wehre eveything is formatted in
        neat div elements, so it is easier to separate the needed information simply by using xpaths
        """
        today = self.get_today_as_timezone()
        today_args_str = today.strftime('%m%d%Y')
        today_event_link = source_url + u"?sdid={}".format(today_args_str)
        today_event_page = self.get(today_event_link).text
        today_event_page = html5parser.fromstring(today_event_page, parser=HTMLParser(namespaceHTMLElements=False))
        event_panes = today_event_page.xpath("//span[@id='ContentPlaceHolder1_lblDisplay']/div/div")
        events = set()
        if event_panes:
            last_date = self.get_today_as_timezone()
            for pane in event_panes:
                data_divs = pane.xpath("./div")
                text = u' '.join([t.strip() for t in data_divs[0].itertext()])
                last_date = parse(text)
                for data_div in data_divs[1:]:
                    if 'cancelled.png' not in data_div.get('style'):
                        func = data_div.get('onclick')
                        event_id = int(func.split("'")[1])
                        events.add(event_id)
            """
            The following block of codes scrolls the website to dates in the future till it can find
            event ids.

            The events have their own separate pages so eveything cna be scraped from the individual
            event page
            """
            last_date += timedelta(days=1)
            last_date_arg = last_date.strftime('%m%d%Y')
            next_event_link = source_url + u"?sdid={}".format(last_date_arg)
            next_event_page = self.get(next_event_link).text
            next_event_page = html5parser.fromstring(next_event_page, parser=HTMLParser(namespaceHTMLElements=False))
            next_event_panes = next_event_page.xpath("//span[@id='ContentPlaceHolder1_lblDisplay']/div/div")
            while next_event_panes:
                for pane in next_event_panes:
                    data_divs = pane.xpath("./div")
                    text = u' '.join([t.strip() for t in data_divs[0].itertext()])
                    last_date = parse(text)
                    for data_div in data_divs[1:]:
                        if 'cancelled.png' not in data_div.get('style'):
                            func = data_div.get('onclick')
                            event_id = int(func.split("'")[1])
                            events.add(event_id)
                last_date += timedelta(days=1)
                last_date_arg = last_date.strftime('%m%d%Y')
                next_event_link = source_url + u"?sdid={}".format(last_date_arg)
                next_event_page = self.get(next_event_link).text
                next_event_page = html5parser.fromstring(next_event_page, parser=HTMLParser(namespaceHTMLElements=False))
                next_event_panes = next_event_page.xpath("//span[@id='ContentPlaceHolder1_lblDisplay']/div/div")

            for n_id in events:
                event_url = source_url + '?nid={}'.format(n_id)
                event_page = self.get(event_url).text
                event_page = html5parser.fromstring(event_page, parser=HTMLParser(namespaceHTMLElements=False))
                event_div = event_page.xpath("//div[@id='divPopUp']")
                event_content_divs = event_div[0].xpath('./div')
                hearing_det_div = event_content_divs[0]
                related_bill_div = event_content_divs[2]
                committee = hearing_det_div.xpath("./b[1]/a")[0].text
                committee = committee.strip()
                related_table = related_bill_div.xpath('./table')
                event_descr = 'Committee Hearing: {}'
                bill_ids = []

                both = False
                if related_table:
                    table = related_table[0]
                    rows = table.xpath('.//tr[position()>1]')
                    for row in rows:
                        fields = row.xpath('./td')
                        is_hearing = 'checked' in fields[0].xpath('./input')[0].attrib
                        is_Esession = 'checked' in fields[1].xpath('./input')[0].attrib
                        if is_hearing and is_Esession:
                            event_descr = 'Public Hearing and Executive Session: {}'
                            both = True
                        elif not both and (is_hearing and not is_Esession):
                            event_descr = 'Public Hearing: {}'
                        elif not both and (not is_hearing and is_Esession):
                            event_descr = 'Executive Session: {}'

                        bill_ids.append(list(fields[2].itertext())[0].strip())

                meeting_details = list(hearing_det_div.itertext())
                chair = ''
                if len(meeting_details) == 5:
                    committee, _, chair, location, dt_str = meeting_details
                elif len(meeting_details) == 3:
                    committee, location, dt_str = meeting_details
                else:
                    self.warning("Unable to find required information from: {}".format(event_url))
                    continue

                committee = committee.strip()
                if chair:
                    chair = re.findall(r"^(.+) \(", chair.strip())[0]
                location = location.strip()
                date_str, time_str = dt_str.split(' - ')[:2]
                date_str = parse(date_str.strip())
                start_has_time = True
                start_time = None
                start = None
                try:
                    start_time = parse(time_str.strip())
                    start = datetime.combine(date_str.date(), start_time.time())
                    start = self._tz.localize(start)
                except ValueError as e:
                    start_time = time_str.strip()
                    start = date_str
                    start_has_time = False

                props = (start, event_descr.format(committee), location)

                if props not in self.scraped_events:
                    event = Event(start, event_descr.format(committee), location, 'committee_hearing',
                                  start_has_time=start_has_time)
                else:
                    event = self.scraped_events[props]
                if not start_has_time:
                    event['start_time_description'] = start_time

                event.add_source(event_url)
                for b_id in bill_ids:
                    match = bill_re.search(b_id)
                    if match:
                        event.add_related_bill(match.group(1), related_type='bill')

                event.add_participant('host', committee)
                if chair:
                    event.add_participant('chair', chair)

                event['chamber'] = 'lower'

                self.scraped_events[props] = event

    # Takes in a raw text representing time. We assume there is a colon
    # separating the hour and minute, and the only non-numerical text is the
    # AM/PM indicator text.
    def parse_time(self, raw):
        try:
            raw_parts = raw.split(':')
            ampm = 'AM'
            if raw_parts[1].find('p') != -1 or raw_parts[1].find('P') != -1:
                ampm = 'PM'

            hours = raw_parts[0].strip()
            minutes = raw_parts[1].rstrip('APMapm. ')

            return datetime.strptime("{0}:{1} {2}".format(hours, minutes, ampm), '%I:%M %p')
        except:
            return None

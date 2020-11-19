"""
SC Event Scraper
"""
from __future__ import absolute_import

import re
import datetime
import lxml.html

from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from dateutil.parser import parse

from fn_scrapers.common.extraction.textpdf import convert_pdf

from fn_scrapers.api.scraper import scraper, tags

skip_re = re.compile(r"(legislative\s(luncheon|breakfast))|(^\s*senate\s*$)|" +
                     r"house\sof\srepresentatives|caucus|ymca|(chamber\s*$)", flags=re.I)


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-SC")
class SCEventScraper(EventScraper):
    jurisdiction = 'sc'
    event_dict = {}

    def __init__(self, *args, **kwargs):
        super(SCEventScraper, self).__init__('sc', __name__, **kwargs)

    def get_page_from_url(self, url):
        page = self.get(url).text
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)
        return page

    def normalize_time(self, time_string):
        """
        normalize time format
        """
        time_string = time_string.lower().strip()
        if re.search(r'adjourn', time_string):
            time_string = '12:00 am'
        if re.search(r' noon', time_string):
            time_string = time_string.replace(' noon', ' pm')
        # remove extra spaces
        if re.search('[^ ]+ ?- ?[0-9]', time_string):
            start, end = re.search(r'([^ ]+) ?- ?([0-9])',
                                   time_string).groups()
            time_string = re.sub(start + ' ?- ?' + end,
                                 start + '-' + end, time_string)
        # if it's a block of time, use the start time
        block_reg = re.compile(
            r'^([0-9]{1,2}:[0-9]{2}( [ap]m)?)-[0-9]{1,2}:[0-9]{2} ([ap]m)')

        if re.search(block_reg, time_string):
            start_time, start_meridiem, end_meridiem = re.search(
                block_reg, time_string).groups()

            start_hour = int(start_time.split(':')[0])
            if start_meridiem:
                time_string = re.search(
                    '^([0-9]{1,2}:[0-9]{2} [ap]m)', time_string).group(1)
            else:
                if end_meridiem == 'pm' and start_hour < 12:
                    time_string = start_time + ' am'
                else:
                    time_string = start_time + ' ' + end_meridiem
        return time_string

    def get_bill_description(self, url):
        """
        Get bill sescription from bill page
        :param url: url of bill page
        """
        try:
            bill_page = self.get_page_from_url(url)
            bill_text = bill_page.xpath(
                './/div[@id="resultsbox"]/div[2]')[0]
            bill_description = bill_text.text_content().encode(
                'utf-8').split('\xc2\xa0\xc2\xa0\xc2\xa0\xc2\xa0')[0]

            bill_description = re.search(
                r'Summary: (.*)', bill_description).group(1).strip()
        except IndexError as err:
            self.error(err.arg)
            bill_description = "Bill up for discussion."
        return bill_description

    def _fetch_pdf_lines(self, href):
        """
        fetch lines from pdf file
        :param href: href of pdf
        :return pdflines: list of lines in pdf context
        """
        fname, resp = self.urlretrieve(href)
        try:
            pdflines = [line.decode('utf-8') for line in convert_pdf(fname, 'text').splitlines()]
        except ValueError:
            pdflines = [line.decode('latin-1') for line in convert_pdf(fname, 'text').splitlines()]
        return pdflines

    def scrape(self):
        """
        SC Event Scrape function
        :param session: session of event
        :param chambers: list of chambers that events belong to
        """
        for chamber in ["upper", "lower", "joint"]:
            try:
                if chamber == 'joint':
                    events_url = 'http://www.scstatehouse.gov/meetings.php'
                else:
                    events_url = 'http://www.scstatehouse.gov/meetings.php?chamber=%s' % (
                        "S" if chamber == "upper" else "H"
                    )
                page = self.get_page_from_url(events_url)

                meeting_year = page.xpath(
                    '//h2[@class="barheader"]/span')[0].text_content()
                meeting_year = re.search(
                    r'Week of [A-Z][a-z]+\s+[0-9]{1,2}, ([0-9]{4})',
                    meeting_year).group(1)
            except IndexError:
                self.error("Failed to scrape url for chamber %s", chamber)
                continue

            date_obj = None
            for meeting_outer in page.xpath("//div[@id='contentsection']/ul"):

                # Not every list element specifies a date - if there are multiple meetings on the same
                # date, only the first one will. So, the date is "sticky" between meetings.
                date_element_list = meeting_outer.xpath("span")
                if date_element_list:
                    date_str = u"".join(date_element_list[0].itertext())
                    try:
                        date_obj = parse(date_str).date()
                    except ValueError:
                        self.error("Couldn't parse date string: %s", date_str)
                        continue
                if date_obj is None:
                    self.error("Failed to find date for meeting - having to skip processing them!")
                    continue

                for meeting in meeting_outer.xpath("li"):
                    if meeting.xpath('contains(., "CANCELED")'):
                        continue

                    meeting_info = u"".join(e for e in meeting.xpath("text() | *[not(self::div)]//text()")).strip()

                    # Two examples of what this meeting_info might look like:
                    # 1. "11:00 am -- Blatt Room 321 -- Executive Subcommittee of the Legislative Oversight Committee"
                    # 2. "Blatt Room 321 -- Executive Subcommittee of the Legislative Oversight Committee"
                    # So, the time is optional. And, the fields are seperated by "--". So, we use the regex below
                    # to split that into its parts
                    m = re.match(r"(?:(.*?)\s+--\s+)?(.*)\s+--\s+(.*)$", meeting_info)
                    if not m:
                        self.error(
                            "Failed to scrape meeting information: Could not recognize meeting_info: %s",
                            meeting_info)
                    time_string, location, description = m.groups()

                    # Extract and normalize the committee name
                    # NOTE: I couldn't find any current examples on the website where these checks did
                    # anything. I Left them in though, because, maybe they do something during other
                    # parts of the session?
                    committee = re.sub(ur'\s+((on\s[HS]\.\s*?\d+)|(budget\s)?hearing)\s*$', u'',
                                       description, flags=re.I).strip()
                    committee = committee.split(' - ')[0].strip()

                    if skip_re.search(committee):
                        # Skip general events such as lunch, breakfast etc.
                        continue

                    comm_ch = chamber
                    if chamber != 'joint':
                        if re.search(r'^joint\s', committee, flags=re.I):
                            comm_ch = 'joint'

                    if time_string:
                        time_string = self.normalize_time(time_string).split('or')[0].strip()
                        try:
                            time_obj = parse(time_string).time()
                            if time_obj:
                                has_time = True
                            else:
                                has_time = False
                        except ValueError as e:
                            has_time = False
                            time_obj = datetime.time(0, 0, 0)
                            self.warning("Unable to parse time from {}".format(time_string))
                    else:
                        time_obj = datetime.time(0, 0, 0)
                        has_time = False
                    date_time = datetime.datetime.combine(date_obj, time_obj)

                    if has_time:
                        date_time = self._tz.localize(date_time)

                    event = Event(
                        date_time,
                        description,
                        location,
                        'committee_markup',
                        start_has_time=has_time,
                        chamber=chamber
                    )
                    event.add_source(events_url)
                    event.add_participant('host', committee, chamber=comm_ch)

                    agenda_url = meeting.xpath(".//a[contains(@href,'agendas')]")
                    if agenda_url:
                        try:
                            agenda_url = agenda_url[0].attrib['href']
                        except IndexError:
                            self.error("Failed to scrape agenda url")
                            continue
                        event.add_source(agenda_url)

                        if '.pdf' in agenda_url:
                            pdflines = self._fetch_pdf_lines(agenda_url)
                            full = '\n'.join(pdflines)
                            full = re.sub(r'\n', ' ', full)
                            bill_id_re = r'([HS]\.\s+\d+)(?:\s+--.*?:(.*?\.))?'
                            billgroup = re.findall(bill_id_re, full)
                            for bill_id, desc in billgroup:
                                bill_id = re.sub('\xa0', '', bill_id)
                                bill_id = re.sub(r'\.', ' ', bill_id)
                                bill_id = re.sub(r'\s+', ' ', bill_id)
                                bill_id = bill_id.strip()
                                desc = desc.strip()

                                event.add_related_bill(bill_id, type='consideration', description=desc)

                        else:
                            agenda_page = self.get_page_from_url(agenda_url)

                            for bill in agenda_page.xpath(
                                    ".//a[contains(@href,'billsearch.php') and text()!='']"):
                                bill_url = bill.attrib['href']
                                bill_id = bill.text_content()
                                bill_id = re.sub('\xa0', '', bill_id)
                                bill_id = re.sub(r'\.', ' ', bill_id)
                                bill_id = re.sub(r'\s+', ' ', bill_id)
                                bill_description = self.get_bill_description(bill_url)
                                event.add_related_bill(
                                    bill_id,
                                    type='consideration',
                                    description=bill_description
                                )
                    event_key = (event['start'], event['location'], event['description'])
                    if event_key not in self.event_dict:
                        self.event_dict[event_key] = True
                        self.save_event(event)

        self.save_events_calendar()

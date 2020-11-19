from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from datetime import datetime

url = "http://www.leg.state.mn.us/calendarday.aspx?jday=all"


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-MN")
class MNEventScraper(EventScraper):
    jurisdiction = 'mn'
    bill_re = re.compile('((HF|S[.]?F[.]?) ?\d+)')
    senate_location_re = re.compile('(Room .*)\n?')

    datetime_format = '%A, %B %d, %Y %I:%M %p'
    date_format = '%A, %B %d'
    date_formats = {date_format: False, datetime_format: True}
    date_re = re.compile('(\w+, \w+ \d+, \d+ ?)(\d{0,2}:\d{0,2} [ap]m)?', re.I)

    def __init__(self, *args, **kwargs):
        super(MNEventScraper, self).__init__('mn', __name__, **kwargs)
        # Initialize empty dict for unique events
        self.scraped_events = {}

    def scrape(self):
        page = self.get_page(url)

        commission_meetings = page.xpath("//div[contains(@class,'comm_item')]")
        self.scrape_meetings(commission_meetings, 'commission', 'joint')

        house_meetings = page.xpath("//div[contains(@class,'house_item')]")
        self.scrape_meetings(house_meetings, 'house', 'lower')

        senate_page = self.get_page(url, convert_br=True)
        senate_meetings = senate_page.xpath("//div[contains(@class,'senate_item')]")
        self.scrape_senate_meetings(senate_meetings)

        self.save_events_calendar()

    def get_page(self, url, convert_br=False):
        """
        Load a URL and turn it into an lxml html page.
        :param url: url to load
        :type url: string
        :param convert_br: flag to convert <br> into newlines
        :type convert_br: bool
        :returns: lxml html page
        :rtype: lxml element
        """
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)

        # Convert <br> to newlines to make text processing easier with text_content()
        if convert_br:
            for br in page.xpath("*//br"):
                br.tail = "\n" + br.tail if br.tail else "\n"

        return page

    def scrape_meetings(self, meetings, group, chamber):
        """
        Scrape and save event data from a list of meetings.
        :param meetings: lxml elements containing event information
        :type meetings: list
        :param group: The type of meeting. The legislature site applies
                 different formatting to events based on which group
                 they correspond to.  `group` should be one of the
                 following strings: 'house', 'senate', or 'commission'.
        :type group: string
        """
        for meeting in meetings:

            meeting = self.get_page("https://www.leg.state.mn.us/cal_getdetails?id={}&type=all"
                                    .format(meeting.get("id")))
            when, has_time = self.get_date(meeting)
            description = self.get_description(meeting)
            location = self.get_location(meeting)
            if location is None:
                location = 'N/A'

            if when and description and location:
                when = self._tz.localize(when)
                kwargs = {}
                if group in ['upper', 'lower']:
                    kwargs['chamber'] = group
                agenda = self.get_agenda(meeting)
                if agenda:
                    kwargs['agenda'] = agenda

                event = Event(when, description, location, 'committee_markup',
                              start_has_time=has_time, chamber=chamber)
                event.add_source(url)
                if description != 'House':
                    event.add_participant('host', description, chamber=chamber)

                if agenda:
                    bill_search = self.bill_re.findall(agenda)
                    bills_added = {}

                    for bill_result in bill_search:
                        bill_id = bill_result[0]
                        if bill_id not in bills_added:
                            event.add_related_bill(bill_id, 'consideration', description='Bill up for discussion')
                            bills_added[bill_id] = True

                self.save_event(event)

    def scrape_senate_meetings(self, meetings):
        """
        Scrape and save event data from a list of meetings.
        :param meetings: lxml elements containing event information
        :type meetings: list
        """
        for meeting in meetings:
            try:
                # Information on each meeting is not being loaded immediately on the main page.
                # Each meeting needs to be queried for.
                meeting = self.get_page("https://www.leg.state.mn.us/cal_getdetails?id={}".format(meeting.attrib["id"]))
                # Get date
                date = None
                date_text = meeting.xpath('./p/span')
                if date_text:
                    date_text = date_text[0].text_content().replace('Senate', '').strip()
                    date_text = date_text.replace(u'\xa0', ' ')
                    date_text = re.split(r'\sor\s', date_text, 1)[0]
                    date_text = date_text.strip()

                else:
                    self.warning('Failed to extract date for an event. Skipping.')
                    continue
                for date_format in self.date_formats:
                    try:
                        date = datetime.strptime(date_text, date_format)
                        has_time = self.date_formats[date_format]
                    except ValueError:
                        continue

                # Get title of meeting which is usually just host name
                title = meeting.xpath('./h3')
                try:
                    title = title[0].text_content().strip()
                except:
                    self.warning('Failed to extract title for an event. Skipping.')
                    continue

                # Get location
                try:
                    location = meeting.xpath('./div/table/tr[1]/td')[0].text_content()
                    location = self.senate_location_re.search(location).group(1).strip()
                except:
                    location = 'TBD'

                # Get raw text of meeting
                content = meeting.xpath('./div/table')
                content = content[0].text_content().encode('ascii', 'ignore') if content else None
                # Get bills by scanning meeting text
                if content is not None:
                    bill_search = self.bill_re.findall(content)
                    bills_added = {}
                    for bill_result in bill_search:
                        bill_id = bill_result[0]
                        if bill_id not in bills_added:
                            bills_added[bill_id] = True

                # Don't save events that are whol Senate meetings or don't have
                # related bills.
                if title == 'Senate':  # or len(bills_added) == 0:
                    continue

                descr = "%s hearing" % title

                date = self._tz.localize(date)
                # Create Event object
                event = Event(date, descr, location, 'committee_markup', start_has_time=has_time, chamber='upper')
                event.add_source(url)
                event.add_participant('host', title, chamber='upper')

                # Insert bills
                for bill_id in bills_added:
                    event.add_related_bill(bill_id, type='consideration', description='Bill up for discussion')

                self.save_event(event)
            except Exception as exc:
                self.exception("Error saving event, no time field")

    def get_date(self, meeting):
        """
        Get the date from a meeting lxml element.
        :param meeting: lxml element containing event information
        :type meeting: lxml element
        :returns: date and time of event
        :rtype: datetime object
        """
        date_raw = meeting.xpath(".//b")
        if len(date_raw) < 1:
            return (None, 'N/A')

        return self._get_date(date_raw[0].text_content().strip())

    def _get_date(self, date_string):
        date_search = self.date_re.search(date_string)
        if date_search and date_search.group(2):
            date = datetime.strptime(date_search.group(1) + date_search.group(2), self.datetime_format)
            return (date, True)
        elif date_search:
            date = datetime.strptime(date_string, self.date_format)
            return (date, False)
        return (None, False)

    def get_description(self, meeting, i=0):
        """
        Get the description from a meeting lxml element.
        :param meeting: lxml element containing event information
        :type meeting: lxml element
        :returns: description of event or 'Hearing' if there is none found
        :rtype: string
        """
        for description in meeting.xpath(".//a"):
            description = description.text_content().strip()
            if description != 'House' and description != 'Senate' \
                    and 'Live Audio' not in description and 'Live Video' not in description:
                return description + " hearing"
        return "Committee Hearing"

    def get_location(self, meeting):
        """
        Get the location from a meeting lxml element.
        Location information follows a `b` element containing the text
        "Room:".
        :param meeting: A lxml element containing event information
        :type meeting: lxml element
        :returns: location of event
        :rtype: string
        """
        return self.get_tail_of(meeting, '^Room:')

    def get_agenda(self, meeting):
        """
        Get the agenda from a meeting lxml element.
        Agenda information follows a `b` element containing the text
        "Agenda:".
        :param meeting: A lxml element containing event information
        :type meeting: lxml element
        :returns: text of event agenda
        :rtype: string
        """
        return self.get_tail_of(meeting, '^Agenda:', include_more=True)

    def get_tail_of(self, meeting, pattern_string, include_more=False):
        """
        Get the tail of a `b` element matching `pattern_string`, all
        inside a `div` tag.
        Surprisingly useful for the markup on the Minnesota
        legislative events calendar page.
        :param meeting: lxml element of event
        :type meeting: lxml event
        :param pattern_string: A regular expression string to match
                          against
        :type pattern_string: string
        :param include_more: flag to include all trailing text
        :type include_more: bool
        :returns: text containing trailing text
        :rtype: string
        """
        pattern = re.compile(pattern_string)

        div_tags = meeting.xpath('.//*[@class="calendar_p_indent"]')
        if len(div_tags) < 1:
            return

        div_tag = div_tags[0]

        # Iterate through children
        for element in div_tag.iter():
            # If include_more flag is true grab everything once we find a
            # pattern match.
            if include_more:
                raw = element.text_content().strip()
                if pattern.search(raw):
                    tail = element.tail + '\n' if element.tail else ''

                    while element.getnext() is not None:
                        element = element.getnext()

                        bills_param_search = None
                        element_id = element.get('id')
                        if element_id is not None:
                            bills_param_re = re.compile('div_\d+_\d+_(added|removed)_\d+_\d+')
                            bills_param_search = bills_param_re.search(element_id)

                            if bills_param_search and bills_param_search.group(1) == 'added':
                                bills_added_url = 'http://www.leg.state.mn.us/cal_getbills.aspx?r='
                                related_bills_url = bills_added_url + element_id
                                related_bills_page = self.get_page(related_bills_url)
                                links = related_bills_page.xpath('//table//td//a')
                                for link in links:
                                    bill_search = self.bill_re.match(link.text_content())
                                    if bill_search:
                                        tail += link.text_content().strip() + '\n'

                        # Get tail text content if <br> or we already parsed as related bills above
                        if (element.tag == 'div' and bills_param_search) or element.tag == 'br':
                            if element.tail:
                                tail += element.tail.strip() + '\n'

                        # Get inside text content otherwise
                        else:
                            tail += element.text_content().strip() + '\n'

                    if tail != '':
                        return tail.strip()

                    break
            else:
                if element.tag == 'b':
                    raw = element.text_content().strip()
                    if element.tail and pattern.search(raw):
                        tail = element.tail.strip()
                        if tail != '':
                            return tail
                        break
        return
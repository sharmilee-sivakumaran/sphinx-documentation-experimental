"""
UT EventScraper
"""
from __future__  import absolute_import
import re
import lxml
import pytz
import datetime as dt
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from lxml.html import html5parser
from lxml.html.html5parser import HTMLParser
import lxml.etree as etree

base_url = "http://le.utah.gov"

calendar_url = "https://le.utah.gov/asp/interim/Cal.asp?year={}&month={}"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-UT")
class UTEventScraper(EventScraper):
    """
    UT EventScraper
    Used to scrape Utah Events
    """
    jurisdiction = 'ut'

    def __init__(self, *args, **kwargs):
        super(UTEventScraper, self).__init__('ut', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url).text
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)
        return page

    def get_day(self, element):
        for anc in element.iterancestors(tag='td'):
            id_val = anc.get('id')
            if id_val and id_val.startswith('cell'):
                day = re.match(r'cell(\d+)', id_val)
                return day.group(1)
        return None

    def scrape(self):
        html5_parser = HTMLParser(namespaceHTMLElements=False)
        cur_date = dt.datetime.utcnow().replace(tzinfo=pytz.UTC)
        cur_date = cur_date.astimezone(self._tz)
        year = cur_date.year
        """
        The new website also has a Calendar page where displays all the events in a month
        we are not interested in past events so we only look for active events in the current
        month and 2 following months after that.
        """
        for month in xrange(cur_date.month, cur_date.month+3):
            if month > 12:
                month %= 12
                year = cur_date.year + 1

            month_link = calendar_url.format(year, month)
            month_page = self.get(month_link).text
            month_page = html5parser.fromstring(month_page, parser=html5_parser)

            meetings = month_page.xpath(
                "//div[contains(@class, 'mtgBox') and contains(@class, 'futureMeeting')]")
            """
            There are two types of events listed in the calendar type interface:
            1 - Events with agendas and locations
            2 - Events without agendas

            Events of type 1 have external links for the Agenda page from which we
            can get the related bills, chairs etc., They also have a separate field
            that tells us the exact location of the event

            Events of type 2 don't have any external links that help us with the event
            info, no location, no related bills etc. Intead, in the event description
            itself, the committee name and the location sometimes along with start and
            end time is present.
            """
            self.info("Number of future events found for {:02d}/{}: {}".format(month, year, len(meetings)))
            for meeting in meetings:
                location = None
                committee = None
                committee_string = meeting.text
                source = month_link
                if committee_string:
                    if committee_string.startswith('House Chamber')\
                       or committee_string.startswith('Senate Chamber'):
                        continue
                    match = re.match(r"^(.+?) - (.+?)$", committee_string.strip())
                    if match:
                        committee, location = match.groups()
                        committee = committee.strip()
                        location = location.strip()
                else:
                    committee_ele = meeting.find('./a')
                    if committee_ele is not None:
                        committee = committee_ele.text.strip()

                if not committee:
                    """
                    There are events on the page that are just mark the important
                    session dates, they can be ignored.
                    """
                    continue

                if committee.startswith('House'):
                    chamber = 'lower'
                elif committee.startswith('Senate'):
                    chamber = 'upper'
                else:
                    chamber = 'joint'

                revised = meeting.findtext('.//span[@class="revised"]')
                if revised is not None and 'CANCELED' in revised:
                    continue
                day = self.get_day(meeting)
                if day:
                    date = "{:02d}/{:0>2}/{}".format(month, day, year)
                else:
                    # We cannot have events without start date
                    self.critical("Unable to get start date")
                    continue
                start_time = meeting.findtext(".//td[@class='timecell']")
                has_start_time = True
                if not start_time:
                    has_start_time = False
                    start = dt.datetime.strptime(date, "%m/%d/%Y")
                else:
                    start = dt.datetime.strptime('{} {}'.format(date, start_time), "%m/%d/%Y %I:%M %p")
                    start = self._tz.localize(start)

                if not location:
                    """
                    The location string can be set from two separate pages, depending on the type
                    of event, (1 or 2 as described before), If the location is already set before, this block
                    won't get called, else we try to find the location element in the event and extract the
                    event location, and if that is also not found, then skip this event, because location is a
                    required field.
                    """
                    location_ele = meeting.find(".//td[@class='mapcell']/div")
                    if location_ele is not None:
                        location = location_ele.get('title').strip()
                    elif not location:
                        self.critical("No location found!")
                        continue

                desc = "{} Hearing".format(committee)
                event = Event(start, desc, location, 'committee_hearing',
                              start_has_time=has_start_time, chamber=chamber)
                event.add_participant("host", committee, chamber=chamber)

                agenda = meeting.find(".//td[@class='agendacell']/div")
                if agenda is not None:
                    match = re.match(r"openPage\('(.*?)'\)", agenda.get('onclick'))
                    if match:
                        agenda_link = match.group(1)
                        if '://' not in agenda_link:
                            source = base_url + agenda_link
                        else:
                            source = agenda_link
                        agenda_data = self.get(source).text
                        agenda_page = html5parser.fromstring(agenda_data,parser=html5_parser)
                        page_text = etree.tostring(agenda_page, method="text", encoding='utf-8')
                        """
                        The pages without related bills did not have any useful information that we don't
                        already have about the event, so we are only interested in the Agendas that have a
                        related bills, and also participant info, this was all at the time of rewrite, may
                        change in the future.
                        """
                        if re.search(
                           ur"The following bills are scheduled for consideration", page_text):
                            agenda_items = agenda_page.find('//a[@name="agendaitems"]')
                            if agenda_items is not None:
                                table = next(agenda_items.itersiblings(tag='table'))
                                bills = table.xpath(".//tr/td[2]/a")
                                for bill in bills:
                                    bill_no = etree.tostring(bill, method='text', encoding='utf-8').strip()
                                    # Sometimes the bill number is of the form HB0032S01, where the
                                    # bill number is HB0032. So we strip out the extra characters.
                                    bill_match = re.search(r"^([A-Z]{2,}\d+)", bill_no)
                                    if bill_match:
                                        bill_no = bill_match.group(1)
                                        event.add_related_bill(bill_no, type='consideration')
                                    else:
                                        self.critical("Unable to extract bill_id: {}".format(bill_no))

                            chair_info_elem = agenda_page.xpath('//tr/td[contains(., "FROM:")]')
                            if chair_info_elem:
                                chair_info_elem = chair_info_elem[0].getnext()
                                for chair in chair_info_elem.itertext():
                                    if chair:
                                        match = re.match(ur"(Rep|Sen)\.\s+(.*?),", chair.strip())
                                        if match:
                                                event.add_participant(
                                                    'chair' if 'Chair' in chair else 'participant',
                                                    match.group(2),
                                                    chamber='upper' if match.group(1) == 'Sen' else 'lower')
                                        elif chair.strip():
                                            event.add_participant('participant', chair.strip())

                event.add_source(source)

                self.save_event(event)

        self.save_events_calendar()

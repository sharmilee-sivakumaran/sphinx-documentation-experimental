from __future__ import absolute_import
import re
import pytz
import lxml.html
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import xml.etree.ElementTree as et
import datetime
from dateutil.parser import parse

senate_url = 'http://www.senate.gov/general/committee_schedules/hearings.xml'
house_url = "http://docs.house.gov/Committee/Calendar/ByDay.aspx?DayID=%s"


@scraper()
@tags(type="events", group="fnleg", country_code="US")
class USEventScraper(EventScraper):
    """
    Workflow:

    The scraper first scrapes the senate level events
    1. It gets an XML file from the URL
       http://www.senate.gov/general/committee_schedules/hearings.xml

       This file contains the meetings that we care about. And since it is an XML,
       it is already structured in a useful way, and information can be extracted easily.
    2. It then iterates over all 'meeting' elements
    3. Extracts the following information for each meeting
        /committee
        /date <-- This can have time as well
        /room
        /matter <-- This is the description, and may contain bill IDs
    4. Save each event

    After this the scraper moves on to the house level events
    1. It goes to a URL like below:
       http://docs.house.gov/Committee/Calendar/ByDay.aspx?DayID=02052018

       and scrapes the event on this day. The events on separate URLs for separate
       days, the scraper is looking 7 days ahead of the current date.
    2. It looks for the meeting URLs on that page using the href attribute of
        //table[@id='MainContent_GridViewMeetings']//a[contains(@href, 'ByEvent.aspx?EventID=')]
    3. Gets the committee name from either the second or the third following sibling of the above element
    4. The scraper then goes to the individual meeting page and looks if the meeting is cancelled or not
        //strong[@class='status-alert']
    5. Then looks for
        //div[@class='meeting-date']/p[@class='meetingTime'] <-- Meeting date time
        //blockquote[@class='location'] <-- Location
        //div[@class='well']/h1 <-- Meeting header to determine event type
    6. Looks for meeting items
        //div[@id='previewPanel']/ul[@class='unstyled']/li <-- Meeting items/bills
    7. Extracts the bill IDs and document for each item to attach with the event
        //div[@id='previewPanel']/ul[@class='unstyled']/li/a[1] <-- Links to item docs
    8. Save all events

    Finally save the event calendar

    """
    jurisdiction = 'us'

    def __init__(self, *args, **kwargs):
        super(USEventScraper, self).__init__('us', __name__, **kwargs)

    def scrape(self):
        self.scrape_upper()
        self.scrape_lower()

        self.save_events_calendar()

    def scrape_upper(self):
        page = self.get(senate_url).text
        page = page.encode('utf-8')
        root = et.fromstring(page)
        for meeting in root.iter('meeting'):
            committee_name = meeting.find('committee').text
            if not committee_name:
                continue
            date = meeting.find('date').text

            try:
                formed_date = datetime.datetime.strptime(date, '%d-%b-%Y %I:%M %p')
                formed_date = self._tz.localize(formed_date)
                has_time = True
            except ValueError:
                try:
                    formed_date = datetime.datetime.strptime(date, '%d-%b-%Y')
                    has_time = False
                except ValueError:
                    self.error("Unknown date format for %s meeting - %s" % (committee_name, date))
                    continue
            location = meeting.find('room').text
            descr = meeting.find('matter').text
            if "No committee hearings scheduled" in descr:
                continue
            bills = re.findall(r"([HS]\.(?:(?:(?:(?:CON|J)\.)?RES|Amdt|R)\.)?\d+)", descr, flags=re.I)
            event_type = 'committee_hearing'
            if 'meeting to markup' in descr:
                event_type = 'committee_markup'
            event = Event(formed_date, descr, location, event_type, chamber='upper',
                          start_has_time=has_time)
            for bill in set(bills):
                """
                Sometimes bill IDs presented ad S.Res.92, the add_related_bill function
                only takes into consideration uppercase letters, so passing the uppercase string
                as an argument.
                """
                event.add_related_bill(bill.upper(), 'bill')
            event.add_participant('host', committee_name, chamber='upper')
            event.add_source('https://www.senate.gov/committees/committee_hearings.htm')
            self.save_event(event)

    def scrape_lower(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        today = today.astimezone(self._tz)

        # scrape meeting in following 7 days
        event_dict = {}
        for x in range(0, 8):
            following_date = today + datetime.timedelta(days=x)
            date_string = "%s%s%s" % (str(following_date.month).zfill(2), str(following_date.day).zfill(2),
                                      following_date.year)
            url = house_url % date_string
            page = self.get_page(url)
            row = page.xpath("//table[@id='MainContent_GridViewMeetings']//a[contains(@href, 'ByEvent.aspx?EventID=')]")
            for link in row:
                descr = link.text_content().strip()
                meeting_url = link.attrib['href']
                # if the meeting is cancelled, there is an additional div after the link
                # this causes the getnext.getnext to fail
                committee_name = link.getnext().getnext().text_content()
                committee_name = re.sub(r'\(.*\)', '', committee_name).strip()
                if not committee_name:
                    # try grabing one more committee name, should have class 'tiny-text'
                    committee_name = link.getnext().getnext().getnext().text_content()
                    committee_name = re.sub(r'\(.*\)', '', committee_name).strip()
                # To prevent duplicates, append the committee name to the description
                if committee_name:
                    descr = u"{} Hearing: {}".format(committee_name, descr)

                meeting_page = self.get_page(meeting_url)
                status_alert = meeting_page.xpath("//strong[@class='status-alert']")
                if status_alert and "cancelled" in status_alert[0].text_content():
                    continue

                try:
                    date = meeting_page.xpath("//div[@class='meeting-date']/p[@class='meetingTime']")[0]
                    date = date.text_content().strip()
                    date = re.sub(r'\s*local time\s*', '', date)
                    formed_date = datetime.datetime.strptime(date, '%A, %B %d, %Y (%I:%M %p)')
                    if formed_date.date() != following_date.date():
                        continue
                    formed_date = self._tz.localize(formed_date)
                except:
                    # try to handle when date is like: Wednesday, February 10, 2016 (1:15 PM - 3:30 PM)
                    try:
                        # try to remove everything after the '-'
                        date = re.sub(r'\-.*\)', '', date)
                        date = re.sub(r'\(', '', date)
                        formed_date = parse(date)
                        if formed_date.date() < following_date.date():
                            continue
                        formed_date = self._tz.localize(formed_date)
                    except Exception as exc:
                        self.error(exc)
                        self.error("Failed to read date '%s' for meeting of %s" % (date, committee_name))
                        continue
                try:
                    location = meeting_page.xpath("//blockquote[@class='location']")[0]
                    location = re.sub(r'\r\n', '', location.text_content())
                    location = re.sub(r'\s+', ' ', location)
                    location = location.strip()
                except:
                    self.error("Failed to read location for meeting of %s" % committee_name)
                    continue

                event_type = 'committee_markup'
                meeting_header_text = meeting_page.xpath("//div[@class='well']/h1")[0].text_content().strip()
                if meeting_header_text.lower().startswith('hearing:'):
                    event_type = 'committee_hearing'

                key = (location, descr, formed_date)
                if key in event_dict:
                    continue

                event = Event(formed_date, descr, location, event_type, chamber='lower',
                              start_has_time=True)
                event.add_participant('host', committee_name, chamber='lower')
                event.add_source(meeting_url)

                text_list = meeting_page.xpath("//div[@id='previewPanel']/ul[@class='unstyled']/li")
                bill_id_re = re.compile(r'([HS]\.\s*(?:R\.|Con\.Res\.|J\.Res\.|Res\.|Amdt\.)?\s*\d+)')
                for doc_row in text_list:
                    text = doc_row.text_content()
                    text = re.sub(r'\s+', ' ', text)

                    doc_name = ""
                    doc_name_matches = re.search(r'(.*)\[', text)
                    if doc_name_matches:
                        doc_name = doc_name_matches.group(1).strip()

                        doc_link = doc_row.find('./a').get('href')
                        bill_id_list = bill_id_re.findall(doc_name)
                        for bill_id in bill_id_list:
                            bill_id = re.sub(r'\s+', ' ', bill_id)
                            event.add_related_bill(bill_id, 'bill')
                        event.add_document(doc_name, doc_link)
                event_dict[key] = event

        for key in event_dict:
            self.save_event(event_dict[key])

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

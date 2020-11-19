"""
OKEventScraper
"""
from __future__ import absolute_import
import re
import datetime
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-OK")
class OKEventScraper(EventScraper):
    """
    OKEventScraper
    Scrape  Event for Oklahoma

    The scraper first scrapes Senate events:
    1. Goes to the URL
       http://www.oksenate.gov/Committees/meetingnotices.html
    2. Scans through all the committees on the listed days:
        //a[contains(@href, 'Meeting_Notices')]
    3. Scrapes each meeting separately. The meeting page is well structured
       so all the needed information is organized into separate elments making it
       easy for the scraper to collect it.
    4. The scraper look for the following elements (CSS Selectors):
        .comm-header <-- Committee Name
        #ip-subject <-- Title of the event
        #ip-date <-- Date of the event
        #ip-time <-- Time of the event
        #ip-location <-- Location of the event

        This covers all the required information we need on the event
    5. After this the scraper looks for the related bills and additional participants:
        .agenda-contents li <-- For related bill IDs
        .com-chairs li <-- Additional participants such as chair etc.
    6. Save each event

    After this the scraper scrapes the House events:
    1. The scraper look goes thorugh the list of committees on the URL:
       https://www.okhouse.gov/Committees/Default.aspx
       And looks for the GET request parameters for each committee.
    2. Goes to the calendar page for each committee by visiting a URL like:
       https://www.okhouse.gov/CommitteeCalendar/Default.aspx?CommID=310&SubCommID=88
    3. Looks for each meeting group:
        //tr[@class='rgGroupHeader']
       and extracts the date, time and location from the text content of this element
    4. Goes through all the following rows/siblings of the above elemnt to look for the Bill IDs

    And save the event calendar
    """
    jurisdiction = 'ok'

    def __init__(self, *args, **kwargs):
        super(OKEventScraper, self).__init__('ok', __name__, **kwargs)

    def scrape(self, ):
        self.scrape_upper()
        self.scrape_lower()
        self.save_events_calendar()

    def scrape_lower(self):
        committee_list_url = "https://www.okhouse.gov/Committees/Default.aspx"
        url = "https://www.okhouse.gov/CommitteeCalendar/Default.aspx?%s"
        committee_page = self.get(committee_list_url)
        committee_page = committee_page.lxml()
        committee_page.make_links_absolute(committee_list_url)

        committee_list = committee_page.xpath("//a[contains(@href, 'CommitteeMembers.aspx?CommID=')]")
        for committee in committee_list:
            link = committee.attrib['href']
            committee_name = committee.text_content().strip()
            committee_id_part = link.split('?')[1]
            committee_url = url % committee_id_part
            page = self.get(committee_url)
            page = page.lxml()
            page.make_links_absolute(committee_url)
            meeting_rows = page.xpath("//tr[@class='rgGroupHeader']")
            for row in meeting_rows:
                text = row.text_content()
                text_group = re.findall(r'Meeting (.*) in (.*)', text)
                date = text_group[0][0].strip()
                try:
                    form_date = datetime.datetime.strptime(date, "%A, %B %d, %Y")
                    has_time = False
                except:
                    try:
                        form_date = datetime.datetime.strptime(date, "%A, %B %d, %Y %I:%M:%S %p")
                        has_time = True
                        form_date = self._tz.localize(form_date)
                    except:
                        self.error("Failed to formate date %s" % date)
                        continue

                location = text_group[0][1].strip()
                descr = "%s Hearing" % committee_name
                event = Event(form_date, descr, location, 'committee_markup',
                              start_has_time=has_time, chamber='lower')
                event.add_source(committee_url)
                event.add_participant("host", committee_name, chamber='lower')

                bill_row = row
                while True:
                    bill_row = bill_row.getnext()
                    if bill_row is None or ('rgRow' not in bill_row.attrib['class'] and 'rgAltRow'
                                            not in bill_row.attrib['class']):
                        break
                    cell = bill_row.xpath("./td")
                    bill_id = cell[2].text_content().strip()

                    if not bill_id:
                        continue
                    event.add_related_bill(bill_id, 'consideration')

                self.save_event(event)

    def scrape_upper(self):
        """
        Scrape committee meeting in upper chmaber
        """
        url = "http://www.oksenate.gov/Committees/meetingnotices.html"
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        meeting_links = page.xpath("//a[contains(@href, 'Meeting_Notices')]")

        event_dict = {}
        for meeting_link in set(meeting_links):
            meeting_url = re.sub(" ", "%20", meeting_link.attrib['href'].strip())

            self.scrape_meeting('upper', meeting_url, event_dict)

    def scrape_meeting(self, chamber, meeting_url, event_dict):
        meeting_page = self.get(meeting_url)
        meeting_page = meeting_page.lxml()
        meeting_page.make_links_absolute(meeting_url)
        start_time_description = None

        committee = ''
        committee_ele = meeting_page.cssselect('.comm-header')
        if committee_ele:
            committee = committee_ele[0].text_content().strip()
        else:
            self.critical("Unable to find committee {}".format(meeting_url))
            return

        title = ''
        title_ele = meeting_page.cssselect('#ip-subject')
        if title_ele:
            title = title_ele[0].text_content().strip()
        else:
            title = "{} Hearing".format(committee)

        date = None
        date_ele = meeting_page.cssselect('#ip-date')
        if date_ele:
            date = parse(date_ele[0].text_content().strip())
        else:
            self.critical("Unable to find date {}".format(meeting_url))
            return

        start_has_time = False
        start_time_description = ''
        time_ele = meeting_page.cssselect('#ip-time')
        if time_ele:
            try:
                time_obj = parse(time_ele[0].text_content().strip())
                start_has_time = True
                _, _, _, h, m, s, _, _, _ = time_obj.timetuple()
                date = date.replace(hour=h, minute=m, second=s)
                date = self._tz.localize(date)
            except ValueError as e:
                start_has_time = False
                start_time_description = time_ele[0].text_content().strip()

        location = ''
        location_ele = meeting_page.cssselect('#ip-location')
        if location_ele:
            location = location_ele[0].text_content().strip()
        else:
            self.critical("Unable to find location {}".format(meeting_url))
            return

        event = Event(date, title, location, 'committee_hearing', start_has_time=start_has_time, chamber=chamber)
        if start_time_description:
            event['start_time_description'] = start_time_description
        event.add_source(meeting_url)
        event.add_participant('host', committee, chamber=chamber if 'Joint' not in committee else 'joint')

        agenda_content = meeting_page.cssselect('.agenda-contents li')
        for item in agenda_content:
            item_text = item.text_content().strip()
            """
            The possible patterns we have in the DB are:
            S/H
            - JM
            - B
            - M
            - CR
            - JR
            - C
            - R
            """
            bill_id = re.search(r"([SH](?:C(?!C|B|M)|J(?!C|B))?[CBRM] \d+) By", item_text)
            if bill_id:
                event.add_related_bill(bill_id.group(1), type='consideration')

        chairs = meeting_page.cssselect('.com-chairs li')
        for chair in chairs:
            text = chair.text_content().strip()
            match = re.search(r"^(Senator|Representative)?\s*(.*),\s+(.*)", text)
            if match:
                chamber_info, name, position = match.groups()
                p_ch = None
                if chamber_info == 'Senator':
                    p_ch = 'upper'
                elif chamber_info == 'Representative':
                    p_ch = 'lower'
                if p_ch:
                    event.add_participant(
                        'chair' if 'chair' in position.lower() else 'participant',
                        name,
                        chamber=p_ch,
                    )
                else:
                    event.add_participant(
                        'chair' if 'chair' in position.lower() else 'participant',
                        name,
                    )

        if (location, str(date), title) not in event_dict:
            event_dict[(location, str(date), title)] = True
            self.save_event(event)

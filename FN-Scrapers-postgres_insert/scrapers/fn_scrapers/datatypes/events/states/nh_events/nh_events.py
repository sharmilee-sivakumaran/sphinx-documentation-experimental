"""
nh.events

:class NHEventScraper: scrapes New Hampshire Events
"""
from __future__ import absolute_import
import re
from fn_scrapers.datatypes.events.common.event_scraper import Event, EventScraper
from fn_scrapers.api.scraper import argument, scraper, tags
from fn_scrapers.common.http import Session
import lxml.html
import datetime
import pytz


HOUSE_URL = 'http://gencourt.state.nh.us/house/'
SENATE_URL = 'http://gencourt.state.nh.us/senate/'
CONFERENCE_URL = 'http://www.gencourt.state.nh.us/committee_of_conference/default.aspx'
FIXED_DATE = datetime.datetime(2000, 01, 01)


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NH")
class NHEventScraper(EventScraper):
    """
    NHEventScraper

    Scrape New Hampshire Events
    """
    jurisdiction = 'nh'
    # only used for posting, would otherwise set for the whole class
    post_hdrs = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    # Map of date, desc, location to event. Used to keep track of and merge duplicate events
    scraped_events = {}

    def __init__(self, *args, **kwargs):
        super(NHEventScraper, self).__init__('nh', __name__, **kwargs)
        user_agent = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36')
        Session.get().user_agent = user_agent

    def scrape(self):
        """
        scrape's events from NH for given session/chambers

        :param session: session to scrape
        :type session: string
        :param chambers: chambers to process
        :type chambers: list
        """
        self._scrape_senate_events()

        self._scrape_house_events()

        self._scrape_conference_committee_events()

        for event in self.scraped_events.values():
            self.save_event(event)

        self.save_events_calendar()

    def _scrape_senate_events(self):
        today_num = _get_cal_number()
        # we'll check for all dates 31 days after today
        viewstate = self._get_post_back_data(SENATE_URL)['__VIEWSTATE']

        # retrys = self.retry_attempts
        # self.retry_attempts = 0
        for day in xrange(today_num, today_num+16):
            post_dict = dict(
                __EVENTTARGET='C1',
                __EVENTARGUMENT=str(day),
                __VIEWSTATE=viewstate
            )
            date = FIXED_DATE + datetime.timedelta(days=day)
            self.info("Checking %s, num: %s", date, day)

            try:
                page_lxml = self.post(SENATE_URL,
                                      data=post_dict,
                                      headers=self.post_hdrs,
                                      timeout=20).lxml()
            except Exception as exc:
                self.warning("Error fetching Senate calendar for %s - %s", date, exc)
                continue
            for bill_row in page_lxml.xpath('//a[contains(@href, "legislation")]'):
                bill_id = bill_row.text_content()

                # their page format is bad, table/td is no bueno mmk, but it happens for the first event
                #   after a committee heading for whatever reason
                ctte_xpath = 'ancestor::tr/preceding-sibling::tr/td/a[contains(@href, "committees")]/text()'
                ctte = bill_row.xpath(ctte_xpath)
                if not ctte:
                    ctte = bill_row.xpath(ctte_xpath.replace('ancestor::tr', 'ancestor::td'))
                if ctte:
                    ctte = ctte[0]

                time = None
                time_ele = bill_row.xpath('ancestor::tr/td[3]/text()')
                if not time_ele:
                    time_ele = bill_row.xpath('ancestor::td/following-sibling::td[1]/text()')
                time_ele = time_ele[0] if len(time_ele) > 0 else None
                try:
                    time_obj = datetime.datetime.strptime(time_ele, "%I:%M %p").time()
                    date = datetime.datetime.combine(date, time_obj)
                    date = self._tz.localize(date)
                    has_time = True
                except:
                    has_time = False

                room = bill_row.xpath('ancestor::tr/td[4]/text()')
                if not room:
                    room = bill_row.xpath('ancestor::td/following-sibling::td[2]/text()')
                try:
                    room = room[0]
                except:
                    room = 'N/A'
                    self.warning("Failed to get room location.")

                description = bill_row.xpath('ancestor::tr/following-sibling::tr[1]')
                if not description:
                    description = bill_row.xpath('ancestor::td/following-sibling::tr[1]')
                description = description[0].text_content().strip()

                props = (date, description, room)

                if props in self.scraped_events:
                    event = self.scraped_events[props]
                else:
                    self.scraped_events[props] = event = Event(date, description, room, 'committee_markup',
                                                               start_has_time=has_time, chamber="upper")

                event.add_source(SENATE_URL)
                if ctte:
                    event.add_participant('host', ctte, chamber='upper')

                event.add_related_bill(bill_id, "consideration")

        # self.retry_attempts = retrys

    def _scrape_house_events(self):
        today_num = _get_cal_number()
        today = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(self._tz)

        # we'll check for all dates 31 days after today
        post_vars = self._get_post_back_data(HOUSE_URL)

        viewstate = post_vars['__VIEWSTATE']
        eventvalidation = post_vars['__EVENTVALIDATION']
        # retrys = self.retry_attempts
        # self.retry_attempts = 0
        for day in xrange(today_num, today_num+16):
            post_dict = dict(
                __EVENTTARGET='Hearings',
                __EVENTARGUMENT=str(day),
                __VIEWSTATE=viewstate,
                __EVENTVALIDATION=eventvalidation,
                txt_bill='',
                textsessionyear=(str(today.year) if today.year % 2 != 0 else str(today.year-1))
                )

            date = FIXED_DATE + datetime.timedelta(days=day)

            self.info("Checking %s, num: %s", date, day)

            try:
                page_lxml = self.post(HOUSE_URL, data=post_dict, headers=self.post_hdrs).lxml()
            except Exception as exc:
                self.warning("Error fetching House calendar for %s - %s", date, exc)
                continue

            for ctte_row in page_lxml.xpath('//tr/td/a[contains(@href, "committeedetails")]'):
                ctte = ctte_row.text_content().strip()

                desc = ctte_row.xpath("ancestor::tr[1]/following-sibling::tr[2]")[0].text_content()
                desc = re.sub('Title:', '', desc).strip()

                bill_id = ctte_row.xpath('ancestor::tr[1]/following-sibling::tr[1]/td[2]')[0].text_content().strip()
                time_raw = ctte_row.xpath('ancestor::tr[1]/following-sibling::tr[1]/td[3]')[0].text_content().strip()
                location = ctte_row.xpath('ancestor::tr[1]/following-sibling::tr[1]/td[4]')[0].text_content().strip()

                try:
                    time_obj = datetime.datetime.strptime(time_raw, "%I:%M %p").time()
                    date = datetime.datetime.combine(date, time_obj)
                    date = self._tz.localize(date)
                    has_time = True
                except:
                    has_time = False

                props = (date, desc, location)

                if props in self.scraped_events:
                    event = self.scraped_events[props]
                else:
                    self.scraped_events[props] = event = \
                        Event(date, desc, location, 'committee_markup', start_has_time=has_time, chamber='lower')

                event.add_participant('host', ctte, chamber='lower')
                event.add_source(HOUSE_URL)
                event.add_related_bill(bill_id, "consideration")

        # self.retry_attempts = retrys

    def _scrape_conference_committee_events(self):
        """
        This scrapes the Committee of Conference events, this committee doesn't really exist
        on the FN App, but these meeting show up in the action timeline, hence are important

        For example for 20172018r:
        - SB 549: Committee of Conference Meeting: 05/15/2018, 11:00 am, Room 103, LOB
        - HB 1766: Conference Committee Meeting: 05/14/2018 01:00 PM LOB 303

        The page that displays these meetings doesn't seem to accept any date params.
        http://www.gencourt.state.nh.us/committee_of_conference/default.aspx
        """
        page = self.get(CONFERENCE_URL).lxml()

        meetings = page.xpath('//table[@class="maintable"]/tr[./td[contains(., "Bill Number")]][2]/' +
                              'following-sibling::tr[./td[2]]')

        self.info("Found %d Committee of Conference events", len(meetings))

        for meeting in meetings:
            meeting_details = meeting.find("./td[2]")
            related_bill = meeting.find("./td[1]")

            related_bill_id = next(related_bill.itertext())
            related_bill_id = re.sub(r'\s-', '', related_bill_id, flags=re.U).strip()

            start = meeting_details.find("meetingtime")
            if start is None:
                self.error("Unable to find meeting time")
                continue
            start_dt = None
            start_has_time = False
            start = start.text_content().strip()
            try:
                start_dt = datetime.datetime.strptime(start, "%m/%d/%Y %I:%M %p")
                start_has_time = True
                start_dt = self._tz.localize(start_dt)
            except ValueError as e:
                match = re.search(r"\d{2}/\d{2}/\d{4}", start)
                if match:
                    start_dt = datetime.datetime.strptime(match.group(0), "%m/%d/%Y")
                    start_has_time = False
                    self.warning("Unable to parse time from %s", repr(start))
                else:
                    self.error("Unable to parse start date & time from %s", repr(start))
                    continue

            location = meeting_details.find("meetinglocation")
            if location is None:
                self.error("Unable to find meeting location")
                continue
            location = location.text_content().strip()

            descr = "Committee of Conference Meeting"

            props = (start_dt, descr, location)

            if props in self.scraped_events:
                event = self.scraped_events[props]
            else:
                self.scraped_events[props] = event = Event(start_dt, descr, location, 'committee_markup',
                                                           start_has_time=start_has_time, chamber='joint')

            for path in ['Senate', 'House']:
                people = meeting_details.xpath('./b[contains(text(), "{}:")]'.format(path))
                cham = 'upper' if path == 'Senate' else 'lower'
                if people:
                    for person in people[0].tail.split(','):
                        person = person.strip()
                        ptype = 'participant'
                        if person.startswith('(C)'):
                            person = person.replace('(C) ', '')
                            ptype = 'chair'
                        event.add_participant(ptype, person, chamber=cham)

            event.add_source(CONFERENCE_URL)
            event.add_participant('host', "Committee of Conference", chamber="joint")
            event.add_related_bill(related_bill_id)

    def _get_post_back_data(self, url, doc=None):
        if not doc:
            doc = lxml.html.fromstring(self.get(url).text)
        return {obj.name: obj.value for obj in doc.xpath(".//input") if obj.name}


def _get_cal_number():
    '''
    NH references calendar days as days since 1/1/2000
    returns the offset # of days for today since then
    '''
    return (datetime.datetime.now().date() - FIXED_DATE.date()).days

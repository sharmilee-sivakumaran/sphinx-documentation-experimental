'''
LA Event Scraper

Effectively three scrapers as there is a house, senate, and joint calendar.

House: http://house.louisiana.gov/H_Sched/Hse_MeetingSchedule.aspx

Scraper will parse the calendar rows on this page, extract committee, location,
and time from the rows, and parse the linked pdf for bills.

Senate: http://senate.la.gov/Committees/default.asp?type=Standing

Iterates across committee links, changing Default.asp to Meetings.asp (for
example: http://senate.la.gov/Insurance/Default.asp to 
http://senate.la.gov/Insurance/Meetings.asp) and iterating through that for
links: https://www.legis.la.gov/legis/Agenda.aspx?m=18787

Joint: http://www.legis.la.gov/legis/ByCmte.aspx

Iterate across the rows for similar links to the Senate
'''

from __future__ import absolute_import

import re
import datetime
import pytz
import logging
import parsedatetime.parsedatetime as pdt
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.extraction.textpdf import get_pdf_text


logger = logging.getLogger(__name__)

def parse_datetime(s, year):
    dt = None
    date_formats = {
        r"%b %d, %I:%M %p": True,
        r"%b %d, %Y, %I:%M %p": True,
        r"%b %d, %Y": True,
        r"%b %d": True
    }

    for f in date_formats:
        try:
            dt = datetime.datetime.strptime(s, f)
            has_time = date_formats[f]
            break
        except ValueError:
            continue
    if dt:
        return dt.replace(year=int(year)), has_time
    else:
        cal = pdt.Calendar()
        result, rtype = cal.parseDT(s)
        dt = result
        has_time = False if rtype<=1 else True
        return dt, has_time

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-LA")
class LAEventScraper(EventScraper):
    jurisdiction = 'la'

    def __init__(self, *args, **kwargs):
        super(LAEventScraper, self).__init__('la', __name__, **kwargs)

    def scrape(self):
        self.scrape_house_weekly_schedule()
        self.scrape_joint_committee_schedule()
        self.scrape_senate_committee_schedule()

        self.save_events_calendar()

    def get_today_as_timezone(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz)

    def scrape_senate_committee_schedule(self):
        committee_list_url = 'http://senate.la.gov/Committees/default.asp?type=Standing'

        committee_list_page = self.get(committee_list_url)
        committee_list_page = committee_list_page.lxml()
        committee_list_page.make_links_absolute(committee_list_url)

        for committee_link in committee_list_page.xpath("//table[@id='table31']//li/a"):
            committee_url = committee_link.attrib['href']
            committee_name = committee_link.text_content().strip()
            committee_meeting_url = re.sub('Default', 'Meetings', committee_url)
            self.scrape_senate_meeting(committee_name, committee_meeting_url)

    def scrape_senate_meeting(self, committee_name, committee_meeting_url):
        committee_meeting_page = self.get(committee_meeting_url)
        committee_meeting_page = committee_meeting_page.lxml()
        committee_meeting_page.make_links_absolute(committee_meeting_url)

        for meeting_link in committee_meeting_page.xpath("//table[@id='table17']//a[contains(@href, 'http://www.legis.la.gov/legis/Agenda.aspx?')]"):
            meeting_url = meeting_link.attrib['href']
            date = meeting_link.getparent().getnext().xpath("./p/b")[0]
            formed_date = datetime.datetime.strptime(date.text_content().strip(), "%m/%d/%Y")
            
            if formed_date.date() < self.get_today_as_timezone().date():
                break
            
            meeting_page = self.get(meeting_url)
            meeting_page = meeting_page.lxml()
            meeting_page.make_links_absolute(meeting_url)

            place = meeting_page.xpath("//span[@id='lLocation']")[0].text_content()
            try:
                time = meeting_page.xpath("//span[@id='lTime']")[0].text_content()
                formated_time = datetime.datetime.strptime(time, '%I:%M %p')
                formed_date = formed_date.replace(hour=formated_time.hour, minute=formated_time.minute)
                has_time = True
            except:
                has_time = False
            if has_time:
                formed_date = self._tz.localize(formed_date)
            desc = '%s Hearing' %committee_name

            event = Event(formed_date, desc, place, 'committee_markup', chamber='upper', start_has_time=has_time)

            event.add_participant('host', committee_name, chamber='upper')
            event.add_source(meeting_url)


            for bill_row in meeting_page.xpath("//a[contains(@href, 'BillInfo.aspx?i=')]"):
                bill_id = bill_row.text_content()
                bill_descr = bill_row.getparent().getnext().getnext().text_content().strip()
                event.add_related_bill(bill_id)

            self.save_event(event)



    def scrape_joint_committee_schedule(self):
        url = "http://www.legis.la.gov/legis/ByCmte.aspx"

        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)

        for link in page.xpath("//a[contains(@href, 'Agenda.aspx')]"):
            self.scrape_meeting(link.attrib['href'])

    def scrape_meeting(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)

        # Check for the presence of each of these fields, they will be absent
        # if the website encounters an internal error.
        title = page.xpath("//a[@id='linkTitle']//text()")
        if title:
            title = title[0]
        else:
            self.warning('Title not found, skipping this meeting.')
            return

        date = page.xpath("//span[@id='lDate']/text()")
        if date:
            date = date[0]
        else:
            self.warning('Date not found, skipping this meeting.')
            return

        time = page.xpath("//span[@id='lTime']/text()")
        if time:
            time = time[0]
        else:
            self.warning('Time not found, skipping this meeting.')
            return

        location = page.xpath("//span[@id='lLocation']/text()")
        if location:
            location = location[0]
        else:
            self.warning('Location not found, skipping this meeting.')
            return

        if ("UPON ADJOURNMENT" in time.upper() or
                "UPON  ADJOURNMENT" in time.upper()):
            return

        substs = {
            "AM": ["A.M.", "a.m."],
            "PM": ["P.M.", "p.m."],
        }

        for key, values in substs.items():
            for value in values:
                time = time.replace(value, key)
        try:
            when = datetime.datetime.strptime("%s %s" % (date, time), "%B %d, %Y %I:%M %p")
            has_time = True
        except ValueError:
            try:
                when = datetime.datetime.strptime("%s %s" % (date, time), "%B %d, %Y %I:%M")
                has_time = True
            except ValueError:
                when, has_time = parse_datetime("%s %s" % (date, time), '')
        if has_time:
            when = self._tz.localize(when)

        description = "Meeting on %s of the %s" % (date, title)
        chambers = {"house": "lower",
                    "senate": "upper",
                    "joint": "joint"}
        chamber = None
        for chamber_, normalized in chambers.items():
            if chamber_ in title.lower():
                chamber = normalized
                break

        if chamber:
            event = Event(when, description, location, 'committee_markup',
                          start_has_time=has_time, chamber=chamber)
            event.add_participant('host', title, chamber=chamber)
        else:
            event = Event(when, description, location, 'committee_markup',
                          start_has_time=has_time)
            event.add_participant('host', title)

        event.add_source(url, urlquote=True)

        trs = iter(page.xpath("//tr[@valign='top']"))
        next(trs)

        for tr in trs:
            try:
                _, _, bill, whom, descr = tr.xpath("./td")
            except ValueError:
                continue

            bill_title = bill.text_content()

            event.add_related_bill(bill_title)

        self.save_event(event)

    def scrape_house_weekly_schedule(self):
        logging.info("Scraping House Weekly Schedule")
        url = "http://house.louisiana.gov/H_Sched/Hse_MeetingSchedule.aspx"
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        now = self.get_today_as_timezone()
        now = now.now()
        for row in page.xpath("//div[@id='linksSCHED']"):
            if len(row.xpath('./div')) < 3:
                continue
            committee = row.xpath('./div[1]/text()')[0]
            time_loc = row.xpath('./div[2]/text()')[0]
            link = row.xpath('./div[3]//a/@href')[0]
            if '.pdf' not in link:
                logger.info('Non-committee link - skipping')
                continue
            logger.info('Scraping committee %s', committee)
            if 'NOT MEETING' in time_loc:
                logger.info('Skipping - not meeting')
                continue
            match = re.match( # Apr 04, 09:00 AM HCR-6
                r'(\w+ \d+, \d+:\d+ [ap]m) (.*)', time_loc, re.IGNORECASE)
            if not match:
                logger.warning('Could not match time/loc: %s', time_loc)
                continue
            dt = datetime.datetime.strptime(match.group(1), '%b %d, %I:%M %p')
            # handle those annoying months on the edge of a year
            closest = None
            for year in range(now.year - 1, now.year + 2): # for 2018, 2017-2019
                if closest is None or abs(now - dt.replace(year=year)) < closest[0]:
                    closest = abs(now-dt.replace(year=year)), dt.replace(year=year)
            dt = closest[1]
            location = match.group(2)

            event = Event(
                self._tz.localize(dt), 'Committee Meeting: ' + committee, location,
                'committee_markup', start_has_time=True, chamber="lower")

            event.add_source(link, urlquote=True)
            bills = re.findall('\s[HSJ][RMBC]\s\d+\s', get_pdf_text(link))
            for bill in bills:
                event.add_related_bill(bill.strip())

            event.add_participant('host', committee, chamber='lower')
            self.save_event(event)


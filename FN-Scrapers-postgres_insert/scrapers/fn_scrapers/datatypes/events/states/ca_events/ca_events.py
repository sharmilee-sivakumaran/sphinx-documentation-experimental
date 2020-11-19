from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import lxml.html
import pytz
import datetime as dt
from dateutil.parser import parse


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-CA")
class CAEventScraper(EventScraper):
    jurisdiction = 'ca'
    event_dict = {}

    def __init__(self, *args, **kwargs):
        super(CAEventScraper, self).__init__('ca', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url).text
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)
        return page

    def scrape(self):
        self.scrape_senate_hearings()
        self.scrape_house_hearings()
        self.save_events_calendar()

    def scrape_senate_hearings(self):
        base_senate_url = "http://senate.ca.gov/calendar?startdate=%s&enddate=%s&committee=&committee-hearings=on"
        today = dt.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        today = today.astimezone(self._tz)
        start_date = "%s-%s-%s" % (today.month, today.day, today.year)

        # Set end day to be today + 30
        end_day = today + dt.timedelta(days=30)
        end_date = "%s-%s-%s" % (end_day.month, end_day.day, end_day.year)

        senate_url = base_senate_url % (start_date, end_date)
        page = self.lxmlize(senate_url)
        event_table = page.xpath("//div[@class='calendarDayHeader list-group-item']")
        for event_block in event_table:
            event_date = event_block.xpath(".//div[@class='calendarDate']")[0].text_content()
            if 'No committee hearings scheduled' in event_block.text_content():
                continue
            event_row = event_block.xpath(".//div[@class='eventContainer']/div[@class='panel panel-default']")
            for event_box in event_row:
                committee_header = event_box.xpath(".//div[@class='panel-heading']")[0].text_content().strip()
                content = event_box.xpath(".//div[@class='panel-content']")[0].text_content().strip()
                chairs = re.findall(r"(.*), Chair", content)
                if chairs:
                    chairs = chairs[0]
                    chairs = re.split('AND', chairs)
                else:
                    chairs = []

                time, location = re.findall(r'(.*) - (.*)', content)[0]
                time = re.findall(r'(?:\d+:)?\d+ [ap]\.m\.', time)
                location = location.strip()
                if time:
                    time = time[0]
                    start_time = True
                    single_event_date = event_date + ' ' + time
                    single_event_date = parse(single_event_date)
                    single_event_date = self._tz.localize(single_event_date)
                else:
                    start_time = False
                    single_event_date = parse(event_date)

                event_id = event_box.xpath(".//div[@class='panel-actions']/button/@data-nid")[0]
                event_url = "http://senate.ca.gov/getagenda?dfid=%s&type=committee" % event_id
                event_detail_page = self.lxmlize(event_url)
                event_page = event_detail_page.text_content()

                subject = re.findall(r"SUBJECT: (.*)", event_page)
                if subject:
                    title = subject[0].strip()
                    title = re.sub(r'\*Pending Receipt', '', title)
                    title = re.sub(r'\"\}', '', title).strip()
                    title = re.split('ADOPTION', title)[0]
                else:
                    title = "Committee Meeting: {}".format(committee_header.strip())
                title = re.sub(r'\s+', ' ', title)

                if 'JOINT HEARING' in committee_header:
                    chamber = 'joint'
                else:
                    chamber = 'upper'

                event = Event(single_event_date,
                              title,
                              location,
                              'committee_markup',
                              chamber=chamber, start_has_time=start_time)
                event.add_source(event_url)

                committee_list = event_box.xpath(".//a[@class='panel-committees']")
                for committee in committee_list:
                    committee_name = committee.text_content()
                    event.add_participant('host', committee_name.strip())

                for people in chairs:
                    people = re.sub(r'SENATOR|(?:ASSEMBLY MEMBER)', '', people).strip()
                    event.add_participant('chair', people)

                bill_ids = re.findall(r"([SHA])\.(?:[CJ]\.)?([ARB])\.\*?No\. (\d+)", event_page)
                for bill_char, bill_type, bill_num in bill_ids:
                    bill_id = "%s%s %s" % (bill_char, bill_type, bill_num)
                    event.add_related_bill(bill_id, type='consideration')

                if (location, single_event_date, title) not in self.event_dict:
                    self.event_dict[(location, single_event_date, title)] = True
                    self.save_event(event)

    def scrape_house_hearings(self):
        house_url = "http://assembly.ca.gov/dailyfile"
        page = self.lxmlize(house_url)

        event_table = page.xpath("//h2[text()='Committee Hearings']/following-sibling::h4")
        for event_block in event_table:
            event_header = event_block.text_content()
            event_header = re.sub(r'\[Edit\]', '', event_header)

            date = event_block.xpath("./preceding-sibling::h5")[-1].text_content()
            event_p = event_block.xpath("./following-sibling::p")[0]

            infor_str = re.split(r'\(', event_p.text_content())[0]
            time, location = re.findall(r'(.*) - (.*)', infor_str)[0]
            time = re.findall(r'(?:\d+:)?\d+ [ap]\.m\.', time)
            location = location.strip()
            if time:
                time = time[0]
                start_time = True
                single_event_date = date + ' ' + time
                single_event_date = parse(single_event_date)
                single_event_date = self._tz.localize(single_event_date)
            else:
                start_time = False
                single_event_date = parse(date)

            if 'HEARING CANCELED' in event_header:
                continue
            if 'Joint Hearing' in event_header:
                chamber = 'joint'
            else:
                chamber = 'lower'
            subject = event_block.xpath("./following-sibling::div[@class='toggle'][1]//span[@class='HearingSubject']")
            if subject:
                title = subject[0].text_content().strip()
                title = re.sub(r'\*Pending Receipt', '', title).strip()
            else:
                subject = event_block.xpath("./following-sibling::div[@class='toggle'][1]//span[@class='HearingTopic']")
                if subject:
                    title = subject[0].text_content().strip()
                else:
                    title = "Committee Meeting: {}".format(event_header.strip())
            title = re.sub(r'\s+', ' ', title)
            event = Event(single_event_date,
                          title,
                          location,
                          'committee_markup',
                          chamber=chamber, start_has_time=start_time)
            event.add_source(house_url)

            committee_list = event_p.xpath("./a[contains(@href, '.assembly.ca.gov')]")
            for committee in committee_list:
                event.add_participant('host', committee.text_content().strip())

            chairs = re.findall(r'(.*),? Chair', event_p.text_content())
            if chairs:
                chairs = chairs[0]
                chairs = re.split(',', chairs)
            else:
                chairs = []
            for people in chairs:
                people = re.sub(r'SENATOR|(?:ASSEMBLY MEMBER)', '', people).strip()
                if people:
                    event.add_participant('chair', people)

            agency = event_block.xpath("./following-sibling::div[@class='toggle'][1]//a[@class='MeasureLink']")
            for bill in agency:
                bill_id = bill.text_content()
                bill_char, bill_type, bill_num = re.findall(r"([SHA])\.(?:[CJ]\.)?([ARB])\.\*?No\. (\d+)", bill_id)[0]
                bill_id = "%s%s %s" % (bill_char, bill_type, bill_num)
                event.add_related_bill(bill_id, type='consideration')

            if (location, single_event_date, title) not in self.event_dict:
                self.event_dict[(location, single_event_date, title)] = True
                self.save_event(event)
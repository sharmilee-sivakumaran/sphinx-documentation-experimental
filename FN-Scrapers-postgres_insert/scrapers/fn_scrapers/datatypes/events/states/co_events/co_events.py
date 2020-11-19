from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from dateutil.parser import parse


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-CO")
class COEventScraper(EventScraper):
    jurisdiction = 'co'

    def __init__(self, *args, **kwargs):
        super(COEventScraper, self).__init__('co', __name__, **kwargs)
        self.urls = set()

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        list_url = "http://leg.colorado.gov/content/committees"
        committee_page = self.get_page(list_url)
        committee_list = committee_page.xpath("//div[@id='block-views-committees-overview-main']//a")
        for committee_ele in committee_list:
            committee_name = committee_ele.text_content()
            committee_url = committee_ele.attrib['href']
            self.scrape_committee(committee_name, committee_url)
        self.save_events_calendar()

    def scrape_committee(self, committee_name, committee_url):
        page = self.get_page(committee_url)
        event_list = page.xpath("//div[@id='block-cga-committees-committee-schedule-items']//a")
        event_dict = {}
        for event in event_list:
            event_url = event.attrib['href']
            if event.attrib['href'] in self.urls:
                continue
            self.urls.add(event.attrib['href'])
            event_page = self.get_page(event_url)
            title = event_page.xpath("//h1")[0].text_content()
            hosts = []
            if 'House' in title and 'Senate' in title:
                chamber = 'joint'
                for com in title.split(';'):
                    if 'House' in com:
                        hosts.append((re.sub(r"House", "", com.strip()), 'lower'))
                    elif 'Senate' in com:
                        hosts.append((re.sub(r"Senate", "", com.strip()), 'upper'))
                    else:
                        hosts.append((com.strip(), 'joint'))
            elif 'Senate' in title:
                chamber = 'upper'
            elif 'House' in title:
                chamber = 'lower'
            else:
                chamber = 'joint'

            date = event_page.xpath("//*[@class='calendar-date']")[0].text_content()
            time_location = event_page.xpath("//*[@class='calendar-details']")[0].text_content()
            try:
                time, location = time_location.split('|')
                time = re.findall(r'\d+:\d+\s+[AaPp][Mm]', time)
            except ValueError:
                """
                CO sources are committee specific, so recreating this situation is difficult.
                Logging a critical so that we know what was missing.
                """
                self.error("Unable to extract time and location from '{}'".format(time_location))
                continue

            start_time = False
            if time:
                date = date + ' ' + time[0].strip()
                start_time = True
            event_date = parse(date)
            event_date = self._tz.localize(event_date)
            location = location.strip()

            # As location is required attribute
            if not location:
                self.warning("Skipping event with title - %s and date - %s as location is not present", title,
                             event_date)
                continue

            descr = "%s Hearing" % title
            key = (event_date, location, descr)
            if key in event_dict:
                continue
            event_dict[key] = True
            event = Event(event_date,
                          descr,
                          location,
                          'committee_markup',
                          chamber=chamber, start_has_time=start_time)
            event.add_source(event_url)

            if hosts:
                for comm, cham in hosts:
                    event.add_participant('host', comm, chamber=cham)
            else:
                event.add_participant('host', committee_name, chamber=chamber)

            bill_list = event_page.xpath("//table[@class='responsive-table']//td[@data-label='Hearing Item']/a")
            for bill in bill_list:
                bill_id = bill.text_content()
                bill_char, bill_id = re.findall(r'(.*?)(\d+.*)', bill_id)[0]
                bill_id = "%s %s" % (bill_char, bill_id)
                event.add_related_bill(bill_id, type='consideration')

            self.save_event(event)
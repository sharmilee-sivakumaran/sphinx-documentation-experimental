from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from dateutil.parser import parse
import lxml.html


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-MA")
class MAEventScraper(EventScraper):
    jurisdiction = 'ma'

    # Works with re.match, might break with re.search
    event_re = re.compile('(\w+) Committee on (.*)', re.I)

    time_re = re.compile('(\d{1,2}):(\d{2}) (\w+)')

    def __init__(self, *args, **kwargs):
        super(MAEventScraper, self).__init__('ma', __name__, **kwargs)
        # Initialize empty dict for unique events
        self.scraped_events = {}

    # Scrape current events and categorize into appropriate session and chamber
    def scrape(self):
        # Get events listing page
        req_url = 'https://malegislature.gov/Events/Hearings'

        page = lxml.html.fromstring(self.get(req_url).text)
        page.make_links_absolute(req_url)

        # TODO: Screen session to see if it matches with current session displayed by page
        link_list = {}
        event_elements = page.xpath('//a[contains(@href, "Events/Hearings/Detail")]')
        for event_ele in event_elements:
            event_link = event_ele.attrib['href']
            if 'class' not in event_ele.attrib or event_ele.attrib['class'] == 'rescheduled':
                continue
            if event_link in link_list:
                continue
            link_list[event_link] = True
            self.scrape_committee_events_new(event_link)

        """
        Save unique events
        """
        for event in self.scraped_events.values():
            self.save_event(event)

        self.save_events_calendar()

    def scrape_committee_events_new(self, link):
        committee_page = lxml.html.fromstring(self.get(link).text)
        committee_page.make_links_absolute(link)
        committee_page_text = committee_page.text_content()
        """
        If MA reschedules and event, they don't remove the old information the website instead,
        they strikethrough the old information, and provide the new information after that
        They use span tags to strikethrough the following text node.
        """
        date = committee_page.xpath("//dt[text()='Event Date:']/following-sibling::dd")[0]
        date_str = ''
        """
        Get the last (most recent) text node
        """
        [None for date_str in date.itertext()]
        if not date_str.strip():
            self.error("Unable to get date from {}".format(link))
            return
        date = date_str

        time_block = committee_page.xpath("//dt[text()='Start Time:']/following-sibling::dd")[0]
        time = ''
        """
        Get the last (most recent) text node
        """
        [None for time in time_block.itertext()]
        if not time:
            time = time_block.xpath("./*")[-1].tail.strip()
        location = re.findall("Location:\s+(.*)", committee_page_text)[0].strip()
        comm_name = committee_page.xpath("//h1")[0].text_content()
        comm_name = re.sub("Hearing Details", "", comm_name).strip()
        comm_chamber = comm_name.split(" ")[0]
        if comm_chamber == 'House':
            comm_chamber = 'lower'
        elif comm_chamber == 'Senate':
            comm_chamber = 'upper'
        else:
            comm_chamber = 'joint'
        has_time = False
        if time:
            date = date + ' ' + time
            has_time = True

        formed_date = parse(date)
        if has_time:
            formed_date = self._tz.localize(formed_date)
        desc = re.findall("Event Description\s+(.*)", committee_page_text)
        if desc:
            desc = desc[0].strip()
        else:
            desc = "Hearing Meeting: %s" % comm_name
        props = (formed_date, desc, location)
        if props not in self.scraped_events:
            event = Event(formed_date, desc, location, 'committee_markup',
                          chamber=comm_chamber, start_has_time=has_time)
        else:
            event = self.scraped_events[props]

        for bill_row in committee_page.xpath("//table[contains(@class, 'agendaTable')]/tbody/tr"):
            try:
                bill_id = bill_row.xpath("td")[0].text_content()
                bill_desc = bill_row.xpath("td")[1].text_content()
                bill_id = re.sub(r"\.", " ", bill_id)
                bill_id = bill_id.replace(u'\u00a0', u'')
                event.add_related_bill(bill_id, 'consideration', description=bill_desc)
            except:
                self.error('Failed to save bill at: ' + link)
        event.add_participant('host', comm_name, chamber=comm_chamber)
        event.add_source(link)

        self.scraped_events[props] = event

    def parse_gmap_link(self, link):
        # Lazily set regex, so it's compiled just once
        try:
            self.gmap_re
        except:
            self.gmap_re = re.compile('http://maps\.google\.com/maps\?daddr=(.*)', re.I)

        addr_raw = self.gmap_re.search(link)
        return addr_raw.group(1) if addr_raw else False
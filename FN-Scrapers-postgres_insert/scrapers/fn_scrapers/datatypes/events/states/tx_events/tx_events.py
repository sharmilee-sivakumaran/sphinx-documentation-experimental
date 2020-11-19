from __future__ import absolute_import
import re
import lxml.html
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from dateutil.parser import parse

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-TX")
class TXEventScraper(EventScraper):
    jurisdiction = 'tx'
    bill_search_re = re.compile(r'([S|J|H][B|M|R] ?\d+)', re.I)

    def __init__(self,*args, **kwargs):
        super(TXEventScraper, self).__init__('tx', __name__ , **kwargs)

    def get_page(self, url):
        page = self.get(url).text
        page = page.replace(u'\xa0', u' ')
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)

        # Convert <br> to newlines to make text processing easier with text_content()
        for br in page.xpath("*//br"):
            br.tail = "\n" + br.tail if br.tail else "\n"

        return page

    def scrape(self):
        for chamber in ["upper", "lower", "joint"]:
            self.scrape_committee_hearings(chamber)

        self.save_events_calendar()

    def scrape_event_page(self, chamber, source_url):
        page = self.get_page(source_url)
        data = page.xpath('//p')
        location = None
        date = None
        for p in data:
            content = p.text_content()
            content = re.sub(r'[\n|\r]', ' ', content)
            content = re.sub(r'\s+', ' ', content)
            content = content.strip()
            if 'COMMITTEE:' in content:
                committee = re.sub("COMMITTEE:", '', content).strip()
            elif 'TIME & DATE:' in content:
                date = re.sub("TIME & DATE:", '', content).strip()
            elif "PLACE:" in content:
                if "CHAIR:" in content:
                    location = re.findall("PLACE:(.*)CHAIR:", content)[0].strip()
                else:
                    location = re.sub("PLACE:", '', content).strip()

        if not location:
            location = 'N/A'

        formed_date = None
        if date:
            date = re.sub(r'or upon adjournment', '', date)
            date = re.sub(r'or upon completion of the local calendar', '', date)
            try:
                formed_date = parse(date)
            except ValueError:
                formed_date = None

        if formed_date is None:
            year, month, day, hour, minute = re.findall(r'/html/.*?\d{3}(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})', source_url)[0]
            date = "%s/%s/%s %s:%s" % (month, day, year, hour, minute)
            formed_date =parse(date)

        bills = self.bill_search_re.findall(page.text_content())
        formed_date = self._tz.localize(formed_date)
        event = Event(formed_date,
                      "Committee Hearing by %s" % committee,
                      location, 'committee_markup', chamber=chamber, start_has_time=True)


        event.add_source(source_url)

        event.add_participant('host', committee, chamber=chamber)

        for bill_id in set(bills):
            event.add_related_bill(bill_id, type='bill', description='Bill up for discussion')
        self.save_event(event)

    def scrape_upcoming_page(self, chamber, url):
        page = self.get_page(url)

        link_lists = page.xpath("//img[@alt='HTML hearing notice']")
        for event_ele in link_lists:
            event_link = event_ele.getparent().attrib['href']
            self.scrape_event_page(chamber, event_link)


    # We scrape from two different sources, the pages for each committee and
    # pages with upcoming events.
    def scrape_committee_hearings(self, chamber):
        chamber_id = {'upper': 'S', 'lower': 'H', 'joint': 'J'}[chamber]

        # Scrape page with upcoming meetings
        meetings_url = 'http://www.capitol.state.tx.us/Committees/MeetingsUpcoming.aspx' + \
                       '?Chamber=' + chamber_id
        self.scrape_upcoming_page(chamber, meetings_url)

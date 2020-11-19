from __future__ import absolute_import
import re
import lxml.html
from datetime import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-ID")
class IDEventScraper(EventScraper):
    jurisdiction = 'id'

    def __init__(self, *args, **kwargs):
        super(IDEventScraper, self).__init__('id', __name__, **kwargs)

    def scrape(self):
        for chamber in ["upper", "lower"]:
            self.scrape_committee_hearings(chamber)

        self.save_events_calendar()
    
    def scrape_committee_hearings(self, chamber):
        base_url = "http://legislature.idaho.gov/sessioninfo/agenda/%sagenda/"

        if chamber == 'upper':
            url = base_url % 's'
        else:
            url =base_url % 'h'
        committee_hearing_page = self.lxmlize(url)
        for agenda_row in committee_hearing_page.xpath("//p/span[contains(text(), 'AGENDA')]"):
            committee_name_ele = agenda_row.getparent().getnext()
            committee_name = committee_name_ele.text_content().strip()
            time_ele = committee_name_ele.getnext()
            while not time_ele.text_content().strip() and time_ele.tag == 'p':
                time_ele = time_ele.getnext()
            time = time_ele.text_content().strip()
            time =re.sub(r'\.', '', time)
            time_match = re.findall(r'\d+:\d+ (?:[AaPp][Mm])?',time)

            if not time_match and time != 'pm' and time != 'am' and "Upon Adjournment" not in time and "After" not in time:
                place = time
                date_ele = time_ele.getnext()

            else:
                if time_match:
                    time = time_match[0]
                place_ele = time_ele.getnext()
                while not place_ele.text_content().strip() and place_ele.tag == 'p':
                    place_ele = place_ele.getnext()
                place = place_ele.text_content().strip()
                date_ele = place_ele.getnext()



            while not date_ele.text_content().strip()  and date_ele.tag == 'p':
                date_ele = date_ele.getnext()
            if date_ele.tag != 'p':
                continue
            date = date_ele.text_content().strip()
            
            try:
                formatted_date = datetime.strptime(date, '%A, %B %d, %Y')
            except ValueError:
                continue

            if time_match:
                try:
                    formatted_time = datetime.strptime(time, '%I:%M %p')
                    formatted_date = formatted_date.replace(hour=formatted_time.hour, minute=formatted_time.minute)
                    formatted_date = self._tz.localize(formatted_date)
                    has_time = True
                except:
                    has_time = False
            else:
                has_time = False
            desc = "%s Hearing" %committee_name
            

            event = Event(formatted_date, desc, place, 'committee_markup', chamber=chamber, start_has_time=has_time)

            event.add_participant('host', committee_name, chamber=chamber)
            event.add_source(url)

            
            table_ele = date_ele.xpath('./following-sibling::div')[0]
            for bill_link in table_ele.xpath(".//a"):
                bill_id = bill_link.text_content().strip()
                check = re.match(r'[HS]([CJ]?[RM])?\s+?\d+.?', bill_id)
                if not check:
                    continue
                if bill_id:
                    event.add_related_bill(bill_id, type="consideration")
            self.save_event(event)

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

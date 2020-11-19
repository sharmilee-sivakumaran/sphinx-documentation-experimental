from __future__ import absolute_import
import datetime as dt
import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

urls = {
    "upper": "http://www.ilga.gov/senate/schedules/weeklyhearings.asp",
    "lower": "http://www.ilga.gov/house/schedules/weeklyhearings.asp"
}

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-IL")
class ILEventScraper(EventScraper):
    jurisdiction = 'il'

    def __init__(self, *args, **kwargs):
        super(ILEventScraper, self).__init__('il', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape_page(self, url, chamber, event_dict):
        page = self.lxmlize(url)
        ctty_name = page.xpath("//span[@class='heading']")[0].text_content()

        tables = page.xpath("//table[@cellpadding='3']")
        info = tables[0]
        rows = info.xpath(".//tr")
        metainf = {}
        for row in rows:
            tds = row.xpath(".//td")
            key = tds[0].text_content().strip()
            value = tds[1].text_content().strip()
            metainf[key] = value

        where = metainf['Location:']
        description = ctty_name

        datetime = metainf['Scheduled Date:']
        datetime = re.sub("\s+", " ", datetime)
        repl = {
            "AM": " AM",
            "PM": " PM"  # Space shim.
        }
        for r in repl:
            datetime = datetime.replace(r, repl[r])

 
        try:
            datetime = dt.datetime.strptime(datetime, "%b %d, %Y %I:%M %p")
            datetime = self._tz.localize(datetime)
            has_time = True
        except:
            datetime = dt.datetime.strptime(datetime, "%b %d, %Y")
            has_time = False

        if (datetime, description, where) in event_dict:
            event = event_dict[(datetime, description, where)]
        else:
            event = Event(datetime, description, where, 'committee_markup', start_has_time=has_time, chamber=chamber)
            ctty_name = re.sub('Hearing Notice For', '', ctty_name)
            event.add_participant('host', ctty_name, chamber=chamber)
        event.add_source(url)

        bills = tables[1]
        for bill in bills.xpath(".//tr")[1:]:
            tds = bill.xpath(".//td")
            if len(tds) < 4:
                continue
            # First, let's get the bill ID:
            bill_id = tds[0].text_content()
            event.add_related_bill(bill_id, 'consideration')

        event_dict[(datetime, description, where)] = event


    def scrape(self):
        for chamber in ["upper", "lower"]:
            try:
                url = urls[chamber]
            except KeyError:
                return  # Not for us.
            page = self.lxmlize(url)
            tables = page.xpath("//table[@width='550']")
            event_dict = {}
            for table in tables:
                meetings = table.xpath(".//a")
                for meeting in meetings:
                    self.scrape_page(meeting.attrib['href'],
                                     chamber, event_dict)
            for event in event_dict:
                self.save_event(event_dict[event])

        self.save_events_calendar()

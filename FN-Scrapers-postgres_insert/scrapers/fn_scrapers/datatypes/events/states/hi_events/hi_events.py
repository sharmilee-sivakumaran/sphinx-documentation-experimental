from __future__ import absolute_import
import datetime as dt
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from .utils import get_short_codes

URL = "http://www.capitol.hawaii.gov/upcominghearings.aspx"
chamber_map = {"lower": "House", "upper": "Senate", "joint": "Joint"}

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-HI")
class HIEventScraper(EventScraper):
    jurisdiction = 'hi'

    def __init__(self, *args, **kwargs):
        super(HIEventScraper, self).__init__('hi', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape_hearings(self):
        get_short_codes(self)

        # Dict where keys are names of notices and values are events.
        notices = {}

        page = self.lxmlize(URL)
        try:
            table = page.xpath("//table[@id='ContentPlaceHolderCol1_GridView1']")[0]
            event_list = table.xpath(".//tr")[1:]
        except IndexError:
            self.error("Failed to scrape meetings")
            return
        for event in event_list:
            tds = event.xpath("./td")
            try:
                committee = tds[0].text_content().strip()
                bills = [x.text_content() for x in tds[1].xpath(".//a")]
                if len(bills) != 1:
                    self.warning("Too many bills in one line!")
                    continue
                bill_id = bills[0]
                when = tds[2].text_content().strip()
                where = tds[3].text_content().strip()
                notice = tds[4].xpath(".//a")[0]
                notice_href = notice.attrib['href']
                notice_name = notice.text
            except IndexError:
                self.warning("Failed to scrape meetings information")
                continue

            notice_key = (when, where, committee)
            # If this is the first time we've seen this notice before, create a new event
            if notice_key not in notices:
                try:
                    when = dt.datetime.strptime(when, "%m/%d/%Y %I:%M %p")
                    when = self._tz.localize(when)
                except ValueError:
                    self.error("Failed to scrape meetings date")
                    continue

                event = Event(when, notice_name, where, 'committee_markup', start_has_time=True)
                event.add_source(URL)
                event.add_document(notice_name, notice_href)

                if "/" in committee:
                    committees = committee.split("/")
                elif "-" in committee:
                    committees = committee.split("-")
                else:
                    committees = [committee]

                committee_names = []
                for committee in committees:
                    if committee == 'INFO':
                        continue
                    if "INFO" not in committee and committee in self.short_ids.keys():
                        committee = self.short_ids[committee]
                    else:
                        committee = {
                            "chamber": "joint",
                            "name": committee,
                        }

                    event.add_participant('host', committee['name'], chamber=committee['chamber'])
                    committee_names.append(chamber_map[committee['chamber']] + " Committee on " + committee['name'])

                if committee_names:
                    title = "Commitee Hearing: " + " ,".join(committee_names)
                    event["description"] = title

                notices[notice_key] = event

            # If we've seen the notice before, get it from the notices dict so we can add this bill to it
            else:
                event = notices[notice_key]
            links = event['documents']
            if notice_href not in [link['url'] for link in links]:
                event.add_document(notice_name, notice_href)
            event.add_related_bill(bill_id, "consideration")

        return notices

    def scrape(self):
        notices = self.scrape_hearings()

        if not notices:
            return

        for notice in notices:
            self.save_event(notices[notice])

        self.save_events_calendar()


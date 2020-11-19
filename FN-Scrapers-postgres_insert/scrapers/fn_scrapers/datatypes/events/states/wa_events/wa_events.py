from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import dateutil.parser


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-WA")
class WAEventScraper(EventScraper):
    jurisdiction = 'wa'

    def __init__(self, *args, **kwargs):
        super(WAEventScraper, self).__init__('wa', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):

        daily_agendas_url = "http://app.leg.wa.gov/mobile/CommitteeAgendas/Starting?AgendaType=0"
        daily_agendas_page = self.get_page(daily_agendas_url)

        day_urls = daily_agendas_page.xpath(
            u"//a[contains(@href, 'http://app.leg.wa.gov/mobile/CommitteeAgendas/Agendas?AgendaType=0&StartDate=')]"
            u"/@href")

        for day_url in day_urls:
            page = self.get_page(day_url)

            agendas = page.xpath("//a[contains(@href,'/mobile/CommitteeAgendas/Agenda/')]")
            for comm_hearings in agendas:
                doc = self.get_page(comm_hearings.attrib['href'])
                description = ""
                for loc in doc.xpath("//li[contains(@class,'partialagendaitems')]/ul/li[contains(@class,'wordWrap')]"):
                    description += loc.text_content()

                if "Joint Select Committee" in comm_hearings.text_content() or "Joint Committee" in comm_hearings.text_content() or \
                        "Jt Legislative Task Force" in comm_hearings.text_content() or "(J)" in comm_hearings.text_content():
                    meeting_chamber = "joint"
                    if "(O)" in comm_hearings.text_content():
                        titl = comm_hearings.text_content()
                        self.log("%s has been tagged as joint", titl)
                elif "(H)" in comm_hearings.text_content():
                    meeting_chamber = "lower"
                elif "(S)" in comm_hearings.text_content():
                    meeting_chamber = "upper"
                else:
                    titl = comm_hearings.text_content()
                    self.log("%s has been ignored due to inability to determine chamber", titl)
                    continue

                location = re.findall(r"[^\n]+\n[^\n]+\n[^,\n]+, WA \d{5}", description)
                if not location:
                    self.log("%s has been ignored due to lack of meeting location", description)
                    continue
                location = location[0]
                location = re.sub(r'\r\n', ' ', location)
                location = re.sub(r'\s+', ' ', location)
                heading = doc.xpath("//ul/li")[0].text_content()
                heading = heading.split(" - ")
                comm = heading[0].strip()
                date_str = re.findall(r'\d+\/\d+\/\d+ \d+:\d+:\d+ \w{2}', heading[1])[0]
                date = dateutil.parser.parse(date_str)
                date = self._tz.localize(date)
                related_bills = doc.xpath("//a[contains(@href,'/mobile/BillSummary/Details')]")

                desc = "%s Hearing" % comm
                event = Event(date, desc, location, 'committee_markup', start_has_time=True, chamber=meeting_chamber)
                for bills in related_bills:
                    bill = bills.text_content()
                    bill_id = bill.split("-")[0]
                    bill_type = "bill"
                    event.add_related_bill(bill_id, type=bill_type)

                event.add_source(comm_hearings.attrib['href'])
                event.add_participant('host', comm, chamber=meeting_chamber)
                self.save_event(event)
        self.save_events_calendar()
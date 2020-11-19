from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
from fn_scrapers.api.scraper import scraper, tags
import datetime

url = "https://iga.in.gov/legislative/%s%s/committees/standing"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-IN")
class INEventScraper(EventScraper):
    jurisdiction = 'in'

    def __init__(self, *args, **kwargs):
        super(INEventScraper, self).__init__('in', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url, verify=False)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        sessions = get_active_sessions('in', meta=self.metadata, get_internal_id=True)
        for session in sessions:
            s_type = ''
            if not session.endswith('r'):
                s_type = session[8:]
            session_url = url % (session[:4], s_type)
            page = self.get_page(session_url)

            senate_committee_list = page.xpath("//div[@id='senate_standing-committees-group']//li")
            for senate_committee_row in senate_committee_list:
                try:
                    committee_info = senate_committee_row.xpath("./a")[0]
                    committee_name = committee_info.text_content()
                    committee_url = committee_info.attrib['href']

                    self.scrape_committee(committee_name, committee_url, "upper", session)
                except IndexError:
                    self.warning("Failed to scrape one senate committee")

            house_committee_list = page.xpath("//div[@id='house_standing-committees-group']//li")
            for house_committee_row in house_committee_list:
                try:
                    committee_info = house_committee_row.xpath("./a")[0]
                    committee_name = committee_info.text_content()
                    committee_url = committee_info.attrib['href']
                    self.scrape_committee(committee_name, committee_url, "lower", session)
                except IndexError:
                    self.warning("Failed to scrape one house committe")

        self.save_events_calendar()

    def scrape_committee(self, committee_name, committee_url, chamber, session):
        committee_page = self.get_page(committee_url)
        meeting_table = committee_page.xpath("//div[contains(@id, 'agenda-item')]")
        for meeting_row in meeting_table:
            if 'CANCELLED' in meeting_row.text_content():
                continue
            try:
                time_place_ele = meeting_row.xpath(".//a[contains(@class, 'accordion-toggle')]")
                time_place = time_place_ele[0].text_content()
            except IndexError:
                self.error("Failed to scrape meeting time and place")
                continue
            time_group = re.split(r'\n', time_place)
            time_group = [x.strip() for x in time_group if x.strip()]
            if len(time_group) == 0:
                self.error("No time and place info")
                continue
            date = time_group[0]
            if len(time_group) >= 3:
                place = time_group[2]
                match = re.match(r'\d+:\d+[ap]m', time_group[1])
                if match:
                    date = date + ' ' + time_group[1]
                else:
                    place = time_group[1] + ' ' + place
            elif len(time_group) == 2:
                place = time_group[1]

            try:
                norm_date = datetime.datetime.strptime(date, "%b. %d, %Y %I:%M%p")
                norm_date = self._tz.localize(norm_date)
                has_time = True
            except ValueError:
                try:
                    norm_date = datetime.datetime.strptime(date, "%B %d, %Y %I:%M%p")
                    norm_date = self._tz.localize(norm_date)
                    has_time = True
                except ValueError:
                    try:
                        norm_date = datetime.datetime.strptime(date, "%b. %d, %Y")
                        has_time = False
                    except ValueError:
                        try:
                            norm_date = datetime.datetime.strptime(date, "%B %d, %Y")
                            has_time = False
                        except ValueError:
                            self.error("Failed to scrape date")
                            continue

            event_desc = "%s meeting at %s" % (committee_name, place)
            event = Event(norm_date, event_desc, place, 'committee_markup',
                          start_has_time=has_time, chamber=chamber, session=session)

            event.add_source(committee_url)
            event.add_participant('host', committee_name, chamber=chamber)
            try:
                agenda = meeting_row.xpath(".//a[@class='hidden-print ico-pdf cmte-open-link pull-left']")[0]
                agenda_url = agenda.attrib['href']
                event.add_document('agenda', agenda_url)
                if not has_time:
                    doc_id = re.findall(r'\/documents\/(.*)', agenda_url)[0]
                    event_desc = "%s [Meeting Angenda ID: %s]" % (event_desc, doc_id)
                    event['description'] = event_desc
            except IndexError:
                self.warning("Failed to get Agenda")

            bill_link = meeting_row.xpath(".//a[contains(text(),'Open Packet')]")[0]
            bill_url = bill_link.attrib['href']
            bills_page = self.get_page(bill_url)
            bills_table = bills_page.xpath("//div[@id='js-accordion-container']//div[@class='accordion-inner']")
            for bill_row in bills_table:
                bill_group = re.findall(r'[HS]B \d+', bill_row.text_content().strip())
                if bill_group:
                    bill_id = bill_group[0]
                    bill_descr = bill_row.text_content().strip()
                    bill_descr = re.sub(r'[\n\t\s]+', ' ', bill_descr)
                    event.add_related_bill(bill_id, description=bill_descr, type='consideration')

            self.save_event(event)
"""
Iowa EventScraper
"""
from __future__ import absolute_import
import re
import datetime
import pytz
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event

url = "https://www.legis.iowa.gov/committees"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-IA")
class IAEventScraper(EventScraper):
    """
    Iowa Event scraper
    Scrape events for Iowa
    """
    jurisdiction = 'ia'
    def __init__(self, *args, **kwargs):
        super(IAEventScraper, self).__init__('ia', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url, verify=False)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def get_today_as_timezone(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz).date()

    def scrape_subcommittee(self, chamber, committee_url, committee_name):
        committee_page = self.lxmlize(committee_url)
        meetings_table = committee_page.xpath('//table[@id="sortableTable"]//tr')
        event_dict = {}
        for meeting_row in meetings_table[1:]:
            subcommittee_link = meeting_row.xpath(".//li/a[contains(@href,'/subCommMeeting?meetingID=')]")

            if not subcommittee_link:
                continue
            date = subcommittee_link[-1].text_content().strip()
            try:
                norm_date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M %p")
                norm_date = self._tz.localize(norm_date)
                has_time = True
            except ValueError:
                date_str = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date.strip())
                if date_str:
                    norm_date = datetime.datetime.strptime(date_str.group(1), "%m/%d/%Y")
                    has_time = False
                else:
                    self.error("Failed to scrape date")
                    continue
            if norm_date.date()< self.get_today_as_timezone():
                continue
            subcommittee_url = subcommittee_link[0].attrib['href']

            bill_id = meeting_row.xpath("./td")[0].text_content().strip()
            bill_doc = meeting_row.xpath("./td[1]/a")[0].attrib['href']
            sub_meeting_page = self.lxmlize(subcommittee_url)

            try:
                location = sub_meeting_page.xpath("//label[text()='Location:']")[0].getparent().getnext().text_content()
            except IndexError:
                self.warning("Failed to scrape location")
                location = 'N/A'
            else:
                if not location:
                    location = 'N/A'

            key = (norm_date, location)

            if key in event_dict:
                event = event_dict[key]
            else:
                desc = "Subcommittee Meeting Hearing: %s" % committee_name
                event = Event(norm_date, desc, location, 'committee_markup', start_has_time=has_time, chamber=chamber)
                event.add_source(subcommittee_url)
                event.add_participant('host', committee_name, chamber=chamber)

            event.add_related_bill(bill_id, 'consideration')
            event.add_document(bill_id, bill_doc)
            event_dict[key] = event
        for event_key in event_dict:
            self.save_event(event_dict[event_key])

    def scrape(self):
        page = self.lxmlize(url)
        try:
            upper_list = page.xpath("//h3[contains(text(), 'Senate')]")[0].getnext()
            for committee in upper_list.xpath("./li/a"):
                committee_name = committee.text_content()
                committee_url = committee.attrib['href']
                subcommittee_url = re.sub(r'committee\?', 'subCommitteeAssignments?', committee_url)
                committee_url = re.sub(r'committee\?', 'meetings/meetingsListComm?', committee_url)

                self.scrape_committee("upper", committee_url, committee_name)
                self.scrape_subcommittee("upper", subcommittee_url, committee_name)
        except IndexError:
            self.error("Failed to scrape upper committee info")
        try:
            upper_list = page.xpath("//h3[contains(text(), 'House')]")[0].getnext()
            for committee in upper_list.xpath("./li/a"):
                committee_name = committee.text_content()
                committee_url = committee.attrib['href']
                subcommittee_url = re.sub(r'committee\?', 'subCommitteeAssignments?', committee_url)
                committee_url = re.sub(r'committee\?', 'meetings/meetingsListComm?', committee_url)

                self.scrape_committee("lower", committee_url, committee_name)
                self.scrape_subcommittee("lower", subcommittee_url, committee_name)
        except IndexError:
            self.error("Failed to scrape lower committee info")

        self.save_events_calendar()

    def scrape_committee(self, chamber, committee_url, committee_name):
        committee_page = self.lxmlize(committee_url)
        meetings_table = committee_page.xpath('//table[@id="sortableTable"]/tbody/tr')
        for row in meetings_table:
            cols = row.xpath('./td')
            if len(cols) == 0:
                self.error("Failed to scrape meeting info")
                continue
            status = cols[0].xpath('.//strong')
            if len(status) > 0 and status[0].text_content().strip() == 'Cancelled':
                continue

            date = cols[0].xpath('./span')[0].text_content()
            try:
                norm_date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M %p")
                norm_date = self._tz.localize(norm_date)
                has_time = True
            except ValueError:
                date_str = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date.strip())
                if date_str:
                    norm_date = datetime.datetime.strptime(date_str.group(1), "%m/%d/%Y")
                    has_time = False
                else:
                    self.error("Failed to scrape date")
                    continue

            if norm_date.date() < self.get_today_as_timezone():
                continue

            try:
                location = cols[1].text_content()
            except IndexError:
                self.warning("Failed to scrape location")
                location = 'N/A'
            else:
                if not location:
                    location = 'N/A'

            try:
                desc = cols[2].text_content()
                desc = re.sub('\r\n', ' ', desc).strip()
                desc = re.sub(r'\s+', ' ', desc).strip()
                if not desc:
                    desc = '%s Hearing' % committee_name
            except IndexError:
                self.warning("Failed to scrape description")
                desc = '%s Hearing' % committee_name
            event = Event(norm_date, desc, location, 'committee_markup', start_has_time=has_time, chamber=chamber)
            event.add_source(committee_url)
            event.add_participant('host', committee_name, chamber=chamber)
            try:
                agenda_urls = cols[2].xpath('.//a')
                for agenda_url in agenda_urls:
                    doc_url = agenda_url.attrib['href']
                    if "=<a href=" in doc_url:
                        continue

                    bill_id = agenda_url.text_content()
                    bill_id = re.sub(r"\s+", " ", bill_id).strip()
                    """
                    The bill IDs are of the form
                    - HSB 123
                    - SSB 123
                    - HCR 123
                    - HJR 123
                    - SCR 1231
                    - SJR 1231
                    - HF 1231
                    - HR 123
                    - SF 123
                    - SR 123
                    """
                    bill_id = re.match(r"([HS](?:[CJ](?=R))?(?:[FR]|SB)\s*\d+)", bill_id)
                    if bill_id:
                        event.add_related_bill(bill_id.group(1), 'consideration')
            except IndexError:
                self.warning("Failed to scrape agenda")

            # Kind of hacky, but gets rid of exact duplicates, which are definitely a mistake on IA's part
            if event not in self._events:
                self.save_event(event)

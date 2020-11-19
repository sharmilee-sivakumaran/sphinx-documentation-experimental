from __future__ import absolute_import
import re
from datetime import datetime
from csv23 import DictReader
from contextlib import closing
import urllib2
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.common.extraction.textpdf import get_pdf_text
from fn_scrapers.datatypes.events.common.utils import get_page, get_today_as_timezone
from six.moves.urllib.parse import urljoin
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-CT")
class CTEventScraper(EventScraper):
    jurisdiction = 'ct'

    def __init__(self, *args, **kwargs):
        super(CTEventScraper, self).__init__('ct', __name__, **kwargs)

    def scrape(self):
        for (code, name) in self.get_comm_codes():
            self.scrape_committee_events(code, name)

        self.save_events_calendar()

    def scrape_committee_events(self, code, name):
        home_url = 'http://www.cga.ct.gov/'
        location_key_words = 'ROOM|HOUSE CHAMBER|SENATE CHAMBER|HALL OF THE HOUSE'

        # Visit committee meeting agendas page
        committee_url = 'http://www.cga.ct.gov/asp/menu/CommDocList.asp?comm_code=' + code + '&doc_type=ca'
        self.log("Committee url: " + committee_url)
        committee_agendas_page = get_page(committee_url)
        # Get committee name
        try:
            new_name = committee_agendas_page.xpath('/html/body//div[@class="container"]//h2')[0].text_content()
            extra_text = "Meeting Agendas - {}".format(get_today_as_timezone(self._tz).year)
            new_name = new_name.replace(extra_text, "").strip()
            if new_name != name:
                self.warning("Committee names do not match: %s != %s", new_name, name)

            committee_name = new_name
        except:
            committee_name = name
        # Get links to all committee meetings (their agenda page)
        # css class - anibutton anibutton-date-full
        committee_agendas_links = committee_agendas_page.cssselect('a.anibutton.anibutton-date-full')

        # Visit every committee meeting agenda page
        for committee_agenda_link in committee_agendas_links:
            # Get date
            date = committee_agenda_link.text_content().strip()

            try:
                date = datetime.strptime(date, "%B %d, %Y %I:%M %p")
                date = self._tz.localize(date)
            except:
                try:
                    date = datetime.strptime(date, "%B %d, %Y")
                except:
                    self.warning('Could not scrape event; can\'t parse datetime')
                    continue

            if date.date() < get_today_as_timezone(self._tz):
                self.info("Skipped old date: %s", date.date())
                continue

            agenda_url = urljoin(home_url , committee_agenda_link.get('href'))
            self.info("Agenda url: %s", agenda_url)
            related_bills = set()
            # Get location
            location = 'N/A'
            if '.pdf' in agenda_url.lower():
                committee_agenda_page = get_pdf_text(agenda_url)
                str_io = StringIO(committee_agenda_page)
                for possible_location_text in str_io:
                    if re.search(location_key_words, possible_location_text, flags=re.I):
                        location = re.sub('\d+:\d+\s+(AM|PM) IN ', '', possible_location_text, flags=re.I).strip()
                        break
                
                # Get/set bills
                for possible_related_bill in str_io:
                    # Get bill ID
                    bill_id_parts = re.search('([HS]\.[RB]\.) No\. (\d+)', possible_related_bill)

                    # This line doesn't contain text in the format of a bill
                    if not bill_id_parts:
                        continue

                    bill_id = bill_id_parts.group(1) + ' ' + bill_id_parts.group(2)
                    bill_id = re.sub('\.', '', bill_id)
                    related_bills.add(bill_id)

            else:
                committee_agenda_page = get_page(agenda_url)
                possible_location_elements = committee_agenda_page.xpath('//p[@style="text-align: center"]')
                for possible_location_element in possible_location_elements:
                    possible_location_text = possible_location_element.text_content()
                    if re.search(location_key_words, possible_location_text, flags=re.I):
                        location = re.sub('\d+:\d+ (AM|PM) IN ', '', possible_location_text, flags=re.I).strip()
                        break
                # Get/set bills
                possible_related_bills = committee_agenda_page.xpath('//a')
                for possible_related_bill in possible_related_bills:
                    # Get bill ID
                    bill_id = possible_related_bill.text_content()
                    bill_id_parts = re.search('([HS]\.[RB]\.) No\. (\d+)', bill_id)

                    # This link doesn't contain text in the format of a bill
                    if not bill_id_parts:
                        continue

                    bill_id = bill_id_parts.group(1) + ' ' + bill_id_parts.group(2)
                    bill_id = re.sub('\.', '', bill_id)
                    related_bills.add(bill_id)

            # Set up event object
            event = Event(date, "Committee Hearing: {}".format(committee_name), location, 'committee_markup', start_has_time=True)

            for bill_id in related_bills:
                event.add_related_bill(bill_id, type='consideration')

            # Get/set source
            event.add_source(agenda_url)

            # All committees are joint committees in CT
            event.add_participant('host', committee_name, chamber="joint")
            self.save_event(event)

    def get_comm_codes(self):
        url = "ftp://ftp.cga.ct.gov/pub/data/committee.csv"
        with closing(urllib2.urlopen(url)) as file_obj:
            for row in DictReader(file_obj):
                yield row['comm_code'].strip(), row['comm_name'].strip()
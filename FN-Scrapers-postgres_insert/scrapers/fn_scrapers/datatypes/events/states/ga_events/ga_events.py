from __future__ import absolute_import
import re
import lxml.html
from datetime import datetime
import traceback
import sys

from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.common.extraction.textpdf import convert_pdf
from fn_scrapers.api.scraper import scraper, tags


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-GA")
class GAEventScraper(EventScraper):
    jurisdiction = 'ga'
    time_re = re.compile('\d{1,2}:\d{2} ?[apm]{2}', re.I)
    agenda_url_re = re.compile('href="(.*?)">.*?AGENDA')
    agenda_url_re_2 = re.compile('href="(.*?)">.*?Agenda')
    bill_re = re.compile('([sh]\.?j?[br]\.? ?\d+)(.*)', re.I)

    def __init__(self, *args, **kwargs):
        super(GAEventScraper, self).__init__('ga', __name__, **kwargs)

    def scrape(self):
        for chamber in ['upper', 'lower']:
            self.scrape_committee_hearings(chamber)

        self.save_events_calendar()

            
    def scrape_committee_hearings(self, chamber):
        base_url = 'http://calendar.legis.ga.gov/Calendar/default.aspx?chamber='
        source_url = base_url + {'lower' : 'house', 'upper' : 'senate'}[chamber]

        source_page = lxml.html.fromstring(self.get(source_url).text)

        source_page.make_links_absolute(source_url)

        # Get divs with event data. Each element contains event info for a date.
        content = source_page.xpath('//*[@class="cssDay"]')
        if len(content) == 0:
            return

        for child in content:
            # Get and set date
            date_ele = child.xpath('.//*[@class="cssDateLabel"]')
            date_ele = date_ele[0] if len(date_ele) > 0 else None
            if date_ele is None:
                continue

            date_text = date_ele.text_content().strip()
            date = datetime.strptime(date_text, '%A, %B %d, %Y')
            date = self._tz.localize(date)
            # Get meetings on date
            meetings = child.xpath('.//*[@class="cssMeetings"]')
            meetings = meetings[0].getchildren() if meetings else None
            if meetings is None:
                continue

            # Iterate through meetings
            for meeting in meetings:
                # Get and set time
                time_ele = meeting.xpath('./*[@class="cssMeetingTime"]')
                time_ele = time_ele[0] if len(time_ele) > 0 else None
                if time_ele is None:
                    continue
                time_text = time_ele.text_content().strip()
                if self.time_re.search(time_text):
                    time = datetime.strptime(time_text, '%I:%M %p')
                    date = date.replace(hour=time.hour, minute=time.minute)
                    has_time = True
                else:
                    has_time = False

                # Get and set location
                location_ele = meeting.xpath('./*[@class="cssMeetingLocation"]')
                location_ele = location_ele[0] if len(location_ele) > 0 else None
                if location_ele is None:
                    location = 'N/A'
                else:
                    location = location_ele.text_content().strip()
                    if not location:
                        location = 'N/A'
                # Get meeting host and source
                subject_ele = meeting.xpath('.//*[@class="cssSubjectLink"]')
                subject_ele = subject_ele[0] if len(subject_ele) > 0 else None
                if subject_ele is None:
                    continue
                event_url = subject_ele.get('href')

                event_html = self.get(event_url).text
                committee_name = subject_ele.text_content().strip()
                if any(committee_name.endswith(canceled) for canceled in ["CANCELLED", "CANCELED"]):
                    continue
                event_page = lxml.html.fromstring(event_html)

                try:
                    description_path = event_page.xpath(".//b[contains(text(), 'AGENDA')]")

                    desc = re.findall(r'AGENDA.*?: (.*)', description_path[0].getparent().text_content())[0].strip()
                except:
                    desc = "%s Hearing" % committee_name
                if not desc.strip():
                    desc = "%s Hearing" % committee_name
                event = Event(date, desc, location, 'committee_markup', chamber=chamber, start_has_time=has_time)

                event.add_source(event_url)

                # Get/set participants
                event.add_participant('host', committee_name, chamber=chamber)

                # Get event's unique page to get PDF agenda, and then bills from the PDF
                agenda_url = self.agenda_url_re.search(event_html)
                agenda_url = agenda_url.group(1) if agenda_url else None
                if not agenda_url:
                    agenda_url = self.agenda_url_re_2.search(event_html)
                    agenda_url = agenda_url.group(1) if agenda_url else None
                if agenda_url is not None:
                    # Get PDF and temporarily store it
                    try:
                        agenda_filename, response = self.urlretrieve(agenda_url)
                        # Read PDF
                        agenda_text = convert_pdf(agenda_filename, type='text')

                        # Search lines for bill IDs and assume any found is a related bill
                        lines = agenda_text.split('\n')
                        related_bills = []
                        for line in lines:
                            search = self.bill_re.search(line)
                            if search:
                                bill_id = search.group(1).strip()
                                if bill_id in related_bills:
                                    continue
                                related_bills.append(bill_id)
                                bill_description = search.group(2).replace('\xad', '')

                                if bill_description.strip() == '':
                                    bill_description = 'N/A'
                                event.add_related_bill(bill_id, type='bill', description=bill_description)
                    except Exception as e:
                        self.warning(traceback.format_exc())
                        self.warning("Failed to scrape related bill")

                self.save_event(event)


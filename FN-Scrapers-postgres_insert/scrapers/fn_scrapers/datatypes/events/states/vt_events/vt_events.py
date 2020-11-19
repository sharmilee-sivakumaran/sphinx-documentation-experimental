from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import datetime as dt
from pytz import utc
import json
import lxml.html
from requests.exceptions import HTTPError


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-VT")
class VTEventScraper(EventScraper):
    jurisdiction = 'vt'

    def __init__(self, *args, **kwargs):
        super(VTEventScraper, self).__init__('vt', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        self.scrape_committee_page()
        self.save_events_calendar()

    def scrape_committee_page(self):
        """
        Getting the date today in VT local time
        This makes it easier to handle cases around new year, as we get the
        correct year according to the timezone.
        """
        today = dt.datetime.utcnow().replace(tzinfo=utc)
        today = today.astimezone(self._tz)
        year = today.year
        if year % 2 != 0:
            year += 1
        url = "http://legislature.vermont.gov/committee/loadAllMeetings/%s" % year
        page = self.get_page(url)
        meeting_list = page.text_content()
        meeting_data = json.loads(meeting_list)

        meeting_dict = []
        for meeting in meeting_data['data']:
            date = meeting['MeetingDate'].strip()
            time = meeting['StartTime'].strip()
            if not time or 'm' not in time.lower():
                time = meeting['TimeSlot'].strip()
            if time:
                time = re.sub(r'\.', '', time)
                time_content, mark = re.findall(r'(.*\d+)(.*)', time)[0]
                time = "%s %s" % (time_content, mark.strip())
            place = meeting['BuildingName'] + ' ' + meeting['Room'] + ' ' + meeting['RoomNbr']
            place = re.sub(r'\s+', ' ', place)
            place = place.strip()
            if place == '':
                place = meeting['AlternateRoomLocation'].strip()
            if place == '':
                place = 'N/A'

            if time and time.strip() not in {'1', 'tba'}:
                has_time = True
                try:
                    norm_date = dt.datetime.strptime("%s %s" % (date, time), "%A, %B %d, %Y %I:%M %p")
                except ValueError:
                    try:
                        norm_date = dt.datetime.strptime("%s %s" % (date, time), "%A, %B %d, %Y %I %p")
                    except ValueError:
                        self.warning("Failed to scrape time: %s %s" % (date, time))
                        time = None
                        has_time = False
                        try:
                            norm_date = dt.datetime.strptime(date, "%A, %B %d, %Y")
                        except ValueError:
                            self.error("Failed to scrape date and time")
                            continue
            else:
                has_time = False
                try:
                    norm_date = dt.datetime.strptime(date, "%A, %B %d, %Y")
                except ValueError:
                    self.error("Failed to scrape date")
                    continue
            if place.strip() == "":
                place = "N/A"

            event_key = (norm_date, meeting['LongName'], place)
            if event_key in meeting_dict:
                continue
            meeting_dict.append(event_key)

            if has_time:
                norm_date = self._tz.localize(norm_date)

            event = Event(norm_date, 'Meeting Notice: {}'.format(meeting['LongName']),
                          place, 'committee_markup', start_has_time=has_time)

            chambers_list = {"house": "lower",
                             "senate": "upper",
                             "joint": "joint"}

            try:
                committee_chamber = meeting['CommitteeType'].split(' ')[0]
                host_chamber = chambers_list[committee_chamber.lower()]
            except (KeyError, IndexError):
                try:
                    cham_index = re.split(r'\s+', meeting['LongName'])[0]
                    host_chamber = chambers_list[cham_index.lower()]
                except (KeyError, IndexError):
                    self.warning("Failed to scrape committee chamber, set it as other")
                    host_chamber = "other"

            event.add_participant('host', meeting['LongName'], chamber=host_chamber)

            doc_url = "http://legislature.vermont.gov/committee/detail/%s/%s" % (year, meeting['PermanentID'])
            event.add_document("document", doc_url)
            event.add_source(doc_url)
            if meeting['AgendaName'] and meeting['AgendaName'] != u'0':
                agenda_url = "http://legislature.vermont.gov/committee/agenda/%s/%s" % (
                year, meeting['AgendaName'].strip())
                event.add_document("Agenda", agenda_url)
                if '.pdf' not in agenda_url:
                    try:
                        agenda_page = self.get(agenda_url).text
                        agenda_page = lxml.html.fromstring(agenda_page)
                        agenda_page.make_links_absolute(agenda_url)
                    except HTTPError:
                        self.warning("Failed to scrape agenda page")
                        continue

                    bill_dict = {}
                    for title_line in agenda_page.xpath("//table[@id='table']//span"):
                        text = title_line.text_content().strip()
                        text = re.sub(r'\r\n', ' ', text)
                        text = re.sub(r'\s+', ' ', text)
                        bill_group = re.findall(r'([HS]. \d+)\s+-\s+(.*)', text)
                        for bill_row in bill_group:
                            bill_id = bill_row[0]
                            if bill_id in bill_dict:
                                continue
                            bill_dict[bill_id] = True
                            event.add_related_bill(bill_id, type="consideration")
            self.save_event(event)
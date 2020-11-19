"""
TN EventScraper
"""
from __future__ import absolute_import
import datetime as dt
import re
import pytz
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from tempfile import NamedTemporaryFile
from fn_scrapers.common.http import request, request_file
from fn_scrapers.common.extraction.textpdf import convert_pdf

cal_weekly_events = "http://wapp.capitol.tn.gov/apps/schedule/WeeklyView.aspx"
cal_chamber_text = {
    "Senate": "upper",
    "House": "lower",
    "Joint":  "joint"
}

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-TN")
class TNEventScraper(EventScraper):
    """
    TN EventScraper
    Used to scrape Tennessee Events
    """
    jurisdiction = 'tn'

    def __init__(self, *args, **kwargs):
        super(TNEventScraper, self).__init__('tn', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def get_today_as_timezone(self):
        today = dt.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz).date()

    def url_xpath(self, url, xpath):
        page = self.get_page(url)
        return page.xpath(xpath)

    def _add_agenda_main(self, url, event):
        page = self.get_page(url)
        # OK. We get three kinds of links. Either a list to a bunch of agendas
        # or actually a list of agendas or a link to a video feed.
        # Need to check for a span id = Title, if it's present then we're at the actual agenda
        span = page.xpath("//span[@id='Title']")
        if span:
            return self._add_agenda_real(url, event)
        else:
            return self._add_agenda_list(url,event)

    def _add_agenda_real(self, url, event):
        lis = self.url_xpath(url, "//li")
        for li in lis:
            billinf = li.attrib['id']  # TN uses bill_ids as the id
            event.add_related_bill(
                billinf,
                "consideration"
            )
        event.add_source(url)
        event.add_document("Agenda", url)
        return event

    def _add_agenda_list(self, url, event):
        trs = self.url_xpath(url, "//tr")
        for tr in trs:
            things = tr.xpath("./td/a")
            for thing in things:
                event = self._add_agenda_real(thing.attrib['href'], event)
        return event

    def _add_agenda_pdf(self, name, url, event):
        pdftext = self.get_pdf_text(url)
        bill_ids = re.findall(r"\d\. ([SH](?:JR|B) \d{4})", pdftext)
        for bill_id in bill_ids:
            event.add_related_bill(bill_id, "consideration")
        event.add_document(name, url)
        return event

    def add_agenda(self, url, name, event):

        if not name:
            if 'schedule' in url.lower() or 'calendar' in url.lower():
                name = 'Calendar'
            else:
                name = 'Agenda'

        if "CalendarMain" in url or "VideoCalendarmain" in url:
            return self._add_agenda_main(url, event)
        elif ".pdf" in url and "ScheduleDocs" in url:
            return self._add_agenda_pdf(name, url, event)
        else:
            return event.add_document(name, url)

    def scrape_week(self, url):
        found_events = False
        page = self.get_page(url)
        event_dict = {}
        tables = page.xpath("//table[@class='date-table']")
        for table in tables:
            try:
                date = table.xpath("../.")[0].getprevious().text_content().strip()
            except IndexError:
                self.error("Failed to scrape date")
                continue
            trs = table.xpath("./tr")
            for tr in trs:
                order = ["time", "chamber", "type", "agenda", "location",
                         "video"]

                tds = tr.xpath("./td")
                metainf = {}

                if not tds:
                    continue

                for el in range(0, len(order)):
                    metainf[order[el]] = tds[el]

                time = metainf['time'].text_content()
                datetime_string = "%s %s" % (date, time)
                datetime_string = datetime_string.replace('\r\n', ' ').strip()
                location = metainf['location'].text_content()
                description = metainf['type'].text_content()
                chamber_string = metainf['chamber'].text_content()
                if chamber_string not in cal_chamber_text:
                    self.warning("Could not match chamber {}".format(chamber_string))
                else:
                    chamber = cal_chamber_text[chamber_string]

                if re.search('[Cc]ancell?ed', time):
                    self.log("Skipping cancelled event.")
                    continue
                else:
                    dtfmt = "%A, %B %d, %Y %I:%M %p"
                    has_time = True
                    relative_re = re.compile(r'(?i)(TBA|Immediately Follows.*)')
                    time_string = None
                    if relative_re.search(datetime_string):
                        time_string = relative_re.search(datetime_string).group(1)
                        datetime_string = relative_re.sub('', datetime_string).strip()

                        datetime_string = datetime_string.strip()
                        dtfmt = "%A, %B %d, %Y"
                        has_time = False

                    when = dt.datetime.strptime(datetime_string, dtfmt)
                    #only scrape events in the future
                    if when.date() < self.get_today_as_timezone():
                        continue
                    if has_time:
                        when = self._tz.localize(when)

                
                if (when, description, location) in event_dict:
                    continue
                event = Event(when, description, location, 'committee_markup',
                              start_has_time=has_time, chamber=chamber)

                if time_string:
                    event['start_time_description'] = time_string

                if "Floor Session" not in description:
                    event["description"] += " Hearing"
                    event.add_participant("host", description, chamber=chamber)
                event.add_source(url)

                agenda = metainf['agenda'].xpath(".//a")
                if len(agenda) > 0:
                    agenda = agenda
                    for doc in agenda:
                        agenda_url = doc.attrib['href']
                        self.add_agenda(agenda_url, doc.text_content(), event)

                event_dict[(when, description, location)] = event
                self.save_event(event)

                found_events = True

        # To get the link to the next week, get the day after the latest day on the page, and append it to the url
        if found_events:
            last_date = page.xpath("//span[@id='lblSunday']/text()")[0]
            date = dt.datetime.strptime(last_date.strip(), "%A, %B %d, %Y")
            date += dt.timedelta(days=1)
            next_week_date = date.strftime("%m/%d/%Y")
            next_week_url = cal_weekly_events + "?&date={}".format(next_week_date)
            return next_week_url

        else:
            return None

    def scrape(self):

        # Keep scraping each week of the calendar until we hit a week with no events
        next_week_url = cal_weekly_events
        while next_week_url:
            next_week_url = self.scrape_week(next_week_url)

        self.save_events_calendar()

    def get_pdf_text(self, url, type='text', **kwargs):
        file_obj = NamedTemporaryFile()
        file_obj, resp = request_file(url, file_obj=file_obj, **kwargs)
        return convert_pdf(file_obj.name, type)
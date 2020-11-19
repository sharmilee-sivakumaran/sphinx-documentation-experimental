from __future__ import absolute_import
import re
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.extraction.textpdf import get_pdf_text
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.datatypes.events.common.utils import get_page, get_today_as_timezone


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-AR")
class AREventScraper(EventScraper):
    """
    Scraper workflow:

    AR uses a calendar type webpage to display all events. Previously this scraper used a
    LegislativeMeasures.txt CSV file to find all bills for the current session and used
    scraped events from the bill pages themseleves, but now the events are not being listed
    on the bill pages, so using the calendar page itself.

    Workflow:

    1. The scraper goes to the following URL:
       http://www.arkleg.state.ar.us/assembly/2017/2018F/Pages/MeetingsAndEventsCalendar.aspx?listview=month

       The lists events in the coming month from starting from the current date.
    2. The page that we GET has a different structure than what you see using the inspect
       element in the browsers (See the saved HTML files in resources folder). The scraper
       looks for the event table on that page
        table.dxgvControl <-- CSS selector for event table
    3. The scraper looks for a the date rows
        tr.dxgvGroupRow <-- CSS selector for date rows
    4. The scraper goes through each row between two date rows
       in this table and extracts the required information
        td[2] <-- Start Time
        td[3] <-- Location
        td[4] <-- Committee Name
        td[5] <-- Link to Agenda PDFs

        The element td[1] is a blank element.
    5. We only consider rows if the committee name `td` element has an `a` tag in it.
    6. If the agenda PDF has bill IDs in it, they are extracted and attached to the event.
    7. Save each event, and finally save the calendar.

    To execute tests:
        pytest scrapers/ar
    """
    jurisdiction = 'ar'
    session_numbers = {
            "First": 1,
            "Second": 2,
            "Third": 3,
            "Fourth": 4
        }
    def __init__(self, *args, **kwargs):
        super(AREventScraper, self).__init__('ar', __name__, **kwargs)

        self.scraped_links = set()
        self.scraped_events = set()

    def scrape_slug(self, fn_session):
        """
        Returns the slug of the fn_session. We categorize sessions by year whereas AR categorizes them by 2 years.
        Because of this, what we treat as special session 1 in 2018 is special session 2 in 2017-2018.
        This scrapes the site for the historical sessions, and transforms our sessions into the session slug to parse
        the rest of their site.
        :param fn_session: session in the form of 20182018ss1
        :return: slug in the form of 2018S2, 2018R, etc.
        """
        year = fn_session[0:4]
        if "r" in fn_session:
            return year + "F" if int(year) % 2 == 0 else year + "R"

        elif "ss" in fn_session:
            session_num = int(fn_session[-1])

            session_url = "http://www.arkleg.state.ar.us/SearchCenter/Pages/historicalbil.aspx"
            session_site = get_page(session_url)

            sessions = session_site.xpath(
                "//td[contains(., 'Extraordinary Session') and contains(@class, 'dxtl dxtl__B0')]")
            for session in sessions:
                # At least for now. The sessions on the site are ordered from oldest to newest for each assembly
                # From this we can just pick the xth element that matches our date
                site_num = re.match("(\S+)\sExtraordinary.+{}".format(year), session.text_content())
                if site_num:
                    session_num -= 1
                    if session_num == 0:
                        return "{}S{}".format(year, self.session_numbers[site_num.group(1)])
            raise ValueError("Could not find session {} on session site".format(fn_session))
        else:
            # Shouldn't ever reach this point
            raise NotImplementedError("Unhandled session_name value: {}".format(fn_session))

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            self.slug = self.scrape_slug(session)
            term_year = int(self.slug[:4])

            if term_year % 2 == 0:
                term_year -= 1

            url = "http://www.arkleg.state.ar.us/assembly/" +\
                  "{}/{}/Pages/MeetingsAndEventsCalendar.aspx?listview=month".format(
                    term_year,
                    self.slug,
                   )

            page = get_page(url, html_parser='html5')
            for event in self.scrape_events(page, url):
                self.save_event(event)
        self.save_events_calendar()

    def scrape_events(self, page, url, download_files=True):
        table = page.cssselect('table.dxgvTable')
        if table:
            table = table[0]
            dates = table.cssselect('tr.dxgvGroupRow')
            # HTML5 introduces a tbody element in the elemtn tree for a table tag
            tbody = next(table.iterchildren(tag='tbody'))
            indices = map(lambda x: tbody.index(x), dates)
            min_date_ind = indices[0]
            assert len(dates) == len(indices)
            date_dict = {i: d for i, d in zip(indices, dates)}
            cur_date = None
            """
            The events are visibly grouped by date, but they are just separated by date rows
            inside a single table. Each date row's following siblings are event rows untill the next
            date row is encountered. The following loop uses the indices of the date rows to
            logically group together events and provide them a start date.
            """
            for ind, tr in enumerate(table.xpath('./tbody/tr')):
                """
                The first row is simply the header
                """
                if ind >= min_date_ind:
                    if ind in date_dict:
                        cur_date = ' '.join([t.strip() for t in date_dict[ind].itertext() if t.strip()]).strip()
                    elif cur_date:
                        _, time, location, committee, agenda = tr.xpath('./td')
                        time = ' '.join([t for t in time.itertext() if t.strip()]).strip()
                        if re.search(r'(?:cancel{1,2}ed)|(?:no meeting)', time, flags=re.I):
                            # Ignoring events that are cancelled or have no meetings scheduled
                            continue
                        if committee.find('./a') is not None:
                            committee = ' '.join([t.strip() for t in committee.itertext() if t.strip()]).strip()
                        else:
                            # The page has many events, but we are only interested in committee related events
                            continue
                        ids = None
                        a = agenda.find('./a')
                        location = ' '.join([t.strip() for t in location.itertext() if t.strip()]).strip()
                        if not location and a is not None:
                            # Haven't encountered this case yet, but if and event has an agenda, but no
                            # location, then it N/A will be assigned as location
                            location = 'N/A'
                        elif not location:
                            continue
                        if time:
                            start = '%s %s' % (cur_date, time)
                            try:
                                start = parse(start)
                                start_has_time = True
                                start = self._tz.localize(start)
                            except ValueError as e:

                                start = parse(cur_date)
                                start_has_time = False
                                time_desc = time
                                self.warning('Unable to parse time from {}, {}'.format(time, url))
                        if re.search(r'senate', committee, flags=re.I):
                            chamber = 'upper'
                        elif re.search(r'house', committee, flags=re.I):
                            chamber = 'lower'
                        else:
                            chamber = 'joint'

                        # only scrape events in the future
                        if start.date() < get_today_as_timezone(self._tz):
                            continue

                        props = (start, committee, location)
                        if props in self.scraped_events:
                            continue
                        if start_has_time:
                            event = Event(
                                start,
                                '%s MEETING' % committee,
                                location,
                                'committee_hearing',
                                start_has_time=start_has_time,
                                chamber=chamber
                            )
                        else:
                            event = Event(
                                start,
                                '%s MEETING' % committee,
                                location,
                                'committee_hearing',
                                start_has_time=start_has_time,
                                chamber=chamber,
                                start_time_description=time_desc
                            )
                        if a is not None and a.get('href') in self.scraped_links:
                            continue
                        event.add_source(url if a is None else a.get('href'))
                        event.add_participant('host', committee, chamber=chamber)
                        if a is not None:
                            if download_files:
                                self.add_bill_ids_from_file(a.get('href'), event)
                                self.scraped_links.add(a.get('href'))
                        self.scraped_events.add(props)
                        yield event

    def add_bill_ids_from_file(self, url, event):
        """
        This function downloads a PDF agenda files and calls the get_bill_ids
        function to extract and add the bill IDs to the event object
        """
        pdftext = get_pdf_text(url)
        bills = set(self.get_bill_ids(pdftext))
        for bill in bills:
            event.add_related_bill(bill, type='consideration')

    def get_bill_ids(self, pdftext):
        """
        This is a generator for all Bill IDs in the provided PDF filename
        """
        IDs = re.findall(r"([SH](?:[CMJ]|CM)?[BR]\s*\d+)", pdftext)
        for b_id in IDs:
            yield b_id


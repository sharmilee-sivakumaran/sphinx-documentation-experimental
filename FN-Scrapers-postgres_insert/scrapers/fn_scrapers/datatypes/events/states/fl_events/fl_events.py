from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.metadata import get_session_from_internal_id,\
    _get_active_sessions as get_active_sessions
from fn_scrapers.common.extraction.textpdf import get_pdf_text
from dateutil.parser import parse
import datetime
import pytz
import parsedatetime.parsedatetime as pdt

"""
FL considers some committee bills in each event along with legislative bills, they are usually of the form
PCB ACP 18-02 etc.

Even though the current database doesn't has these bills, the scraper should still send them just in case they
are added later.
Legislative bills are of the form:
HB 1231
HCR 1231
HJR 1231
HM 1231
HR 1231
SB 1231
SCR 1231
SJR 1231
SM 1231
SPB 1231
SR 1231

Bills in special sessions often have bill IDs like above but end with alphabets A, B, C, or D as well.
Examples:
http://www.myfloridahouse.gov/Sections/Committees/meetingnotice.aspx?MeetingId=11347&SessionId=85&CommitteeId=2892
"""
bill_re = re.compile(r"((?:[HS](?:[CJ](?=R)|P(?=B))?[BRM]\s+?\d+(?:-?[ABCD])?)|(?:PCB\s+?[A-Z]{3,4}\s+?\d{2}-\d{2}))")


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-FL")
class FLEventScraper(EventScraper):
    """
    Scraper workflow:

    We hit three different sites for FL leg events. One for Senate,
    one for House, and one for Joint.

    Lower (House):
    1. We go to myfloridahouse.gov committee list page.
    2. We find the term id, which is the integer representing the two year session container that
       FL uses to organize its sessions. We then navigate to the committee list page for that term.
    3. From this page, we grab the session id for the specific session in question.
       For example, let's say we are scraping events for 20152015ss1.
       In legislative term 86 (2014-2016) we want session id 79 (Special Session 2015A)
    4. We get the numerical id for that committee, and navigate to the page for that
       committee within that session. For example, let's say we are scraping 20182018r, and
       we want the Agriculture & Natural Resources Appropriations Subcommittee.
       We go to:
       http://www.myfloridahouse.gov/Sections/Documents/publications.aspx? \
       CommitteeId=2893&PublicationType=Committees&DocumentType=Meeting%20Notices&SessionId=86
    4. We then go to each meeting notice page and get the location, time, description, and list of bills
    5. We then save the event.

    Upper (Senate):
    1. We go to flsenate.gov and get the list of Senate committee urls
       NOTE: There is only data for some sessions, and so we check to make sure the session in question is
             in the dropdown. Otherwise, we get redirected back to the latest session.
    2. We then append the session name (2017A or 2018) to the url in order to get the list of
       events for that committee from the given session.
       Example: http://flsenate.gov/Committees/Show/AFT/2018
    3. For each event, we get the date and time.
    4. Then we navigate to the pdf agenda for the event to get the location and the bill ids.
    5. We then save the event.

    Joint:
    1. While scraping the committees from the Senate site (see above), if we get to a joint committee,
       we scrape that differently.
    2. Joint committee pages are on a totally different domain, and the url looks like this:
       http://www.leg.state.fl.us/cgi-bin/View_Page.pl? \
       File=index_css.html&Tab=committees&Directory=committees/joint/JCLA/
    3. We've never seen a joint committee event for a Special Session, so we only scrape regular session
       joint events.
    4. We parse all the data from the PDF meeting documents. While we're at it, we check to make sure
       that the text 'Regular Session' appears in the pdf. If not, then we have found our first Special Session
       joint agenda. We log an error and return in that case.
    5. We then save the event.

    """
    jurisdiction = 'fl'

    def __init__(self, *args, **kwargs):
        super(FLEventScraper, self).__init__('fl', __name__, **kwargs)

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            # Session name looks like: Regular Session 2016 or Special Session 2017A
            # That final part of the name, which is the year plus a letter, is what we use
            # to find the right session.
            session_name = get_session_from_internal_id(self.jurisdiction, session, self.metadata)["name"]
            session_name = session_name.split()[-1]
            self.scrape_lower(session, session_name)
            self.scrape_upper_and_joint(session, session_name)
        self.save_events_calendar()

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def get_today_as_timezone(self):
        today = datetime.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz).date()

    def _fetch_pdf_lines(self, href):
        """
        fetch lines from pdf file
        :param href: href of pdf
        :return pdflines: list of lines in pdf context
        """
        try:
            pdflines = [line.decode('utf-8') for line in get_pdf_text(href).splitlines()]
        except ValueError:
            pdflines = [line.decode('latin-1') for line in get_pdf_text(href).splitlines()]
        return pdflines

    def scrape_lower(self, session, session_name):
        url = "http://www.myfloridahouse.gov/Sections/Committees/committees.aspx"
        committee_page = self.get_page(url)
        # FL organizes its sessions into two year containers. The ranges listed actually
        # include the second year, but exclude the first. For example, '2014-2016' is actually
        # data for sessions in 2015 and 2016.
        options = committee_page.xpath("//div[@class='c_SessionSelection']/select/option")
        for option in options:
            opt_text = option.text
            year_range = re.search(r"(\d{4})\s-\s(\d{4})", opt_text)
            years = {int(year_range.group(1)) + 1, int(year_range.group(2))}
            if int(session[:4]) in years:
                term_id = option.attrib["value"]
                break
        else:
            self.error("Could not find committee list for session %s", session)
            return

        # First we get the committee list for the two year session container
        url = "http://www.myfloridahouse.gov/Sections/Committees/committees.aspx?LegislativeTermId=%s" % term_id
        committee_year_page = self.get_page(url)
        # Then we grab the specific session ID for the session in question, either regular or special
        session_selector = committee_year_page.xpath("//select[@id='ddlBillFinderSession']/option")
        for sess in session_selector:
            if sess.text.endswith(session_name):
                session_id = sess.attrib["value"]
                break
        else:
            self.error("Could not find committee list for session %s", session)
            return

        notice_url = "http://www.myfloridahouse.gov/Sections/Documents/publications.aspx?CommitteeId=%s&" \
                     "PublicationType=Committees&DocumentType=Meeting Notices&SessionId=%s"
        committee_list = committee_year_page.xpath("//div[@class='c_ActiveCommittees']//a")
        for committee_row in committee_list:
            committee_name = committee_row.text_content().strip()
            if "Joint" in committee_name:
                continue
            committee_link = committee_row.attrib['href']
            committee_id = re.findall(r'CommitteeId=(\d+)', committee_link)[0]
            notice_link = notice_url % (committee_id, session_id)
            notice_page = self.get_page(notice_link)
            notice_list = notice_page.xpath("//a[contains(@href, 'meetingnotice.aspx')]")
            for notice in notice_list:
                meeting_url = notice.attrib['href']
                meeting_page = self.get_page(meeting_url)
                # If there are no notices for this committee in this session, we skip it
                if meeting_page.xpath("//span[@id='lblNoResults']"):
                    self.info("%s: No hearings found for %", session, committee_name)
                    continue
                try:
                    start_string = meeting_page.xpath("//span[@id='ctl00_ContentPlaceHolder1_Label1']")[
                        0].getparent().getnext().text_content().strip()
                    start_date = parse(start_string)
                    if not start_date:
                        self.warning("Failed to get meeting start date")
                        continue
                    else:
                        if start_date.date() < self.get_today_as_timezone():
                            continue
                except:
                    self.warning("Failed to get meeting date")
                    continue

                # location
                try:
                    location = meeting_page.xpath("//span[@id='ctl00_ContentPlaceHolder1_Label3']")[
                        0].getparent().getnext().text_content().strip()
                except:
                    self.warning("Failed to read meeting location")
                    location = 'N/A'

                # description
                try:
                    desc = meeting_page.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblOverview']")[
                        0].text_content().strip()
                    desc = re.sub(r'\r+', ' ', desc)
                    if not desc:
                        desc = "Hearing"
                    desc = committee_name + " " + desc
                    if "[NOT MEETING]" in desc:
                        continue
                    if not desc:
                        raise Exception
                except:
                    # case http://www.myfloridahouse.gov/Sections/Committees/meetingnotice.aspx? \
                    # MeetingId=10852&SessionId=80&CommitteeId=2827
                    # some meetings don't have a summary which causes a pillar rejection as a consequence
                    # Grab the text of the considered bills as a backup description
                    try:
                        regex_ns = "http://exslt.org/regular-expressions"
                        desc = ""
                        bill_list = meeting_page.xpath(
                            "//span[re:test(@id, 'ctl00_ContentPlaceHolder1_rptOutput_ctl\d+_lblText', 'i')]",
                            namespaces={'re': regex_ns})
                        for bill in bill_list:
                            desc += bill.text_content()
                    except:
                        self.warning("Failed to get meeting description")
                        desc = "%s Hearing" % committee_name

                event = Event(self._tz.localize(start_date), desc, location, 'committee_markup',
                              start_has_time=True, chamber="lower", session=session)
                event.add_source(meeting_url)
                event.add_participant('host', committee_name, chamber='lower')

                # bills
                try:
                    regex_ns = "http://exslt.org/regular-expressions"
                    bill_list = meeting_page.xpath(
                        "//span[re:test(@id, 'ctl00_ContentPlaceHolder1_rptOutput_ctl\d+_lblText', 'i')]",
                        namespaces={'re': regex_ns})
                    for bill in bill_list:
                        bill_detail = bill.text_content().split("--")
                        bill_id = bill_detail[0].strip()
                        # some bills will have a CS number as well, this will throw off pillar bill lookup
                        # detect and remove
                        if "CS/" in bill_id:
                            bill_id = bill_id.replace("CS/", "")
                        match = bill_re.search(bill_id)
                        if match:
                            event.add_related_bill(match.group(1), 'consideration')

                except:
                    self.warning("Failed to read meeting bill")

                self.save_event(event)

    def scrape_upper_and_joint(self, session, session_name):
        agenda_url = "http://flsenate.gov/Committees/#com-list"
        page = self.get_page(agenda_url)

        upper_committees = page.xpath("//ul[@class='fls_list']")[0].xpath("./li//a")
        for com in upper_committees:
            committee_url = com.attrib['href']
            self.scrape_upper_agenda(committee_url, session, session_name)
        joint_tables = page.xpath("//ul[@class='fls_list']")[1:]
        if len(joint_tables) > 0:
            for table in joint_tables:
                for joint_com in table.xpath("./li//a"):
                    joint_name = joint_com.text
                    committee_url = joint_com.attrib['href']
                    if u"Joint" not in joint_name:
                        self.scrape_upper_agenda(committee_url, session, session_name)
                    else:
                        # We've never seen joint committee events in special sessions, so we
                        # need to assume right now that these are only in regular sessions.
                        if session.endswith(u"r"):
                            self.scrape_joint_events(committee_url, session)

    def scrape_upper_agenda(self, url, session, session_name):
        url = url + session_name
        committee_page = self.get_page(url)

        # We only have a few sessions on the Senate page. If we try to scrape one that
        # doesn't exist, we should error out.
        session_names = committee_page.xpath("//select[@id='session-name']/option/@value")
        if session_name not in session_names:
            self.error("Could not find Senate data for session %s", session)
            return

        committee_name = committee_page.xpath('//h2[@class="committeeName"]')[0].text_content()
        meeting_rows = committee_page.xpath('//table[@id="meetingsTbl"]//tr')[1:]
        for row in meeting_rows:
            if 'No Meeting Records at this time' in row.text_content():
                return
            elif 'Not meeting' in row.text_content() or \
                    ("Date" in row.text_content() and "Time" in row.text_content()):
                continue
            date = row.xpath('./td')[0].text_content() + ' ' + row.xpath('./td')[1].text_content()
            date = self.parse_datetime(date)
            if date.date() < self.get_today_as_timezone():
                continue
            notice_url = row.xpath(".//a[@class='mtgrecord_notice']")[0].attrib['href']

            pdflines = self._fetch_pdf_lines(notice_url)
            full = '\n'.join(pdflines)
            content = re.sub(r'\n', ' ', full)
            content = re.sub(r'\s+', ' ', content)

            place = re.findall(r'(?m)PLACE: (.*?) (?:AMENDMENT|MEMBERS:)', content)[0]

            place = place.strip()

            desc = "%s Hearing" % committee_name
            event = Event(date, desc, place, 'committee_markup',
                          start_has_time=True, chamber="upper", session=session)
            event.add_source(url)

            bill_group = bill_re.findall(full)
            for bill_id in bill_group:
                event.add_related_bill(bill_id, 'consideration')
            event.add_participant("host", committee_name, chamber="upper")
            self.save_event(event)

    def scrape_joint_events(self, url, session):
        committee_page = self.get_page(url)
        meeting_link = committee_page.xpath('//a[contains(text(), "Committee Meeting Records")]')
        if not meeting_link:
            return
        meeting_url = meeting_link[0].attrib['href']
        committee_name = committee_page.xpath("//div[@id='title']/h1")[0].text_content()
        self.scrape_joint_agenda(meeting_url, session, committee_name)

    def scrape_joint_agenda(self, url, session, committee_name):
        committee_page = self.get_page(url + "&sessionYear={}".format(session[:4]))
        meeting_rows = committee_page.xpath("//a[contains(text(), 'Meeting Notice')]")
        bill_line = r'((?:CS/)?SP?B \d+(?:-[ABCD])?) by (.*?)--(.*?)\n'
        for row in meeting_rows:
            notice_url = row.attrib['href']
            pdflines = self._fetch_pdf_lines(notice_url)
            full = '\n'.join(pdflines)

            # The agenda pdf contains the session in the header. If the session is not regular,
            # as mentioned above, we don't know how to handle it. So in that case, we throw an error.
            if u"Regular Session" not in full:
                self.error(u"%s: Got joint agenda for Special Session at url '%s'",
                           session, notice_url)
                continue

            content = re.sub(r'\n', ' ', full)
            content = re.sub(r'\s+', ' ', content)

            date = re.findall(r'(?m)MEETING DATE: (.*) TIME:', content)[0]
            time = re.findall(r'(?m)TIME: (.*) PLACE:', content)[0]

            place = re.findall(r'(?m)PLACE: (.*) (?:SENATE)? MEMBERS:', content)[0]

            if time:
                if 'p.m.' in time or 'pm' in time:
                    time = time.split('--')[0].strip() + ' pm'
                else:
                    time = time.split('--')[0].strip() + ' am'
            date_time = self.parse_datetime(date.strip() + " " + time.strip())
            if date_time.date() < self.get_today_as_timezone():
                continue
            desc = "%s Hearing" % committee_name
            event = Event(date_time, desc, place, 'committee_markup',
                          start_has_time=True, chamber="joint", session=session)
            event.add_source(url)

            bill_group = re.findall(bill_line, full)
            for (bill_id, sponsor, descr) in bill_group:
                event.add_related_bill(bill_id, 'consideration')
            event.add_participant("host", committee_name, chamber='joint')
            self.save_event(event)

    def parse_datetime(self, s):
        dt = None

        date_formats = ["%m/%d/%Y %I:%M %p",
                        "%A, %B %d, %Y %I:%M %p"
                        ]

        for f in date_formats:
            try:
                dt = datetime.datetime.strptime(s, f)
                break
            except:
                continue

        if dt:
            return self._tz.localize(dt)
        else:
            cal = pdt.Calendar()
            result, rtype = cal.parseDT(s)
            return self._tz.localize(result)
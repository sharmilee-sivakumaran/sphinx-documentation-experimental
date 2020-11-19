from __future__ import absolute_import
import re
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.datatypes.events.common.utils import get_page, get_today_as_timezone
from fn_scrapers.api.scraper import scraper, tags

AL_BASE_URL = 'http://alisondb.legislature.state.al.us/Alison/'
ORDINAL_NUMBERS = ["First", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth", "Ninth", "Tenth"]


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-AL")
class ALEventScraper(EventScraper):
    jurisdiction = 'al'
    _session = None
    def __init__(self, *args, **kwargs):
        super(ALEventScraper, self).__init__('al', __name__, **kwargs)

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            self._set_session(session)
            base_committee_url = "http://alisondb.legislature.state.al.us/alison/CommitteeMeetings.aspx?BODYID=%s"
            chamber_code_dict = {'upper': '1753',
                                 'lower': '1755'
                                }
            meeting_dict = {}
            for chamber in ['upper', 'lower']:
                committee_url = base_committee_url % chamber_code_dict[chamber]
                doc = get_page(committee_url)
                meeting_doc = doc.xpath('//table[@id="ContentPlaceHolder1_gvCommittees"]/tr[@class="CommGrid"]')

                for meeting_row in meeting_doc:
                    meeting_ele = meeting_row.xpath('./td')
                    date = meeting_ele[0].text_content()
                    committee = meeting_ele[1].text_content()
                    location = meeting_ele[2].text_content().strip()
                    if not location:
                        location = 'N/A'
                    # Currently, cancelled is only seen in location
                    elif "cancelled" in location.lower():
                        continue
                    time = meeting_ele[3].text_content()
                    time = re.sub(r'- .*', '', time).strip()
                    time_str = re.sub(r'[,\.]', '', time)
                    time = re.findall(r'\d+:\d+\s?[PpAa][Mm]', time_str)

                    if not time:
                        time = re.findall(r'\d+\s?[PpAa][Mm]', time_str)

                    if not time and 'noon' in time_str.lower():
                        time = ["12:00 pm"]

                    start_has_time = False
                    if time:
                        time = time[0]
                        date = "%s %s"% (date, time)
                        start_has_time = True

                    formed_date = parse(date)
                    formed_date = self._tz.localize(formed_date)
                    #only scrape events in the future
                    if formed_date.date() < get_today_as_timezone(self._tz):
                        continue

                    description = "Commiittee Meeting: %s" % committee
                    bill_list = meeting_row.xpath('.//input')
                    if (formed_date, committee, location) in meeting_dict:
                        event = meeting_dict[(formed_date, committee, location)]
                    else:
                        event = Event(formed_date, description, location, 'committee_markup',
                                      start_has_time=start_has_time, chamber=chamber, session=session)
                        event.add_source(committee_url)
                        event.add_participant('host', committee, chamber=chamber)

                    for bill in bill_list:
                        bill_id = bill.attrib['value']
                        """
                        Wrote the following regex based on the bills we have in the DB:
                        - SJR
                        - HB
                        - HJR
                        - SB
                        - HR
                        - SR
                        """
                        match = re.search(r"([HS](?:JR|B|R)\s*?\d+)", bill_id)
                        if match:
                            event.add_related_bill(match.group(1))
                    meeting_dict[(formed_date, committee, location)] = event

            for key in meeting_dict:
                self.save_event(meeting_dict[key])
        self.save_events_calendar()

    def _get_view_state(self, lxml_doc):
        vsg = dict(
            __VIEWSTATE= lxml_doc.xpath('//input[@id="__VIEWSTATE"]/@value')[0],
            __VIEWSTATEGENERATOR=lxml_doc.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value')[0]
        )
        return vsg

    def _set_session(self, session):
        ''' Activate an ASP.NET session, and set the legislative session '''
        set_session_url = AL_BASE_URL + 'SelectSession.aspx'
        self._session = session
        doc = get_page(set_session_url)
        
        #grabs a list of session names, then finds the one we're looking for
        sessions = []
        session_rows = doc.xpath('//tr/td')
        for item in session_rows:
            sessions.append(item.text_content())

        event_arg = None
        name = self._get_session_name(session)
        for x in range(0,len(sessions)):
            if sessions[x] == name:
                event_arg = x
                break

        #got these arguments by messing around with the site using a web proxy
        post_data = {'__EVENTTARGET': 'ctl00$ContentPlaceHolder1$gvSessions',
            '__EVENTARGUMENT':'${}'.format(event_arg),
            'ctl00$ScriptManager1':'ctl00$UpdatePanel1|ctl00$ContentPlaceHolder1$gvSessions',
            '__ASYNCPOST' : True}
        post_data.update(self._get_view_state(doc))
        

        self.post(set_session_url, data=post_data, allow_redirects=False)

    def _get_session_name(self, session):
        year = session[:4]
        if "r" in session:
            session_type = "Regular Session"
        else:
            session_number = int(session[-1])
            session_type = "{} Special Session".format(ORDINAL_NUMBERS[session_number - 1])
        return "{} {}".format(session_type, year)


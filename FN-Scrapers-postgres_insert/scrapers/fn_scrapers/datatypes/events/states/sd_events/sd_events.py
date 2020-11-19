from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.metadata import get_session_from_internal_id, container_for_session,\
    _get_active_sessions as get_active_sessions
from .session_details import session_details
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from dateutil.parser import parse

url = "http://www.sdlegislature.gov/Legislative_Session/Committees/default.aspx?Session=%s"

# uncomment following url to do test
# url = "http://web.archive.org/web/20150313222925/
# http://legis.sd.gov/Legislative_Session/Committees/default.aspx?Session=%s"
# url = "http://web.archive.org/web/20150116222335/
# http://legis.sd.gov/Legislative_Session/Committees/default.aspx?Session=%s"
# url = "http://web.archive.org/web/20150130223041/
# http://legis.sd.gov/Legislative_Session/Committees/default.aspx?Session=%s"
# url = "http://web.archive.org/web/20150227222324/
# http://legis.sd.gov/Legislative_Session/Committees/default.aspx?Session=%s"


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-SD")
class SDEventScraper(EventScraper):
    jurisdiction = 'sd'

    def __init__(self, *args, **kwargs):
        super(SDEventScraper, self).__init__('sd', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            external_id = get_session_from_internal_id("sd", session, self.metadata)["external_id"]
            start_year = container_for_session("sd", external_id, self.metadata)["start_year"]

            session_name = session_details[str(start_year)]['slug']
            page = self.get_page(url % session_name)

            """
            The first result of the XPath is ignore because it refers to the House of Representatives page
            and not to a specific committee of House
            """
            house_committee_list = page.xpath("//div[@id='ctl00_ContentPlaceHolder1_Committees_divCommittees']" +
                                              "/a[starts-with(., 'House ')]")[1:]

            senate_committee_list = page.xpath("//div[@id='ctl00_ContentPlaceHolder1_Committees_divCommittees']" +
                                               "/a[starts-with(., 'Senate ')]")

            joint_committee_list = page.xpath("//div[@id='ctl00_ContentPlaceHolder1_Committees_divCommittees']" +
                                              "/a[starts-with(., 'Joint')]")

            self.scrape_chamber('lower', session, house_committee_list)
            self.scrape_chamber('upper', session, senate_committee_list)
            self.scrape_chamber('joint', session, joint_committee_list)

        self.save_events_calendar()

    def scrape_chamber(self, chamber, session, committee_list):
        for committee in committee_list:
            committee_page = self.get_page(committee.get("href"))

            agenda_list = committee_page.xpath("//div[@id='ctl00_ContentPlaceHolder1_divAgendaAccordian']//h4")

            agenda_prefix = "http://www.sdlegislature.gov"

            for agenda in agenda_list:
                agenda_url = agenda_prefix + agenda.get("importfile")
                self.scrape_agenda(agenda_url, chamber, session)

    def scrape_agenda(self, agenda_url, chamber, session):
        agenda_page = self.get_page(agenda_url)

        related_bills = agenda_page.xpath("//a")
        committee = re.match(r"Committee\:\s+(.*)",
                             agenda_page.xpath("//div[2]/text()[1]")[0].strip(), flags=re.U).group(1)
        room = re.match(r"Room\:\s+(.*)",
                        agenda_page.xpath("//div[2]/text()[2]")[0].strip(), flags=re.U).group(1)
        date = re.match(r"Date\:\s+([a-zA-Z0-9,./ -]*)",
                        agenda_page.xpath("//div[2]/text()[3]")[0].strip(), flags=re.U).group(1)

        try:
            time = re.match(r"Time\:\s+([a-zA-Z0-9,./ :()-]*)",
                            agenda_page.xpath("//div[2]/text()[4]")[0].strip(), flags=re.U).group(1)

            date_time_temp = "%s %s" % (date, time)
            date_time = parse(date_time_temp)
            date_time = self._tz.localize(date_time)
            start_has_time = True
        except (ValueError, AttributeError, IndexError) as e:
            self.warning("Datetime is unable to be parsed")
            try:
                date_time = parse(date)
                start_has_time = False
            except ValueError as e:
                self.warning("Unable to parse date from {}, {}".format(date, agenda_url))
                return

        event = Event(date_time, 'Committee Hearing: {}'.format(committee),
                      room, 'committee_hearing', start_has_time=start_has_time, chamber=chamber, session=session)
        event.add_source(agenda_url)
        event.add_participant('host', committee, chamber=chamber)

        # Bill processing
        for related_bill in related_bills:
            event.add_related_bill(related_bill.text, type='consideration')

        self.save_event(event)
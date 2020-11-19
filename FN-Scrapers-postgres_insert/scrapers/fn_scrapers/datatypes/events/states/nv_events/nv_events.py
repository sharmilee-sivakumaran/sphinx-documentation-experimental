from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.datatypes.events.common.event import NoDataForPeriod
from fn_scrapers.datatypes.events.common.metadata import get_session_from_internal_id,\
    _get_active_sessions as get_active_sessions
from fn_scrapers.api.scraper import scraper, tags
from collections import defaultdict

import datetime
import json

committee_url = "https://www.leg.state.nv.us/App/NELIS/REL/%s/HomeCommittee/LoadCommitteeListTab?selectedTab=List"
meeting_list_url = "https://www.leg.state.nv.us/App/NELIS/REL/%s/Committee/" \
                   "FillSelectedCommitteeTab?selectedTab=Meetings&committeeOrSubCommitteeKey=%s"
meeting_info_url = "https://www.leg.state.nv.us/App/NELIS/REL/%s/Meeting/%s"
meeting_bills_url = "https://www.leg.state.nv.us/App/NELIS/REL/%s/NavTree/" \
                    "GetMeetingHierarchy?itemKey=%s&meetingKey=%s&billKey=0&budgetKey=0&id=4000000"


def get_session_stub(session, year):
    sessionsuffix = 'th'
    if str(session)[-1] == '1':
        sessionsuffix = 'st'
    elif str(session)[-1] == '2':
        sessionsuffix = 'nd'
    elif str(session)[-1] == '3':
        sessionsuffix = 'rd'
    if 'Special' in session:
        session_stub = session[-2:] + sessionsuffix + str(year) + "Special"
    else:
        session_stub = str(session) + sessionsuffix + str(year)
    return session_stub


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NV")
class NVEventScraper(EventScraper):
    """
    NVEventScraper
    Scrape Nevada Events
    """
    jurisdiction = 'nv'
    meeting_dict = defaultdict(list)

    def __init__(self, *args, **kwargs):
        super(NVEventScraper, self).__init__('nv', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            external_id = get_session_from_internal_id("nv", session, self.metadata)["external_id"]

            if 'Special' in external_id:
                year = external_id[0:4]
            elif int(external_id) >= 71:
                year = ((int(external_id) - 71) * 2) + 2001
            else:
                raise NoDataForPeriod(session)

            session_slug = get_session_stub(external_id, year)

            comittee_list_url = committee_url % session_slug
            page = self.get_page(comittee_list_url)

            try:
                ass_committee_table = page.xpath('//ul[@id="panelAssemblyCommittees"]')[0]
                ass_committee_list = ass_committee_table.xpath('.//div[@class="row listing"]')
                self.scrape_page(session_slug, 'lower', ass_committee_list, session)
            except IndexError:
                self.error("Failed to scrape meetings of assembly committees")
            try:
                ass_subcommittee_list = ass_committee_table.xpath('.//div[@class="row"]')[1:]
                self.scrape_page(session_slug, 'lower', ass_subcommittee_list, session)
            except IndexError:
                self.error("Failed to scrape meetings of assembly subcommittees")

            try:
                sen_committee_table = page.xpath('//ul[@id="panelSenateCommittees"]')[0]
                sen_committee_list = sen_committee_table.xpath('.//div[@class="row listing"]')
                self.scrape_page(session_slug, 'upper', sen_committee_list, session)
            except IndexError:
                self.error("Failed to scrape meetings of senate committees")

            try:
                sen_subcommittee_list = sen_committee_table.xpath('.//div[@class="row"]')[1:]
                self.scrape_page(session_slug, 'upper', sen_subcommittee_list, session)
            except IndexError:
                self.error("Failed to scrape meetings of senate subcommittees")

            for event_id in self.meeting_dict:
                self.save_event(self.meeting_dict[event_id])

        self.save_events_calendar()

    def scrape_page(self, session_slug, chamber, committee_list, session):
        """
        scrape meetings from page
        :param session_slug: Session of meetings
        :param chamber: Chamber of meetings
        :param committee_list: htmlelement type of committee page
        """

        # get committee list
        for row in committee_list:
            try:
                committee_div = row.xpath('.//div[@class="col-md-4"]')[0]
                committee_name = committee_div.text_content().strip()
            except IndexError:
                try:
                    committee_div = row.xpath('.//div[@class="col-md-11"]')[0]
                    committee_name = committee_div.text_content().strip()
                except IndexError:
                    self.warning("Failed to scrape committee name")
                    continue
            if committee_name == 'Subcommittees':
                continue
            try:
                comm_url = committee_div.xpath('./a')[0].attrib['href']
                committee_code = re.findall(r'/Committee/(\d+)/Overview', comm_url)[0]
            except IndexError:
                self.warning("Failed to get page for comittee %s", committee_name)
                continue

            regex_ns = "http://exslt.org/regular-expressions"
            meeting_page = self.get_page(meeting_list_url % (session_slug, committee_code))
            meeting_table = meeting_page.xpath("//a[re:test(@href, '/App/NELIS/REL/.*/Meeting/\d+', 'i')]",
                                               namespaces={'re': regex_ns})

            # get meeting list for each committees
            for meeting_row in meeting_table:
                meeting_url = meeting_row.attrib['href']

                date = meeting_row.text_content()

                date = re.sub(r'\[.*\]', '', date).strip()
                date = re.sub(r' 0:', ' 12:', date)

                try:
                    norm_date = datetime.datetime.strptime(date, "%A, %B %d, %Y %I:%M %p")
                    norm_date = self._tz.localize(norm_date)
                    has_time = True
                except ValueError:
                    try:
                        norm_date = datetime.datetime.strptime(date, "%A, %B %d, %Y")
                        has_time = False
                    except ValueError:
                        self.error("Failed to scrape date")
                        continue

                        # uncomment to do test
                        # if norm_date.date() < datetime.date.today(): #- datetime.timedelta(days=20):
                #	break

                try:
                    meeting_key = re.findall(r'Meeting/(\d+)', meeting_url)[0]
                except IndexError:
                    self.warning("Failed to get meeting code")
                    continue

                full_meeting_info_url = meeting_info_url % (session_slug, meeting_key)
                meeting_info = self.get_page(full_meeting_info_url)

                try:
                    title = meeting_info.xpath("//div[@class='col-md-7 ']")[0].text_content().strip()
                    if "Cancelled." in title or "No Meeting Scheduled." in title:
                        continue
                    title = re.sub(r'Upon Call of Chair', '', title)
                    title = re.sub(r'\n', ' ', title)
                    title = re.sub(r'\s+', ' ', title.strip())
                except IndexError:
                    title = '%s Hearing' % committee_name
                try:
                    place = meeting_info.xpath("//div[text()='Location:']/following-sibling::div/span//li")[
                        0].text_content().strip()
                except IndexError:
                    self.warning("Failed to get place of meeting, set it as 'N/A'")
                    place = 'N/A'
                if not has_time:
                    try:
                        meeting_text = self.get(meeting_url).text
                        time_string = re.search(r"(\d{1,2}:\d{2}:\d{2}\s[AP]M) class", meeting_text).group(1)
                        time = datetime.datetime.strptime(time_string, "%X %p")
                        norm_date = norm_date.replace(hour=time.hour, minute=time.minute, second=time.second)
                        norm_date = self._tz.localize(norm_date)
                        has_time = True
                    except Exception as e:
                        self.warning(e)
                        pass

                if (title, place, norm_date) not in self.meeting_dict:
                    event = Event(norm_date, title, place, 'committee_hearing', start_has_time=has_time,
                                  chamber=chamber, session=session)
                    event.add_source(meeting_url)
                else:
                    event = self.meeting_dict[(title, place, norm_date)]

                event.add_participant('host', committee_name, chamber=chamber)

                full_meeting_bills_url = meeting_bills_url % (session_slug, meeting_key, meeting_key)

                bills_page = self.get_page(full_meeting_bills_url)
                bills = json.loads(bills_page.text_content())

                for bill in bills:
                    bill_id = re.sub(r'\(WS\)|\*', '', bill['text']).strip()
                    event.add_related_bill(bill_id, 'consideration')

                self.meeting_dict[(title, place, norm_date)] = event
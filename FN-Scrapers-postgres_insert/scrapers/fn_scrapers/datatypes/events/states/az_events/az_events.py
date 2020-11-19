from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from .session_details import session_details
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
from dateutil.parser import parse
import logging

logger = logging.getLogger(__name__)
committee_url = "https://apps.azleg.gov/api/Committee/?includeOnlyCommitteesWithAgendas=true"\
                "&legislativeBody=%s&sessionId=%s&standingOnly=%s&interimOnly=%s&jointCommitteesOnly=%s"

'''
Minimum parameters required for a valid link:
- sessionID
- body
- isInterimAgenda
'''
meeting_url = "https://apps.azleg.gov/api/Agenda/?showPassed=true&sessionId=%s&isInterimAgenda=%s&body=%s"\
              "&includeItems=false&committeeId=%s"

bills_list_url = "https://apps.azleg.gov/api/Bill/?agendaId=%s&includeSponsors=true&showNonPublicBills=false"

chamber_short = {'upper': 'S', 'lower': 'H', 'joint': ''}


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-AZ")
class AZEventScraper(EventScraper):
    jurisdiction = 'az'

    def __init__(self, *args, **kwargs):
        super(AZEventScraper, self).__init__('az', __name__, **kwargs)
        self.scraped_events = {}

    def get_session_id(self, session):
        """
        returns the session id for a given session
        """
        for containers in self.metadata['legislative_session_containers']:
            leg_session = containers['sessions']
            for sess in leg_session:
                if sess['id'] == session:
                    return session_details[sess['external_id']]['session_id']
        return None

    def scrape(self):
        """
        given a session returns the events
        """
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            self.scrape_committee(session, 'upper', 'standing')
            self.scrape_committee(session, 'upper', 'interim')
            self.scrape_committee(session, 'lower', 'standing')
            self.scrape_committee(session, 'lower', 'interim')
            self.scrape_committee(session, 'joint', 'interim')

        for event in self.scraped_events.values():
            self.save_event(event)
        self.save_events_calendar()

    def scrape_committee(self, session, chamber, committee_type):
        """
        Scrape committee page
        """
        session_id = self.get_session_id(session)
        chamber_code = chamber_short[chamber]
        if committee_type == 'standing':
            stading = 'true'
            interim = 'false'
        else:
            stading = 'false'
            interim = 'true'
        if chamber == 'joint':
            joint = 'true'
        else:
            joint = 'false'
        committee_link = committee_url % (chamber_code, session_id, stading, interim, joint)
        committee_html = self.get(committee_link).text.strip()

        committee_group = re.findall(r'\"CommitteeId\"\:(.*?),\"CommitteeName\"\:\"(.*?)\"', committee_html)

        # Scrape committee name and id
        for committee_model in committee_group:
            committee_id = committee_model[0]
            committee_name = committee_model[1]

            meeting_link = meeting_url % (session_id, interim, chamber_code, committee_id)
            meeting_html = self.get(meeting_link).text.strip()

            if meeting_html == '[]':
                continue

            meeting_group = re.findall(r'\{\"AgendaId\"\:(\d+),.*?\"Date\"\:\"(.*?)\",\"Time\"\:\"(.*?)\",' +
                                       r'\"Room\"\:\"(.*?)\".*?\"HttpPath\"\:\"(.*?)\"', meeting_html)

            # Scrape meeting information
            for meeting_entry in meeting_group:
                agenda_id = meeting_entry[0]
                date = meeting_entry[1]
                time = meeting_entry[2]
                source = meeting_entry[4]

                if any(words in time.upper() for words in ['NOT MEETING', 'CANCELLED', 'NOT MEETNG']):
                    continue

                try:
                    """
                    Sometimes the date also has some text content which causes the date parser to fail
                    with a ValueError so paring the date and time separately
                    
                    https://apps.azleg.gov/api/Agenda/?showPassed=true&sessionId=119&isInterimAgenda=false&body=H&includeItems=false&committeeId=1574
                    """
                    date_obj = parse(date)
                except ValueError:
                    logger.warning('Unable to parse date from %s', date)
                    continue

                try:
                    time = re.search(r'.*?\s?[AP]\.?M\.?', time).group(0)
                    date_string = "{0:%b %d, %Y} {1}".format(date_obj, time)
                    formed_date = parse(date_string)
                    has_time = True
                    formed_date = self._tz.localize(formed_date)
                except (ValueError, AttributeError):
                    logger.warning("Unable to find time: %s", time)
                    formed_date = date_obj
                    has_time = False

                location = re.findall(r'\"Room\"\:\"(.*?)\"', meeting_html)[0]

                bills_list_link = bills_list_url % (agenda_id)
                bills_list_html = self.get(bills_list_link).text.strip()

                descr = "%s Hearing" % committee_name

                key = (formed_date, descr, location)

                if key in self.scraped_events:
                    event = self.scraped_events[key]
                else:
                    event = Event(formed_date, descr, location, 'committee_hearing',
                                  start_has_time=has_time, chamber=chamber, session=session)

                event.add_source(source)
                event.add_participant('host', committee_name, chamber=chamber)

                # Scrape related bills
                bills_list = re.findall(r'\"Number\":\"(.*?)\",\"Description\"\:\".*?\"', bills_list_html)
                for bill_entry in bills_list:
                    bill_id = bill_entry
                    event.add_related_bill(bill_id)

                self.scraped_events[key] = event
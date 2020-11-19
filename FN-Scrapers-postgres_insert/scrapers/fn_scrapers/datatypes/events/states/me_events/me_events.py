'''Maine Leg Events Scraper.

Implementation Note (2017-11-02):

The Maine Legislation page seems to have three legislative calendars. This
code uses the committee calendar endpoint. It is gropued by committee, 
includes bill information and other useful information, but must be looked
up by committee code, committee name, and committee office room number.

    http://legislature.maine.gov/backend/breeze/data/getCommitteeEvents?room=228&committeeCode=AFA&committeeName=Appropriations%20and%20Financial%20Affairs&startDate=2017-10-12T04%3A00%3A00.000Z&endDate=2018-06-27T03%3A59%3A59.999Z

There is also the general calendar endpoint and this is what is shown at

    http://legislature.maine.gov/Calendar
    http://legislature.maine.gov/backend/breeze/data/getCalendarEventsRaw?startDate=2017-10-29T04%3A00%3A00.000Z&endDate=2017-11-05T03%3A59%3A59.999Z&OnlyPHWS=false

This calendar does not link to bills though.

Finally there is a bills calendar, however this calendar does not actually
show dates but rather references to events on the general calendar. This
was not discovered until this scraper was mostly done though so it is only
being mentioned for future development.

    http://legislature.maine.gov/backend/breeze/data/getCalendarEventsBills?startDate=2017-10-20T04%3A00%3A00.000Z&endDate=2017-10-21T04%3A00%3A00.000Z

- Stephen
'''

from __future__ import absolute_import
from datetime import datetime
import logging
import re
import traceback
import pytz
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
types = {
    'WS': 'Work Session',
    'LM': 'Legislative Meeting',
    'PH': 'Public Hearing',
}

logger = logging.getLogger(__name__)

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-ME")
class MEEventScraper(EventScraper):
    '''Maine Leg Events Scraper. '''
    jurisdiction = 'me'

    bill_id_re = re.compile(r'(?:[SH]P|LD)\s*?\d+')

    # http://legislature.maine.gov/backend/breeze/data/getCommittees?legislature=128
    COMMS_URL = ('http://legislature.maine.gov/backend/breeze/data/getCommitte'
                 'es?legislature={}')
    # http://legislature.maine.gov/backend/breeze/data/getCommitteeDetails?legislature=128&committeeCode=MLI
    COMM_DETAIL_URL = ('http://legislature.maine.gov/backend/breeze/data/getCo'
                       'mmitteeDetails?legislature={}&committeeCode={}')
    # http://legislature.maine.gov/backend/breeze/data/getCommitteeEvents?room=216&committeeCode=MLI&committeeName=Marijuana%20Legalization%20Implementation&startDate=2017-09-26T04%3A00%3A00.000Z&endDate=2017-10-04T03%3A59%3A59.999Z
    COMM_CAL_URL = ('http://legislature.maine.gov/backend/breeze/data/getCommi'
                    'tteeEvents?room={room}&committeeCode={code}&committeeName'
                    '={name}&startDate={start:%Y-%m-%d}T04%3A00%3A00.000Z&endD'
                    'ate={end:%Y-%m-%d}T03%3A59%3A59.999Z')

    
    # See http://legislature.maine.gov/committee/App/config.js - getRoomNumber
    ROOM_PATTERN = re.compile(r'\d{3}|house_chamber|senate_chamber|civic_cente'
                              r'r|hall_of_flags|welcome_center')

    def __init__(self, *args, **kwargs):
        super(MEEventScraper, self).__init__('me', __name__, **kwargs)

    def scrape(self):
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        '''Loop over each committee. '''
        for session in sessions:
            self.session_metadata = self.get_session_metadata(session)

            assert self.session_metadata, (
                "Could not load metadata for session {}".format(session))
            res = self.get(self.COMMS_URL.format(self.session_metadata['external_id']))
            assert res.status_code == 200, "Could not load committee details url."
            ''' committee example:
                    {
                        "$id": "2",
                        "$type": "Backend.SelectCommittees_Result, Backend",
                        "CommitteeCode": "AFA",
                        "CommitteeName": "Appropriations and Financial Affairs",
                        "CommitteeId": 26
                    },
            '''
            for committee in res.json():
                try:
                    self.scrape_committee_events(session, committee)
                except Exception as exception:
                    # TODO add rabbit logging for this.
                    traceback.print_exc()
                else:
                    # TODO add rabbit logging for this too.
                    pass

        self.save_events_calendar()

    def scrape_committee_events(self, session, committee):
        '''Scrape a specific committee's calendar. '''
        logger.info("Scraping {} committee".format(committee['CommitteeCode']))
        payload = None
        code = committee['CommitteeCode']
        comm_name = committee['CommitteeName']
        sess_id = self.session_metadata['external_id']

        res = self.get(self.COMM_DETAIL_URL.format(sess_id, code))
        assert res.status_code == 200, "Failed to load committee details page."
        committee_details = res.json()[0]
        room_number = self.ROOM_PATTERN.search(
            committee_details['HearingRoom'].lower().replace(' ', '_'))
        if room_number:
            room_number = room_number.group(0)
        else:
            room_number = committee_details['HearingRoom'].lower().replace(' ', '_')
        

        event_dict = {}
        # Set up JSON data request
        for subsession in self.session_metadata['subsessions']:
            session_start_date = datetime.utcnow().replace(tzinfo=pytz.utc) # datetime.strptime(subsession['start_date'], '%Y-%m-%d')
            session_start_date = session_start_date.astimezone(self._tz).date()
            session_end_date = datetime.strptime(subsession['end_date'], '%Y-%m-%d')
            """
            Uncomment the following 2 lines to run between specific dates
            """
            # session_start_date = datetime.strptime('2018-02-01', '%Y-%m-%d')
            # session_end_date = datetime.strptime('2018-02-10', '%Y-%m-%d')

            req_url = self.COMM_CAL_URL.format(
                room=room_number,
                code=code,
                name=comm_name,
                start=session_start_date,
                end=session_end_date
            ).replace(' ', '%20')
            res = self.get(req_url)
            if not res.status_code == 200:
                logger.error('Failed to get committee events.')
                return False

            payload = res.json()
            logger.info("Received {} records ({} chars)".format(
                len(payload), len(res.text)))
            for record in payload:
                if record['Event_Cancelled']:
                    continue
                date = self._tz.localize(datetime.strptime(
                    record['Event_FromDateTime'], "%Y-%m-%dT%H:%M:%S.%f"))
                location = record['Event_Location']

                if (date, location) in event_dict:
                    event = event_dict[(date, location)]
                else:
                    event_type = record['Event_EventType']
                    desc = record['Event_Description'] or u"{} {}".format(comm_name, types[event_type])
                    logger.info("Found event {} at {}".format(desc, date))
                    e_type = 'committee_markup' if event_type == 'WS' else 'committee_hearing'
                    event = Event(date, desc, location, e_type, start_has_time=True, session=session)

                    # Get/set source
                    url = "http://legislature.maine.gov/calendar/#Daily/{:%Y-%m-%d}".format(date)
                    event.add_source(url)

                    # Get/set participants
                    event.add_participant('host', comm_name, chamber="joint")

                # Get bill
                bill_id_parts = self.bill_id_re.findall(record['Event_Description'] or ' ')
                for part in bill_id_parts:
                    event.add_related_bill(part)
                event_dict[(date, location)] = event
                bill = dict()
                bill_id = record['Request_PaperNumber']
                if not bill_id:
                    continue
                bill_id_parts = self.bill_id_re.search(bill_id)
                if not bill_id_parts:
                    logger.warning("Couldn't add bill " + bill_id)
                    continue
                formed_bill_id = bill_id_parts.group(0)
                event.add_related_bill(formed_bill_id, 'consideration')

        for key in event_dict:
            self.save_event(event_dict[key])

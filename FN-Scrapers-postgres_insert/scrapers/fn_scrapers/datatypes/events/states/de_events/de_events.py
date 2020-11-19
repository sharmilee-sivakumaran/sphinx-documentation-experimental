from __future__ import absolute_import

import re
import json
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from dateutil.parser import parse

chambers = {
    "Senate": "upper",
    "House": "lower",
    "Joint": "joint"
}


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-DE")
class DEEventScraper(EventScraper):
    jurisdiction = 'de'

    def __init__(self, *args, **kwargs):
        super(DEEventScraper, self).__init__('de', __name__, **kwargs)

    def scrape(self):
        url = "http://www.legis.delaware.gov/json/CommitteeMeetings/GetUpcomingCommitteeMeetings"
        page = self.post(url).text
        if not page:
            return
        events_table_json = json.loads(page)
        for event in events_table_json['Data']:
            event_chamber = event['CommitteeTypeName']
            event_id = event['CommitteeMeetingId']
            event_date = event['MeetingDateTime']
            event_date = parse(event_date)
            event_date = self._tz.localize(event_date)
            event_committee = event['CommitteeDescription']
            event_location = event['AddressAliasNickname']
            event = Event(event_date,
                          "Committee Meeting: {}".format(event_committee),
                          event_location,
                          'committee_markup',
                          chamber=chambers[event_chamber], start_has_time=True)
            if event_committee:
                event.add_participant('host', event_committee)
            event_url = "http://legis.delaware.gov/MeetingNotice/%s" % event_id
            event.add_source(event_url)

            event_item_url = "http://www.legis.delaware.gov/json/MeetingNotice" \
                             "/GetCommitteeMeetingItems?committeeMeetingId=%s" % event_id
            page = self.post(event_item_url).text
            if page:
                events_json = json.loads(page)

                for eventitem in events_json['Data']:
                    bill_id = eventitem['LegislationDisplayCode']
                    bill_id_pattern = re.compile(r"[SH][CJ]?[ARB] \d+")
                    if bill_id is not None and bill_id_pattern.match(bill_id):
                        event.add_related_bill(bill_id, type='consideration')
            self.save_event(event)
        self.save_events_calendar()
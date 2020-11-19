from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.utils import get_page
from dateutil.parser import parse


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NM")
class NMEventScraper(EventScraper):
    jurisdiction = 'nm'

    def __init__(self, *args, **kwargs):
        super(NMEventScraper, self).__init__('nm', __name__, **kwargs)

    def scrape(self):
        # For chambers upper and lower
        for chamber in ["upper", "lower"]:
            self.scrape_committee_hearings(chamber)

        self.save_events_calendar()

    def scrape_committee_hearings(self, chamber):
        base_url = "https://www.nmlegis.gov/Entity/%s/Committee_Calendar"
        if chamber == 'lower':
            url = base_url % 'House'
        else:
            url = base_url % 'Senate'
        committee_page = get_page(url)

        for committee_name_link in committee_page.xpath(".//a[contains(@id,'_linkLocation')]"):
            committee_name = committee_name_link.text_content().strip()
            committee_num_match = re.search(r"_linkLocation_(\d+)", committee_name_link.attrib["id"])
            committee_num = "repeaterDates_{}".format(int(committee_num_match.group(1)))

            dates_xpath = "./following-sibling::span[contains(@id,'{}_lblHearingDate')]".format(committee_num)
            dates = committee_name_link.xpath(dates_xpath)

            for i in xrange(len(dates)):
                date = dates[i].text_content().strip()

                time_xpath = "./following-sibling::span[contains(@id,'{}_lblHearingTime_{}')]".format(committee_num, i)
                time = committee_name_link.xpath(time_xpath)[0].text_content().strip()

                has_time = False

                if time:
                    time = re.split('or', time)[0]
                    time = re.sub(r'\.', '', time)
                    time = re.sub(r';', ':', time)
                    time = re.sub(r'`', '', time)
                    time = time.replace(u"REVISED", "")
                    if re.search(r'\d+:\d+', time):
                        date = date + " " + time
                        has_time = True
                parsed_date = parse(date)
                if has_time:
                    parsed_date = self._tz.localize(parsed_date)

                room_xpath = "./following-sibling::span[contains(@id,'{}_lblRoomNumber_{}')]".format(committee_num, i)
                place = committee_name_link.xpath(room_xpath)[0].text_content().strip()

                desc = '%s Hearing' % committee_name

                event = Event(parsed_date, desc, place, 'committee_markup', chamber=chamber, start_has_time=has_time)

                event.add_participant('host', committee_name, chamber=chamber)
                event.add_source(url)

                bill_table_xpath = "./following-sibling::div" \
                                   "/table[contains(@id,'{}_gridViewBills_{}')]".format(committee_num, i)
                bill_table = committee_name_link.xpath(bill_table_xpath)[0]
                for bill_row in bill_table:
                    bill_url = bill_row.xpath(".//a[contains(@id,'_linkBillNumber')]/@href")[0]
                    b_cham, b_type, b_num = re.search(ur"Chamber=(H|S)&LegType=([^&]+)&LegNo=(\d+)", bill_url).groups()
                    bill_id = "{}{} {}".format(b_cham, b_type, b_num)
                    event.add_related_bill(bill_id, type="consideration")

                self.save_event(event)
from __future__ import absolute_import
import re
import urlparse
import lxml.html
from datetime import datetime
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.events.common.utils import parse_str_to_date
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-PA")
class PAEventScraper(EventScraper):
    jurisdiction = 'pa'

    skip_rgx = re.compile(r"Add to .*Calendar", re.I)
    time_rgx = re.compile(r'\n(?!\d{1,2}:\d{2})')

    def __init__(self, *args, **kwargs):
        super(PAEventScraper, self).__init__('pa', __name__, **kwargs)

    def scrape_committee_hearings(self, chamber):
        """
        Scrape all committee hearing events for the chamber and the current session.

        :param chamber: 'upper' for Senate or 'lower' for House
        :param session: year range for this session
        :type chamber: string
        :type session: string
        :returns: None
        :rtype: NoneType
        """

        if chamber == 'upper':
            url = "http://www.legis.state.pa.us/cfdocs/legis/cms/index.cfm?chamber=S"
        elif chamber == 'lower':
            url = "http://www.legis.state.pa.us/cfdocs/legis/cms/index.cfm?chamber=H"

        page = self.get(url)
        page = page.lxml()
        for day_div in page.xpath("//*[@class='CMS-MeetingDetail']"):
            date = day_div.xpath(".//*[@class='CMS-MeetingDetail-Header']")
            if date:
                date = date[0].text_content().strip()
            else:
                self.warning("Expected day of meetings, but cannot find date - skipping.")
                continue
            date = parse_str_to_date(date).date()
            for event_ele in day_div.xpath(".//*[@class='CMS-MeetingDetail-CurrMeeting']"):
                has_time = False
                time_ele = event_ele.xpath(".//*[@class='CMS-MeetingDetail-Time']")
                if time_ele:
                    time_txt = time_ele[0].text_content().strip()
                else:
                    # If the time column cannot be found, default to 12am.
                    self.warning("Meeting time column not found under date %s - fall back to default." % date)
                    time_txt = 'N/A'

                try:
                    time_obj = datetime.strptime(time_txt, "%I:%M %p").time()
                    date = datetime.combine(date, time_obj)
                    has_time = True
                    date = self._tz.localize(date)
                except ValueError:
                    date = datetime.combine(date, datetime.min.time())

                desc_el = event_ele.xpath(".//*[@class='CMS-MeetingDetail-Agenda']")
                if desc_el:
                    desc_el = desc_el[0]
                else:
                    self.warning("Event committee & description column not found for datetime %s - skipping" % date)
                    continue

                committee_ele = desc_el.xpath(".//*[@class='CMS-MeetingDetail-Agenda-CommitteeName']")
                if committee_ele:
                    committee_ele = committee_ele[0]
                    ctty = committee_ele.text_content().strip()
                else:
                    self.warning("Event committee not found - scraper may be out of date.")
                    ctty = None

                # The description of the event comes on the line following the committee name - clean it up.
                event_desc_div = committee_ele.getnext()
                event_desc = 'Committee Hearing'
                while event_desc_div is not None:
                    event_desc = event_desc_div.text_content().strip()
                    if self.skip_rgx.search(event_desc):
                        event_desc = 'Committee Hearing'
                        # Skip lines that say "Add to Calendar" to get to the real description.
                        event_desc_div = event_desc_div.getnext()
                    else:
                        # If this seems to be a proper description, exit the loop and use it.
                        break

                # We want to keep the newlines before each row with a time since it makes multiline schedules
                # easier to read. But remove unnecessary newlines within phrases, since it looks tacky.
                event_desc = re.sub(self.time_rgx, '', event_desc)

                # The related bills appear as links in the event description text.
                related_bills = desc_el.xpath(".//a[contains(@href, 'billinfo')]")
                bills = []

                for bill_a in related_bills:
                    bill_url = bill_a.attrib['href']
                    # Parse the arguments from this url - want to extract the bill_id from args body, type, and bn.
                    o = urlparse.urlparse(bill_url)
                    qs = urlparse.parse_qs(o.query)
                    try:
                        bill_desc = lxml.html.fromstring(bill_a.attrib['title']).xpath(
                                ".//*[@class='ToolTip-BillSummary-ShortTitle']")[0].text_content().strip()
                    except (KeyError, IndexError, AttributeError):
                        # If we can't successfully parse the tooltip, fall back to a default event description.
                        self.warning("Cannot find description for bill url %s, fall back to default." % bill_url)
                        bill_desc = 'Consideration'

                    bills.append({
                        "bill_id": "%s%s %s" % ( qs['body'][0], qs['type'][0], qs['bn'][0] ),
                        "descr": bill_desc
                    })

                location = event_ele.xpath(".//*[@class='CMS-MeetingDetail-Location']")
                if location:
                    location = location[0].text_content().strip()
                else:
                    location = 'N/A'

                location = re.sub(r'\s+', ' ', location)
                if not location:
                    location = 'N/A'

                # I assume all document saves are executed back-to-back at the end for cache/locality reasons.
                event = Event(date, event_desc, location, 'committee_markup', start_has_time=has_time)

                # committee page url
                try:
                    comm_page_url = committee_ele.xpath(".//a")[0].get("href")
                    event.add_source(comm_page_url)
                except:
                    event.add_source(url)
                    self.warning("Failed to get link to host committee for %s, %s meeting" %(date, location))

                if ctty is not None:
                    event.add_participant('host', ctty,
                                          chamber=chamber)

                for bill in bills:
                    event.add_related_bill(
                        bill['bill_id'],
                        description=bill['descr'],
                        type='bill'
                    )
                self.save_event(event)

    def scrape(self):
        """
        Scrape all relevant events for the specified chamber and session.

        :param chamber: 'upper' for Senate or 'lower' for House
        :param session: year range for this session
        :type chamber: string
        :type session: string
        :returns: None
        :rtype: NoneType
        """
        for chamber in ["upper", "lower"]:
            self.scrape_committee_hearings(chamber)
        self.save_events_calendar()

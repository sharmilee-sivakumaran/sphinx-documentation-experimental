from __future__ import absolute_import
import datetime as dt
import re
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

mi_events = "http://legislature.mi.gov/doc.aspx?CommitteeMeetings"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-MI")
class MIEventScraper(EventScraper):
    jurisdiction = 'mi'

    def __init__(self, *args, **kwargs):
        super(MIEventScraper, self).__init__('mi', __name__, **kwargs)

    def get_page(self,url):
        try:
            page = self.get(url)
        except Exception as err:
            self.error("Failed: %s, %s" % (err, url))
            return None

        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape_event_page(self, url, chamber):
        page = self.get_page(url)
        # Page couldn't be open
        if page is None:
            self.info('Cannot open page: %s' % url)
            return

        trs = page.xpath("//table[@id='frg_committeemeeting_MeetingTable']/tr")
        metainf = {}
        for tr in trs:
            tds = tr.xpath(".//td")
            if len(tds) <= 1:
                continue
            key = tds[0].text_content().strip()
            val = tds[1]
            metainf[key] = {
                "txt": val.text_content().strip(),
                "obj": val
            }

        if metainf == {}:
            return

        # Wednesday, 5/16/2012 3:00 pm
        event_datetime = "%s %s" % (
            metainf['Date']['txt'],
            metainf['Time']['txt']
        )
        if "Cancelled" in event_datetime:
            return

        translate = {
            "noon": " PM",
            "a.m.": " AM",
            "am": " AM"  # This is due to a nasty line they had.
        }


        for t in translate:
            if t in event_datetime:
                event_datetime = event_datetime.replace(t, translate[t])

        event_datetime = re.sub("\s+", " ", event_datetime)

        flag = "or after committees are given leave"

        if flag in event_datetime:
            event_datetime = event_datetime[:event_datetime.find(flag)].strip()


        event_datetime = event_datetime.replace('p.m.', 'pm').split('or')[0].strip()
        try:
            event_datetime = dt.datetime.strptime(event_datetime, "%A, %m/%d/%Y %I:%M %p")
            event_datetime = self._tz.localize(event_datetime)
        except:
            try:
                self.warning("Could not parse datetime {}. Trying fuzzy matching".format(event_datetime))
                event_datetime = parse(event_datetime, fuzzy=True)
                event_datetime = self._tz.localize(event_datetime)
            except:
                self.error("Couldn't parse datetime for event - %s" % event_datetime)
                return
        if metainf['Location']['txt'] != "":
            where = metainf['Location']['txt']
        else:
            where = 'N/A'
        if metainf['Committee']['txt'] != "":
            title = metainf['Committee']['txt']  # XXX: Find a better title
        else:
            title = "N/A"


        if chamber == 'other':
            chamber = 'joint'

        event = Event(event_datetime, title, where, 'committee_markup',
                      start_has_time=True, chamber=chamber)

        event.add_source(url)
        event.add_source(mi_events)

        if metainf['Chair']['txt'] != "":
            event.add_participant('chair', metainf['Chair']['txt'], chamber=chamber)
        if metainf['Committee']['txt'] != "":
            event.add_participant('host', metainf['Committee']['txt'], chamber=chamber)

        agenda = metainf['Agenda']['obj']
        agendas = agenda.text_content().split("\r")

        related_bills = agenda.xpath("//a[contains(@href, 'getObject')]")
        for bill in related_bills:
            description = agenda
            for a in agendas:
                if bill.text_content() in a:
                    description = a

            event.add_related_bill(bill.text_content(), 'consideration', description=description)

        self.save_event(event)


    def scrape(self):
        page = self.get_page(mi_events)
        if page is None:
            self.error("Failed to scrape MI Committee Meetings Page")
            return
        xpaths = {
            "lower": "//span[@id='frg_committeemeetings_HouseMeetingsList']",
            "upper": "//span[@id='frg_committeemeetings_SenateMeetingsList']",
            "other": "//span[@is='frg_committeemeetings_JointMeetingsList']"
        }
        for chamber in xpaths:
            span = page.xpath(xpaths[chamber])
            if len(span) > 0:
                span = span[0]
            else:
                continue
            events = span.xpath(".//a[contains(@href, 'committeemeeting')]")
            event_name = []
            for event in reversed(events):
                name = event.text_content()
                if name in event_name:
                    continue
                event_name.append(name)
                self.scrape_event_page(event.attrib['href'], chamber)

        self.save_events_calendar()

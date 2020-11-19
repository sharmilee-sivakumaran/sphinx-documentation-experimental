from __future__ import absolute_import
import re
import datetime as dt
from dateutil.parser import parse
from pytz import UTC
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

house_agenda = 'http://www.legis.state.wv.us/committees/house/main.cfm'
senate_agenda = 'http://www.legis.state.wv.us/committees/senate/main.cfm'
joint_comm = 'http://www.wvlegislature.gov/committees/Interims/interims.cfm'

# This is very necessary
month_misspellings = {"Jaunary": "January",
                      "Feburary": "February"}

bill_regexes = {r"((?<![A-Za-z0-9])[HS]\.?\s?[BCR]{1,2}\.?\s{0,2}\d+)\-?\,?\s+?\-?(.+?)\s{2,}": "",
                r"Senate Bill No\.\s+?(\d+)\-?\,?\s+?\-?(.+?)\s{3,}": "SB ",
                r"House Bill No\.\s+?(\d+)\-?\,?\s+?\-?\,?(.+?)\s{3,}": "HB ",
                r"(?i)SENATE BILL (\d+)\s+?\-?\,?(.+?)\s{3,}": "SB ",
                r"(?i)HOUSE BILL (\d+)\s+?\-?\,?(.+?)\s{3,}": "HB ",
                r"House Resolution No\. (\d+)\s+?\-?(.+?)\s{3,}": "HR ",
                r"Senate Resolution No\. (\d+)\s+?\-?(.+?)\s{3,}": "SR ",
                r"Senate Concurrent Resolution No\. (\d+)\s+?\-?(.+?)\s{3,}": "SCR ",
                r"House Concurrent Resolution No\. (\d+)\s+?\-?(.+?)\s{3,}": "HCR "}

end_time_re = re.compile(r"-\s*?\d{1,2}(?::\d{1,2})?\s*?[AP]\.?M\.?", flags=re.I | re.U)

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-WV")
class WVEventScraper(EventScraper):
    jurisdiction = 'wv'

    def __init__(self, *args, **kwargs):
        super(WVEventScraper, self).__init__('wv', __name__, **kwargs)

        self.event_dict = {}

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        for chamber, link, cham in [('upper', senate_agenda, 'senate_com_'),
                                    ('lower', house_agenda, 'house_com_')]:
            self.scrape_house_senate_meetings(link, chamber, cham)

        self.scrape_joint_meetings(joint_comm)

        for event in self.event_dict.values():
            self.save_event(event)

        self.save_events_calendar()

    def scrape_house_senate_meetings(self, link, chamber, cham):
        today = dt.datetime.utcnow().replace(tzinfo=UTC)
        today = today.astimezone(self._tz)
        page = self.lxmlize(link)
        path = "//a[contains(@href,'%sagenda')]" % cham
        links = page.xpath(path)

        for agendas in links:
            committee_name = agendas.getprevious().text_content().strip()
            if agendas.text_content().strip() == '':
                continue
            url = agendas.attrib['href']
            # This is a link to the latest date with events for the given committee. The url contains that date,
            # so we can check to make sure there have been any events at all in this comittee for this year.
            url_date = re.match(r".+?input=([^&]+)(?:&|$)", url)
            if url_date:
                url_date = url_date.group(1)
            else:
                self.warning("No Meeting for %s" % committee_name)
                continue
            for misspelling in month_misspellings:
                if misspelling in url_date:
                    url_date = url_date.replace(misspelling, month_misspellings[misspelling])
                    break
            try:
                event_year = parse(url_date).year
            except ValueError:
                self.warning("Badly formatted date, '{}'. Url: {}".format(url_date, url))
                continue
            if event_year < today.year:
                self.warning("No events at all have occured in this session in the {} Committee".
                             format(committee_name))
                continue

            self.scrape_meetings(url, committee_name, chamber, cham)

    def scrape_joint_meetings(self, link):
        page = self.lxmlize(link)

        for path in page.xpath("//a[contains(@href, 'committee.cfm')]"):
            comm_name = path.text_content().strip()
            if not comm_name:
                continue
            path = self.lxmlize(path.get('href'))
            link = path.xpath(".//a[contains(@href, 'Interims/agenda.cfm')]")
            if not link:
                self.warning("{} has no agenda link".format(comm_name))
                continue
            else:
                url = link[0].get('href')
                self.scrape_meetings(url, comm_name, 'joint', 'Interims/')

    def scrape_meetings(self, url, committee_name, chamber, cham):
        today = dt.datetime.utcnow().replace(tzinfo=UTC)
        today = today.astimezone(self._tz)
        try:
            doc = self.get(url, timeout=5)
            doc = doc.lxml()
            doc.make_links_absolute(url)
        except:
            self.warning("Couldn't get url %s" % url)
            return

        # We need to scrape the events for the latest day before we can scrape the rest of the days on the
        # calendar, which are found at the bottom of each date page.
        self.scrape_specific_meeting(url, committee_name, chamber)

        # There is a link for every meeting on a particular day, but each link takes you to the same page,
        # which has all the events for that day. So we need to make sure we only scrape each date page once.
        seen_dates = set()
        for meetings in doc.xpath("//table//a[contains(@href,'%sagenda')]" % cham):
            date_string = meetings.text_content()
            if not date_string.strip():
                continue
            try:
                date = parse(date_string.strip())
            except ValueError:
                self.warning("Could not parse date string: {}".format(date_string))
            else:
                if date in seen_dates:
                    continue
                elif date.year < today.year:
                    self.info("{} is not in the current_session".format(date_string))
                else:
                    seen_dates.add(date)
                    self.scrape_specific_meeting(meetings.attrib['href'], committee_name, chamber)

    # Scrapes a meeting from a web page
    # date is a datetime
    # url is string
    # session is a string
    def scrape_specific_meeting(self, url, committee_name, chamber):
        today = dt.datetime.utcnow().replace(tzinfo=UTC)
        today = today.astimezone(self._tz)

        doc = self.lxmlize(url)
        date_events = doc.xpath("//h1")

        for date_event in date_events:
            datetime_string = date_event.text
            if 'Canceled' in datetime_string:
                continue
            start_time_description = None
            for misspelling in month_misspellings:
                    if misspelling in datetime_string:
                        datetime_string = datetime_string.replace(misspelling, month_misspellings[misspelling])
                        break
            has_time = True
            end_time_match = end_time_re.search(datetime_string)
            datetime_string = end_time_re.sub('', datetime_string)
            try:
                start = parse(datetime_string)
                start = self._tz.localize(start)
            except ValueError:
                if datetime_string.count(',') > 1:
                    datetime_string = re.search(r"(.*),", datetime_string).group(1)
                    start_time_description = datetime_string.strip()

                    try:
                        start = parse(datetime_string)
                        start = self._tz.localize(start)
                    except ValueError:
                        self.warning("Badly formatted date, '{}'. Url: {}".format(datetime_string, url))
                        continue
                else:
                    """
                    Sometimes, there is a human input time data which causes the parser to fail
                    For Example:
                    http://www.wvlegislature.gov/committees/house/house_com_agendas.cfm?input=2.22.18&chart=jud
                    """
                    try:
                        start, tokens = parse(datetime_string, fuzzy_with_tokens=True)
                        for token in tokens:
                            """
                            The following condition is looking at the rejected tokens from the fuzzy matching
                            and will ignore the extracted time if there are any words in the rejected tokens
                            because that most probably means that the time string was a human input, so the time may
                            not be reliable
                            """
                            if len(token.strip()) > 1:
                                self.warning("Ignoring time from {}".format(datetime_string))
                                start = dt.datetime.combine(start.date(), dt.time(0, 0, 0))
                                has_time = False
                                break
                        else:
                            has_time = True
                            start = self._tz.localize(start)
                    except ValueError:
                        self.warning("Badly formatted date, '{}'. Url: {}".format(datetime_string, url))
                        continue

            if start.date() < today.date():
                continue

            if start.time():
                has_time = True
            else:
                has_time = False

            location_data = date_event.xpath("following-sibling::b/text()")
            if not location_data:
                location_data = date_event.xpath("following-sibling::strong/text()")
            if location_data:
                location = re.sub("Location:", '', location_data[0]).strip()
                if location.endswith('-'):
                    location = location[:-1].strip()
            if not location:
                location = "N/A"

            event_data = date_event.xpath("following-sibling::blockquote")[0].text_content()

            descr = set()
            for bill_regex in bill_regexes:
                for bill in re.findall(bill_regex, event_data):
                    descr.add(bill_regexes[bill_regex] + bill[0])

            description = "Committee Hearing: %s" % committee_name

            key = (start, description, location)
            if key in self.event_dict:
                event = self.event_dict[key]
            else:
                event = Event(start, description, location, 'committee_markup', start_has_time=has_time,
                              chamber=chamber, start_time_description=start_time_description)
                if end_time_match:
                    end_time_string = "{:%b %d %Y} {}".format(start, end_time_match.group(0))
                    try:
                        end_time = parse(end_time_string)
                        end_time = self._tz.localize(end_time)
                    except ValueError:
                        pass
                    else:
                        event['end'] = end_time
            event.add_participant('host', committee_name, chamber=chamber)
            url = re.sub(',', '%2C', url)
            url = re.sub(' ', '%20', url)
            event.add_source(url)

            for bill in descr:
                bill = re.sub(r'\s+|\.', '', bill)
                event.add_related_bill(bill.strip(), type='bill')

            self.event_dict[key] = event

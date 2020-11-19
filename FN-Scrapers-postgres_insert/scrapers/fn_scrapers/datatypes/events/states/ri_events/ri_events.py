from __future__ import absolute_import
import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import urllib2
import re
from dateutil.parser import parse

agenda_url = "http://status.rilin.state.ri.us/agendas.aspx"

all_day = [  # ugh, hack
             "rise of the senate",
             "rise of the house",
             "rise of house",
             "rise of senate",
             "rise of the house & senate",
             "scheduled for the rise"
             ]
replace = {
    "House Joint Resolution No.": "HJR",
    "House Resolution No.": "HR",
    "House Bill No.": "HB",

    "Senate Joint Resolution No.": "SJR",
    "Senate Resolution No.": "SR",
    "Senate Bill No.": "SB",
    u"\xa0": " ",
    "SUB A": "",
    "SUB A as amended": ""
}


def parse_datetime(s):
    dt = None
    date_formats = [
        "%A, %B %d, %Y %I:%M %p",
        "%A, %B %d, %Y %I:%M",
    ]
    for f in date_formats:
        try:
            dt = datetime.datetime.strptime(s, f)
            break
        except:
            continue

    if not dt:
        try:
            dt = datetime.datetime.strptime(s, "%A, %B %d, %Y")
        except:
            raise

    return dt

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-RI")
class RIEventScraper(EventScraper):
    jurisdiction = 'ri'

    def __init__(self, *args, **kwargs):
        super(RIEventScraper, self).__init__('ri', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape_agenda(self, url):
        page = self.lxmlize(url)
        if "This meeting has been cancelled" in page.text_content():
            self.warning("The Meeting has been cancelled %s" % url)
            return
        # Get the date/time info:
        date_time = page.xpath("//table[@class='time_place']")
        if date_time == []:
            return
        try:
            date_time = date_time[0]
        except IndexError as err:
            self.error(err.args)
            return

        lines = date_time.xpath("./tr")
        metainf = {}
        for line in lines:
            tds = line.xpath("./td")
            try:
                metainf[tds[0].text_content()] = tds[1].text_content()
            except IndexError as err:
                self.error(err.args)
                continue
        date = metainf['DATE:']
        time = metainf['TIME:']
        where = metainf['PLACE:']

        # New time type: http://status.rilin.state.ri.us/documents/agenda-11721.aspx
        for day_string in all_day:
            if day_string in time.lower():
                when = date
                has_time = False
                break
        else:
            when = "%s %s" % (date, time)
            has_time = True

        if re.search(r"CANCEL{1,2}ED", when, flags=re.I):
            # Handle American and British spell.
            return

        transtable = {
            "P.M": "PM",
            "PM.": "PM",
            "P.M.": "PM",
            "A.M.": "AM",
            "POSTPONED": "",
            "RESCHEDULED": "",
            r'(?i),?\s?and rise of the senate': "",
            r'(?i),?\s?and continuing at the Rise': "",
            r'(?i),?\s?if necessary': "",
            r'(?i)\.?\s?This hearing will continue at the Rise': "",
            r'(?i)\.?\s?This hearing will continue at the rise': "",
            r'(?i),?\s?continue at the rise': "",
        }

        for old, new in transtable.items():
            when = re.sub(old, new, when)
        when = when.split('-')[0]
        try:
            when = parse(when.strip())
        except ValueError as e:
            self.critical(u"Unable to parse date from {}, {}".format(when.strip(), url))
            return
        if has_time:
            when = self._tz.localize(when)

        if where.strip() == '':
            return
        event = Event(when, 'Meeting Notice', where, 'committee_markup', start_has_time=has_time)
        url = urllib2.quote(url, "://?=&%+")
        event.add_source(url)
        # aight. Let's get us some bills!
        bills = page.xpath("//b/a")
        for bill in bills:
            bill_ft = bill.attrib['href']
            event.add_document(bill.text_content(), bill_ft)
            root = bill.xpath('../../*')
            root = [x.text_content() for x in root]
            bill_id = "".join(root)

            # delete line-throught type bills
            if bill.attrib['style'] == "text-decoration:line-through;":
                continue

            if "SCHEDULED FOR" in bill_id:
                try:
                    bill_id = bill_id.split('CONSIDERATION')[1].strip()
                except IndexError:
                    try:
                        bill_id = bill_id.split('HEARING')[1].strip()
                    except IndexError as err:
                        self.error(err.args)
                        continue
                if bill_id == '':
                    continue

            for thing in replace:
                bill_id = bill_id.replace(thing, replace[thing])
            bill_id = bill_id.strip()
            event.add_related_bill(bill_id,
                                   type='consideration')
        try:
            committee = page.xpath("//span[@id='lblSession']")[0].text_content()
        except IndexError:
            try:
                committee = page.xpath("//table[@class='committee']")[0].text_content().strip()
            except IndexError as err:
                self.error(err.args)
        if committee:
            chambers = {
                "house": "lower",
                "joint": "joint",
                "senate": "upper"
            }
            chamber = None
            for key in chambers:
                if key in committee.lower():
                    chamber = chambers[key]
            if chamber:
                event.add_participant("host", committee, chamber=chamber)
            else:
                event.add_participant("host", committee)
            event["description"] = "Committee Meeting: {}, {}".format(committee, time)
        self.save_event(event)

    def scrape_agenda_dir(self, url):
        page = self.lxmlize(url)
        rows = page.xpath("//table[@class='agenda_table']/tr")[1:]
        for row in rows:
            try:
                # If we scrape the page with all agendas, we end up getting duplicates for every one.
                if row.xpath("./td[contains(text(), 'View All Upcoming Agendas')]"):
                    continue
                else:
                    url = row.xpath("./td")[-1].xpath(".//a")[0]
            except IndexError as err:
                self.error(err.args)
                continue

            self.scrape_agenda(url.attrib['href'])

    def scrape(self):
        page = self.lxmlize(agenda_url)
        try:
            rows = page.xpath("//table[@class='agenda_table']/tr")[1:]
        except IndexError as err:
            self.error(err.args)
            return
        for row in rows:
            for column in range(0, 3):
                ctty = row.xpath("./td")[column]
                to_scrape = ctty.xpath("./a")
                for page in to_scrape:
                    self.scrape_agenda_dir(page.attrib['href'])
        self.save_events_calendar()

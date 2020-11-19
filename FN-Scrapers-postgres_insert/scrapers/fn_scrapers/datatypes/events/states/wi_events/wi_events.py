from __future__ import absolute_import

import re
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.extraction.textpdf import get_pdf_text
from fn_scrapers.common.http import HttpException
import datetime as dt
from requests import HTTPError

meeting_url = "http://committeeschedule.legis.wisconsin.gov/?filter=Upcoming&committeeID="

chamber_code = {
                "upper": "-3",
                "lower": "-2",
                "joint": "-4"}

bill_id_code = {
                "Assembly Bill" : "AB",
                "Senate Bill" : "SB",
                "Joint Bill" : "JB",
                "Assembly Resolution" : "AR",
                "Senate Resolution" : "SR",
                "Assembly Joint Resolution" : "AJR",
                "Senate Joint Resolution": "SJR"}


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-WI")
class WIEventScraper(EventScraper):
    jurisdiction = 'wi'

    def __init__(self, *args, **kwargs):
        super(WIEventScraper, self).__init__('wi', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def _fetch_pdf_lines(self, href):
        """
        fetch lines from pdf file
        :param href: href of pdf
        :return pdflines: list of lines in pdf context
        """
        try:
            pdflines = [line.decode('utf-8') for line in get_pdf_text(href).splitlines()]
        except ValueError:
            pdflines = [line.decode('latin-1') for line in get_pdf_text(href).splitlines()]
        return pdflines

    def scrape_participants(self, href):
        page = self.get_page(href)
        legs = page.xpath("//a[contains(@href, '/Pages/leg-info.aspx')]/text()")
        role_map = {"participant": "participant",
                    "Chair": "chair",
                    "Co-Chair": "chair",
                    "Vice-Chair": "participant"}
        ret = []
        for leg in legs:
            name = leg
            title = 'participant'
            if "(" and ")" in leg:
                name, title = leg.split("(", 1)
                title = title.replace(")", " ").strip()
                name = name.strip()
            title = role_map[title]
            ret.append({
                "name": name,
                "title": title
            })
        return ret

    def scrape(self):

        for chamber in chamber_code:
            code = chamber_code[chamber]
            page = self.get_page(meeting_url + code)
            events = page.xpath("//table[@class='agenda-body']//tr")[1:]

            for event_list in events:
                committee_page_url = event_list.xpath(
                    ".//a[contains(@href, '/Pages/comm-info.aspx?c=')]")

                if len(committee_page_url) != 1:
                    self.error("Failed to scrape committee page url")
                    continue

                try:
                    committee_page_url = committee_page_url[0]
                except IndexError:
                    self.error("Failed to scrape committee page")
                    continue

                try:
                    who = self.scrape_participants(committee_page_url.attrib['href'])
                except (HTTPError, HttpException):
                    self.error("Failed to scrape scrape_participants")
                    continue
                tds = event_list.xpath("./*")
                try:
                    date = tds[0].text_content().strip()
                    cttie = tds[1].text_content().strip()
                    cttie_chamber, cttie = [x.strip() for x in cttie.split(" - ", 1)]
                    info = tds[2]
                    name = info.xpath("./a[contains(@href, 'raw')]")[0]
                    notice = name.attrib['href']
                    name = name.text
                    description = cttie + ", " + name.replace("REVISED:", "Revised")
                    time, where = info.xpath("./i/text()")
                except IndexError:
                    self.error("Failed to scrape committee inforamtion")
                    continue

                when = ", ".join([date, str(dt.datetime.now().year), time])
                when = dt.datetime.strptime(when, "%a %b %d, %Y, %I:%M %p")
                when = self._tz.localize(when)

                event = Event(when, description, where, 'committee_markup', start_has_time=True, chamber=chamber)

                event.add_source(meeting_url)
                event.add_participant('host', cttie, chamber=cttie_chamber)
                event.add_document("notice", notice)

                for thing in who:
                    event.add_participant(thing['title'], thing['name'], chamber=cttie_chamber)
                pdflines = self._fetch_pdf_lines(notice)
                full = '\n'.join(pdflines)
                content = re.sub(r'\n', ' ', full)
                content = re.sub(r'\s+', ' ', content)
                bill_re = r'((?:Assembly|Senate)? (?:Assembly|Senate|Joint) (?:Bill|Resolution)) (\d+) Relating to:(.*?) By'
                bills = re.findall(bill_re, content)
                for bill in bills:
                    try:
                        pre_bill_id = bill[0].strip()
                        pre_bill_id = re.sub(r'\s+', ' ', pre_bill_id)
                        pre_bill_id = pre_bill_id.strip()
                        bill_id = bill_id_code[pre_bill_id]
                        bill_id += bill[1]
                        event.add_related_bill(bill_id, 'consideration')
                    except IndexError:
                        self.error("Failed to scrape bill inforamtion")
                        continue

                self.save_event(event)

        self.save_events_calendar()
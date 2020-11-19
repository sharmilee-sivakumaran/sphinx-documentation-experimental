from __future__ import absolute_import

from datetime import datetime
import logging
import re

import lxml.html

from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-KY")
class KYEventScraper(EventScraper):
    jurisdiction = 'ky'
    time_pattern = re.compile('^(\d+:\d+ ?[ap]m)[, ]*(.*)$', re.I)
    nontime_pattern = re.compile(r'^(\d{2}/\d{2}/\d{4}) (Upon .+?),\s*(.*)$', re.I)

    def __init__(self, *args, **kwargs):
        super(KYEventScraper, self).__init__('ky', __name__, **kwargs)

    def scrape(self):
        url = "http://www.lrc.ky.gov/legislativecalendarv2/sp_bss_calendar_/index"
        page = self.get(url).text
        page = lxml.html.fromstring(page)

        for event in self.scrape_page(url, page, self._tz):
            self.save_event(event)
        self.save_events_calendar()

    @classmethod
    def scrape_page(cls, url, page, timezone):
        '''Generator function that yields event objects. '''
        event = None
        base_date = None
        for div in page.xpath(
                "//div[@class = 'container body-content']/div[@class = 'col-md-6']"):
            text = ''.join(div.xpath(".//text()")).strip()

            if div.attrib['style'] == 'margin-left: 12px':
                base_date = datetime.strptime(text, "%A, %B %d, %Y")
                logging.info("Date: " + str(base_date))
                continue

            if not base_date:
                continue

            match = cls.time_pattern.match(text)
            if match:
                when = datetime.combine(base_date.date(), datetime.strptime(
                    match.group(1), '%I:%M %p').time())
                when = timezone.localize(when)
                logging.info("Time: " + str(when))
                location = match.group(2)
                if event:
                    yield event
                event = Event(when, '', location, 'committee_hearing', True)
                event.add_source(url)
                continue

            match = cls.nontime_pattern.match(text)
            if match:
                when = datetime.strptime(match.group(1), '%m/%d/%Y')
                logging.info("Time: " + str(when))
                if event:
                    yield event
                event = Event(when, match.group(2), match.group(3),
                              'committee_hearing', False)
                event.add_source(url)
                continue

            if not event or text == 'No Meetings Scheduled':
                continue
            # left over from previous scraper
            if 'Canceled' in event['location'] or div.xpath(".//del"):
                event = None
                continue

            for committee in div.xpath(
                    ".//a[contains(@href, 'lrc.ky.gov/committee/')]/text()"):
                event.add_participant('host', committee)
                logging.info("Committee: " + committee)

            if text.startswith("Members:"):
                continue

            if text in ['Senate Convenes', 'House Convenes']:
                event = None
                continue

            for bill in div.xpath(
                    ".//a[contains(@href, 'lrc.ky.gov/record/')]/text()"):
                logging.info("Bill: " + bill)
                event.add_related_bill(bill)

            if text.startswith("Agenda:"):
                # drop the <b>Agenda:</b>
                event['description'] = text[7:].strip()
                yield event
                event = None
            else:
                if event.get("participants"):
                    event['description'] = ", ".join(x['name'] for x in event['participants']) + " hearing"
                else:
                    event['description'] = "Committee Hearing"

        if event:
            yield event

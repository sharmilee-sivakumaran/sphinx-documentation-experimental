from __future__ import absolute_import
import lxml.html
from datetime import datetime

from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event

from fn_service.server import fmt

from fn_scrapers.api.scraper import scraper, tags
import logging


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NE")
class NEEventScraper(EventScraper):
    """
    Sraper workflow:

    - The scraper scrapes the current week and the next week meetings, by going to the following URLs
      http://nebraskalegislature.gov/calendar/hearings_range.php?weekly=this
      http://nebraskalegislature.gov/calendar/hearings_range.php?weekly=next

    - Events are grouped together by the dates on the website, the scraper looks for the XPath
       //div[@class="main-content"]//h2[@class="text-center"] <-- Dates on which events pccur in the week

    - Then it generates a URL for each date and follows that URL
      https://nebraskalegislature.gov/calendar/hearings_range.php?startMonth=02&startDay=22&startYear=2018

    - Here the scraper looks for the elements
       //div[@class="main-content"]//h2[@class="text-center"]/following::*//div[@class="panel-heading middle-heading"]/h2
         ^^Committee name^^
       //div[@class="main-content"]//h2[@class="text-center"]/following::*//div[@class="panel-heading middle-heading"]/h2/small
         ^^Location and time^^
       //div[@class="main-content"]//h2[@class="text-center"]/following::*//table[@class="table table-condensed table-striped"]//tr/td[1]/a
         ^^Bill IDs^^

    - Finally save all the events
    """

    jurisdiction = 'ne'
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        super(NEEventScraper, self).__init__('ne', __name__, **kwargs)

    def scrape(self):

        self.scrape_committee_hearings()
        self.save_events_calendar()

    def scrape_committee_hearings(self):
        # TODO: We could query (POST request) for a list of events in a time
        # range, but for some reason the request isn't working properly, so we use these weekly pages.

        # Use following link to test
        # 'http://nebraskalegislature.gov/calendar/hearings_range.php?startMonth=08&startDay=21&startYear=2015&endMonth=09&endDay=22&endYear=2015',
        source_urls = [
                'http://nebraskalegislature.gov/calendar/hearings_range.php?weekly=this',
                'http://nebraskalegislature.gov/calendar/hearings_range.php?weekly=next'
        ]

        for source_url in source_urls:
            source_page = self.get(source_url).lxml()

            for event in self.get_Events(source_page, self._tz):
                self.save_event(event)

    @classmethod
    def get_Events(cls, source_page, tz):
        date_lists = source_page.xpath('//div[@class="main-content"]//h2[@class="text-center"]')
        base_url = "http://nebraskalegislature.gov/calendar/hearings_range.php?startMonth"\
                   "=%02d&startDay=%02d&startYear=%04d"
        for date_row in date_lists:
            date_string = date_row.text
            # print date_string
            try:
                start_date = datetime.strptime(date_string, '%A %B %d, %Y')
            except:
                cls.logger.error(fmt("Failed to format date {}", date_string))
                continue
            day = start_date.day
            month = start_date.month
            year = start_date.year
            source_link = base_url % (month, day, year)

            content = date_row.getnext()

            for comm_event in cls.generate_committee_events(content, start_date, source_link, tz):
                yield comm_event

    @classmethod
    def generate_committee_events(cls, content_ele, start_date, source_url, tz):
        headers = content_ele.xpath('.//div[@class="panel-heading middle-heading"]/h2')
        for header in headers:
            bill_ids = []
            comm_name = header.text
            location_time = next(header.iterchildren(tag='small'), None)
            if location_time is None:
                cls.logger.error("Location not found")
                continue
            location, time = location_time.text.split('-')
            location = location.strip()
            time_string = time.strip()

            new_start_date = start_date
            try:
                time = datetime.strptime(time_string, "%I:%M %p")
                new_start_date = new_start_date.replace(hour=time.hour, minute=time.minute)
                new_start_date = tz.localize(new_start_date)
                has_time = True
            except ValueError as e:
                cls.logger.warning(fmt('Unable to extract time from {!r}', time_string), exc_info=True)
                has_time = False

            event = Event(new_start_date, comm_name, location, 'committee_markup', start_has_time=has_time)
            event.add_source(source_url)
            event.add_participant('host', comm_name)

            bills = header.getparent().getnext()
            if bills.tag == 'table':
                bills = bills.xpath('.//tr/td[1]/a')
                for bill_row in bills:
                    event.add_related_bill(bill_row.text.strip())
            yield event

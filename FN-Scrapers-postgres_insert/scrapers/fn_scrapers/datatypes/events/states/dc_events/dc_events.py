from __future__ import absolute_import
import re
import lxml.html
from datetime import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-DC")
class DCEventScraper(EventScraper):
    jurisdiction = 'dc'

    # Regex for identifying key pieces of information
    # Types of 'bills': PR xx-xxxx, CA xx-xxxx, B xx-xxxx, CER xx-xxxx
    bill_re = re.compile('(PR|CA|B|CER) ?(\d+)-(\d+)(.*)\n')
    committee_title_re = re.compile('committee on', re.I)
    committee_re = re.compile(r'(?i)committee? on (.*?) will', re.I)

    def __init__(self, *args, **kwargs):
        super(DCEventScraper, self).__init__('dc', __name__, **kwargs)

    def lxmlize(self, url):
        page = self.get(url).text
        page = page.replace(u'\xa0', u' ')
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)

        # Convert <br> to newlines to make text processing easier with text_content()
        for br in page.xpath("*//br"):
            br.tail = "\n" + br.tail if br.tail else "\n"

        return page

    def scrape(self):

        self.scrape_legislative_events()
        self.save_events_calendar()

    def scrape_legislative_events(self):
        # Source page lists all events and their details
        source_url = 'http://dccouncil.us/calendar/index'
        page = self.lxmlize(source_url)
        events = page.xpath('//div[@class="event-description-dev"]')
        event_dict = {}
        for event in events:
            metadata = event.xpath('.//div[@class="event-description-dev-metabox"]')
            metadata = metadata[0] if len(metadata) > 0 else None
            if metadata is None:
                continue

            # Get date, time, and location
            metadata_parts = metadata.xpath('.//p')
            date_raw = metadata_parts[0].text_content().strip()
            time_raw = metadata_parts[1].text_content().strip()

            if len(metadata_parts) < 3:
                location = 'N/A'
            else:
                location = metadata_parts[2].text_content().strip()

            date = datetime.strptime(date_raw, '%A, %m/%d/%Y')
            if len(time_raw) > 0:
                time_obj = datetime.strptime(time_raw, '%I:%M%p')
                date = date.replace(hour=time_obj.hour, minute=time_obj.minute)
                date = self._tz.localize(date)
                has_time = True
            else:
                has_time = False

            data = event.xpath('.//div[@class="event-description-content-dev"]')
            data = data[0] if len(data) > 0 else None
            if data is None:
                continue

            # Get all text associated with event to search through it
            data_text = data.text_content()

            # Get participant
            committee = None
            title = event.xpath('.//h3')
            title = title[0] if len(title) > 0 else None
            if title is None:
                continue
            title_text = title.text_content().strip()
            if self.committee_title_re.search(title_text):
                committee = title_text.replace('Committee on', '')\
                        .replace('POH', '').replace('the', '').strip()

            # Get URL to event page
            event_url = title.xpath('.//a')
            event_url = event_url[0] if len(event_url) > 0 else None
            event_url = event_url.get('href')
            if event_url is None:
                continue
            if (date, title_text, location) in event_dict:
                continue
            event_dict[(date, title_text, location)] = True
            event = Event(date, title_text, location, 'committee_markup', start_has_time=has_time)
            event.add_source(event_url)

            # Set participant
            if committee:
                event.add_participant('host', committee)
            # If no participant, try again to find one
            else:
                data_text = re.sub(r'\xa0', ' ', data_text)
                search_res = self.committee_re.search(data_text)

                if search_res:
                    committee = str(search_res.group(1).strip())
                    if committee[-1] == ',':
                        committee = committee[:-1]
                    event.add_participant('host', committee)

            # Get/set related bills
            list_items = data.xpath('.//li')
            list_text = ''
            for list_item in list_items:
                list_text += list_item.text_content() + '\n'
            bill_search = self.bill_re.findall(list_text)
            for res in bill_search:
                bill_id = "%s %s-%s" %(res[0], res[1], res[2].zfill(4))
                event.add_related_bill(bill_id, 'bill')

            self.save_event(event)

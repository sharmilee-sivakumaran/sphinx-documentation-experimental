"""
:class NYEventScraper: scrapes New York Bills
"""
from __future__ import absolute_import
import re
import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.datatypes.events.common.utils import iter_months
import traceback
from fn_scrapers.common.http import request_json, HttpException
import sys
from fn_scrapers.api.scraper import tags, scraper
import calendar
import lxml.html
from requests import HTTPError


@scraper()
@tags(group="fnleg", type="events", country_code="US", subdivision_code="US-NY")
class NYEventScraper(EventScraper):
    """
    NYEventScraper

    Scrape New York Events
    """
    jurisdiction = 'ny'

    def __init__(self, *args, **kwargs):
        super(NYEventScraper, self).__init__('ny', __name__, **kwargs)

    def _lxmlize(self, url):
        page = self.get(url).text
        page = lxml.html.fromstring(page)
        page.make_links_absolute(url)
        return page

    def scrape(self):
        """
        scrape's events from NY for given session/chambers
        """
        self._scrape_upper()
        self._scrape_lower()
        self.save_events_calendar()

    def _scrape_upper(self):
        event_descriptions = set([])

        url = 'http://www.nysenate.gov/events/month'
        url += '?type=meeting&field_date_value[value][date]={month}/{year}&page={pageIdx}'

        today = datetime.date.today()
        for d in iter_months(today - datetime.timedelta(days=2), today + datetime.timedelta(days=90)):
            page_index = 0
            while True:
                qurl = url.format(
                    month=d.month,
                    year=d.year,
                    pageIdx=page_index
                )
                page_index += 1
                page = self._lxmlize(qurl)
                event_links = page.xpath('//h3[@class="c-event-name"]/span/a/@href')
                if not len(event_links):
                    # if resp is invalid or there are no more events, move onto the next month
                    break
                for link in event_links:
                    self._upper_scrape_event(link, str(d.year), event_descriptions)

    def _scrape_lower(self):
        hearing_url = "http://assembly.state.ny.us/leg/?sh=hear"
        page = self._lxmlize(hearing_url)
        tables = page.xpath("//table[@class='pubhrgtbl']")
        chamber = 'lower'
        for table in tables:
            try:
                event_meta = self._parse_table_rows_to_dict(table)
                when, has_time, is_postponed = self._date_from_metainf(event_meta)
                if when == None:
                    continue

                meeting_types = ['meeting', 'hearing', 'roundtable', 'summit']
                title_key = [x for x in event_meta.keys() if any(y in x for y in meeting_types)]
                if 'public hearing' in event_meta:
                    desc = event_meta['public hearing']
                else:
                    desc = 'committee:meeting'
                if len(title_key) != 1: 
                    self.warning("Couldn't determine event title. %s", title_key)
                if len(title_key) == 0:
                    self.warning("No title, skipping")
                    continue
                title_key = list(title_key).pop()
                title = event_meta[title_key]

                # If event was postponed, add a warning to the title.
                if is_postponed:
                    title = 'POSTPONED: %s' % title

                event = Event(when, desc, event_meta['place'], 'committee_markup', chamber='lower', start_has_time=has_time)

                event.add_source(hearing_url)

                for committee in event_meta['committee']:
                    event.add_participant('host', committee[0], chamber=committee[1])

                self.save_event(event)
            except Exception as exc:
                self.exception(traceback.format_exc())
                self.exception("Exception raised when scraping NY events")


    def _upper_scrape_event(self, event_url, year, event_descriptions): #pylint: disable=R0201
        has_time = False
        meeting_details = []

        committee_name_keyword = 'Committee on '
        chair_keyword = ', Chair'

        try:
            page = self._lxmlize(event_url)
        except Exception as exc:
            self.warning(exc)
            return

        # we need to figure out which line has meeting details
        for p in page.xpath('//div[@class="c-meeting-detail--descript"]/p'):
            if len(meeting_details) == 0: # if meeting details haven't been populated yet
                for line in p.itertext():
                    # if it contains comm_name or chair, we're in the right section
                    if committee_name_keyword in line or chair_keyword in line:
                        meeting_details = [line for line in p.itertext() if line.split()]
                        break
            else:
                break # meeting details already populated

        # check to see if all 4 lines are there: committee name, chair, datetime, and location
        if len(meeting_details) != 4:
            self.warning('%s has a different meeting details format with %s line(s)',
                         event_url, len(meeting_details))

        committee_name = ''
        date = None
        chair = ''
        location = ''

        # scan meeting details for relevant info
        for detail in meeting_details:
            if committee_name_keyword in detail:
                start_index = detail.find(committee_name_keyword) + len(committee_name_keyword)
                committee_name = detail[start_index:].strip()
            elif 'Senator ' and chair_keyword in detail:
                start_index = detail.find('Senator ') + len('Senator ')
                end_index = detail.find(chair_keyword)
                chair = detail[start_index:end_index].strip()
            elif detail.split()[-1] == year:
                # sometimes there are random extra spaces, so let's compress
                raw_date = ''.join(detail.split())
                try:
                    raw_date = re.sub(r'12noon', '12:00PM', raw_date, flags=re.I) # special case
                    date = datetime.datetime.strptime(raw_date,
                        '%I:%M%p,%A,%B%d,%Y')
                    has_time = True

                except:
                    try:
                        date = datetime.datetime.strptime(raw_date,
                            '%A,%B%d,%Y')
                    except:
                        try:
                            date = datetime.datetime.strptime(raw_date,
                                '%A,%b.%d,%Y')
                        except:
                            self.warning('could not parse date with this format %s', raw_date)
            else:
                # there is no structured location format
                location = detail

        # log if there were missing details in meeting details
        if not committee_name:
            self.warning('committee name not in meeting details, %s', event_url)

        if not chair:
            self.warning('chair not in meeting details, %s', event_url)

        if not date:
            self.warning('date not in meeting details, %s', event_url)

        if not location:
            self.warning('location not in meeting details, %s', event_url)


        # in case committee name could not be extracted from meeting details
        if not committee_name and page.xpath('//h2[@class="c-title"]/text()'):
            raw_name = page.xpath('//h2[@class="c-title"]/text()')[0].strip()
            if ' Committee' in raw_name:
                committee_name = raw_name[:raw_name.find(' Committee')]
            else:
                committee_name = raw_name
        if not committee_name and page.xpath('//h2[@class="c-meeting-detail--title"]/text()'):
            # if it's still not populated try meeting title
            self.warning('this page did not have committee name at the bottom: %s', event_url)
            meeting_title = page.xpath('//h2[@class="c-meeting-detail--title"]/text()')[0].strip()
            if ' Meeting' in meeting_title:
                committee_name = meeting_title[:meeting_title.find(' Meeting')]
            else:
                committee_name = meeting_title
        if not committee_name:
            # if it's still not populated use the URL
            self.warning('this page did not have committee name in meeting title: %s', event_url)
            raw_name = event_url.split('/')[-3]
            committee_name = ' '.join(raw_name.title().split('-'))

        # in case date could not be extracted from meeting details
        if not has_time:
            time = None
            if page.xpath('//p[@class="c-meeting-detail--time"]/text()'):
                raw_time = page.xpath('//p[@class="c-meeting-detail--time"]/text()')[0].strip()
                raw_time = ''.join(raw_time.split())
                raw_time = raw_time[:raw_time.find('M') + 1]
                try:
                    time = datetime.datetime.strptime(raw_time, '%I:%M%p').time()
                    has_time = True
                except:
                    self.warning('unable to parse time %s from %s', raw_time, event_url)
            if date:
                date = date.combine(date, time)
            else:
                self.warning('getting date from URL. %s', event_url)
                raw_date = event_url.split('/')[-2]
                try:
                    date = datetime.datetime.strptime(raw_date,
                        '%B-%d-%Y')
                    if time: # add time component if it exists
                        date = date.combine(date, time)
                except:
                    self.warning('no date in URL. %s', event_url)
                    self.warning('Not scraping because no date exists %s', event_url)
                    return

        # in case location could not be extracted from meeting details
        if not location:
            if page.xpath('//p[@class="c-meeting-detail--location"]/text()'):
                location = page.xpath('//p[@class="c-meeting-detail--location"]/text()')[0].strip()

        description = '{} Hearing: {}'.format(committee_name, date.strftime("%c"))

        # Duplicate Event
        if description in event_descriptions:
            self.warning(u"Duplicate event found with description: '%s'", description)
            return
        else:
            event_descriptions.add(description)

        date = self._tz.localize(date)
        # construct event object
        event = Event(date, description, location or 'No location given.',
                      'committee_markup', start_has_time=has_time, chamber='upper')

        event.add_source(event_url)
        if chair:
            event.add_participant('chair', chair, chamber='upper')
        event.add_participant('host', committee_name, chamber='upper')

        if page.cssselect("#edit-submit-meeting-agenda-block"):
            """
            This means that the event page has mmore data which needs to be requested

            curl 'https://www.nysenate.gov/views/ajax'
            -H 'content-type: application/x-www-form-urlencoded; charset=UTF-8'
            -H 'accept: application/json, text/javascript, */*; q=0.01'
            --data 'items_per_page=All&view_name=meeting_agenda_block&view_display_id=meeting_agenda&view_args=6670952'
            --compressed
            """
            page_class = page.find('body').get('class')
            node_match = re.search(r'page-node-(\d+)', page_class)
            if node_match:
                view_arg = node_match.group(1)
            else:
                view_arg = ''

            self.info("MORE DATA AVAILABLE")
            form_data = {
                'items_per_page': 'All',
                'view_name': 'meeting_agenda_block',
                'view_display_id': 'meeting_agenda',
                'view_args': view_arg,
            }
            headers = {
                'accept': 'application/json',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            }
            try:
                resp = request_json("https://www.nysenate.gov/views/ajax", data=form_data, method="POST",
                                    headers=headers)
                bill_html = resp[-1]['data']
                page = lxml.html.fromstring(bill_html)
            except (HTTPError, HttpException):
                self.warning("Unable to load all bills from %s", event_url)

        for row in page.xpath("//h4[@class='c-listing--bill-num']"):
            bill_id = row.text_content()
            descr = row.xpath("./ancestor::div/following-sibling::div/p")[0].text_content().strip()
            event.add_related_bill(bill_id, type='bill', description=descr)

        self.save_event(event)




    def _parse_table_rows_to_dict(self, table):
        date = None
        committee = None
        metainf = {}
        rows = table.xpath(".//tr")

        for row in rows:
            tds = row.xpath("./*")
            if len(tds) < 2:
                continue
            key, value = tds
            if key.tag == 'th':
                date = key.text_content()
                date = re.sub(r'\s+', ' ', date)
                date = re.sub(r'.*POSTPONED NEW DATE', '', date).strip()
                committee_list = value.xpath(".//div[@class='comm-txt bold']")
                for committee in committee_list:
                    committee = committee.text_content()

                    # Some committee names use \x96 instead of '-', causing unicode related failures
                    committee = committee.replace(u'\x96', "-")
                    committee = re.sub(r'^and', '', committee).strip()

                    chamber = 'lower'
                    if "senate" in committee.lower():
                        chamber = 'upper'
                    if "house" in committee.lower():
                        chamber = 'lower'
                    if "joint" in committee.lower():
                        chamber = 'joint'

                    metainf['date'] = date
                    if 'committee' not in metainf:
                        metainf['committee'] = []
                    metainf['committee'].append([committee, chamber])
            elif key.tag == 'td':
                key = key.text_content().strip().replace(':', '').lower()
                value = value.text_content().strip()
                value = value.replace(u'\x96', '-')
                value = re.sub(r'\s+', ' ', value)
                metainf[key] = value
        return metainf


    def _date_from_metainf(self, metainf):
        time = metainf['time']
        date = metainf['date']
        repl = {"A.M.": "AM", "P.M.": "PM"}
        drepl = {"Sept": "Sep"}

        for rep in repl:
            time = time.replace(rep, repl[rep])
        for rep in drepl:
            date = date.replace(rep, drepl[rep])


        year = datetime.datetime.now().year
        if not re.search(r'(?i)\d{1,2}:\d{2} [AP]M', time):
            date = "%s %s" % (date, year)
            has_time = False
        else:
            time = re.sub("-.*", "", time)
            time = time.strip()
            date = "%s %s %s" % (date, year, time)
            has_time = True

        if "tbd" in date.lower():
            self.warning("To be determined!?")

        date = date.replace(' PLEASE NOTE NEW TIME', '')

        # Check if the event has been postponed.
        postponed = 'postponed' in date.lower()
        if postponed:
            date = date.replace(' POSTPONED', '')

        date_formats = ["%B %d %Y %I:%M %p",
                        "%b. %d %Y %I:%M %p",
                        "%B %d %Y", "%b. %d %Y",
                        ]
        date =re.sub(" NOTE NEW DATE AND TIME ", " ", date)
        formed_date = None
        for fmt in date_formats:
            try:
                formed_date = datetime.datetime.strptime(date, fmt)
                break
            except ValueError:
                continue

        if formed_date is None:
            self.warning("Couldn't parse date %s", date)
        return self._tz.localize(formed_date), has_time, postponed

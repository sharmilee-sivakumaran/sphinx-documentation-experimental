from __future__ import absolute_import
import re
import pytz
from datetime import datetime
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from .session_details import session_details
from fn_scrapers.datatypes.events.common.metadata import get_session_from_internal_id
from fn_scrapers.datatypes.events.common.metadata import _get_active_sessions as get_active_sessions
from fn_scrapers.common.http import HttpException


@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-VA")
class VAEventScraper(EventScraper):
    jurisdiction = 'va'

    base_url = 'http://lis.virginia.gov'

    # For date and time extraction from raw text
    time_re = re.compile('(1|2|3|4|5|6|7|8|9|10|11|12)(:\d{2})? ?([ap]\.?m\.?)', re.I)
    date_re = re.compile('[a-zA-Z]+ [1-3]?[0-9], 20[0-9]{2}')

    # For strings containing absolute time and location
    location_re = re.compile('(1|2|3|4|5|6|7|8|9|10|11|12)(:\d{2})? ?([ap]\.?m\.?);?\s*[-,/;](.*)[\n\r]?', re.I)
    # For strings containing relative time and location
    location_re2 = re.compile('(after|adjournment|conclusion).*[-,;/](.*)[\n\r]?', re.I)
    # For strings containing the word 'room'
    location_re3 = re.compile('Time and Place: (.*room.*)\n\r', re.I)

    scraped_events = {}

    def __init__(self, *args, **kwargs):
        super(VAEventScraper, self).__init__('va', __name__, **kwargs)

    def get_session_name(self, session):
        session_dict = get_session_from_internal_id(self.jurisdiction, session, self.metadata)
        return session_dict['name']

    def get_today_as_timezone(self):
        today = datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz)

    def scrape(self):
        # Set URL extension for page containing list of committees
        comms_list_extension = '/com/COM.HTM'
        sessions = get_active_sessions(self.jurisdiction, self.metadata, True)
        for session in sessions:
            for year in [session[4:8], session[:4]]:
                """
                To handle special sessions and avoid scraping the same urls again,
                we need to look at the session name as well, to make sure we get the
                correct site id from the session_details file.
                """
                if not session.endswith('r'):
                    name = self.get_session_name(session)
                    if year not in name:
                        continue
                    year = '{}special{}'.format(year, session[-1])
                comms_url_code = session_details[year]["site_id"]
                # Get committees list page for a session
                last_comm_url = self.base_url + '/' + comms_url_code + comms_list_extension
                try:
                    comms_list_page = self.get(last_comm_url)
                    comms_list_page = comms_list_page.lxml()
                except HttpException as e:
                    self.warning(e)
                    continue

                # Set xpath to get links to committee pages

                house_links_xpath = '//div[@class="lColLt"]/ul/li/a'
                comm_link_elements = comms_list_page.xpath(house_links_xpath)
                for comm_link_element in comm_link_elements:
                    link = self.base_url + self.convert_path(comm_link_element.get('href'))
                    name = comm_link_element.text_content()
                    result = self.scrape_house_committee_events(link, name, 'lower', session)
                    if not result:
                        self.warning('Failed to scrape a committee\'s events: ' + link)

                senate_links_xpath = '//div[@class="lColRt"]/ul/li/a'
                # Get committee links and scrape them
                comm_link_elements = comms_list_page.xpath(senate_links_xpath)
                for comm_link_element in comm_link_elements:
                    link = self.base_url + self.convert_path(comm_link_element.get('href'))
                    name = comm_link_element.text_content()
                    # TODO: Scrape subcommittee events
                    result = self.scrape_committee_events(link, name, 'upper', session)
                    if not result:
                        self.warning('Failed to scrape a committee\'s events: ' + link)

        self.save_events_calendar()

    def scrape_house_committee_events(self, link, name, chamber, session):
        try:
            comm_page = self.get(link)
            comm_page = comm_page.lxml()
        except HttpException:
            return None

        # Get link to committee dockets (committee events info)
        comm_agendas_link_elements = comm_page.xpath('//div[@id="mainC"]/ul/li/a[text()="Agendas"]')
        if len(comm_agendas_link_elements) > 0:
            comm_agendas_link = comm_agendas_link_elements[0].get('href')
        else:
            return False
        try:
            agenda_page = self.get(comm_agendas_link)
            agenda_page = agenda_page.lxml()
        except:
            return None
        meeting_base_url = 'http://virginiageneralassembly.gov/house/agendas/'
        flag = False
        for meeting_link in agenda_page.xpath(".//div[@class='agendaContainer']/table//tr/td[1]/" +
                                              "a[not(./following-sibling::span[contains(., 'Cancelled')])]"):
            meeting_url = meeting_base_url + meeting_link.attrib['href']
            event = self.scrape_house_committee_event(name, chamber, session, meeting_url)
            if event:
                flag = True
            else:
                self.warning('Bad event link or event has been cancelled: %s' % meeting_url)
        for event in self.scraped_events.values():
            self.save_event(event)

        self.scraped_events = {}

        # Get link to subcommittee dockets (committee events info)
        subcommittee_link_elements = comm_page.xpath('//div[@id="mainC"]/h4[text()="SUB-COMMITTEES:"]/following-sibling::ul[1]//a')
        for subcommittee_link_element in subcommittee_link_elements:
            subcommittee_link = self.base_url + self.convert_path(subcommittee_link_element.get('href'))

            try:
                subcommittee_page = self.get(subcommittee_link)
                subcommittee_page = subcommittee_page.lxml()
            except:
                self.warning('Couldn\'t get subcommittee dockets for %s' % subcommittee_link)
                continue

            subcommittee_docket_link_elements = subcommittee_page.xpath('//div[@id="mainC"]/p/a[text()="Agendas"]')
            if len(subcommittee_docket_link_elements) > 0:
                subcomm_agendas_link = subcommittee_docket_link_elements[0].get('href')
            else:
                continue
            agenda_page = self.get(subcomm_agendas_link)
            agenda_page = agenda_page.lxml()
            committee_name = subcommittee_page.xpath("//div[@id='mainC']/h3[@class ='xpad']")[0].text_content()
            committee_name = re.sub(r'\s+', ' ', committee_name).strip()

            for meeting_link in agenda_page.xpath(".//div[@class='agendaContainer']/table//tr/td[1]/" +
                                                  "a[not(./following-sibling::span[contains(., 'Cancelled')])]"):
                meeting_url = meeting_base_url + meeting_link.attrib['href']
                event = self.scrape_house_committee_event(committee_name, chamber, session, meeting_url)
                if event:
                    flag = True
                else:
                    self.warning('Bad event link or event has been cancelled: %s' % meeting_url)

            for event in self.scraped_events.values():
                self.save_event(event)

            self.scraped_events = {}

        return flag

    def scrape_house_committee_event(self, name, chamber, session, meeting_url):
        meeting_page = self.get(meeting_url)
        meeting_page = meeting_page.lxml()
        if "This meeting has been CANCELLED." in meeting_page.text_content():
            return None
        try:
            date = meeting_page.xpath("//section[@class='agenda-show-info']//td[text()='Meeting Date & Time:']")[0].getnext()
            date = date.text_content().strip()

            date = re.sub(r'\s+', ' ', date)
            date = re.sub(r'\.', '', date).strip()
            form_date = re.findall(r'\d{2}\/\d{2}\/\d{4}', date)[0]
            time = re.findall(r'\d{1,2}:\d{2}\s?(?:[aApP][Mm])?', date)
            if len(time) > 0:
                time = time[0].strip()
                if 'm' not in time.lower():
                    hour = re.findall(r'(\d+):', time)[0]
                    if int(hour) > 6:
                        time = time + ' ' + 'am'
                    else:
                        time = time + ' ' + 'pm'

                form_date = form_date + ' ' + time
                try:
                    date = datetime.strptime(form_date, "%m/%d/%Y %I:%M %p")
                    has_time = True
                except:
                    try:
                        date = datetime.strptime(form_date, "%m/%d/%Y %I:%M%p")
                        has_time = True
                    except:
                        return None
            else:
                try:
                    date = datetime.strptime(form_date, "%m/%d/%Y")
                    has_time = False
                except:
                    return None
            date = self._tz.localize(date)
        except:
            return None

        try:
            place = meeting_page.xpath("//section[@class='agenda-show-info']//td[text()='Location:']")[0].getnext()
            place = place.text_content().strip()
        except:
            place = 'N/A'
        try:
            desc = meeting_page.xpath("//section[@class='agenda-show-info']//td[text()='Notes:']")[0].getnext()
            desc = desc.text_content().strip()
        except IndexError:
            desc = '%s Hearing' % name

        if not desc:
            desc = '%s Hearing' % name
        else:
            desc = "%s - %s" % (name, desc)

        if date.date() < self.get_today_as_timezone().date():
            return False

        event = Event(date, desc, place, 'committee_markup', chamber=chamber, start_has_time=has_time, session=session)

        props = (date, desc, place,)
        event.add_source(meeting_url)
        # Get/set participants
        event.add_participant('host', name, chamber=chamber)

        no_bills_msg = meeting_page.xpath("//div[@class='agenDockOpts']/a[@class='notifierText']/text()")
        if not(no_bills_msg and u"No Bills" in no_bills_msg[0]):
            for bill_row in meeting_page.xpath("//div[@class='agendaContainer']//tr[@class='standardZebra']"):
                bill_col = bill_row.xpath('./td')
                if len(bill_col) < 3:
                    continue
                bill_id = bill_col[0].text_content().strip()
                match = re.search(r"^([A-Z]{2,}\s*\d+)\s", bill_id)
                if match:
                    bill_id = match.group(1)
                bill_desc = bill_col[2].text_content().strip()
                if props not in self.scraped_events:
                    event.add_related_bill(bill_id)
                else:
                    self.scraped_events[props].add_related_bill(bill_id)

        if props in self.scraped_events:
            self.scraped_events[props].add_source(meeting_url)
            if not any(name == x['name'] for x in self.scraped_events[props]['participants']):
                self.scraped_events.add_participant('host', name, chamber=chamber)
        else:
            self.scraped_events[props] = event
        return True

    def scrape_committee_events(self, link, name, chamber, session):
        # Open page with links to committee pages
        try:
            comm_page = self.get(link)
            comm_page = comm_page.lxml()
        except HttpException:
            return None
        comm_dockets_link_elements = None
        comm_dockets_link = None
        dockets_page = None
        docket_link_elements = None

        # Get link to committee dockets (committee events info)
        comm_dockets_link_elements = comm_page.xpath('//div[@id="mainC"]/ul/li/a[text()="Committee dockets"]')
        if len(comm_dockets_link_elements) > 0:
            comm_dockets_link = self.convert_path(comm_dockets_link_elements[0].get('href'))
        else:
            return False

        # Get committee dockets
        comm_docket_url = self.base_url + comm_dockets_link
        dockets_page = self.get(comm_docket_url)
        dockets_page = dockets_page.lxml()
        docket_link_elements = dockets_page.xpath('//div[@id="mainC"]/ul/li/a')
        event_dict = {}
        for docket_link_element in docket_link_elements:
            docket_link = self.base_url + self.convert_path(docket_link_element.get('href'))
            self.scrape_committee_event(name, chamber, session, docket_link, event_dict)

        for event in event_dict:
            self.save_event(event_dict[event])

        # Get subcommittee dockets
        subcommittee_link_elements = comm_page.xpath('//div[@id="mainC"]/h4[text()="SUB-COMMITTEES:"]/following-sibling::ul[1]//a')

        for subcommittee_link_element in subcommittee_link_elements:
            subcommittee_link = self.base_url + self.convert_path(subcommittee_link_element.get('href'))
            try:
                subcommittee_page = self.get(subcommittee_link)
                subcommittee_page = subcommittee_page.lxml()
                subcommittee_docket_link_elements = subcommittee_page.xpath('//div[@id="mainC"]/h4/b[text()="Sub-Committee dockets:"]/ancestor::h4/following-sibling::ul[1]//a')
                committee_name = subcommittee_page.xpath("//div[@id='mainC']/h3[@class ='xpad']")[0].text_content()
            except:
                self.warning('Couldn\'t get subcommittee dockets for %s' % subcommittee_link)
                continue
            event_dict = {}

            committee_name = re.sub(r'\s+', ' ', committee_name).strip()

            for subcommittee_docket_link_element in subcommittee_docket_link_elements:
                subcommittee_docket_link = self.base_url + self.convert_path(subcommittee_docket_link_element.get('href'))
                self.scrape_committee_event(committee_name, chamber, session, subcommittee_docket_link, event_dict)

            for event in event_dict:
                self.save_event(event_dict[event])

        return True

    def scrape_committee_event(self, name, chamber, session, docket_link, event_dict):
        p_elements = None
        raw_time_location = None
        time_str = None
        relative_time_str = None
        date_str = None
        datetime_str = None
        location = None
        try:
            docket_page = self.get(docket_link)
            docket_page = docket_page.lxml()
        except:
            return False

        # Iterate through p elements to compile raw text which should contain date, location, and time
        p_elements = docket_page.xpath('//div[@id="mainC"]/p')
        raw_time_location = ''
        raw_time_re = re.compile('Time and Place', re.I)
        raw_date_re = re.compile('Date of Meeting', re.I)
        for p_element in p_elements:
            p_element_text = p_element.text_content()
            found_time = raw_time_re.search(p_element_text)
            found_date = raw_date_re.search(p_element_text)
            if found_time or found_date:
                raw_time_location += p_element_text + ';'
            if found_time and found_date:
                break
        # If fail to get date, location, and time
        if raw_time_location == '':
            return False

        # Get date and time
        datetime_str = self.extract_date(raw_time_location)
        if not datetime_str:
            return False

        time_str = self.extract_time(raw_time_location)
        if not time_str:
            # Edge case: no absolute time, attempt to extract relative time such as '30 min. after adjournment'
            relative_time_re = re.compile('Time and place:(.*(after|adjournment|conclusion).*)(,|-|\r|\n)', re.I)
            relative_time_match = relative_time_re.search(raw_time_location)
            time_str = relative_time_match.group(1).strip() if relative_time_match else 'N/A'
            date = datetime.strptime(datetime_str, "%B %d, %Y")
            has_time = False
        else:
            datetime_str += ' ' + time_str
            date = datetime.strptime(datetime_str, "%B %d, %Y %I:%M %p")
            has_time = True

        # Get location
        location = self.extract_location(raw_time_location)
        location = re.sub(r"\d+ minutes after Adjournment", "", location).strip()
        location = re.sub(r"\d+ [Mm]inutes after (Senate)? Adj\.", "", location).strip()

        if date.date() < self.get_today_as_timezone().date():
            return None

        if not location:
            location = 'N/A'

        if (location, date) in event_dict:
            event = event_dict[(location, date)]
        else:
            # Create event object
            descr = "Committee Hearing: %s" % name
            event = Event(date, descr, location, 'committee_markup', chamber=chamber, start_has_time=has_time,
                          session=session)
            event_dict[(location, date)] = event

        # Get/set source
        event.add_source(docket_link)

        # Get/set participants
        event.add_participant('host', name, chamber=chamber)

        # Get related bills
        a_elements = docket_page.xpath('//div[@id="mainC"]//a')
        for a_element in a_elements:
            raw_bill_id = a_element.text_content().strip()
            bill_id_parts = re.search('([HS]\.[J]?\.?[RB]\.) +(\d+)', raw_bill_id)

            # This link doesn't contain text in the format of a bill
            if not bill_id_parts:
                continue

            """
            For some reason, whenever Joint Resolutions are listed on the Senate committee dockets,
            they use bill ids like S.J.R instread of SJ. This makes extracting bill ID from text unreliable
            hence extracting them from the hrefs instead.

            The relative URL in href is '/cgi-bin/legp604.exe?181+sum+SJ19'
            """
            bill_id = a_element.get('href').split('+')[-1]

            event.add_related_bill(bill_id, type='consideration')

    # raw (string): a raw string possibly containing a time string in the format time_re
    # Return a time, if can't find time, return False
    def extract_time(self, raw):
        time = False
        time_match = None
        time_match = self.time_re.search(raw)

        if not time_match:
            return False

        if time_match.group(1):
            time = time_match.group(1)
        else:
            return time

        if time_match.group(2):
            time += time_match.group(2)
        else:
            time += ':00'

        if time_match.group(3):
            time += ' ' + re.sub('\.', '', time_match.group(3))
        else:
            return False

        return time

    def extract_date(self, raw):
        date = False
        date_match = None

        date_match = self.date_re.search(raw)
        if date_match:
            date = date_match.group()

        return date

    def extract_location(self, raw):
        location = 'Unknown location.'
        location_match = None

        location_match = self.location_re.search(raw)
        location_match2 = self.location_re2.search(raw)
        location_match3 = self.location_re3.search(raw)

        if location_match and location_match.group(4):
            location = location_match.group(4).strip()
        elif location_match2 and location_match2.group(2):
            location = location_match2.group(2).strip()
        elif location_match3 and location_match3.group(1):
            location = location_match3.group(1).strip()

        return location

    def convert_path(self, path):
        parts = re.search('\?(.*)', path)
        new_path = None
        if parts:
            new_path = '/' + parts.group(1) + '.HTM'
            new_path = re.sub('\+', '/', new_path)
        return new_path

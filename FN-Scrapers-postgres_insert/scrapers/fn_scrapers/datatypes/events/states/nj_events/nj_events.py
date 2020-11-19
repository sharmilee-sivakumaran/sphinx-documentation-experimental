from __future__ import absolute_import
import re
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags

base_url = "http://www.njleg.state.nj.us/committees/Committees.asp?House=%s"
committee_url = "http://www.njleg.state.nj.us/committees/CommiteeSchedule.asp"
hearing_url = "http://www.njleg.state.nj.us/BillsForAgendaView.asp"

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-NJ")
class NJEventScraper(EventScraper):
    jurisdiction = 'nj'

    def __init__(self, *args, **kwargs):
        super(NJEventScraper, self).__init__('nj', __name__, **kwargs)

    def scrape(self):
        """
        Scrape NJ Scraper

        Scraper workflow:
        - The scraper GETs to URLs for each chamber like below (for lower chamber):
          http://www.njleg.state.nj.us/committees/Committees.asp?House=A

        - On this page it looks for the folllowing XPath
           //a/font[contains(text(), "View Schedule")] <-- View Schedule hyperlink

           The committee ccode is extracted from the 'href' of the 'a' tag.

        - The previous hyperlink is actually a javascript call to get the committee schedule using
          a POST request to the url
          http://www.njleg.state.nj.us/committees/CommiteeSchedule.asp

          with the POST data {
            'CommCode': 'AED' <-- Assembly Education
          }

        - The committee name is extracted using the XPath
           //a[./font[contains(text(), "View Schedule")]]/preceding::*[3] <-- Commitee Name

        - The scraper then looks for the element
           //a[@title="View Committee"] <-- Meeting Date
           //td[.//a[@title="View Committee"]]/following::td[1] <-- Meeting time
           //td[.//a[@title="View Committee"]]/following::td[2] <-- Meeting

        - The Meeting Date a hyperlink actually makes a javascript call to get the Meetign agenda page
          which we need to get the related bills of the of the event, if there are any.

          The scraper makes a POST request to the website using the post data {
            'House': 'AED',
            'ADate': '3/12/2018 10:00:00 AM',
            'AType': 'M',
            'ASequence': 0
          }

          All these are extracted form the href attribute of the meeting a tag

        - Once the scraper gets to this page, the scraper looks for the bill IDs with the XPath
           //a[@title="View Bill"] <-- Bill ID

        - Saves all events
        """
        event_dict = {}
        for chamber in ['A', 'S']:
            if chamber == 'A':
                cham = 'lower'
            else:
                cham = 'upper'
            chamber_url = base_url % chamber
            chamber_page = self._lxmlize(chamber_url)
            committee_link_group = chamber_page.xpath('//a/font[contains(text(), "View Schedule")]')
            for committee_link in committee_link_group:
                com_url = committee_link.getparent().attrib['href']
                com_name = committee_link.getparent().getprevious().getprevious().getprevious().text_content().strip()
                com_name = re.sub(r'\(.*?\)', '', com_name).strip()
                committee_code = re.findall(r'javascript\:ShowCommitteeSchedule\(\'(.*?)\'\)', com_url)[0]
                post_data = {}
                post_data['CommCode'] = committee_code

                committee_page = self.post(committee_url, data=post_data)
                committee_page = committee_page.lxml()
                committee_page.make_links_absolute(committee_url)

                event_link_group = committee_page.xpath('//a[@title="View Committee"]')
                for event_link in event_link_group:
                    font_ele = next(event_link.iterchildren(tag='font'), None)
                    if font_ele is None:
                        self.error("Unable to find font element")
                        continue
                    if font_ele.get('color').lower() == 'darkred':
                        """
                        NJ color codes the events, and we are not interested in past events
                        """
                        continue
                    event_url = event_link.attrib['href']
                    event_date = event_link.text_content().strip()
                    event_td = event_link.getparent().getparent()
                    event_time_td = event_td.getnext()
                    event_time = event_time_td.text_content().strip()

                    event_addr = event_time_td.getnext().text_content().strip()

                    formed_date = parse("%s %s" % (event_date, event_time))

                    event_url = re.sub(r'[\r\n\t]+', r' ', event_url)
                    event_url = re.sub(r'\s+', r' ', event_url)
                    event_key = (formed_date, event_addr, com_name)

                    committee_code, date, atype, asequence = re.findall(r'javascript\:AgendaBills\(\'(.*?)\', \'(.*?)\' , \'(.*?)\',(.*?)\)', event_url)[0]

                    post_data = {}
                    post_data['House'] = committee_code
                    post_data['ADate'] = date
                    post_data['AType'] = atype
                    post_data['ASequence'] = asequence

                    if event_time:
                        start_has_time = True
                        formed_date = self._tz.localize(formed_date)
                    else:
                        start_has_time = False

                    # Merge duplicate events
                    if event_key in event_dict:
                        event = event_dict[event_key]
                    else:
                        event = Event(formed_date, '%s Hearing' % com_name, event_addr, 'committee_markup', start_has_time=start_has_time)
                    event.add_source(hearing_url)
                    event.add_participant('host', com_name, chamber=cham)

                    event_page = self.post(hearing_url, data=post_data)
                    event_page = event_page.lxml()
                    event_page.make_links_absolute(hearing_url)

                    bill_group = event_page.xpath('//a[@title="View Bill"]')
                    for bill in bill_group:
                        bill_id = bill.text_content().strip()
                        event.add_related_bill(bill_id, 'consideration')

                    event_dict[event_key] = event

        for event in event_dict.values():
            self.save_event(event)

        self.save_events_calendar()

    def _lxmlize(self, url):
        """
        lxmlize a web page
        :param url: a web url
        :type url: string
        :returns: page in lxml type
        """
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

from __future__ import absolute_import
import datetime as dt
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import re
import pytz
from requests.exceptions import HTTPError

committee_url = "https://olis.leg.state.or.us/liz/Committees/list"
@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-OR")
class OREventScraper(EventScraper):
    """
    Oregon EventScraper
    Scrape Oregon Events
    """
    jurisdiction = 'or'

    def __init__(self, *args, **kwargs):
        super(OREventScraper, self).__init__('or', __name__, **kwargs)

    def get_page(self, url):
        page = self.get(url)
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def get_today_as_timezone(self):
        today = dt.datetime.utcnow()
        today = today.replace(tzinfo=pytz.UTC)
        return today.astimezone(self._tz)

    def scrape_chamber(self, committe_name, link, chamber):
        
        page = self.get_page(link)
        meeting_tree = page.xpath('//ul[@id="meetingsTree"]/li')

        for meeting in meeting_tree:
            date = meeting.text_content().strip()
            fmt = "%m/%d/%Y %I:%M %p"
            date = dt.datetime.strptime(date, fmt)

            #Only scrape event now and in the future
            if date.date() < self.get_today_as_timezone().date():
                continue
            date = self._tz.localize(date)

            meeting_url = meeting.xpath('./a')[1].attrib['href']
            
            meeting_page = self.get_page(meeting_url)
            place = meeting_page.xpath('//div[contains(text(),"Meeting Details")]')[0].text_content()
            
            if "No Meeting Scheduled" in place:
                continue
            place = place.split(',')[1].strip()


            descr = '%s hearing' %committe_name
            event = Event(date, descr, place, 'committee_markup', chamber=chamber, start_has_time=True)

            event.add_source(meeting_url)
            event.add_participant("host", committe_name, chamber=chamber)

            document_url = meeting_url+'/MeetingMaterials'
            try:
                document_page = self.get_page(document_url)

                doc_list = document_page.xpath('//div[@id="meetingMaterials"]//i[@class="glyphicon glyphicon-file"]')
                for doc_link in doc_list:
                    url = doc_link.xpath('./parent::a')[0].attrib['href']
                    event.add_document("Meeting Material", url)
            except HTTPError:
                self.warning("No Meeting Material are founded")


            bills_url = meeting_url + '/AgendaItems'
            bills_page = self.get_page(bills_url)
            bills_list = bills_page.xpath('//div[@class="row"]')
            for bills_row in bills_list:
                try:
                    bill_id = bills_row.xpath("./span[@class='col-md-2']/a")[0].text_content().strip()
                    desc = bills_row.xpath("./span[@class='col-md-10']")[0].text_content().strip()
                    event.add_related_bill(bill_id, "consideration", description=desc)
                except IndexError:
                    continue
            self.save_event(event)

    def scrape(self):
        """
        scrape's events from OR for given session/chambers
        :param session: session to scrape
        :type session: string
        :param chambers: chambers to process
        :type chambers: list
        """
        page = self.get_page(committee_url)
        for chamber in ["upper", "lower", "joint"]:
            if chamber == 'upper':
                committee_list = page.xpath('//ul[@id="SenateCommittees_search"]/li')
            elif chamber == 'lower':
                committee_list = page.xpath('//ul[@id="HouseCommittees_search"]/li')
            elif chamber == 'joint':
                committee_list = page.xpath('//ul[@id="JointCommittees_search"]/li')

            for committee_row in committee_list:
                committe_list = committee_row.xpath('.//a')
                for committee in committe_list:
                    committe_name = committee.text_content()
                    link = committee.attrib['href']
                    link = re.sub(r'Overview', 'CommitteeTree', link)
                    self.scrape_chamber(committe_name, link, chamber)

        self.save_events_calendar()





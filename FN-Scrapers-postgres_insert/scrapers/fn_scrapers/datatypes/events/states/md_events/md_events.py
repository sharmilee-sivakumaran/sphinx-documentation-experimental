from __future__ import absolute_import
import datetime as dt
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
import re

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-MD")
class MDEventScraper(EventScraper):
    jurisdiction = 'md'

    def __init__(self, *args, **kwargs):
        super(MDEventScraper, self).__init__('md', __name__, **kwargs)

    def lxmlize(self, url):
        page = 0
        try:
            page = self.get(url)
        except ValueError:
            self.error("Error loading %s" % (url))
            return
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def scrape(self):
        url = "http://mgaleg.maryland.gov/webmga/frmcommittees.aspx?pid=commpage&tab=subject7"
        page = self.lxmlize(url)
        if page is None:
            self.warning("Couldn't get events page for MD")
            return
        try:
            senate_table = page.xpath("//table[@class='grid']")[0]
            senate_links = senate_table.xpath(".//a[contains(@href, 'frmMain.aspx?pid=cmtepage')]")
            for link in senate_links:
                committee_meeting_url = re.sub('stab=01', 'stab=03', link.attrib['href'])
                self.scrape_event('upper', committee_meeting_url)
        except IndexError:
            self.error("Failed to scrape event in upper chamber")
        try:
            house_table = page.xpath("//table[@class='grid']")[1]
            house_links = house_table.xpath(".//a[contains(@href, 'frmMain.aspx?pid=cmtepage')]")
            for link in house_links:
                committee_meeting_url = re.sub('stab=01', 'stab=03', link.attrib['href'])
                self.scrape_event('lower', committee_meeting_url)
        except IndexError:
            self.error("Failed to scrape event in lower chamber")
        self.save_events_calendar()

    def scrape_event(self, chamber, committee_meeting_url):
        page = self.lxmlize(committee_meeting_url)
        if page is None or "The committee has no hearings or meetings currently scheduled." in page.text_content():
            self.warning("Couldn't get events page for MD")
            return
        try:
            committee_name = page.xpath("//h2")[0].text_content()
            days = page.xpath("//h4")[1:]
            headers = page.xpath("//h3/text()")
            headers = [x for x in headers if x.strip() != ""]
            place = headers[3].strip()
        except IndexError:
            self.error("Failed to scrape events page")
            return

        for day in days:
            scraped_events = []
            date = day.text_content().strip()
            try:
                info = day.xpath('./following-sibling::pre')[0]
                info_text = info.text_content()

                # Each day on the calendar contains timeslots which contain an event, and also a list of schedule
                # changes for that day. The format of the schedule changes is different, and thus must be handled by a
                # separate method.
                schedule_changes = re.search(r'Changes made to schedule through addenda(.*)End of schedule changes',
                                             info_text, re.DOTALL)
                if schedule_changes:
                    schedule_changes = schedule_changes.group(1)
                    info_text = re.sub(r'Changes made to schedule through addenda.*End of schedule changes', '',
                                       info_text, re.DOTALL)
                    event = self.scrape_schedule_changes(schedule_changes, date, committee_name, place)
                    if event:
                        event.add_source(committee_meeting_url)
                        event.add_participant("host", committee_name, chamber=chamber)
                        self.merge_event(event, scraped_events)

                time_slots = re.split(r'(1?[0-9]:[0-5][0-9]\s[AP]\.M\.)', info_text)
                if len(time_slots) > 1:
                    for i in xrange(1, len(time_slots), 2):
                        time = time_slots[i]
                        event_info = time_slots[i+1]
                        event = self.scrape_timeslot(date, time, event_info, committee_name, place)
                        if event:
                            event.add_source(committee_meeting_url)
                            event.add_participant("host", committee_name, chamber=chamber)
                            self.merge_event(event, scraped_events)

            except IndexError as err:
                self.warning("Failed to scrape events information: {}".format(err))
                continue

            for event in scraped_events:
                self.save_event(event)


    def scrape_schedule_changes(self, schedule_changes, date, commmittee_name, place):
        bills_info = re.split(r'[\n\s\t]+([HS][JB]\s[0-9]+)', schedule_changes)
        date = re.sub(r"\s+", ' ', date)
        try:
            date = dt.datetime.strptime(date, "%A %B %d, %Y")
            date = self._tz.localize(date)
        except ValueError:
            self.warning("Failed to scrape date")
            return None

        event = Event(date, "Hearing: {}".format(commmittee_name),
                      place, 'committee_markup', start_has_time=True)


        if len(bills_info) > 1:
            # If every bill listed says cancelled or rescheduled, then the schedule change is an event being removed, not added.
            event_added = False

            for i in xrange(1, len(bills_info), 2):
                bill_id = bills_info[i]
                bill_desc = " ".join(re.split(r'\s\s+', bills_info[i+1])[2:])
                if "CANCELLED" not in bill_desc and 'RESCHEDULED' not in bill_desc:
                    event_added = True
                    time = re.findall(r'\d{1,2}:\d{1,2} [AP]M', bill_desc)
                    if len(time) > 0:
                        time = time[-1]
                        formed_time = dt.datetime.strptime(time.strip(), "%I:%M %p")
                        date = date.replace(hour=formed_time.hour, minute=formed_time.minute)
                        event['start'] = date
                    event.add_related_bill(bill_id, "consideration")
            if event_added:
                return event

    def scrape_timeslot(self, date, time, event_info, committee_name, place):
        time = re.sub(r'\.', '', time)
        datetime_string = "%s %s" % (date, time)
        try:
            when = dt.datetime.strptime(datetime_string.strip(), "%A %B %d, %Y %I:%M %p")
            when = self._tz.localize(when)
        except ValueError:
            self.warning("Failed to scrape date")
            return None

        if "To Be Announced" not in event_info:
            # If a timeslot contains any bill ids, the event is a hearing, and the format of the event is simply a list
            # of bills. Each bill is represented as the id, the name of the sponsor, and a description of the bill,
            # separated by multiple spaces
            bills_info = re.split(r'[\n\s\t]+([HS][JB]\s[0-9]+)', event_info)

            if len(bills_info) > 1:

                event = Event(when, "Hearing: {}".format(committee_name),
                              place, 'committee_markup', start_has_time=True)
                # If every bill listed says cancelled, then the schedule change is an event being removed, not added.
                event_added = False

                for i in xrange(1, len(bills_info), 2):
                    bill_id = bills_info[i]
                    bill_desc = " ".join(re.split(r'\s\s+', bills_info[i+1])[2:])
                    if "CANCELLED" not in bill_desc:
                        event_added = True
                        event.add_related_bill(bill_id, "consideration")
                if not event_added:
                    return

            else:
                # Extract the event type from the description
                if 'Briefing' in event_info:
                    event_type = "committee:briefing"
                else:
                    event_type = "committe:meeting"

                desc = ' '.join(event_info.split()).strip()
                if not desc:
                    desc = "Hearing: {}".format(committee_name)
                event = Event(when, desc, place, 'committee_markup', start_has_time=True)

                # Check if this non-hearing event is still associated with any bills
                bills = re.findall(r'[HS]B\s[0-9]+', event_info)
                for bill in bills:
                    event.add_related_bill(bill, "consideration")

            return event

    def merge_event(self, new_event, event_list):
        """
        Check if a list of events already contains a particular event, and if it does, merge them
        by adding the related bills to the existing event. Otherwise, simply add the event to the list

        :param new_event: A newly scraped event to be added
        :param event_list: The list of already scraped events
        """
        description = new_event["description"]
        when = new_event["start"]
        for event in event_list:
            if event["description"] == description and event["start"] == when:
                if "related_bills" in new_event and "related_bills" in event:
                    event["related_bills"] += new_event["related_bills"]
                return

        event_list.append(new_event)



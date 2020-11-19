from __future__ import absolute_import
import re
import datetime
from dateutil.parser import parse
from fn_scrapers.datatypes.events.common.event_scraper import EventScraper, Event
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.extraction.textpdf import get_pdf_text
from requests import HTTPError
from fn_scrapers.datatypes.events.common.utils import get_today_as_timezone

chamber_map = {"lower": "House", "upper": "Senate"}

@scraper()
@tags(type="events", group="fnleg", country_code="US", subdivision_code="US-WY")
class WYEventScraper(EventScraper):
    jurisdiction = 'wy'

    def __init__(self, *args, **kwargs):
        super(WYEventScraper, self).__init__('wy', __name__, **kwargs)

    def get_page_from_url(self, url):
        try:
            page = self.get(url)
        except HTTPError:
            return None
        page = page.lxml()
        page.make_links_absolute(url)
        return page

    def normalize_time(self, time_string):
        time_string = time_string.lower()

        if re.search(r'(upon|after)(\?)? adjournment', time_string):
            # We don't really know when adjournment might be.
            return None
        elif re.search(r'(noon (adjournment|recess)|afternoon)', time_string):
            # May say something like "15 minutes after noon recess"
            # Take this offset into account.
            timing = re.search(r'(\d+) minutes (after|before)', time_string)
            if timing:
                timedelta = datetime.timedelta(minutes=int(timing.group(1)))
                if timing.group(2) == 'before':
                    timedelta * -1
                # Calculate the time as an offset from 12:00 noon.
                # (1/1/2000 is a placeholder date and has no effect)
                time_obj = (datetime.datetime(2000, 1, 1, 12, 0, 0) + timedelta).time()
                # Format the time in the way we want (e.g. "2:15 pm", no leading '0')
                time_string = re.sub('^0','', time_obj.strftime('%I:%M %p').lower())
            else:
                time_string = '12:00 pm'

        # Change "a.m." and "p.m.", if they exist, to "am" and "pm"
        ap = re.search(r'([ap])\.m\.', time_string)
        if ap:
            ap = ap.group(1)
            time_string = time_string.replace(ap + '.m.', ap + 'm')

        # Put a space between hour:minutes and am/pm if there is none yet.
        # This condition is broader than valid time strings, but we're trying
        # to get data with minimal errors, not verify the site's data.
        timing = re.search(r'([0-9]{1,2}:[0-9]{1,2})([ap]m)', time_string)
        if timing:
            hour_minutes, meridiem = timing.groups()
            time_string = hour_minutes + ' ' + meridiem

        # If the string starts with a time, use it and discard the rest.
        time_format_str = '[0-9]{1,2}:[0-9]{1,2} [ap]m'
        timing = re.search(r'^(%s)' % time_format_str, time_string)
        if timing:
            time_string = timing.group(1)

        if not re.search(r'^%s$' % time_format_str, time_string):
            # If at this point it doesn't match expected format return 12:00 am
            return None
        return time_string

    def get_committee(self, meeting_data):
        committee = meeting_data[0].xpath(
            './/p[@class="MsoNormal"]')[1].text_content().strip()
        if committee == '':
            committee = None
        else:
            committee = re.sub(r'^[0-9]+-','',committee)
            committee = self.clean_string(committee)

        return committee

    def get_location(self, meeting_data):
        tr = meeting_data[0].xpath('.//p[@class="MsoNormal"]')
        room = tr[len(tr)-1].text_content().strip()

        room = self.clean_string(room)
        if room == '':
            room = None

        return room

    # The meeting description is a concatenation of all its bill titles, or
    # just the committee's name if there is no separate bill information.
    def get_meeting_description(self, meeting_data):
        descriptions = ''
        if len(meeting_data) > 1:
            # Start at the 2nd row where the bill rows start.
            start_at = 1
        else:
            # There are no bills, so just use the committee's name.
            start_at = 0

        for tr in meeting_data[start_at:]:
            # Each bill's title/description is in the 3rd of 4 columns.
            # But the committee's name is in the 2nd of 3 columns.
            description = tr[len(tr)-2].text_content().strip()
            descriptions += ' ' + description

        descriptions = self.clean_string(descriptions).strip()

        return descriptions

    # Scrape all bill data that falls under a particular committee meeting
    def get_bills(self, meeting_data):
        bill_data = []

        for tr in meeting_data:
            # Identify the link to the bill by looking for "/bills/", ignoring case.
            # Note that this may either be in the 2nd column (url usually contains "bills")
            # or linked from part of the description in the 3rd column (url usually contains "Bills").
            bills = tr.xpath('.//a[@href[contains( translate(., "BILS", "bils"), "/bills/")]]')
            if bills:
                for bill in bills:
                    # The bill link's text is the bill_id - sometimes it ends with "ENG"
                    # - not sure why, but it won't match the bill_ids we scrape.
                    bill_id = re.sub("ENG$", "", bill.text_content().strip())
                    # The description in this case is sometimes the title,
                    # assumed to be in the 3rd table column.
                    bill_description = self.clean_string(
                        tr.xpath('.//td[3]/p')[0].text_content().strip())
                    bill_url = bill.attrib['href'].strip()  #pdf file

                    # dont include bad HTML links for bills. thankfully
                    # they're duplicates and already listed properly
                    if 'href' not in bill_url and '</a>' not in bill_url:
                        bill_data.append({
                            'bill_id': bill_id,
                            'bill_description' : bill_description,
                            'bill_url' : bill_url
                        })
        return bill_data

    def clean_string(self, my_string):
        my_string = my_string.encode('ascii','ignore')
        my_string = re.sub(r'(\n|\r\n)',' ', my_string)
        my_string = re.sub(r'\s{2,}',' ', my_string)
        my_string = my_string.strip()

        return my_string

    # Determines if this row of the table is a header for a committee meeting.
    # Individual bill info would appear in the directly following rows.
    def is_row_a_new_meeting(self, row):
        if len(row) == 3:
            td1 = row.xpath('.//td[1]/p[@class="MsoNormal"]')
            td2 = row.xpath('.//td[2]/p[@class="MsoNormal"]')
            td3 = row.xpath('.//td[3]/p[@class="MsoNormal"]')

            if len(td2) == 0:
                td2 = row.xpath('.//td[2]/h1')

            if len(td1) == 0 or len(td2) == 0:
                return False

            if (self.clean_string(td1[0].text_content()) == ''
                    or self.clean_string(td2[0].text_content()) == ''
                    or self.clean_string(td3[0].text_content()) == ''):
                return False
        else:
            return False
        return True

    def scrape(self):
        for chamber in ["upper", "lower"]:
            self.scrape_committee_hearings(chamber)
            
        self.save_events_calendar()

    def scrape_committee_hearings(self, chamber):
        """
        WY has one year sessions, so replacing the session object usage with today's date's year
        """
        session_year = get_today_as_timezone(self._tz).year
        calendar_url = ("http://legisweb.state.wy.us/%s/Calendar/"
            "CalendarMenu/CommitteeMenu.aspx" % str(session_year))

        page = self.get_page_from_url(calendar_url)
        if page is None:
            self.warning("Failed to find events page for year {}".format(datetime.datetime.today().year))
            return

        # The calendar of dates with links to the House and Senate committee
        # schedule pages are in this table.
        rows = page.xpath('//table[@id="ctl00_cphContent_gvCalendars"]/tr')

        for i,row in enumerate(rows):

            # This is the numbering convention used by the site to
            # differentiate ids of spans in each row.  "+ 2" seems arbitrary.
            row_ident = '%02d' % (i + 2)

            # The only information to save from this table is the date.
            date_xpath = ('.//span[@id="ctl00_cphContent_gv'
                'Calendars_ctl%s_lblDate"]' % str(row_ident))
            date_string = row.xpath(date_xpath)[0].text_content()

            # Determine whether to follow the House or Senate link.
            chamber_char = "H" if chamber == "lower" else "S"
            meeting_xpath = ('.//a[@id="ctl00_cphContent_gv'
                'Calendars_ctl%s_hl%scallink"]' % (
                    str(row_ident), chamber_char
                ))
            meeting_url = row.xpath(meeting_xpath)

            if (len(meeting_url) == 1 and
                    meeting_url[0].text_content().strip() != '' and
                    meeting_url[0].text_content().strip() != 'Holiday Break'):

                # Follow the appropriate link to the full chamber schedule for that day.
                meeting_url = meeting_url[0].attrib['href']
                if ".pdf" in meeting_url:
                    self.scrape_pdf_calendar(chamber, meeting_url, date_string)
                else:
                    self.scrape_html_calendar(chamber, meeting_url, date_string)

    def scrape_pdf_calendar(self, chamber, meeting_url, date_string):
        when = datetime.datetime.strptime(date_string, '%m/%d/%Y')
        meeting_text = get_pdf_text(meeting_url)
        event_dict = {}
        # We only want the main body of each page of the document,
        # which is between the column names and the page numbers
        meeting_lists = re.findall(r'Time\s+Committee\s+Room\s+\n(.+?)Page \d of \d', meeting_text, re.DOTALL)
        for meeting_list in meeting_lists:
            meetings = meeting_list.split("\n\n")
            for meeting in [x.strip() for x in meetings if x.strip()]:
                meeting_data = meeting.split("\n")
                fields = re.split(r'\s{2,}', meeting_data[0])
                if len(fields) != 3:
                    continue
                if len(meeting_data) > 1:
                    # If the next line contains one column all the way to the left,
                    # then it is spillover text about the time of the meeting.
                    next_line = re.split(r'\s{2,}', meeting_data[1])
                    spill_over = " " + next_line[0] if len(next_line) == 1 else ""
                    time_string = fields[0] + spill_over
                else:
                    time_string = fields[0]
                    spill_over = ""
                time_string = self.normalize_time(time_string)
                if time_string:
                    try:
                        time = parse(time_string)
                        when = when.replace(hour=time.hour, minute=time.minute)
                        when = self._tz.localize(when)
                        has_time = True
                    except ValueError:
                        has_time = False
                else:
                    has_time = False
                committee_name = re.sub(r'\d{1,2}-', "", fields[1])
                description = "Hearing: " + chamber_map[chamber] + " Committee on " + committee_name
                location = fields[2]
                event = Event(when, description, location, 'committee_markup',
                              start_has_time=has_time, start_time_description=time_string, chamber=chamber)
                event.add_source(meeting_url)
                event.add_participant("host", committee_name, chamber=chamber)

                # If there was a spillover line, we need to remember to skip it
                if spill_over:
                    start = 2
                else:
                    start = 1

                agenda = " ".join(meeting_data[start:])
                bill_ids = re.findall(r'(?:HB|HJ|SF|SJ)\d{4}', agenda)
                for bill_id in bill_ids:
                    event.add_related_bill(bill_id, "consideration")
                if (when.date(), description, location) not in event_dict:
                    event_dict[(when.date(), description, location)] = True
                    self.save_event(event)


    def scrape_html_calendar(self, chamber, meeting_url, date_string):
        meeting_page = self.get_page_from_url(meeting_url)
        # meetings is a table with rows that comprise of committee meeting
        # headers, directly followed by rows with related bills info.
        meetings = meeting_page.xpath(
            './/table[@class="MsoNormalTable"]/tr')
        meeting_idents = []
        meeting_ident = 0

        # Breaking the meetings into arrays (meeting_data) for
        # processing. meeting_idents indicates which rows in the table are
        # committee meeting headings. They have the columns
        # (time, committee, room)
        for meeting in meetings:
            if self.is_row_a_new_meeting(meeting):
                meeting_idents.append(meeting_ident)
            meeting_ident += 1

        # meeting_idents indicates which rows in the table are
        # committee meeting headings.
        for i,meeting_ident in enumerate(meeting_idents):

            if len(meeting_idents) == 1 or i + 1 == len(meeting_idents):
                ident_start, ident_end = [meeting_ident, 0]
                meeting_data = meetings[ident_start:]
            else:
                # Get this meeting's range of row numbers for related bills
                ident_start, ident_end = [
                    meeting_ident, meeting_idents[i+1] - 1
                ]

                if ident_end - ident_start == 1:
                    # Not sure of the special case for which this would be needed.
                    ident_end = ident_start + 2
                # meeting_data is this meeting's header followed by
                # the rows of related bills.
                meeting_data = meetings[ident_start:ident_end]

            # Scrape meeting details from the committee meeting heading
            committee = self.get_committee(meeting_data)
            raw_meeting_time = meeting_data[0].xpath('.//p[@class="MsoNormal"]')[0].text_content().strip()
            meeting_time = self.normalize_time(raw_meeting_time)

            if meeting_time is None:
                meeting_date_time = datetime.datetime.strptime(date_string, '%m/%d/%Y')
                has_time = False
            else:
                meeting_date_time = datetime.datetime.strptime(date_string + ' ' + meeting_time, '%m/%d/%Y %I:%M %p')
                meeting_date_time = self._tz.localize(meeting_date_time)
                has_time = True

            location = self.get_location(meeting_data)
            description = self.get_meeting_description(meeting_data)
            # Scrape the rest of the rows for the bill data.
            bills = self.get_bills(meeting_data)

            if description == '':
                description = committee

            event = Event(
                meeting_date_time,
                description,
                location,
                'committee_markup',
                start_has_time=has_time,
                chamber=chamber)

            event.add_source(meeting_url)

            for bill in bills:
                event.add_related_bill(
                    bill['bill_id'],
                    type='consideration'
                )
                event.add_document(
                    name=bill['bill_id'],
                    url=bill['bill_url']
                )

            event.add_participant('host', committee, chamber=chamber)

            self.save_event(event)

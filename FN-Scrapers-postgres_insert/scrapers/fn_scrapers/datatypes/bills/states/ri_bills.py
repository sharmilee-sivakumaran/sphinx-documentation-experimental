"""
:class RIBillScraper: scrapes Rhode Island Bills
"""
from __future__ import absolute_import

import re

import logging
from dateutil.parser import parse
import datetime

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('RIBillScraper')
    

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-RI", group="fnleg")
class RIBillScraper(BillScraper):
    """
    RIBillScraper

    Scrape Rhode Island Bills
    """
    def __init__(self):
        super(RIBillScraper, self).__init__("ri")

    def scrape_bill_ids(self, session):
        bill_ids = {}
        bill_base_url = "http://webserver.rilin.state.ri.us/BillText%s/%sText%s/%sText%s.html"
        for session_id in [session[2:4], session[6:8]]:
            for chamber in ['House', 'Senate']:
                bill_list_url = bill_base_url % (session_id, chamber, session_id, chamber, session_id)

                bill_doc = self.scraper.url_to_lxml(bill_list_url, BRP.test)
                if bill_doc is None:
                    continue
                bill_table = bill_doc.xpath("//td[@class='bill_col1']")
                year = '20' + session_id
                for bill in bill_table:
                    bill_id = bill.text_content()
                    bill_type, bill_num = re.findall(r'([HS])(\d+)', bill_id)[0]
                    norm_bill_id = "%s %d" % (bill_type, int(bill_num)) 
                    bill_ids[norm_bill_id] = {'year': year}
        for vote_chamber in ['lower', 'upper']:
            self.scrape_votes(vote_chamber, session, bill_ids)
        
        self.scrape_committee_votes(session, bill_ids)
        return bill_ids
    


    def scrape_bill(self, session, bill_id, **kwargs):
        search_url = "http://status.rilin.state.ri.us/"

        from_bill = "ctl00$rilinContent$txtBillFrom"
        to_bill = "ctl00$rilinContent$txtBillTo"
        year = "ctl00$rilinContent$cbYear"
        session_year = kwargs['bill_info']['year']
        vote_list = []
        if 'vote_url' in kwargs['bill_info']:
            vote_list = kwargs['bill_info']['vote_url']

        committee_vote_list = []
        if 'committee_vote_url' in kwargs['bill_info']:
            committee_vote_list = kwargs['bill_info']['committee_vote_url']

        bill_type, bill_num = re.findall(r'([HS]) (\d+)', bill_id)[0]

        post_data = self.get_default_postbody(search_url)
        post_data[from_bill] = bill_num
        post_data[to_bill] = bill_num
        post_data[year] = session_year

        bill_page = self.scraper.url_to_lxml(search_url, BRP.bill, 'POST',
                                             request_args={"data": post_data})

        title = bill_page.xpath_single(".//b[contains(text(), 'ENTITLED,')]").element.tail.strip().title()

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        version_ele = bill_page.xpath_single(".//a[contains(text(), %s)]" % bill_num, BRP.bill)
        type_str = version_ele.xpath_single("./ancestor::div[1]").text_content()
        bill_char, bill_num = re.findall(r'([HS]) (\d+)', bill_id)[0]
        if 'Bill' in type_str:
            bill_type = 'bill'
            bill_id = "%sB %s" % (bill_char, bill_num)
        elif 'Resolution' in type_str:
            bill_type = 'resolution'
            bill_id = "%sR %s" % (bill_char, bill_num)
        bill = Bill(session, chamber, bill_id, title, bill_type)

        
        version_link = version_ele.get_attrib('href')
        
        download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(version_link,
                                                                     BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True,
                                                                     content_type='application/pdf')

        doc_service_document = Doc_service_document('Current Version', 'version',
                                                    "complete",
                                                    download_id=download_id,
                                                    doc_id=doc_ids[0])
        bill.add_doc_service_document(doc_service_document)
        

        sponsors = bill_page.xpath_single(".//b[contains(text(), 'BY')]").element.tail.strip()
        sponsors = sponsors.split(',')
        for sponsor in sponsors:
            sponsor = sponsor.strip()
            bill.add_sponsor('primary', sponsor)

        action_table = bill_page.xpath(".//div[@style='margin-left: 5%']")
        for action_ele in action_table:
            action_text = action_ele.text_content().strip()

            action_date, action = re.findall(r'(\d+/\d+/\d+)(.*)', action_text)[0]
            action = action.strip()
            action_date = parse(action_date)

            if 'Senate' in action:
                action_chamber = 'upper'
            elif 'House' in action:
                action_chamber = 'lower'
            else:
                action_chamber = 'other'

            action_dict=dict(actor=action_chamber, action=action, date=action_date)
            bill.add_action(**action_dict)

        for vote_url in vote_list:
            self.scrape_vote_page(bill, vote_url)

        for vote_url in committee_vote_list:
            self.scrape_committee_vote_page(bill, vote_url)
        self.save_bill(bill)



    def get_default_postbody(self, url):
        post_body = {}
        search_doc = self.scraper.url_to_lxml(url, BRP.test)
        for element in search_doc.xpath("//*[@name]", BRP.test):
            name = element.get_attrib('name')
            value = element.get_attrib('value')
            if not value:
                value = element.text

            if value:
                value = value.strip()

            post_body[name] = value or ""
        post_body['__EVENTTARGET'] = ""
        post_body['__EVENTARGUMENT'] = ""
        post_body['__LASTFOCUS'] = ""

        return post_body


    def scrape_votes(self, chamber, session, bill_ids):
        vote_base_url = "http://webserver.rilin.state.ri.us"
        url = {
            "upper" : "%s/%s" % (vote_base_url, "SVotes"),
            "lower" : "%s/%s" % (vote_base_url, "HVotes")
        }
        url = url[chamber]
        action_url = "%s/%s" % (url, "votes.asp")
        dates = self.get_dates(url)
        for date in dates:
            year = date[-4:]
            if year not in [session[:4], session[4:8]]:
                continue
            self.parse_vote_page(action_url, date, url, session, bill_ids)


    def get_dates(self, url):
        vote_doc = self.scraper.url_to_lxml(url, BRP.bill_votes)
        dates = vote_doc.xpath("//select[@name='votedate']/option")
        ret = [date.text_content() for date in dates]
        return ret


    def parse_vote_page(self, url, date, context_url, session, bill_ids):
        vote_page = self.scraper.url_to_lxml(url, BRP.test, 'POST',
                                             request_args={"data": {'votedate':date}})

        votes = vote_page.xpath("//center/div[@class='vote']")
        for vote in votes:
            vote_text = vote.text_content()
            bill_infor = re.findall(r'-([HS] \d+)', vote_text)
            if not bill_infor:
                continue
            bill_id = bill_infor[0]
            if bill_id not in bill_ids:
                continue
            vote_url = vote.xpath_single('.//a[contains(text(), "Details")]').get_attrib('href')
            if 'vote_url' not in bill_ids[bill_id]:
                bill_ids[bill_id]['vote_url'] = []
            bill_ids[bill_id]['vote_url'].append(vote_url)


    def scrape_vote_page(self, bill, vote_url):
        # Sample vote page: http://webserver.rilin.state.ri.us/HVotes/votereport.asp?id=15146

        if 'SVotes' in vote_url:
            chamber = 'upper'
        elif 'HVotes' in vote_url:
            chamber = 'lower'
        else:
            chamber = 'other'

        # The vote page has a _bunch_ of nested tables - with the main content of the
        # vote page divided across two different tables.
        vote_page = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)

        # There is an outer table and then, inside of that table, inside of a "td" element,
        # lives the other two tables that we care about. It turns out that we can find
        # that element since its the only one that has a height attribute set to 400.
        content = vote_page.xpath_single("//td[@height=400]")

        # The first table inside of that "td" contains the name of the motion,
        # its date, and the overall vote counts. We extract those here:
        motion = content.xpath_single("div/table[1]//tr[2]/td[1]").text_content()
        motion = re.sub(r"\s+", " ", motion.strip().title())

        vote_date_str = content.xpath_single("div/table[1]//tr[2]/td[3]").text_content()
        vote_date_str = re.sub(r"\s+", " ", vote_date_str.strip())
        vote_date = datetime.datetime.strptime(vote_date_str, "%A, %B %d, %Y%I:%M:%S %p")

        vote_row_text = content.xpath_single("div/table[1]//tr[4]/td[1]").text_content()
        yes_count, no_count, notvoting_count, recused_count = re.search(
            r"YEAS - (\d+)\s+NAYS - (\d+)\s+NOT VOTING - (\d+)\s+RECUSED - (\d+)",
            vote_row_text).groups()
        yes_count = int(yes_count)
        no_count = int(no_count)
        notvoting_count = int(notvoting_count)
        recused_count = int(recused_count)
        other_count = notvoting_count + recused_count

        vote = Vote(
            chamber,
            vote_date,
            motion,
            yes_count > (no_count + other_count),
            yes_count,
            no_count,
            other_count)
        vote.add_source(vote_url)

        # The legislator-by-legislator votes are in a 2nd table that follows the
        # first one we used above. Inside of that table are multiple columns - a fact
        # we can mostly just ignore. What is important is that each vote is recorded
        # as either "Y", "N", or "NV" and that those strings are always inside of a
        # "span" element inside some column. After the span element, is a single block
        # of text that contains the legislator name. Next follows another span element
        # and another legislator name. So, all we need to do is to find all of the
        # span elements. From those, we can extract the vote - yes, no, or other -
        # and then look at the following text to extract the legislator name.
        vote_indicators = content.xpath("div/table[2]//tr[1]//span[@style='width: 40px; display: inline-block;']")

        for vi in vote_indicators:
            result = vi.text_content()
            voter = vi.element.tail.strip()
            if result == 'Y':
                vote.yes(voter)
            elif result == 'N':
                vote.no(voter)
            else:
                vote.other(voter)

        bill.add_vote(vote)


    def scrape_committee_votes(self, session, bill_ids):
        committee_serch_url = "http://status.rilin.state.ri.us/Committees.aspx"
        post_data = self.get_default_postbody(committee_serch_url)
        post_data['__EVENTTARGET'] = 'ctl00$rilinContent$cbYear'
        for year in [session[:4], session[4:8]]:
            post_data['ctl00$rilinContent$cbYear'] = year
            vote_page = self.scraper.url_to_lxml(committee_serch_url, BRP.bill_votes, 'POST',
                                                 request_args={"data": post_data})
            committee_list = vote_page.xpath("//table[@id='rilinContent_tblCommittees']//a")
            for committee_row in committee_list:
                committee_url = committee_row.get_attrib('href')
                date_doc = self.scraper.url_to_lxml(committee_url, BRP.bill_votes)
                date_table = date_doc.xpath("//table[@id='rilinContent_tblAgenda']//a")
                for date_row in date_table:
                    date_url = date_row.get_attrib('href')
                    vote_doc = self.scraper.url_to_lxml(date_url, BRP.bill_votes)
                    vote_table = vote_doc.xpath("//table[@id='rilinContent_tblVotes']/tr")
                    for vote_row in vote_table:
                        vote_text = vote_row.text_content()
                        bill_id = re.findall(r'^\d{4} ([HS] \d+)(?:\s+)?By:', vote_text)
                        if bill_id:
                            bill_id = bill_id[0]
                            committee_vote_url = vote_row.xpath_single(".//a[contains(text(), 'Details')]").get_attrib('href')
                            if 'committee_vote_url' not in bill_ids[bill_id]:
                                bill_ids[bill_id]['committee_vote_url'] = []
                            bill_ids[bill_id]['committee_vote_url'].append(committee_vote_url)

    def scrape_committee_vote_page(self, bill, vote_url):
        vote_page = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
        vote_text = vote_page.text_content()
        if 'Rhode Island House Committee' in vote_text:
            chamber = 'lower'
        elif 'Rhode Island Senate Committee' in vote_text:
            chamber = 'upper'
        else:
            chamber = 'joint'
        count_text = vote_page.xpath_single("//span[@id='rilinContent_lblVotes']").text_content()
        yes_count = re.findall(r'Yeas - (\d+)', count_text)
        yes_count = int(yes_count[0])

        no_count = re.findall(r'Nays - (\d+)', count_text)
        no_count = int(no_count[0])

        other_count = 0
        abs_count = re.findall(r'Absent - (\d+)', count_text)
        if abs_count:
            other_count += int(abs_count[0])

        nv_count = re.findall(r'Not Voting - (\d+)', count_text)
        if nv_count:
            other_count += int(nv_count[0])

        vote_date = vote_page.xpath_single("//span[@id='rilinContent_lblDate']").text_content()
        vote_date = parse(vote_date)

        motion = re.findall(r'By:.*?[a-z]([A-Z\s-]+)', vote_text)[0].title()

        vote = Vote(chamber, vote_date, motion, yes_count > (no_count + other_count),
                    yes_count, no_count, other_count)
        vote.add_source(vote_url)
        vote_result = vote_page.xpath("//table[@id='rilinContent_tblVotes']/tr")
        for vote_row in vote_result:
            vote_ele = vote_row.xpath('./td')
            voter = vote_ele[0].text_content()
            result = vote_ele[1].text_content()
            if result == 'Y':
                vote.yes(voter)
            elif result == 'N':
                vote.no(voter)
            else:
                vote.other(voter)
        bill.add_vote(vote)
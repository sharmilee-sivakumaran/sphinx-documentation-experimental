from __future__ import absolute_import

import datetime as dt
import re
from dateutil.parser import parse
import logging

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_bill_type_from_normal_bill_id, get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags


base_url = "http://leg.colorado.gov/bills/"
logger = logging.getLogger('COBillScraper')

bill_type_dict = {"SB": "bill",
                  "HB": "bill",
                  "HR": "resolution",
                  "SR": "resolution",
                  "SCR": "concurrent_resolution",
                  "HCR": "concurrent_resolution",
                  "SJR": "joint_resolution",
                  "HJR": "joint_resolution",
                  "SM": "memorial",
                  "HM": "memorial",
                  "SJM": "joint_memorial",
                  "HJM": "joint_memorial"
                }


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-CO", group="fnleg")
class COBillScraper(BillScraper):
    """
   CO Bill Scraper
    """
    def __init__(self):
        super(COBillScraper, self).__init__("co")

    @staticmethod
    def create_session_name(session_id):
        year = session_id[:4]
        if 'ss' in session_id:
            sess_type = 'Extraordinary'
        else:
            sess_type = 'Regular'

        return u"{} {} Session".format(year, sess_type)

    def scrape_bill_ids(self, session):
        """
        Scrape the bill list
        """
        bill_ids = set()

        session_name = self.create_session_name(session)
        main_url = "http://leg.colorado.gov/bill-search"
        bill_list_page_doc = self.scraper.url_to_lxml(main_url, BRP.bill_list)

        # If there is no field with the session name,
        # this xpath will throw an exception. This is the
        # desired behavior.
        field_session_id = bill_list_page_doc.xpath_single("//select[@id='edit-field-sessions']"\
                                                           "/option[contains(text(), '%s')]" % session_name,
                                                            BRP.bill_list).get_attrib('value')
        bill_list_url  = "http://leg.colorado.gov/bill-search?field_chamber=All&field_bill_type=All"\
                         "&field_sessions=%s&sort_bef_combine=search_api_relevance DESC" % field_session_id
        bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.warning)

        bill_list_table = bill_list_doc.xpath("//div[@class='view-content']//article/@about")
        for bill_list_row in bill_list_table:
            bill_id = re.findall(r'/bills/(.*)', bill_list_row.lower())[0]
            bill_ids.add(bill_id.upper())
        next_button = bill_list_doc.xpath_single("//a[text()='Next']")

        while next_button:
            bill_list_url = next_button.get_attrib('href')
            bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.warning)
            bill_list_table = bill_list_doc.xpath("//div[@class='view-content']//article/@about")
            for bill_list_row in bill_list_table:
                bill_id = re.findall(r'/bills/(.*)', bill_list_row)
                if len(bill_id) == 0:
                    continue
                bill_id= bill_id[0]
                bill_ids.add(bill_id.upper())
            next_button = bill_list_doc.xpath_single("//a[text()='Next']")

        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):
        """
        scrape individual bill
        """
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_url = base_url + re.sub(r'\s+', '', bill_id).lower()
        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        if not bill_page:
            logger.warning("No Bill Page for %s Are Found, Skip" % bill_id)
            return

        title = bill_page.xpath_single("//h1[@class='node__title node-title']").text_content()

        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)


        #summary
        summary = bill_page.xpath_single("//div[contains(@class, 'field-name-field-bill-summary')]")
        if summary and summary.text_content():
            bill.add_summary(summary.text_content())

        #subjects
        subjects = bill_page.xpath("//div[@class='bill-subjects']//a", BRP.debug)
        for sub_entity in subjects:
            sub = sub_entity.text_content()
            bill.add_subject(sub)

        #sponsors
        sponsors = bill_page.xpath("//div[@class='member-details']//a", BRP.bill_sponsors)
        for spon in sponsors:
            sponsor_name = spon.text_content()
            if not sponsor_name:
                sponsor_url = spon.get_attrib('href')
                sponsor_name = re.findall(r'/legislators/(.*)', sponsor_url)[0]
                sponsor_name = re.sub(r'-', ' ', sponsor_name).title()
            if sponsor_name:
                bill.add_sponsor('primary', sponsor_name)


        
        #Action 
        history_table = bill_page.xpath("//h2[text()='Bill History']/following-sibling::table/tbody/tr", BRP.bill_actions)
        for his_row in history_table:
            his_cols = his_row.xpath("./td")
            date = his_cols[0].text_content()
            action = his_cols[2].text_content()
            if not date:
                logger.warning("Action \"{}\" does not have a action date. Skipping it for now.".format(action))
                continue
            formed_date = parse(date)
            action_chamber = his_cols[1].text_content()
            if action_chamber == 'Senate':
                actor = 'upper'
            elif action_chamber == 'House':
                actor = 'lower'
            elif action_chamber == 'Governor':
                actor = 'executive'
            else:
                logger.warning("Unknown actor %s" % action_chamber)
                continue

            bill.add_action(actor, action, formed_date)

        #Version
        version_table = bill_page.xpath("//h5[contains(text(), 'All Versions')]/"\
                                        "ancestor::a/following-sibling::div//tr", BRP.bill_versions)
        ver_dict = []
        for ver_row in version_table[1:]:
            ver_cols = ver_row.xpath('./td')
            ver_type = ver_cols[1].text_content()
            ver_url = ver_cols[2].xpath_single('.//a').get_attrib('href')
            if ver_url in ver_dict:
                continue
            ver_dict.append(ver_url)
            ver_name = "%s Version" % ver_type
            try:
                download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(ver_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, content_type='application/pdf', should_skip_checks=True)
            except:
                logger.warning("Invalid Document Link %s" % ver_url)
                continue


            if len(doc_ids) != 1 or doc_ids[0] is None:
                logger.warning("Unhandled Document %s, ignore" % ver_url)
                continue
            doc_service_document = Doc_service_document(ver_name, "version", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])

            bill.add_doc_service_document(doc_service_document)


        preamend_table = bill_page.xpath("//h5[contains(text(), 'Preamended Versions')]/"\
                                         "ancestor::a/following-sibling::div//tr", BRP.bill_versions)


        #Amendment
        per_dict = []
        for preamend_row in preamend_table[1:]:
            preamend_cols = preamend_row.xpath('./td')
            preamend_type = preamend_cols[1].text_content()
            preamend_url = preamend_cols[2].xpath_single('.//a').get_attrib('href')
            if preamend_url in per_dict:
                continue
            per_dict.append(preamend_url)
            preamend_name = "%s Preamended Versions" % preamend_type
            try:
                download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(preamend_url, BRP.bill_versions,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         True, content_type='application/pdf')
            except:
                logger.warning("Invalid Document Link %s" % ver_url)
                continue

            if len(doc_ids) != 1:
                logger.warning("Document %s get more than one doc id, ignore" % preamend_url)
                continue

            doc_service_document = Doc_service_document(preamend_name, "amendment", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        #Fiscal Note
        fiscal_table = bill_page.xpath("//h5[contains(text(), 'Fiscal Notes')]/"\
                                       "ancestor::a/following-sibling::div//tr", BRP.bill_documents)
        fiscal_dict = []
        for fiscal_row in fiscal_table[1:]:
            fiscal_cols = fiscal_row.xpath('./td')
            fiscal_type = fiscal_cols[1].text_content()
            fiscal_url = fiscal_cols[2].xpath_single('.//a').get_attrib('href')
            if fiscal_url in fiscal_dict:
                continue
            fiscal_dict.append(fiscal_url)
            fiscal_name = "%s Fiscal Notes" % fiscal_type
            download_id = self.scraper.download_and_register(fiscal_url, BRP.bill_documents, True)

            doc_service_document = Doc_service_document(fiscal_name, "fiscal_note", "partial",
                                                        download_id)
            bill.add_doc_service_document(doc_service_document)


        #Session Law
        session_law_table = bill_page.xpath("//h2[contains(text(), 'Session Laws')]/"\
                                            "following-sibling::table//tr", BRP.bill_versions)
        sl_dict = []
        for session_law_row in session_law_table[1:]:
            session_law_cols = session_law_row.xpath('./td')
            session_law_title = session_law_cols[2].text_content()
            session_law_url = session_law_cols[3].xpath_single('.//a').get_attrib('href')
            if session_law_url in sl_dict:
                continue
            sl_dict.append(session_law_url)
            try:
                download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(session_law_url, BRP.bill_versions,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         True, content_type='application/pdf')
            except:
                logger.warning("Invalid Document Link %s" % ver_url)
                continue

            if len(doc_ids) != 1:
                logger.warning("Document %s get more than one doc id, ignore" % session_law_url)
                continue
            session_law_title = 'Session Law - %s' % session_law_title
            doc_service_document = Doc_service_document(session_law_title, "version", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        #Votes
        vote_table = bill_page.xpath("//a[text()='Vote Summary']/@href", BRP.bill_votes)
        vote_dict = []
        for vote_link in vote_table:
            if vote_link in vote_dict:
                continue
            vote_dict.append(vote_link)
            self.scrape_vote(bill, vote_link)

        #Floor Votes
        floor_table = bill_page.xpath("//h2[text()='Votes']/following-sibling::ul//tr")
        for vote_ele in floor_table:
            if 'Vote Document' in vote_ele.text_content():
                continue
            vote_en = vote_ele.xpath("./td")
            vote_date = parse(vote_en[0].text_content())
            action = vote_en[1].text_content()
            motion = vote_en[2].text_content()
            result = vote_en[3].text_content()
            yes_count = int(re.findall(r'Aye: (\d+)', result)[0])
            no_count = int(re.findall(r'No: (\d+)', result)[0])
            other_count = int(re.findall(r'Other: (\d+)', result)[0])
            vote_url = vote_en[4].xpath_single('./a').get_attrib('href')

            motion = motion + ':' + action
            vote_page = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
            vote_header = vote_page.xpath_single("//div[@class='field-item even']/form/table/tr[1]")
            if vote_header is None:
                logger.warning("Bad Vote Page %s" % vote_url)
                continue
            vote_header = vote_header.text_content()
            if 'SENATE' in vote_header:
                vote_chamber = 'upper'
            elif 'HOUSE' in vote_header:
                vote_chamber = 'lower'
            else:
                logger.warning("Unknown chamber for %s" % vote_url)
                continue
            vote = Vote(vote_chamber, vote_date, motion, yes_count>no_count+other_count, yes_count, no_count, other_count)
            vote.add_source(vote_url)

            voter_table = vote_page.xpath("//div[@class='field-item even']/form/table/tr[3]//tr")
            for voter_ele in voter_table:
                if 'Member Name' in voter_ele.text_content():
                    continue
                voter_name, vote_re = re.split(r'\n+', voter_ele.text_content())
                if vote_re == 'Y':
                    vote.yes(voter_name)
                elif vote_re == 'N':
                    vote.no(voter_name)
                else:
                    vote.other(voter_name)
            bill.add_vote(vote)
        self.save_bill(bill)


    def scrape_vote(self, bill, vote_link):
        """
        Function to Scrape votes
        """
        chamber_char = re.findall(r'content/(.)', vote_link)[0]
        if chamber_char == 'h':
            vote_chamber = 'lower'
        elif chamber_char == 's':
            vote_chamber = 'upper'
        else:
            vote_chamber = bill['chamber']

        vote_page = self.scraper.url_to_lxml(vote_link, BRP.bill_votes)
        date = re.findall(r'\d{4}-\d{2}-\d{2}', vote_link)[0]
        time = vote_page.xpath_single("//font[contains(text(),'TIME:')]/ancestor::td/following-sibling::td")
        if time is None:
            return
        formed_date = parse("%s %s" % (date, time.text_content()))



        motion = vote_page.xpath_single("//font[contains(text(),'MOTION:')]/ancestor::td/following-sibling::td")
        motion = motion.text_content()


        voter_table = vote_page.xpath("//font[text()='VOTE']/ancestor::tr/following-sibling::tr")

        yes_count = 0
        no_count = 0
        other_count = 0
        exc_count = 0
        abs_count = 0
        passed = False

        action = vote_page.xpath_single("//font[contains(text(), 'FINAL ACTION:')]/"\
                                        "ancestor::b/following-sibling::b")
        
        if action:
            if 'PASS' in action.text_content():
                passed = True
        elif 'passed without objection' in motion:
            passed = True


        #Get yes vote number
        yes_row = vote_page.xpath_single("//font[contains(text(), 'YES:')]/ancestor::b/following-sibling::font")
        if yes_row:
            yes_count = int(yes_row.text_content())

        #Get nay vote number
        no_row = vote_page.xpath_single("//font[contains(text(), 'NO:')]/ancestor::b/following-sibling::font")
        if no_row:
            no_count = int(no_row.text_content())

        #Get excused vote number
        exc_row = vote_page.xpath_single("//font[contains(text(), 'EXC:')]/ancestor::b/following-sibling::font")
        if exc_row:
            exc_count = int(exc_row.text_content())

        #Get absent number
        abs_row = vote_page.xpath_single("//font[contains(text(), 'ABS:')]/ancestor::b/following-sibling::font")
        if abs_row:
            abs_count = int(abs_row.text_content())

        other_count = exc_count + abs_count

        vote = Vote(vote_chamber, formed_date, motion, passed, yes_count, no_count, other_count)
        vote.add_source(vote_link)

        for voter_row in voter_table:
            voter = voter_row.text_content()
            if not voter:
                continue
            if 'FINAL ACTION:' in voter:
                break

            voter_dict = re.split(r'\n+', voter)
            if len(voter_dict) == 1:
                vote.yes(voter_dict[0])
            else:
                if voter_dict[1] == 'Yes':
                    vote.yes(voter_dict[0])
                elif voter_dict[1] == 'No':
                    vote.no(voter_dict[0])
                elif voter_dict[1] == 'Excused' or voter_dict[1] == 'absent':
                    vote.other(voter_dict[0])

        bill.add_vote(vote)

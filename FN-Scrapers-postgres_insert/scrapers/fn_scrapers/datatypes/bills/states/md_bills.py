from __future__ import absolute_import

import re
from dateutil.parser import parse

import logging
from fn_scraperutils.doc_service.util import ScraperDocument

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id, normalize_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('MDBillScraper')


def generate_session_id(session):
    """
    Generate session_id which will be used in bill link
    """
    return "%srs" % session[:4]

def generate_bill_list(bill_str, bill_ids):
    """
    Generate bill_id from a bill_list_string such as "SB0001 - SB1173"
    """
    if ' - ' in bill_str:
        bill_type, start_number, end_number = re.findall(r'([A-Z]+)(\d+)\s+-.*?(\d+)', bill_str)[0]
        for number in range(int(start_number), int(end_number)+1):
            bill_id = "%s%d" % (bill_type, number)
            bill_ids.add(bill_id)
    else:
        bill_type, bill_number = re.findall(r'([A-Z]+)(\d+)', bill_str)[0]
        bill_id = "%s%d" % (bill_type, int(bill_number))
        bill_ids.add(bill_id)

def summary_parser(element_wrapper):
    text = element_wrapper.xpath_single("//th[text()='Synopsis:']/following-sibling::td").text_content()
    return [ScraperDocument(text)]


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-MD", group="fnleg")
class MDBillScraper(BillScraper):
    def __init__(self):
        super(MDBillScraper, self).__init__("md")

    def scrape_bill_ids(self, session):
        session_id = generate_session_id(session)
        base_bill_list_url = "http://mgaleg.maryland.gov/webmga/frmLegislation.aspx?"\
                             "pid=legisnpage&tab=subject3&ys=%s"
        bill_list_url = base_bill_list_url % session_id
        bill_list_page = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)

        bill_ids = set()

        #Generate list of bills
        bill_rows = bill_list_page.xpath('//td[text()="Bills"]/following-sibling::td', BRP.test)
        for bill_row in bill_rows:
            bill_str = bill_row.text_content()
            generate_bill_list(bill_str, bill_ids)

        #Generate list of resolutions
        bill_rows = bill_list_page.xpath('//td[text()="Joint Resolutions"]/following-sibling::td', BRP.test)
        for bill_row in bill_rows:
            bill_str = bill_row.text_content()
            if 'JR' in bill_str:
                continue
            generate_bill_list(bill_str, bill_ids)


        return list(bill_ids)

    def scrape_bill(self, session, bill_id, **kwargs):
        session_id = generate_session_id(session)
        bill_base_url = "http://mgaleg.maryland.gov/webmga/frmMain.aspx?id=%s&stab=%s"\
                        "&pid=billpage&tab=subject3&ys=%s"

        bill_type, bill_number = re.findall(r'([A-Z]+) (\d+)', bill_id)[0]
        url_bill_id = "%s%04d" % (bill_type, int(bill_number))

        bill_url = bill_base_url % (url_bill_id, '01', session_id)
        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        if "Unable to retrieve the requested information. Please try again." in bill_page.text_content():
            logger.warning("Bad Bill ID %s" % bill_id)
            return

        title = bill_page.xpath('//h3[contains(text(), "Entitled:")]/ancestor::td/following-sibling::td'
                                )[0].text_content()


        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        sh_bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, sh_bill_type)
        bill.add_source(bill_url)


        #Summary
        summary_text = bill_page.xpath_single('//th[text()="Synopsis:"]/following-sibling::td').text_content()
        if summary_text:
            bill.add_summary(summary_text)

        #primary sponsor
        sponsor_name = bill_page.xpath_single('//span[text()="Sponsored by:"]/ancestor::td/'\
                                              'following-sibling::td/h3/a').text_content()
        sponsor_name = re.sub('Delegate|Senator', '', sponsor_name).strip()
        bill.add_sponsor('primary', sponsor_name)

        #cosponsors
        cosponsors_list = bill_page.xpath('//th[text()="All Sponsors:"]/following-sibling::td/a')
        for cosponsor in cosponsors_list:
            cosponsor_name = cosponsor.text_content()
            if cosponsor_name == sponsor_name:
                continue
            bill.add_sponsor('cosponsor', cosponsor_name)

        # Related bills
        # "Additional Facts:" will sometimes have hyperlinks of related bills
        related_bills = bill_page.xpath('//th[text()="Additional Facts:"]/following-sibling::td/a')
        for related_bill in related_bills:
            related_bill = re.match(r"[HS][BJ]\d+", related_bill.text_content())
            if related_bill:
                bill.add_companion(normalize_bill_id(related_bill.group()))

        #subjects
        subjects_list = bill_page.xpath('//span[text()="Broad Subject(s):"]/ancestor::th/'\
                                        'following-sibling::td/a')
        for subject in subjects_list:
            subject_name = subject.text_content()
            bill.add_subject(subject_name)

        #Document Page
        doc_url = bill_base_url % (url_bill_id, '02', session_id)
        doc_page = self.scraper.url_to_lxml(doc_url, BRP.bill_documents) 
        doc_table = doc_page.xpath("//table[@class='billdocs']//tr")
        for doc_row in doc_table[1:]:
            doc_link = doc_row.xpath_single('.//a').get_attrib('href')
            doc_name = doc_row.xpath_single('./td').text_content()
            if  doc_name.startswith('Text'):
                doc_type = 'version'
                doc_service_type = "complete"
            elif doc_name.startswith('Amendments'):
                doc_type = 'amendment'
                doc_service_type = "complete"
            elif 'Fiscal' in doc_name or 'Analysis' in doc_name:
                doc_type = 'fiscal_note'
                doc_service_type = "partial"
            elif 'Committee' in doc_name:
                doc_type = 'committee_document'
                doc_service_type = "partial"
            elif 'Veto' in doc_name:
                doc_type = 'summary'
                doc_service_type = 'partial'
            else:
                doc_type = 'other'
                doc_service_type = "partial"
            if doc_link.lower().endswith('.pdf'):
                if doc_type == 'version':
                    doc_policy = BRP.bill_versions
                else:
                    doc_policy = BRP.bill_documents
                try:
                    if doc_service_type == 'complete':
                        download_id, _, doc_ids = \
                                    self.scraper.register_download_and_documents(doc_link, doc_policy,
                                                                                 self.scraper.extraction_type.text_pdf,
                                                                                 True, content_type='application/pdf')
                        doc_service_document = Doc_service_document(doc_name, doc_type, doc_service_type,
                                                                    download_id, doc_id=doc_ids[0])
                    else:
                        download_id = self.scraper.download_and_register(doc_link, doc_policy, False,
                                                                         content_type='text/html')
                        doc_service_document = Doc_service_document(doc_name, doc_type, "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)
                except:
                    logger.warning("Failed to register document %s" % doc_link)
            else:
                download_id = self.scraper.download_and_register(doc_link, BRP.bill_documents, False,
                                                                 content_type='text/html')
                doc_service_document = Doc_service_document(doc_name, doc_type, "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

            #Scrape Votes
            if 'flrvotepage' in doc_link:
                self.scrape_vote(bill, doc_link, doc_name)

        #Action
        action_url = bill_base_url % (url_bill_id, '03', session_id)
        action_page = self.scraper.url_to_lxml(action_url, BRP.bill_actions) 
        action_table = action_page.xpath("//table[@class='billgrid']//tr[contains(@class, 'row')]")
        action_chamber = 'other'
        action_date = None
        for action_row in action_table:
            action_col = action_row.xpath('./td')
            if action_col[0].text_content():
                action_chamber = action_col[0].text_content()
                action_chamber = {'Senate': 'upper',
                                  'House': 'lower',
                                  'Post Passage': 'executive'
                                  }[action_chamber]
            action_str =  action_col[3].text_content()
            if action_col[1].text_content():
                action_date = parse(action_col[1].text_content())
            bill.add_action(action_chamber, action_str, action_date)

        self.save_bill(bill)


    def scrape_vote(self, bill, doc_link, doc_name):
        if 'Senate' in doc_name:
            vote_chamber = 'upper'
        elif 'House' in doc_name:
            vote_chamber = 'lower'
        vote_page = self.scraper.url_to_lxml(doc_link, BRP.bill_votes)
        vote_text = vote_page.xpath_single('//div[@id="ContentPlaceHolder1_div_03"]/table').text_content()
        motion = doc_name
        yes_count = 0
        no_count = 0
        other_count = 0
        yes_votes = []
        no_votes = []
        other_votes = []
        vote_links = re.split(r'(\r|\n|\t)+', vote_text)
        flag = None
        for line in vote_links:

            line = line.strip()
            if not line:
                continue
            if 'Voting Yea' in line:
                flag = 'Y'
                yes_count = int(re.findall(r'Voting Yea - (\d+)', line)[0])
            elif 'Voting Nay' in line:
                flag = 'N'
                no_count = int(re.findall(r'Voting Nay - (\d+)', line)[0])
            elif 'Not Voting -' in line:
                flag = 'O'
                other_count += int(re.findall(r'Not Voting - (\d+)', line)[0])
            elif 'Excused from Voting -' in line:
                flag = 'O'
                other_count += int(re.findall(r'Excused from Voting - (\d+)', line)[0])
            elif 'Excused (Absent) -' in line:
                flag = 'O'
                other_count += int(re.findall(r'Excused \(Absent\) - (\d+)', line)[0])
            elif 'Calendar Date:' in line:
                date = re.findall(r'Calendar Date:(.*)', line)[0].strip()
                formed_date = parse(date)
            elif flag:
                if flag == 'Y':
                    yes_votes.append(line)
                elif flag == 'N':
                    no_votes.append(line)
                else:
                    other_votes.append(line)
        vote = Vote(vote_chamber, formed_date, motion, yes_count>no_count,\
                    yes_count, no_count, other_count)
        if len(yes_votes) > 0:
            vote['yes_votes'] = yes_votes
        if len(no_votes) > 0:
            vote['no_votes'] = no_votes
        if len(other_votes) > 0:
            vote['other_votes'] = other_votes
        bill.add_vote(vote)



"""
:class UTBillScraper: scrapes Utah Bills
"""
from __future__ import absolute_import
import re

from dateutil.parser import parse
from fn_scraperutils.doc_service.util import ScraperDocument
from ..common.bill import Bill
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.bill_scraper import BillScraper
from ..common.doc_service_document import Doc_service_document
from ..common.vote import Vote
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
import logging
from ..common import metadata
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY
from fn_scrapers.api.scraper import scraper, tags

logger = logging.getLogger('UTBillScraper')

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-UT", group="fnleg")
class UTBillScraper(BillScraper):
    """
    Scrape Utah Bills
    """
    def __init__(self, *args, **kwargs):
        super(UTBillScraper, self).__init__(
            'ut', *args, retry_policy=RETRY500_REQUESTS_RETRY_POLICY, **kwargs)

    def get_slug(self, session):
        session_name = metadata.get_session_name(self.scraper.metadata_client, 'ut', session)
        year = session[:4]
        slug = None
        if 'Special Session' in session_name:
            """
            WARNING: This is not going to work for sessions prior to 2017, unless the session names
            are renamed. (07/17/2018)

            The session names are like:
            - 2018 2nd Special Session
            - 2017 1st Special Session
            - 2013 1st House Session
            """
            ses_num = re.search(r'(\d+)[a-z]{2} Special Session', session_name).group(1)
            slug = "%sS%s" % (year, ses_num)
        elif 'General' in session_name or 'Regular' in session_name:
            slug = "%sGS" % year
        else:
            match = re.search(r'(\d+)[a-z]{2} ([A-Z])[a-z]+? Session', session_name)
            if match:
                sess_num, sess_code = match.groups()
                slug = "%s%s%s" % (year, sess_code, sess_num)
            else:
                raise ValueError("Invalid session name {!r}".format(session_name))
        return slug

    def scrape_bill_ids(self, session):
        slug = self.get_slug(session)
        bill_ids = set()
        bill_list_url = "http://le.utah.gov/DynaBill/BillList?session=%s" % slug
        doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        type_table = doc.xpath("//a[contains(@href, 'javascript:toggleObj')]")
        for type_row in type_table:
            code_url = type_row.get_attrib('href')
            code = re.findall(r'\(\'r(.*?)\'\)', code_url)[0]
            type_list_url = bill_list_url + "&bills=%s" % code
            type_doc = self.scraper.url_to_etree(type_list_url, BRP.bill_list)
            bill_table = type_doc.xpath_single('//DATA').text
            bill_id_group = re.findall(r'\<A HREF=.*?\>(.*?)\<\/A\>', bill_table)
            for bill_id in bill_id_group:
                bill_id = re.sub(r'\.', '', bill_id)
                bill_id = re.findall(r'[A-Z]+ \d+', bill_id)[0]
                bill_ids.add(bill_id)
        logger.info("a total of %s bill ids scraped for %s session" % (len(bill_ids), session))
        return list(bill_ids)

    def scrape_bill(self, session, bill_id, **kwargs):
        slug = self.get_slug(session)
        slug = re.sub('GS', '', slug)
        bill_char, bill_num = bill_id.split()
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        if len(bill_char) < 3:
            bill_url = "http://le.utah.gov/~%s/bills/static/%s%04d.html" % (slug, bill_char, int(bill_num))
        else:
            bill_url = "http://le.utah.gov/~%s/bills/static/%s%03d.html" % (slug, bill_char, int(bill_num))
        logger.info("scraping bill id %s at url %s" % (bill_id, bill_url))
        bill_doc = self.scraper.url_to_lxml(bill_url, BRP.bill)
        title = bill_doc.xpath_single("//h3[@class='heading']").text_content()
        title = re.findall(r'.*? \d+ (.*)', title)[0].strip()

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)

        sponsor_table = bill_doc.xpath("//div[@id='legislatordiv']//a", BRP.debug)
        for sponsor_row in sponsor_table:
            sponsor_name = sponsor_row.text_content()
            if sponsor_name:
                sponsor_name = re.sub(r'Rep\.|Sen\.', '', sponsor_name).strip()
                bill.add_sponsor('primary', sponsor_name)

        action_table = bill_doc.xpath("//div[@id='billStatus']//table/tr")
        for action_row in action_table[1:]:
            action_cel = action_row.xpath("./td")
            action_date = re.sub(r'[\(\)]', ' ', action_cel[0].text_content())
            action_date = parse(action_date)
            action = action_cel[1].text_content()
            color = action_cel[0].xpath_single('./font').get_attrib('color')
            if color == '#25B223':
                """
                Some of these actions actually initiate in the upper or lower
                chamber, so we check the location for them
                """
                if 'House' in action_cel[2].text_content():
                    actor = 'lower'
                elif 'Senate' in action_cel[2].text_content():
                    actor = 'upper'
                else:
                    actor = 'other'
            elif color == '#CC0000':
                actor = 'lower'
            elif color == '#3333FF':
                actor = 'upper'
            elif color == 'black':
                """
                Some of these actions actually initiate in the upper or lower
                chamber, so we check the location for them
                """
                if 'House' in action_cel[2].text_content():
                    actor = 'lower'
                elif 'Senate' in action_cel[2].text_content():
                    actor = 'upper'
                else:
                    actor = 'executive'
            else:
                actor = 'other'
                logger.warning("Unable to extract chamber for action: {}".format(action))

            location = action_cel[2].text_content().strip()
            committee_entity = None
            if location.lower().endswith('committee'):
                # Attach committee/subcommittee name with action text to trigger committee extraction
                action += ' : {}'.format(location)

            bill.add_action(actor=actor, action=action, date=action_date) #pylint: disable=W0142

            """
            Since the vot chamber is being determined by the Action chamber, the code
            is also looking at the action text to make a better decision on the vote chamber
            to avoid mis-categorization
            """
            if action.lower().startswith('house'):
                actor = 'lower'
            elif action.lower().startswith('senate'):
                actor = 'upper'
            elif 'house' in action.lower() and 'senate' in action.lower():
                actor = 'joint'
            
            vote_link = action_cel[3].xpath_single('.//a', BRP.test)
            if vote_link:
                if 'Voice vote' in vote_link.text_content():
                    continue
                vote_link = vote_link.get_attrib('href')
                self.scrape_vote(bill, vote_link, action_date, action, actor)

        
        version_table = bill_doc.xpath(
            "//ul[@id='billTextDiv'][1]//a[@title='Open PDF']", BRP.debug)
        for version_row in version_table:
            version_url = version_row.get_attrib('href')
            version_name = version_row.getparent().text_content()
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(version_url,
                                                                     BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True)
            doc_service_document = Doc_service_document(version_name, 'version',
                                                        "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)


        document_table = bill_doc.xpath(
            "//b[contains(text(), 'Related Documents')]/following-sibling::ul[1]/li",
            BRP.debug)
        for document_row in document_table:
            document_name = document_row.text_content()
            document_url = document_row.xpath("./a", BRP.debug)[0].get_attrib('href')
            if not document_url:
                onclick_url = document_row.xpath("./a", BRP.debug)[0].get_attrib('onclick')
                document_url = re.findall(r'\(\'(.*?)\'', onclick_url)[0]
                document_url = 'http://le.utah.gov' + document_url
            if 'Amendment' in document_name:
                download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(document_url,
                                                                     BRP.bill_documents,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True)
                doc_service_document = Doc_service_document(document_name, 'amendment',
                                                            "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
                continue


            if 'Fiscal Note' in document_name or 'Agency Perf Note' in document_name:
                document_type = 'fiscal_note'
            elif 'Committee' in document_name:
                document_type = 'committee_document'
            else:
                document_type = 'other'
            download_id = self.scraper.download_and_register(document_url, BRP.bill_documents, False)


            doc_service_document = Doc_service_document(document_name, document_type,
                                                        "partial",
                                                        download_id=download_id)
            bill.add_doc_service_document(doc_service_document)


        self.save_bill(bill)


    def scrape_vote(self, bill, vote_link, date, motion, actor):
        if actor in {'other', 'executive'}:
            actor = 'joint'
        if 'mtgvotes.jsp' in vote_link:
            self.scrape_mtgvotes_page(bill, vote_link, date, motion, actor)
        elif 'svotes.jsp' in vote_link:
            self.scrape_svotes_page(bill, vote_link, date, motion, actor)
        elif 'comvotes.asp' in vote_link:
            self.scrape_comvotes_page(bill, vote_link, date, motion, actor)
        else:
            logger.warning("Unknown Vote Link %s" % vote_link)
    

    def scrape_mtgvotes_page(self, bill, vote_link, date, motion, chamber):
        vote_doc = self.scraper.url_to_lxml(vote_link, BRP.bill_votes)
        vote_table = vote_doc.xpath("//table[./tr/td/b/text()='Votes:']/tr")
        result = vote_table[1].text_content()
        yes_count, no_count, other_count = re.findall(
            r'Yeas\s+-\s+(\d+)\s+Nays\s+-\s+(\d+)\s+Absent\s+-\s+(\d+)',
            result)[0]
        yes_count = int(yes_count)
        no_count = int(no_count)
        other_count = int(other_count)

        vote = Vote(chamber, date, motion, yes_count > (no_count + other_count),
                    yes_count, no_count, other_count)

        voter_cols = vote_table[2].xpath('./td')
        yes_voters_list = re.split(r'\r\n', voter_cols[0].text_content())
        for people in yes_voters_list:
            if not people.strip():
                continue
            vote.yes(people)
        abs_voters_list = re.split(r'\r\n', voter_cols[-1].text_content())
        for people in abs_voters_list:
            if not people.strip():
                continue
            vote.other(people)
        no_voters_list = ""
        for col in voter_cols[1:-1]:
            if col.text_content():
                no_voters_list = re.split(r'\r\n', col.text_content())
                for people in no_voters_list:
                    if not people.strip():
                        continue
                    vote.no(people)

        agenda_url = vote_doc.xpath_single("//a[text()='Agenda']/@href", BRP.debug)
        if agenda_url:
            vote.add_source(agenda_url)

        committee_report_url = vote_doc.xpath(
            "//a[contains(text(), 'Committee Report')]/@href", BRP.debug)
        for comm_url in committee_report_url:
            vote.add_source(comm_url)
        bill.add_vote(vote)

    def scrape_comvotes_page(self, bill, vote_link, date, motion, chamber):
        vote_doc = self.scraper.url_to_lxml(vote_link, BRP.bill_votes)
        vote_table = vote_doc.xpath("//div[@id='content']//table/tr/td")
        yes_line = vote_table[0].text_content()
        yes_count, yes_people = self.scrape_vote_line(yes_line)
        no_count = 0
        no_people = []
        other_count = 0
        other_people = []
        
        if len(vote_table) > 1:
            line = vote_table[1].text_content()
            if 'Nays' in line:
                no_count, no_people = self.scrape_vote_line(line)
            else:
                other_count, other_people = self.scrape_vote_line(line)
        
        if len(vote_table) > 2:
            other_line = vote_table[2].text_content()
            other_count, other_people = self.scrape_vote_line(other_line)
       

        vote = Vote(chamber, date, motion, yes_count > (no_count + other_count),
                    yes_count, no_count, other_count)
        if len(yes_people):
            vote['yes_votes'] = yes_people
        if len(no_people):
            vote['no_votes'] = no_people
        if len(other_people):
            vote['other_votes'] = other_people

        bill.add_vote(vote)


    @staticmethod
    def scrape_vote_line(line):
        try:
            count, people_line = re.findall(r"- (\d+)(.*)", line)[0]
            count = int(count)
        except IndexError:
            people_line = re.findall(r"-(.*)", line)[0]
            count = None
        people = []
        for ppl in re.split(r'\s{2,}', people_line.strip()):
            if ppl.strip():
                people.append(ppl.strip())
        if count is None:
            count = len(people)
        return count, people

            
    def scrape_svotes_page(self, bill, vote_link, date, motion, chamber):
        vote_doc = self.scraper.url_to_lxml(vote_link, BRP.bill_votes)

        yes_header =  vote_doc.xpath_single("//b[contains(text(), 'Yeas -')]")
        yes_count = int(re.findall(r'\d+', yes_header.text_content())[0])

        nay_header =  vote_doc.xpath_single("//b[contains(text(), 'Nays -')]")
        no_count = int(re.findall(r'\d+', nay_header.text_content())[0])

        other_header =  vote_doc.xpath_single("//b[contains(text(), 'Absent or not voting -')]")
        other_count = int(re.findall(r'\d+',  other_header.text_content())[0])

        vote = Vote(chamber, date, motion, yes_count > (no_count + other_count),
                    yes_count, no_count, other_count)

        for people in self.scrape_vote_table(yes_header):
            if not people:
                continue
            vote.yes(people)

        for people in self.scrape_vote_table(nay_header):
            if not people:
                continue
            vote.no(people)

        for people in self.scrape_vote_table(other_header):
            if not people:
                continue
            vote.other(people)
        bill.add_vote(vote)

    @staticmethod
    def scrape_vote_table(header):
        vote_table = header.xpath("./following-sibling::center[1]/table//td")
        for people in vote_table:
            yield people.text_content()


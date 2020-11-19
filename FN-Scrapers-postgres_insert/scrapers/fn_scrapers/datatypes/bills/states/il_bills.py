from __future__ import absolute_import

import re
from collections import defaultdict
import datetime
import logging
from dateutil.parser import parse

from fn_scraperutils.doc_service.util import ScraperDocument
from fn_document_service.blocking import ttypes

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as Brp
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(u'ILBillScraper')

TITLE_REMOVING_PATTERN = re.compile(ur".*(Rep|Sen). (.+)$")

SPONSOR_REFINE_PATTERN = re.compile(ur'^Added (?P<spontype>.+) (?P<title>Rep|Sen)\. (?P<name>.+)')
SPONSOR_TYPE_REFINEMENTS = {
    u'Chief Co-Sponsor': u'cosponsor',
    u'as Chief Co-Sponsor': u'cosponsor',
    u'Alternate Chief Co-Sponsor': u'cosponsor',
    u'as Alternate Chief Co-Sponsor': u'cosponsor',
    u'as Co-Sponsor': u'cosponsor',
    u'Alternate Co-Sponsor':  u'cosponsor',
    u'as Alternate Co-Sponsor':  u'cosponsor',
    u'Co-Sponsor': u'cosponsor',
}


VERSION_TYPES = (u'Introduced', u'Engrossed', u'Enrolled', u'Re-Enrolled', u'Public Act')

DOC_TYPES = {
    u'B': u'bill',
    u'O': u'bill',
    u'R': u'resolution',
    u'JR': u'joint_resolution',
    u'JRCA': u'constitutional_amendment',
    u'AM': u'resolution',
    u'JSR': u'joint_resolution'
}


VOTE_VALUES = [u'NV', u'Y', u'N', u'E', u'A', u'P', u'-']

LEGISLATION_URL = u'http://www.ilga.gov/legislation/default.asp'
BILL_URL = u'http://ilga.gov/legislation/BillStatus.asp'


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-IL", group="fnleg")
class ILBillScraper(BillScraper):
    def __init__(self):
        super(ILBillScraper, self).__init__("il")

    def scrape_bill_ids(self, session):
        bill_ids = []
        ga_num, sess_id = self.get_session_information(session)
        bill_session_id = None
        session_url = LEGISLATION_URL + u"?GA={}&SessionID={}".format(ga_num, sess_id)
        bill_list = self.scraper.url_to_lxml(session_url, Brp.bill_list)
        doc_type_sections = bill_list.xpath(u"//div/table")
        for section in doc_type_sections:
            first_link = section.xpath_single(u"tr[td][1]/td[a][1]/a/@href")
            last_link = section.xpath_single(u"tr[td][last()]/td[a][last()]/a/@href")
            first_bill_num, first_bill_type = re.search(ur"num1=(\d+)&num2=\d+&DocTypeID=([A-Z]+)", first_link).groups()
            last_bill_num, last_bill_type = re.search(ur"num1=\d+&num2=(\d+)&DocTypeID=([A-Z]+)", last_link).groups()
            if not bill_session_id:
                bill_session_id = int(re.search(ur"&SessionID=(\d+)", last_link, re.IGNORECASE).group(1))
            if last_bill_type != first_bill_type:
                logger.error(u"Bill types don't match for range: %s and %s", first_bill_type, last_bill_type)
                return
            # AM's are not numbered contiguously, so we need to scrape them separately.
            if first_bill_type == u"AM":
                continue
            first_bill = int(first_bill_num)
            last_bill = int(last_bill_num)
            bill_ids += [u"{} {}".format(first_bill_type, bill_num) for bill_num in xrange(first_bill, last_bill+1)]

        am_links = bill_list.xpath(u"//a[contains(@href, 'DocTypeID=AM')]/@href")
        for am_link in am_links:
            am_list_page = self.scraper.url_to_lxml(am_link, Brp.bill_list)
            am_bill_links = am_list_page.xpath(u"//a[contains(@href, 'DocNum')]/@href", Brp.bill)
            for am_bill_link in am_bill_links:
                bill_num = re.search(ur"DocNum=(\d+)", am_bill_link).group(1)
                am_bill_id = u"AM {}".format(bill_num)
                bill_ids.append(am_bill_id)

        bills = {bill_id: {u"ga_num": ga_num, u"session_id": bill_session_id} for bill_id in bill_ids}
        return bills

    def scrape_bill(self, session, bill_id, **kwargs):
        chamber = self.chamber_from_bill_id(bill_id)
        num_part = re.search("\d", bill_id)
        doc_type = bill_id[:num_part.start()].strip()
        doc_num = bill_id[num_part.start():].strip()

        bill_info = kwargs.get(u"bill_info")
        ga_num = bill_info[u"ga_num"]
        sess_id = bill_info[u"session_id"]
        bill_url = BILL_URL + u"?GA={}&SessionID={}&DocTypeID={}&DocNum={}".format(ga_num, sess_id, doc_type, doc_num)
        doc = self.scraper.url_to_lxml(bill_url, Brp.bill)
      
        not_found = doc.xpath_single(u"//td/table/tr/td[contains(., 'Request Not Found')]", Brp.test)
        no_status = doc.xpath_single(u"//td/table/tr/td[contains(., 'No Bill Status')]", Brp.test)
        if not_found or no_status:
            logger.warning(u"Could not find any data for bill: %s %s %s", u"IL", session, bill_id)
            return

        if doc_type in [u'AM', u'JSR']:
            bill_type = DOC_TYPES[doc_type]
        else:
            bill_type = DOC_TYPES[doc_type[1:]]  # removes the chamber prefix for lookup
        title = doc.xpath_single(u'//span[text()="Short Description:"]/following-sibling::span[1]/text()',
                                 Brp.bill_title).strip().title()
        summary = ''
        for i in doc.xpath(u'//span[text()="Synopsis As Introduced"]/following-sibling::span',
                                   Brp.bill_summary):
            content = i.element.cssselect("SPAN.content")
            if content:
                summary += '\n\n' + content[0].text_content()
        summary = summary.strip()
        bill = Bill(session, chamber, bill_id, title, bill_type, summary=summary)

        bill.add_source(bill_url)

        # Build sponsor list
        sponsor_list = self.build_sponsor_list(doc.xpath(u'//a[@class="content"]', Brp.bill_sponsors))

        # Scrape actions, and refine sponsors based on the actions
        action_tds = doc.xpath(u'//a[@name="actions"]/following-sibling::table[1]/td', Brp.bill_actions)
        for i in xrange(0, len(action_tds), 3):
            date = action_tds.get_element(i)
            actor = action_tds.get_element(i+1)
            action = action_tds.get_element(i+2)
            date = datetime.datetime.strptime(date.text_content(), u"%m/%d/%Y")
            actor = actor.text_content()
            if actor == u'House':
                actor = u'lower'
            elif actor == u'Senate':
                actor = u'upper'

            action = action.text_content()
            bill.add_action(actor, action, date)

            # If the action is sponsor related, we can use it to refine the sponsor list.
            if u'sponsor' in action.lower():
                self.refine_sponsor_list(action, sponsor_list, bill_id)

        # now add sponsors
        for spontype, sponsor, chamber in sponsor_list:
            if chamber:
                bill.add_sponsor(spontype, sponsor, chamber=chamber)
            else:
                bill.add_sponsor(spontype, sponsor)

        # versions
        version_url = doc.xpath_single(u'//a[text()="Full Text"]/@href', Brp.bill_versions)
        if version_url:
            self.scrape_documents(bill, version_url)

        # if there's more than 1 votehistory link, there are votes to grab
        votes = doc.xpath(u'//a[contains(@href, "votehistory")]/@href', Brp.test)
        if len(votes) > 1:
            # The first votehistory link is a link to a Votes page, that is sometimes empty even when there are votes on
            # the main page.
            self.scrape_votes(bill, votes[1])

        self.save_bill(bill)

    def scrape_documents(self, bill, version_url):
        doc = self.scraper.url_to_lxml(version_url, Brp.bill_versions)

        for link in doc.xpath(u'//a[contains(@href, "fulltext")]'):
            name = link.text
            url = link.get_attrib(u'href')
            document_type = u"other"
            doc_service_type = u"partial"

            if u'Printer-Friendly' in name:
                continue

            version_doc = self.scraper.url_to_lxml(url, Brp.bill_versions)
            if name == u'Introduced' and \
                    u"HTML full text does not exist for this appropriations document." in \
                    version_doc.text_content():
                url = doc.xpath_single(u'//a[text()="PDF"]/@href')
                document_type = u"version"
                doc_service_type = u"complete"
            else:
                text_url = url + u'&print=true'
                text_doc = self.scraper.url_to_lxml(text_url, Brp.bill_documents)
                if u"You may either view the PDF version linked above or " in text_doc.text_content():
                    try:
                        text_url = text_doc.xpath_single(u'//a[contains(text(), "click here")]/@href')
                    except IndexError:
                        logger.warning(u"Failed to scrape full file of version")
                url = text_url
            if name in VERSION_TYPES:
                document_type = u"version"
                doc_service_type = u"complete"
            elif u'Amendment' in name:
                document_type = u"amendment"
                doc_service_type = u"complete"

            if doc_service_type == u"partial":
                download_id = self.scraper.download_and_register(url, Brp.bill_documents, True)
                if download_id is not None:
                    doc = Doc_service_document(name, document_type, doc_service_type, download_id)
                    bill.add_doc_service_document(doc)
                else:
                    logger.warning(u"Doc service failed to handle document at url {} "
                                   u"probably because it is a bad link or the document has no text".format(url))
            elif u".pdf" in url:
                download_id, _,  doc_ids = \
                    self.scraper.register_download_and_documents(url, Brp.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf, True)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc = Doc_service_document(name, document_type, doc_service_type, download_id, doc_id=doc_ids[0])
                    bill.add_doc_service_document(doc)
                else:
                    logger.warning(u"Doc service failed to handle document at url {} "
                                   u"probably because it is a bad link or the document has no text".format(url))
            else:
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(url, Brp.bill_documents,
                                                                 self.scraper.extraction_type.html,
                                                                 True, parse_function=self.html_parser)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc = Doc_service_document(name, document_type, doc_service_type, download_id, doc_id=doc_ids[0])
                    bill.add_doc_service_document(doc)
                else:
                    logger.warning(u"Doc service failed to handle document at url {} "
                                   u"probably because it is a bad link or the document has no text".format(url))

    def scrape_votes(self, bill, votes_url):
        doc = self.scraper.url_to_lxml(votes_url, Brp.bill_votes)
        if not doc:
            return
        for link in doc.xpath(u'//a[contains(@href, "votehistory")]'):

            pieces = link.text.split(u' - ')
            date = pieces[-1]
            motion = u" - ".join(pieces[:-1])

            chamber = link.xpath_single(u'../following-sibling::td/text()')
            if chamber == u'HOUSE':
                chamber = u'lower'
            elif chamber == u'SENATE':
                chamber = u'upper'
            else:
                logger.warning(u'unknown chamber %s', chamber)

            date = parse(date)
            if date is None:
                continue

            # Column parsing is finicky, so we tell the pdf extractor to not expect any columns.
            _, docs, _ = self.scraper.register_download_and_documents(link.get_attrib(u'href'), Brp.bill_votes,
                                                                      self.scraper.extraction_type.text_pdf, True,
                                                                      column_spec=ttypes.ColumnSpec.NONE)
            assert len(docs) == 1
            vote_text = docs[0].text
            vote_text = vote_text.splitlines()
            vote = self.scrape_pdf_for_votes(chamber, date, motion, vote_text)
            if vote:
                bill.add_vote(vote)
            else:
                logger.warning(u"Could not scrape vote from pdf at %s", link.get_attrib(u'href'))

    def scrape_pdf_for_votes(self, chamber, date, motion, vote_text):
        # vote indicator, a few spaces, a name, newline or multiple spaces
        vote_re = re.compile(u'(?:(?:Y|N|P|NV|E)\s+\S{2,})+')
        vote_split_re = re.compile(u'(NV|Y|N|P|E|A)(?!\s+(?:NV|Y|N|P|E|A)\s+|\s+$)\s+')
        count_re_pt1 = re.compile(u'^(\d+)\s+YEAS?\s+(\d+)\s+NAYS?\s+(\d+)\s+PRESENT$')
        count_re_pt2 = re.compile(u'^(\d+)\s+YEAS?\s+(\d+)\s+NAYS?\s+(\d+)\s+PRESENT\s+(\d+)\s+NOT\s+VOTING$')
        count_re_pt3 = re.compile(u'^.*\s+(\d+)\s+NAYS?\s+(\d+)\s+PRESENT$')
        pass_fail_words = {
            u'PASSED': True,
            u'PREVAILED': True,
            u'ADOPTED': True,
            u'CONCURRED': True,
            u'FAILED': False,
            u'LOST': False,
        }

        yes_count = no_count = present_count = other_count = no_voting_count = 0
        yes_votes = []
        no_votes = []
        present_votes = []
        not_voting = []
        other_vote_detail = defaultdict(list)
        passed = None
        counts_found = False
        counts3_found = False
        # flag to check if there is "NOT VOTING" in lines
        p2_flag = False
        votes = []
        for line in vote_text:
            # consider pass/fail as a document property instead of a result of the vote count
            # extract the vote count from the document instead of just using counts of names
            if line.strip() == "":
                continue
            if line.strip() in pass_fail_words:
                if passed is not None:
                    raise Exception(u"Duplicate pass/fail matches in [%s]", motion)
                passed = pass_fail_words[line.strip()]
            elif count_re_pt1.match(line):
                yes_count, no_count, present_count = map(int, count_re_pt1.match(line).groups())
                counts_found = True
            elif count_re_pt2.match(line):
                p2_flag = True
                yes_count, no_count, present_count, no_voting_count = map(int, count_re_pt2.match(line).groups())
                counts_found = True
            elif count_re_pt3.match(line):
                counts3_found = True
                no_count, present_count = map(int, count_re_pt3.match(line).groups())
            elif not counts_found and counts3_found:
                try:
                    vote_group = re.findall(ur'^(\d+)\s+YEAS\s+(.*)', line)[0]
                    yes_count = int(vote_group[0])
                    counts_found = True
                    counts3_found = False
                except:
                    continue
            elif vote_re.search(line) and counts_found:
                vote_split = vote_split_re.split(line)
                votes += zip(vote_split[1::2], vote_split[2::2])
        for vcode, name in votes:
            vcode = vcode.strip()
            name = name.strip()
            if vcode == u'Y':
                yes_votes.append(name)
            elif vcode == u'N':
                no_votes.append(name)
            else:
                other_vote_detail[vcode].append(name)
                other_count += 1
                if vcode == u'P':
                    present_votes.append(name)
                elif vcode == u'NV':
                    not_voting.append(name)
        # fake the counts
        if yes_count == 0 and no_count == 0 and present_count == 0 and no_voting_count == 0:
            yes_count = len(yes_votes)
            no_count = len(no_votes)

        else:  # audit
            if yes_count != len(yes_votes):
                logger.warning(u"Mismatched yes count [expect: %s] [have: %s] on page %s",
                               yes_count, len(yes_votes), motion)
            if no_count != len(no_votes):
                logger.warning(u"Mismatched no count [expect: %s] [have: %s] on page %s",
                               no_count, len(no_votes), motion)
            if present_count != len(present_votes):
                logger.warning(u"Mismatched present count [expect: %s] [have: %s] on page %s",
                               present_count, len(present_votes), motion)
            if p2_flag and no_voting_count != len(not_voting):
                logger.warning(u"Mismatched Not Voting count [expect: %s] [have: %s] on page %s",
                               no_voting_count, len(not_voting), motion)

        if passed is None:
            if chamber == u'lower':  # senate doesn't have these lines
                logger.warning(u"No pass/fail word found; fall back to comparing yes and no vote.")
            passed = yes_count > no_count
        vote = Vote(chamber, date, motion, passed, yes_count, no_count,
                    other_count)
        for name in yes_votes:
            vote.yes(name)
        for name in no_votes:
            vote.no(name)
        for other_type, names in other_vote_detail.iteritems():
            for name in names:
                vote.other(name)

        return vote

    @staticmethod
    def refine_sponsor_list(action, sponsor_list, bill_id):
        """
        The actions timeline includes removal and addition of sponsors. We use those actions to refine the sponsor list
        """
        if u'removed' in action.lower():
            return
        if action.startswith(u'Chief'):
            logger.debug(u"[%s] Assuming we already caught 'chief' for %s" % (bill_id, action))
            return
        match = SPONSOR_REFINE_PATTERN.match(action)
        if match:
            if match.groupdict()[u'title'] == u'Rep':
                chamber = u'lower'
            else:
                chamber = u'upper'
            for i, tup in enumerate(sponsor_list):
                spontype, sponsor, this_chamber = tup
                if this_chamber == chamber and sponsor == match.groupdict()[u'name']:
                    try:
                        sponsor_list[i] = (SPONSOR_TYPE_REFINEMENTS[match.groupdict()[u'spontype']],
                                           sponsor, this_chamber)
                    except KeyError:
                        logger.warning(u'[%s] Unknown sponsor refinement type [%s]',
                                       bill_id, match.groupdict()[u'spontype'])
                    return
            logger.warning(u"[%s] Couldn't find sponsor [%s,%s] to refine",
                           bill_id, chamber, match.groupdict()[u'name'])
        else:
            logger.warning(u"[%s] Don't know how to refine [%s]", bill_id, action)

    @staticmethod
    def get_session_information(session):
        # The first session on the website which we can scrape is the 93rd, from 2003 to 2004
        first_session = 93
        first_year = 2003
        session_year = int(session[:4])
        ga_num = (session_year - first_year)/2 + first_session

        sess_id = 1 if u"r" in session else int(session[-1]) + 1

        return ga_num, sess_id

    @staticmethod
    def build_sponsor_list(sponsor_atags):
        """return a list of (spontype,sponsor,chamber) tuples"""
        sponsors = []
        house_chief = senate_chief = None
        for atag in sponsor_atags:
            sponsor = atag.text
            if u'house' in atag.get_attrib(u'href').split(u'/'):
                chamber = u'lower'
            elif u'senate' in atag.get_attrib(u'href').split(u'/'):
                chamber = u'upper'
            else:
                chamber = None
            if chamber == u'lower' and house_chief is None:
                spontype = u'primary'
                house_chief = sponsor
            elif chamber == u'upper' and senate_chief is None:
                spontype = u'primary'
                senate_chief = sponsor
            else:
                spontype = u'cosponsor'
            sponsors.append((spontype, sponsor, chamber))
        return sponsors

    @staticmethod
    def html_parser(root):
        text = u""
        text_rows = root.xpath(u"//td[@class='xsl' and not(@align) and not(@colspan)]")
        for text_row in text_rows:
            text += text_row.text_content(Brp.test) + u"\n"
        return [ScraperDocument(text)]

    @staticmethod
    def chamber_from_bill_id(bill_id):
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        if not chamber:
            chamber = {'E': 'upper'}[bill_id[0]]
        return chamber


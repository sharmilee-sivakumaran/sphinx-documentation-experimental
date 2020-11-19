"""
hi.bills
:class HIBillScraper: scrapes Hawaii Bills
"""
from __future__ import absolute_import
import re
from fn_scrapers.datatypes.bills.common import (
    BillScraper, Bill, Vote, BillReportingPolicy as BRP, Doc_service_document)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id)
from fn_scrapers.datatypes.bills.common.metadata import get_session_name
import logging
import string
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags

logger = logging.getLogger('HIBillScraper')

BASE_URL = 'http://www.capitol.hawaii.gov'

ORDINALS = ["first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eight", "ninth", "tenth"]


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-HI", group="fnleg")
class HIBillScraper(BillScraper):
    """
    HIBillScraper
    Scrape HI Bills
    """
    def __init__(self):
        super(HIBillScraper, self).__init__('hi')

    @staticmethod
    def convert_name_to_id(session_name):
        """
        Convert session_name (2011 First Special Session) to session id (2011a)
        """
        split_name = session_name.split()
        year = int(split_name[0])
        ordinal = split_name[1]
        session_num = ORDINALS.index(ordinal.lower())
        session_char = string.ascii_letters[session_num]
        return "{}{}".format(year, session_char)

    def scrape_bill_ids(self, session):
        """
        Scrape Hawaii bill ids.

        For regular sessions, we can scrape a document list for each year within the two year session
        Example urls: https://www.capitol.hawaii.gov/session2017/bills/
                      https://www.capitol.hawaii.gov/session2018/bills/

        For special sessions, we need to find the correct session id and scrape
        the bill ids from the special session page.
        Example url: https://www.capitol.hawaii.gov/splsession.aspx?year=2017a

        :param session: Standardized session id (e.g. 20172018r)
        :return: A dictionary mapping bill ids to their year within the two year session.
        """
        bill_id_to_year = {}
        if "ss" in session:
            # The session name contains the year in which the special session occurred, which we need
            # in order to generate the id. This is because the count for special sessions starts over
            # each year. In our system, we keep a count of special sessions within a two year container.
            # Thus, 20172018ss2 could be the second special session in 2017, or the first in 2018, if there
            # was one in 2017.
            session_name = get_session_name(self.scraper.metadata_client, self.locality, session)
            session_id = self.convert_name_to_id(session_name)
            logger.info("Session %s with name '%s' mapped to session id %s",
                        session, session_name, session_id)
            year = session_id[:4]
            special_session_url = BASE_URL + "/splsession.aspx?year=" + session_id
            spec_sess_page = self.scraper.url_to_lxml(special_session_url, BRP.bill_list)
            bill_list = spec_sess_page.xpath(
                "//a[contains(@href, 'measure_indivSS')]"
            )
            for bill_link in bill_list:
                bill_str = bill_link.text_content()
                bill_id = bill_str.replace("_", "")
                # DCs are Department Communications. MC are Miscellaneous Communications.
                # We skip both of those.
                if not bill_id.lower().startswith("dc") and not bill_id.lower().startswith("mc"):
                    bill_id_to_year[bill_id] = {'session': year}


        else:
            # For regular sessions, we simply scrape all bills from both years within the two year session.
            for year in (session[:4], session[4:8]):
                url_former = '%s/session%s/bills/' % (BASE_URL, year)
                bill_list_page = self.scraper.url_to_lxml(url_former, BRP.bill_list)

                for link in bill_list_page.xpath(
                        '//a[contains(@href, ".HTM") or contains(@href, ".htm")]/text()',
                        BRP.debug):
                    link = link[:link.find("_")]
                    # DCs are Department Communications. We skip those.
                    if not link.lower().startswith("dc"):
                        bill_id_to_year[link] = {'session': year}

        return bill_id_to_year

    def scrape_bill(self, session, bill_id, **kwargs):
        # GMs are Governor messages. We treat these as upper bills of type resolution
        # JCs are Judiciary Confirmations. We also treat those as upper bills of type resolution
        if bill_id.lower().startswith("g") or bill_id.lower().startswith("j"):
            chamber = "upper"
            bill_type = "resolution"
        else:
            chamber = get_chamber_from_ahs_type_bill_id(bill_id)
            bill_type = get_bill_type_from_normal_bill_id(bill_id)
        session_id = kwargs.get("bill_info")['session']
        bill_abbrev, bill_number = bill_id.split(" ")
        bill_url = 'http://www.capitol.hawaii.gov/Archives/measure_indiv_Archives.aspx?billtype=%s&billnumber=%s&year=%s' % (
            bill_abbrev, bill_number, session_id)
        # check if the URL is good otherwise raise a NoBillDataForPeriod
        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        assert bill_page, "No Data for Bill %s" % bill_id

        # We need to construct the xpath statement this way because of the apostrophe in the text
        not_exist_xpath = '//font[contains(text(), "{}")]'.format("Measure Doesn't Exist")
        # Some bills (especially GMs) exist in the document list, but not on the site.
        if bill_page.xpath_single(not_exist_xpath, BRP.debug):
            logger.warning("%s does not have a bill page.", bill_id)
            return

        bill = self.create_bill(bill_page, bill_id, session, chamber, bill_type)

        self.scrape_sponsors(bill, bill_page)
        self.scrape_actions(bill, bill_page)
        self.scrape_docs(bill, bill_page)

        self.save_bill(bill)

    @classmethod
    def find_row(cls, page, header):
        '''Returns the text of the second column after finding the first.'''
        return page.xpath_single(
            "//tr[starts-with(td[1],'{}')]/td[2]".format(header)).text_content(BRP.debug)

    @classmethod
    def create_bill(cls, bill_page, bill_id, session, chamber, bill_type):
        '''Returns a bill object with basic metadata.'''
        # Get title
        title = cls.find_row(bill_page, 'Measure Title:')
        description = cls.find_row(bill_page, 'Description:')

        if title.upper() == title:
            title = title.title()

        bill = Bill(session, chamber, bill_id, title, bill_type)
        if description:
            bill.add_summary(description)
        bill.add_source(bill_page.url)
        return bill

    @classmethod
    def scrape_sponsors(cls, bill, bill_page):
        '''Add sponsors to bill.'''
        sponsors = cls.find_row(bill_page, 'Introducer(s):')
        if sponsors:
            for sponsor in (s.strip() for s in sponsors.split(",")):
                if sponsor.upper() == sponsor:
                    bill.add_sponsor("primary", sponsor.title())
                else:
                    bill.add_sponsor('cosponsor', sponsor)

    @classmethod
    def scrape_actions(cls, bill, bill_page):
        '''Scrape bill actions. '''
        for actions in bill_page.xpath("//table[contains(@id,'GridViewStatus')]/tr")[1:]:
            date, actor, action = actions.xpath("td")
            actor = "lower" if "H" in actor.text_content() else "upper"
            date = parse(date.text_content())
            attrs = dict(action=action.text_content(),
                         date=date,
                         actor=actor
                        )
            bill.add_action(**attrs)
            action = action.text_content().strip()
            if "Aye" in action and "No" in action and "Excuse" in action:
                action = re.sub("and", "", action)
                action = re.sub(":", "", action)
                action = re.sub(";", "", action)
                # We will match vote_count_1 most of the time. 2 and 3 are for very specialized cases that occur often
                # enough for us to capture. other_capture will match most of other cases if 1 doesn't match.
                vote_count_1 = re.findall("(\d+)? Aye\S+([\s\S]*?)(\d+)? Aye\S+ with reservations([\s\S]*?)(\d+)? "
                                          "No\S+([\s\S]*?)(\d+)? Excused([\s\S]+)\.", action)
                vote_count_2 = re.findall("(\d+) Ayes; (\d+) Noes ([\s\S]+?) (\d+) Excused ([\s\S]+)\.", action)
                vote_count_3 = re.findall("Ayes, (\d+); Aye\(s\) with reservations: ([\s\S]+?)\. "
                                          "Noes, (\d+) ([\s\S]+?) Excused, (\d+) ([\s\S]+)\.", action)
                other_capture = re.findall("(\d+)? Aye[\s\S]+?(\d+)? No[\s\S]+?(\d+)? Excused", action)
                if vote_count_1 or vote_count_2 or vote_count_3 or other_capture:
                    yes_votes = no_votes = excused_votes = None
                    if vote_count_3:
                        yes_count, _, no_count, no_votes, excused_count, excused_votes = vote_count_3[0]
                        no_votes = cls._get_votes(no_votes)
                        excused_votes = cls._get_votes(excused_votes)
                    elif vote_count_2:
                        yes_count, no_count, no_votes, excused_count, excused_votes = vote_count_2[0]
                        no_votes = cls._get_votes(no_votes)
                        excused_votes = cls._get_votes(excused_votes)
                    elif vote_count_1:
                        yes_count, yes_votes, _, _, no_count, no_votes, excused_count, excused_votes = vote_count_1[0]
                        yes_votes = cls._get_votes(yes_votes)
                        no_votes = cls._get_votes(no_votes)
                        excused_votes = cls._get_votes(excused_votes)
                    else:
                        yes_count, no_count, excused_count = other_capture[0]

                    yes_count = int(yes_count) if yes_count else 0
                    no_count = int(no_count) if no_count else 0
                    excused_count = int(excused_count) if excused_count else 0

                    if yes_count == no_count == excused_count == 0:
                        continue

                    vote = Vote(actor, date, action, yes_count > no_count, yes_count, no_count, excused_count)
                    if yes_votes and len(yes_votes) <= yes_count:
                        vote['yes_votes'] = yes_votes
                    if no_votes and len(no_votes) <= no_count:
                        vote['no_votes'] = no_votes
                    if excused_votes and len(excused_votes) <= excused_count:
                        vote['other_votes'] = excused_votes
                    bill.add_vote(vote)

    @staticmethod
    def _get_votes(vote_string):
        # Empty string and spaces only check
        if vote_string and vote_string.strip():
            vote_string = re.sub(r"\(s\)", "", vote_string)
            vote_string = re.sub(r"Senator", "", vote_string)
            return vote_string.split(",")
        else:
            return None

    def scrape_docs(self, bill, bill_page):
        '''Scrape versions, committee reports, testimonies, and transcripts. All
        require doc service integration therefore excepted from unit tests at
        this time. '''
        version_lists = bill_page.xpath("//a[contains(@id,'VersionsLink')]",
                                        policy=BRP.debug)
        for version in version_lists:
            download_id, _, doc_ids = self.scraper.register_download_and_documents(
                version.get_attrib("href"), BRP.bill_versions,
                self.scraper.extraction_type.html, True, should_download=True)
            doc_service_document = Doc_service_document(
                version.text_content(), "version", "complete", download_id,
                doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        committee_report = bill_page.xpath("//a[contains(@id, 'CategoryLink')]",
                                           policy=BRP.debug)
        for report in committee_report:
            download_id = self.scraper.download_and_register(
                report.xpath_single("@href"), BRP.bill_documents, False)
            doc_service_document = Doc_service_document(
                "Committee Report - " + report.text_content(), "committee_document",
                "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        testimonies = bill_page.xpath("//a[contains(text(),'TESTIMONY')]",
                                      policy=BRP.debug)
        for testimony in testimonies:
            download_id = self.scraper.download_and_register(
                testimony.xpath_single("@href"), BRP.bill_documents, False)
            doc_service_document = Doc_service_document(
                "Testimony - " + testimony.text_content(), "committee_document",
                "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        transcripts = bill_page.xpath("//a[contains(@id,'hearingNoticeLink')]",
                                      policy=BRP.debug)
        for transcript in transcripts:
            download_id = self.scraper.download_and_register(
                transcript.xpath_single("@href"), BRP.bill_documents, False)
            name = re.findall("((?:HEARING|CONF).*)_.HTM",
                              transcript.xpath_single("@href"))[0]
            doc_service_document = Doc_service_document(
                "Hearing Notice - " + name, "committee_document", "partial",
                download_id)
            bill.add_doc_service_document(doc_service_document)

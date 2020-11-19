"""
:class TNBillScraper: scrapes Tennessee Bills
"""
from __future__ import absolute_import

import datetime
import re

import logging
from dateutil.parser import parse

from fn_scraperutils.doc_service.util import ScraperDocument

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id

from fn_scrapers.api.scraper import scraper, tags

logger = logging.getLogger('TNBillScraper')

TN_BASE_URL = 'http://wapp.capitol.tn.gov/apps'  # pylint:disable=invalid-name
TN_SP_URL = 'http://wapp.capitol.tn.gov/apps/indexes/SPSession1.aspx'  # pylint:disable=invalid-name


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-TN", group="fnleg")
class TNBillScraper(BillScraper):
    """
    TNBillScraper
    Scrape Tennessee Bills
    """

    def __init__(self):
        super(TNBillScraper, self).__init__("tn")

    def scrape_bill_ids(self, session):
        """
        :param session: session to scrape
        :type session: string
        :rtype: list of bills
        """
        if 'ss' in session:
            return self.scrape_special(session)

        bill_ids = set()
        # Check if the session is latest or not
        archives_url = "http://www.capitol.tn.gov/legislation/archives.html"
        archives_page = self.scraper.url_to_lxml(archives_url, BRP.test)
        latest_archives_id = archives_page.xpath("//li[contains(text(), 'General Assembly')]")[0].text_content()
        latest_session_id = int(re.findall('\d+', latest_archives_id)[0])
        session_id = self.get_session_id(session)

        if latest_session_id < session_id:
            list_url = "http://wapp.capitol.tn.gov/apps/indexes/"
        else:
            list_url = "http://wapp.capitol.tn.gov/apps/archives/default.aspx?year=%s" % session_id
        main_list_page = self.scraper.url_to_lxml(list_url, BRP.bill_list)
        bill_list_sub_links = main_list_page.xpath("//a[contains(@href, 'BillIndex.aspx?StartNum=')]/@href")

        for sub_link in bill_list_sub_links:
            sub_list_page = self.scraper.url_to_lxml(sub_link, BRP.test)
            bill_list_links = sub_list_page.xpath("//a[contains(@href, 'BillNumber')]")

            for bill_link in bill_list_links:
                bill_id = bill_link.text_content()
                bill_ids.add(bill_id)
        return list(bill_ids)

    def scrape_special(self, session):
        """
        Scrape special session
        """
        main_list_page = self.scraper.url_to_lxml(TN_SP_URL, BRP.bill_list)
        sp_num = re.findall(r'ss(\d+)', session)[0]
        sp_num = int(sp_num)
        session_list = main_list_page.xpath("//h4")
        if len(session_list) < sp_num:
            logger.error("No Special Seesion %s are found" % session)
            return []

        session_header = session_list[sp_num - 1]
        session_name = session_header.text_content()
        session_name = re.sub(r'\(.*\)', '', session_name).strip()

        bill_ids = set()
        session_bill_list = session_header.xpath('./following-sibling::table[1]//a', BRP.test)
        for bill_type in session_bill_list:
            type_list_url = bill_type.get_attrib('href')
            if '/indexes/' not in type_list_url:
                bill_listing = TN_BASE_URL + '/indexes/' + type_list_url
            else:
                bill_listing = type_list_url

            bill_list_page = self.scraper.url_to_lxml(bill_listing, BRP.bill_list)

            for bill_links in bill_list_page.xpath("//a[contains(@href, 'BillNumber')]"):
                bill_id = bill_links.text_content()
                bill_ids.add(bill_id)

        return list(bill_ids)

    def scrape_bill(self, session, bill_id, **kwargs):
        """
        :param session: session of bill
        :type session: string
        :param bill_id: Bill id to scrape
        :type bill_id: string
        """
        bill_char, bill_num = re.findall(r"([A-Za-z]+)\s+(\d+)", bill_id)[0]
        url_bill_id = "%s%04d" % (bill_char, int(bill_num))

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        session_id = self.get_session_id(session)
        bill_url = TN_BASE_URL + '/BillInfo/default.aspx?BillNumber=%s&ga=%s' % (url_bill_id, session_id)
        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)

        bill_id = self.clean_bill_id(bill_id)

        title = bill_page.xpath("//span[@id='lblAbstract']")[0].text_content()

        if not title:
            title = "A title is not available at this time"
            logger.warning("No title available for bill {} on page {}. Falling back to a default value".
                           format(bill_id, bill_url))

        bill = Bill(session, chamber, bill_id, title, bill_type)
        # checking if there is a companion bill
        secondary_bill_id = bill_page.xpath_single('//span[@id="lblCompNumber"]/a', BRP.test)
        if secondary_bill_id:
            secondary_bill_id = secondary_bill_id.text
            secondary_bill_id = self.clean_bill_id(secondary_bill_id)

            secondary_chamber = 'upper' if secondary_bill_id[0] == 'S' else 'lower'
            bill.add_companion(secondary_bill_id, bill_type='companion')
            bill.add_alternate_id(secondary_bill_id)

        bill.add_source(bill_url)
        # Primary Sponsor
        sponsor = bill_page.xpath("//span[@id='lblBillPrimeSponsor']")[0].text_content().split("by ")[-1]
        sponsor = sponsor.replace('*', '').strip()
        if sponsor:
            bill.add_sponsor('primary', sponsor)

        # Version
        # TN is unlike other states in that it only posts one version of its bills, the introduced version.
        # The enacted versions are published on another site and linked as public chapter
        version_url = bill_page.xpath("//div[@id='udpBillInfo']//a")[1].get_attrib('href')
        download_id, _, doc_ids = \
            self.scraper.register_download_and_documents(version_url, BRP.bill_versions,
                                                         self.scraper.extraction_type.text_pdf,
                                                         True, should_download=True,
                                                         should_skip_checks=True)
        if len(doc_ids) > 0 and doc_ids[0] is not None:
            doc_service_document = Doc_service_document("Introduced", "version", "complete",
                                                        download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        # Public Chapter
        public_chapter = bill_page.xpath_single("//div[@class='public-chapter']")
        # Companion bills mean that this public chapter is not associated with this bill
        if public_chapter and "Companion bill" not in public_chapter.text:
            public_chapter_link = public_chapter.xpath_single("a/@href")
            if public_chapter_link:
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(public_chapter_link, BRP.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, should_download=True,
                                                                 should_skip_checks=True)

                if len(doc_ids) > 0 and doc_ids[0] is not None:
                    doc_service_document = Doc_service_document("Current Version", "version", "complete",
                                                                download_id, doc_id=doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)

        # Summary
        summary = bill_page.xpath_single('//h3[contains(text(), "Bill Summary")]')
        if summary:
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(bill_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html,
                                                             False, self.summary_parser)

            doc_service_document = Doc_service_document("Bill Summary", "summary", "complete",
                                                        download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        # Fiscal Note
        fiscal = bill_page.xpath_single('//span[@id="lblFiscalNoteLink"]//a', BRP.test)
        if fiscal:
            fiscal_url = fiscal.get_attrib('href')
            download_id = self.scraper.download_and_register(fiscal_url, BRP.bill_documents, False)
            doc_service_document = Doc_service_document("Fiscal Note", "fiscal_note", "partial",
                                                        download_id)
            bill.add_doc_service_document(doc_service_document)
        # Amendment
        amendments = bill_page.xpath('//a[contains(@href, "/Amend/")]', BRP.test)
        for amendment in amendments:
            amend_url = amendment.get_attrib('href')
            amend_name = amendment.xpath_single("./ancestor::td/following-sibling::td").text_content()
            if url_bill_id not in amend_name:
                continue
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(amend_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True, should_skip_checks=True)
            if len(doc_ids) == 1 and doc_ids[0] is not None:
                doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                            download_id, doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

            fiscal_atr = amendment.xpath_single("./ancestor::tr/td/a[contains(@href, '/Fiscal/')]/@href")
            # Check if fiscalNote Document existes
            if not fiscal_atr:
                continue
            download_id = self.scraper.download_and_register(fiscal_atr, BRP.bill_documents, False)
            doc_service_document = Doc_service_document("Fiscal Memo - %s" % amend_name, "fiscal_note", "partial",
                                                        download_id)
            bill.add_doc_service_document(doc_service_document)
        # action
        for action in self._parse_bill_actions(bill_page, chamber):
            bill.add_action(**action)  # pylint: disable=star-args
        if bill_page.xpath("//a[@id='lnkShowCoPrimes']", BRP.debug):
            post_data = self._get_post_data(bill_page, 'lnkShowCoPrimes', 'udpBillInfo|lnkShowCoPrimes')
            co_page = self.scraper.url_to_lxml(bill_url, BRP.bill, 'POST', request_args={"data": post_data})
            cosponsor_page = co_page.xpath('//span[@id="lblBillCoPrimeSponsor"]/text()', policy=BRP.debug)
            if cosponsor_page:
                cosponsor_list = cosponsor_page[0]
                cosponsors = cosponsor_list.split(',')
                for cosponsor in cosponsors:
                    cosponsor = cosponsor.strip()
                    if cosponsor == '':
                        continue
                    bill.add_sponsor('cosponsor', cosponsor)

        for vote in self._parse_votes_from_page(bill_page, url_bill_id):
            bill.add_vote(vote)

        self.save_bill(bill)
        print bill

    def _parse_bill_actions(self, bill_page, chamber):
        """
        Parse bill actions from bill page
        :Param bill_page, bill page in htmlelement type
        """
        atable = bill_page.xpath_single("//table[@id='gvBillActionHistory']")
        for action in self._parse_action_table(atable, chamber):
            yield action

    def _parse_action_table(self, atable, bill_chamber):
        """
        parse action information from action table
        :Param atable action table in htmlelement type
        """
        action_rows = atable.xpath("tr", policy=BRP.info)
        # first row will say "Actions Taken on S|H(B|R|CR)..."
        if 'Actions Taken on S' in action_rows[0].text_content():
            chamber = 'upper'
        elif 'Actions Taken on H' in action_rows[0].text_content():
            chamber = 'lower'
        else:
            chamber = 'other'
        last_date = None
        for ar in action_rows[1:]:
            action_chamber = ar.get_attrib('class')
            if not action_chamber:
                action_chamber = chamber
            if action_chamber == 'house':
                action_chamber = 'lower'
            elif action_chamber == 'senate':
                action_chamber = 'upper'
            tds = ar.xpath('td')
            action_taken = tds[0].text
            date_string = tds[1].text.strip()
            action_date = None
            if date_string.strip() != '':
                action_date = parse(date_string)
                last_date = action_date
            else:
                action_date = last_date
            action = dict(action=action_taken, date=action_date, actor=action_chamber)
            yield action

    def _parse_votes_from_page(self, bill_page, bill_id):
        """
        parse votes from page
        param bill_page: bill page in htmlelement type
        param bill_id: bill_id of bill which votes belong to
        """
        if bill_id[0] == 'H':
            vote_table = bill_page.xpath("//span[@id='lblHouseVoteData']")
            vote_chamber = 'lower'
        elif bill_id[0] == 'S':
            vote_table = bill_page.xpath("//span[@id='lblSenateVoteData']")
            vote_chamber = 'upper'

        if "are not available." not in vote_table[0].text_content():
            moved_match = re.match(r"moved\sto\ssubstitute\sand\sconform\sto", \
                                   vote_table[0].text_content(), re.I)
            votes_page = vote_table[0].text_content()
            if moved_match:
                return

            votes_page = votes_page.replace(u'\xa0', '  ')
            if moved_match != "":
                votes_content = re.split(bill_id, votes_page)
                for vote_str in votes_content:
                    vote_str = vote_str.strip()
                    if vote_str != "":
                        vote = self._parse_vote(vote_str, vote_chamber)
                        if vote is not None:
                            yield vote

    def _parse_vote(self, votes_content, chamber):
        """
        parse a single vote from a string
        param: votes_content, a string which include all vote information
        param chamber: chamber of vote
        """
        votes_part = re.split(r'\s{2,}', votes_content)

        try:
            motion = re.findall(r'FLOOR VOTE: (.*?)\s+\d+/', votes_content)[0]

            date = votes_part[1]
            if motion == '':
                motion = 'FLOOR VOTE: %s' % date
            date = datetime.datetime.strptime(date, '%m/%d/%Y')
            passed = votes_part[2] == "Passed"
            yes_count = re.split(r'\.+', votes_part[3])[1]
            yes_count = int(yes_count)
            other_count = 0

            no_count = re.split(r'\.+', votes_part[4])[1]
            no_count = int(no_count)
            if yes_count == 0 and no_count == 0 and other_count == 0:
                return None
            if len(votes_part) == 5:
                vote = Vote(chamber, date, motion, passed, yes_count, no_count, other_count)
                return vote
            other_match = re.match(r'Present and not voting\.+(\d+)', votes_part[5], re.I)

            index = 5
            if other_match:
                other_count = int(other_match.groups()[0])
                index = 6

            vote = Vote(chamber, date, motion, passed, yes_count, no_count, other_count)
            if index == len(votes_part):
                return vote
            line = votes_part[index].split(":")
            if 'voting aye were' in line[0]:
                votes_dict = line[1].split("--")
                for name in votes_dict[0].split(','):
                    vote.yes(name)
                if len(vote['yes_votes']) != yes_count:
                    self.warning("Mismatched yes count [expect: %i] [have: %i]", yes_count, len(vote['yes_votes']))
                index += 1
                if index == len(votes_part):
                    return vote
                line = votes_part[index].split(":")
            if 'voting no were' in line[0] or 'voting noes were' in line[0]:
                votes_dict = line[1].split("--")
                for name in votes_dict[0].split(','):
                    vote.no(name)
                if len(vote['no_votes']) != no_count:
                    self.warning("Mismatched no count [expect: %i] [have: %i]", no_count, len(vote['no_votes']))
                index += 1
                if index == len(votes_part):
                    return vote
                line = votes_part[index].split(":")

            if 'present and not voting' in line[0]:
                votes_dict = line[1].split("--")
                for name in votes_dict[0].split(','):
                    vote.other(name)
                if len(vote['other_votes']) != other_count:
                    self.warning("Mismatched no count [expect: %i] [have: %i]", other_count, len(vote['other_votes']))
        except:  # pylint: disable=bare-except
            return None
        return vote

    @staticmethod
    def summary_parser(element_wrapper):
        text = element_wrapper.xpath_single("//span[@id='lblSummary']").text_content()
        return [ScraperDocument(text)]

    @staticmethod
    def html_parser(element_wrapper):
        text = element_wrapper.xpath_single("//body").text_content()
        return [ScraperDocument(text)]

    @staticmethod
    def get_session_id(session):
        year = int(session[:4])
        session_id = (year - 2015) / 2 + 109
        return session_id

    @staticmethod
    def _get_post_data(doc, post_target, script_manager):
        default_vals = dict(ScriptManager1=script_manager, __ASYNCPOST=True, \
                            __EVENTARGUMENT=None, __EVENTTARGET=post_target)

        post_vars = {element.get_attrib('name'): element.get_attrib('value') or \
                                                 element.text for element in doc.xpath('//*[@name]', policy=BRP.debug)}
        post_vars.update(default_vals)
        return post_vars

    @staticmethod
    def clean_bill_id(bill_id):
        bill_id = bill_id.replace('*', '').replace(' ', '').strip()
        prefix = re.findall(r'[A-Za-z]+', bill_id)[0]
        bill_no = re.findall(r'\d+', bill_id)[0]
        return "%s %s" % (prefix, int(bill_no))

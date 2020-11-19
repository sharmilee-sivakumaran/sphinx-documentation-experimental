# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import re
import dateutil.parser
import datetime
from fn_scraperutils.events.reporting import ScrapeError
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_document_service.blocking.ttypes import ColumnSpec
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as Brp
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(__name__)

STATUS_URL = u"http://www.scstatehouse.gov/statusact.php"
PREFILE_URL = u"http://www.scstatehouse.gov/sessphp/prefil{}.php"


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-SC", group="fnleg")
class SCBillScraper(BillScraper):
    def __init__(self):
        super(SCBillScraper, self).__init__("sc", retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

    def scrape_bill_ids(self, session):
        bill_ids = set([])
        session_code = self._get_session_code(session)
        # SC has no separate special sessions, so no need to check session type
        session_start = datetime.date(int(session[:4]), 1, 1)
        end_date = datetime.date.today()

        status_post_data = {u"session": session_code,
                            u"usedate2": 0,
                            u"summary": u"T",
                            u"headerfooter": 1}
        for chamber_code in [u"H", u"S"]:
            status_post_data[u"reporttype2"] = chamber_code
            current_date = session_start
            # We query for all the bills by activity in two week chunks.
            # Otherwise, the search page crashes if it tries to load
            # too many bills.
            while current_date <= end_date:
                two_weeks = current_date + datetime.timedelta(weeks=2)
                status_post_data[u"begin_date"] = current_date.strftime(u"%m/%d/%Y")
                status_post_data[u"end_date"] = two_weeks.strftime(u"%m/%d/%Y")
                bill_list_page = self.scraper.url_to_lxml(
                    STATUS_URL,
                    policy=Brp.bill_list,
                    method=u"POST",
                    request_args={u"data": status_post_data})
                # There may be no bills returned for a query,
                # if there was no activity over those weeks
                bill_nums = bill_list_page.xpath(
                    u"//div[@id='resultsbox']/div/a/@name",
                    Brp.test)
                new_ids = set([u"{} {}".format(chamber_code, int(bill_num))
                              for bill_num in bill_nums if bill_num.isdigit()])
                bill_ids |= new_ids
                current_date = two_weeks

        prefiles = self.scrape_prefiles(session)
        bill_ids |= prefiles
        return list(bill_ids)

    def scrape_prefiles(self, session):
        prefile_ids = set([])
        # Prefiles weirdly do have urls with the session within them,
        # so no POST magic required. We just insert the last two digits
        # of the start year of the session into the url.
        url = PREFILE_URL.format(session[2:4])
        prefile_page = self.scraper.url_to_lxml(url, Brp.bill_list)
        date_links = prefile_page.xpath(u"//dd/a/@href")
        for date_link in date_links:
            list_page = self.scraper.url_to_lxml(date_link, Brp.bill_list)
            bill_list = list_page.xpath(u"//a[contains(@href, '/billsearch.php')]", Brp.test)
            for bill in bill_list:
                bill_id = bill.text_content()
                bill_id = re.sub(r'\.', '', bill_id)
                prefile_ids.add(bill_id)

        return prefile_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        generic_bill_url = u"http://www.scstatehouse.gov/billsearch.php?billnumbers={num}&session={sess}&summary=B"

        sess_code = self._get_session_code(session)
        bill_num = bill_id.split()[-1]
        bill_chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_url = generic_bill_url.format(num=bill_num, sess=sess_code)

        bill_page = self.scraper.url_to_lxml(bill_url, Brp.bill)
        bill_div = bill_page.xpath_single(u'//div[@style="margin:0 0 40px 0;"]')

        summary_header = bill_div.xpath_single(u'b[text()="Summary:"]', Brp.bill_title)
        summary = summary_header.xpath_single(u"following-sibling::text()[1]").strip()
        title = summary_header.xpath_single(u"following-sibling::text()[2]").strip()

        bill_title_span = bill_div.xpath_single(u'span')

        bill_type_txt = bill_title_span.text
        if u'General Bill' in bill_type_txt:
            bill_type = u'bill'
        elif u'Concurrent Resolution' in bill_type_txt:
            bill_type = u'concurrent_resolution'
        elif u'Joint Resolution' in bill_type_txt:
            bill_type = u'joint_resolution'
        elif u'Resolution' in bill_type_txt:
            bill_type = u'resolution'
        else:
            raise ScrapeError(u"%s: Unknown Bill type: %s", bill_id, bill_type_txt)

        # We swap the title and summary. What they call the summary is short and well formatted, while their titles
        # are long and all caps.
        bill = Bill(session, bill_chamber, bill_id, summary, bill_type)
        bill.add_source(bill_url)
        # The "title" is always in all caps, so we convert it to title caps
        bill.add_summary(title.title())

        sponsor_links = bill_title_span.xpath(u"a")

        # For SC, the first sponsor always is the primary, and the rest are cosponsors.
        spon_type = u"primary"
        for spon_link in sponsor_links:
            spon_name = spon_link.text
            spon_url = spon_link.get_attrib(u"href")
            spon_chamber_code = re.search(ur"chamber=(H|S)", spon_url).group(1)
            spon_chamber = u"lower" if spon_chamber_code == u"H" else u"upper"
            bill.add_sponsor(spon_type, spon_name, chamber=spon_chamber)
            spon_type = u"cosponsor"

        similar_bills = bill_div.xpath(u"//a[contains(@href, 'billnumbers=')]/text()")
        for similar_bill in similar_bills:
            if re.match(ur"(H|S) \d+", similar_bill):
                bill.add_companion(similar_bill, bill_type=u"companion")

        action_table = bill_div.xpath(u'table/tr')
        for row in action_table:
            date_td, chamber_td, action_td = row.xpath(u'td')
            date = dateutil.parser.parse(date_td.text)
            action_chamber = {u'Senate': u'upper',
                              u'House': u'lower',
                              None: u'other'}[chamber_td.text]

            action = action_td.text_content()
            bill.add_action(action_chamber, action, date)

        versions_url = bill_div.xpath_single(u'//a[text()="View full text"]/@href')
        versions = self.scrape_versions(versions_url)
        for version in versions:
            bill.add_doc_service_document(version)

        fiscal_note_url = bill_div.xpath_single(u'//a[text()="View Fiscal Impact"]/@href', Brp.test)
        if fiscal_note_url:
            fiscal_notes_page = self.scraper.url_to_lxml(fiscal_note_url, Brp.bill_partial_documents)
            for fn_link in fiscal_notes_page.xpath(u'//a[contains(@href, "/fiscalimpactstatements/")]'):
                fn_name = u"Fiscal Note: {}".format(fn_link.text)
                fn_url = fn_link.get_attrib(u"href")
                download_id = self.scraper.download_and_register(fn_url, Brp.bill_partial_documents, True)
                if download_id:
                    fiscal_note = Doc_service_document(fn_name, u"fiscal_note", u"partial", download_id)
                    bill.add_doc_service_document(fiscal_note)

        vote_history_url = bill_div.xpath_single(u'//a[text()="View Vote History"]/@href', Brp.test)
        if vote_history_url:
            votes, amendments = self.scrape_votes_and_amendments(vote_history_url)
            for vote in votes:
                bill.add_vote(vote)
            for amend in amendments:
                bill.add_doc_service_document(amend)

        self.save_bill(bill)

    def scrape_versions(self, versions_url):
        versions = []

        versions_page = self.scraper.url_to_lxml(versions_url, Brp.bill_documents)
        version_links = versions_page.xpath(u"//a[contains(@href, '/prever/')]")
        for version_link in version_links:
            version_name = version_link.text
            version_url = version_link.get_attrib(u"href")
            download_id, _, doc_ids = self.scraper.register_download_and_documents(version_url,
                                                                                   Brp.bill_documents,
                                                                                   self.scraper.extraction_type.html,
                                                                                   False,
                                                                                   parse_function=self._parse_html)
            if download_id and doc_ids:
                version = Doc_service_document(version_name, u"version", u"complete", download_id, doc_ids[0])
                versions.append(version)

        return versions

    def scrape_votes_and_amendments(self, vote_history_url):
        amendments = []
        votes = []
        vote_history_page = self.scraper.url_to_lxml(vote_history_url, Brp.bill_votes)
        vote_table = vote_history_page.xpath(u"//table/tr")

        vote_headers = vote_table[0].xpath(u"//th/a/span/text()")

        # The scraper skips Judicial election votes, as they are in a totally different format, and not useful
        if u"Candidate" in vote_headers:
            logger.info(u"Skipping judicial election")
            return votes, amendments

        vote_trs = vote_table[2:]
        for vote_tr in vote_trs:
            vote_tds = vote_tr.xpath(u"td")

            # Amendments are found as links in the motion name for the vote on that amendment
            amendment_link = vote_tds[1].xpath_single(u"a", Brp.test)
            if amendment_link:
                amendment_name = amendment_link.text
                amendment_url = amendment_link.get_attrib(u"href")
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(amendment_url,
                                                                 Brp.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True)
                # Some amendment texts are html, but there's no way to tell in advance. If the pdf parsing fails, we
                # try html.
                if not download_id and doc_ids:
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(amendment_url,
                                                                     Brp.bill_documents,
                                                                     self.scraper.extraction_type.text_html,
                                                                     True)

                if download_id and doc_ids:
                    version = Doc_service_document(amendment_name, u"amendment", u"complete", download_id, doc_ids[0])
                    amendments.append(version)

            vote_date = dateutil.parser.parse(vote_tds[0].text)
            motion = vote_tds[1].text_content()
            if motion.startswith(u"to"):
                motion = u"Motion " + motion
            vote_pdf_link = vote_tds[2].xpath_single(u"a")
            m = re.match(r"^\[([SH])]-\d+$", vote_pdf_link.text)
            if not m:
                raise Exception("Could not determine vote chamber")
            if m.group(1) == u"H":
                vote_chamber = u"lower"
            elif m.group(1) == u"S":
                vote_chamber = u"upper"
            else:
                raise Exception("Unexpected match")
            vote_url = vote_pdf_link.get_attrib(u"href")
            yes_count = int(vote_tds[3].text)
            no_count = int(vote_tds[4].text)
            other_count = sum(int(col.text) for col in vote_tds[5:9])
            passed = True if vote_tds[10].text is u"Passed" else False
            vote = Vote(vote_chamber, vote_date, motion, passed, yes_count, no_count, other_count)
            yes_votes, no_votes, other_votes = self.scrape_vote_pdf(vote_url)

            # If the yes count or no count doesn't line up, it may be due to a scraper issue. The other counts don't
            # always line up, though, because not all other votes are written out.
            if yes_count != len(yes_votes):
                logger.warning(u"Incorrect vote parsing for pdf at %s. Expected %s but got %s yes votes.",
                               vote_url, yes_count, len(yes_votes))
            if no_count != len(no_votes):
                logger.warning(u"Incorrect vote parsing for pdf at %s. Expected %s but got %s no votes.",
                               vote_url, no_count, len(no_votes))

            for yes in yes_votes:
                vote.yes(yes)
            for no in no_votes:
                vote.no(no)
            for other in other_votes:
                vote.other(other)
            votes.append(vote)

        return votes, amendments

    def scrape_vote_pdf(self, vote_url):
        yes_votes = []
        no_votes = []
        other_votes = []
        _, docs, _ = self.scraper.register_download_and_documents(vote_url,
                                                                  Brp.bill_votes,
                                                                  self.scraper.extraction_type.text_pdf,
                                                                  True,
                                                                  column_spec=ColumnSpec.NONE)
        full_text = docs[0].text
        pageless_text = re.sub(ur"Page\s+\d+\s+of\s+\d+", u"", full_text)
        vote_sections = re.split(ur"([A-Z\s]+)\s+-\s+\d+", pageless_text)
        for vote_type, vote_names in zip(vote_sections[1::2], vote_sections[2::2]):
            # This regex finds all the parts of this vote section that are last name followed by a comma. Some names
            # have multiple commas, if somebody is a Jr., and the III, or a Ed.D. for example. This regex splits on only
            # last names, and then we pair each last name with the element after it to get the full name.
            last_name_split = re.split(ur"([A-Za-z\-]+, )(?![IV]+(?!\.|[a-z])|Jr\.|Sr.|[A-Z][a-z]+\.[A-Z][a-z]*\.)",
                                       vote_names)
            voters = []
            for last, first in zip(last_name_split[1::2], last_name_split[2::2]):
                if u"," in first:
                    # If there are multiple commas in a name, the first section is the first name, but the last section
                    # is a suffix like a Jr. or Sr. or III. It goes at the end of the name
                    name_components = first.split(u",")
                    first = name_components[0]
                    last += u" " + u" ".join(n.strip() for n in name_components[1:])
                voters.append(first.strip() + " " + last.strip())
            if u"YEA" in vote_type or u"AYE" in vote_type:
                yes_votes = voters
            elif u"NAY" in vote_type:
                no_votes = voters
            else:
                other_votes += voters

        return yes_votes, no_votes, other_votes


    @staticmethod
    def _parse_html(element_wrapper):
        full_text = element_wrapper.xpath_single(u"body").text_content()
        text_lines = full_text.splitlines()
        start = text_lines.index(next(line for line in text_lines if u"(" in line and u")" in line))
        end = text_lines.index(u"----XX----")
        version_text = "\n".join(text_lines[start+1:end])
        return [ScraperDocument(version_text)]

    @staticmethod
    def _get_session_code(session):
        # The first session on the website is the 101st from 1975 to 1976.
        first_sess_code = 101
        first_year = 1975

        session_year = int(session[:4])

        if session_year >= first_year:
            sess_delta = (session_year - first_year) / 2
            sess_code = first_sess_code + sess_delta
            return sess_code

        else:
            logger.error(u"The SC site does not contain any data prior to the 19751976r session")
            return None

from __future__ import absolute_import

import re
import logging
import datetime

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY as RETRY500

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('PABillScraper')
bill_type_map = {"B": "bill",
                 "R": "resolution"}


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-PA", group="fnleg")
class PABillScraper(BillScraper):
    def __init__(self):
        super(PABillScraper, self).__init__("pa", retry_policy=RETRY500)

    def scrape_bill_ids(self, session):
        bill_ids = []
        start_year = session[0:4]
        sIndex = self.get_special_index(session)
        for chamber in ["H", "S"]:
            bill_list_url = "http://www.legis.state.pa.us/cfdocs/legis/bi/BillIndx.cfm?sYear={}&sIndex={}&bod={}". \
                format(start_year, sIndex, chamber)
            logger.info("Scraping bill ids from {}".format(bill_list_url))
            bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
            tables = bill_list_doc.xpath("//table[@class='DataTable']")
            for table in tables:
                for anchor_tag in table.xpath("./tbody//a", BRP.bill):
                    bill_id = re.findall(r"[A-Z]+\d+", anchor_tag.get_attrib("id"))[0]
                    bill_ids.append(bill_id)
        logger.info("Finished scraping bill ids for PA bills")
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        logger.info("Scraping bill id {}".format(bill_id))
        syear = session[0:4]
        sind = self.get_special_index(session)
        body = bill_id[0]
        type = bill_id[1]
        bn = bill_id.split()[1]
        bill_page_url = "http://www.legis.state.pa.us/cfdocs/billinfo/billinfo.cfm?" \
                        "syear={}&sind={}&body={}&type={}&bn={}".format(syear, sind, body, type, bn)

        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        title = bill_page_doc.xpath_single("//div[@class='BillInfo-Section BillInfo-ShortTitle']/"
                                           "div[@class='BillInfo-Section-Data']").text_content()

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type =get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_page_url)

        # primary sponsor
        sponsors_set = set()  # there are more sponsors on history page, including this primary sponsor
        primary_sponsor_anchor_tag = bill_page_doc. \
            xpath_single("//div[@class='BillInfo-Section BillInfo-PrimeSponsor']/"
                         "div[@class='BillInfo-Section-Data']/a")

        # This assertion was initially added (with "== 1") to test if there can be more than one primary sponsor
        # However if fails on, and only on this bill where the "PRIME SPONSOR WITHDREW"
        # http://www.legis.state.pa.us/cfdocs/billinfo/billinfo.cfm?syear=2015&sind=0&body=H&type=B&bn=1198
        assert len(bill_page_doc.xpath("//div[@class='BillInfo-Section BillInfo-PrimeSponsor']/"
                                       "div[@class='BillInfo-Section-Data']/a")) <= 1
        if primary_sponsor_anchor_tag:
            primary_sponsor = self.get_sponsor_name(primary_sponsor_anchor_tag)
            sponsors_set.add(primary_sponsor)
            spon_url = primary_sponsor_anchor_tag.get_attrib('href')
            if 'senate_bio' in spon_url:
                spon_chamber = 'upper'
            elif 'house_bio' in spon_url:
                spon_chamber = 'lower'
            else:
                spon_chamber = None
            if spon_chamber:
                bill.add_sponsor("primary", primary_sponsor, chamber=spon_chamber)
            else:
                bill.add_sponsor("primary", primary_sponsor)
        else:
            logger.warning("Cannot get primary sponsor for bill {} on {}. It's likely to be because "
                           "the sponsor has withdrawn".format(bill_id, bill_page_url))

        # memo
        memo_doc = bill_page_doc.xpath_single("//div[@class='BillInfo-Section BillInfo-CosponMemo']/"
                                              "div[@class='BillInfo-Section-Data']/a")
        if memo_doc:
            memo_url = memo_doc.get_attrib("href")
            name = "Memo: " + memo_doc.text_content()
            content_type = None
            normalized_memo_url = memo_url.lower().strip()
            get_static_content = self.memo_static_content
            if normalized_memo_url.endswith("pdf"):
                content_type = "application/pdf"
                get_static_content = None
            elif normalized_memo_url.endswith("html") or normalized_memo_url.endswith("htm"):
                content_type = "text/html"
            else:
                logger.info("Failed to guess and assign a fallback mimetype for document at {}. "
                            "ScraperUtils will try to get the mimetype from the header. "
                            "But if that fails, we get a download id of None".format(memo_url))
            download_id = self.scraper.download_and_register(memo_url, BRP.bill_partial_documents, False,
                                                             content_type=content_type,
                                                             get_static_content=get_static_content)
            if download_id:
                doc_service_document = Doc_service_document(name, "other", "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        # versions
        version_table = bill_page_doc.xpath_single("//div[@class='BillInfo-Section BillInfo-PN']/"
                                                   "div[@class='BillInfo-Section-Data']/table")
        for tr in version_table.xpath("./tbody/tr"):
            cells = tr.xpath("./td")
            # fallback value just in case extraction of version name from text fails
            fallback_name = "Printer's No. " + cells[0].xpath_single("./a").text_content()
            version_html_url = cells[1].xpath("./a")[0].get_attrib("href")
            download_id, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(version_html_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html, False,
                                                             self.version_parser,
                                                             get_static_content=self.version_static_content)
            assert len(doc_ids) == 1 and len(scraper_docs) == 1
            doc_id = doc_ids[0]
            scraper_doc = scraper_docs[0]
            # we use the title section of the version as name for the version, if we cannot find it
            # we will have to use the print no, which is the "fallback_name"
            if scraper_doc.additional_data and "name" in scraper_doc.additional_data:
                name = fallback_name + ": " + scraper_doc.additional_data["name"]
                logger.info(u"The document parser function has extract {} as the document's name at {}".
                            format(name, version_html_url))
            else:
                name = fallback_name
                logger.warning(u"The document parser function failed to extract a name for document at {}. Using the "
                               u"fallback name of {}".format(version_html_url, name))
            version_doc_service_document = Doc_service_document(name, "version", "complete", download_id, doc_id=doc_id)
            # TODO: removed alternate format downloads to boost scraping speed
            # version_pdf_url = cells[1].xpath("./a")[1].get_attrib("href")
            # version_word_url = cells[1].xpath("./a")[2].get_attrib("href")
            # pdf_download_id = self.scraper.download_and_register(version_pdf_url, BRP.bill_documents, True)
            # word_download_id = self.scraper.download_and_register(version_word_url, BRP.bill_documents, True)
            # version_doc_service_document.add_alternate_representation(pdf_download_id)
            # version_doc_service_document.add_alternate_representation(word_download_id)

            # not adding this doc-service document to bill yet -- all amendments and fiscal notes within the
            # same row are the "children" of this version, per the new pillar schema, and they need to be added
            # to this version doc-service document

            # amendments
            amdt_map = {2: "House", 3: "Senate"}  # maps the index of the cell to the chamber of the amendment
            for key in amdt_map:
                if cells[key].xpath(".//a", BRP.test):
                    amdt_page_url = cells[key].xpath_single("./a").get_attrib("href")
                    amdt_page_doc = self.scraper.url_to_lxml(amdt_page_url, BRP.bill_documents)
                    amdt_divs = amdt_page_doc.xpath("//div[contains(@class, 'AmendList-Wrapper clearfix')]")
                    for amdt_div in amdt_divs:
                        amdt_number = amdt_div.xpath_single(".//div[@class='AmendList-AmendNo']").text_content()
                        amdt_proposed_by = amdt_div.xpath_single(".//div[@class='AmendList-MemberName']").text_content()
                        amdt_name = "{} - {}".format(amdt_number, amdt_proposed_by)
                        amdt_formats_div = amdt_div.xpath_single(".//div[@class='AmendList-FileTypes']")
                        amdt_html_url = amdt_formats_div.xpath("./a")[0].get_attrib("href")
                        # TODO: removed alternate format download to boost scraping speed
                        # amdt_pdf_url = amdt_formats_div.xpath("./a")[1].get_attrib("href")
                        # amdt_word_url = amdt_formats_div.xpath("./a")[2].get_attrib("href")
                        try:
                            download_id, _, doc_ids = self.scraper.register_download_and_documents(
                                amdt_html_url, BRP.bill_documents,
                                self.scraper.extraction_type.html, False,
                                self.html_parser, 
                                get_static_content=self.version_static_content)
                        except ValueError:
                            logging.warning("Invalid amendment HTML: %s", amdt_html_url)
                            continue
                        assert len(doc_ids) == 1
                        doc_id = doc_ids[0]
                        amdt_doc_service_document = Doc_service_document(amdt_name, "amendment", "complete",
                                                                         download_id, doc_id=doc_id)
                        # TODO: removed alternate format download to boost scraping speed
                        # amdt_pdf_download_id = self.scraper.download_and_register(amdt_pdf_url, BRP.bill_documents,
                        #                                                           True)
                        # amdt_word_download_id = self.scraper.download_and_register(amdt_word_url, BRP.bill_documents,
                        #                                                            True)
                        # amdt_doc_service_document.add_alternate_representation(amdt_pdf_download_id)
                        # amdt_doc_service_document.add_alternate_representation(amdt_word_download_id)
                        amdt_index = bill.add_doc_service_document(amdt_doc_service_document)
                        # add amendment to the corresponding version as a "child"
                        version_doc_service_document.add_child(amdt_index)

            # fiscal notes
            fn_map = {4: "House", 5: "Senate"}  # maps index of the cell to the chamber of the fiscal note
            for key in fn_map:
                if cells[key].xpath(".//a", BRP.test):
                    fn_pdf_url = cells[key].xpath_single("./a").get_attrib("href")
                    fn_download_id = self.scraper.download_and_register(fn_pdf_url, BRP.bill_documents, True)
                    fn_name = "{} Fiscal Note".format(fn_map[key])
                    fn_doc_service_document = Doc_service_document(fn_name, "fiscal_note", "partial", fn_download_id)
                    fn_index = bill.add_doc_service_document(fn_doc_service_document)
                    # add fiscal not to the corresponding version as a "child"
                    version_doc_service_document.add_child(fn_index)

            # version doc-service document fully configured, add it to the bill
            # we don't really have to do this at the end, but it is clearer
            bill.add_doc_service_document(version_doc_service_document)

        # history page gives info about cosponsors and actions
        history_page_url = bill_page_doc.xpath_single("//a[text()='[History]']").get_attrib("href")
        history_page_doc = self.scraper.url_to_lxml(history_page_url, BRP.bill_actions)
        sponsors_anchor_tag = history_page_doc.xpath("//div[@class='BillInfo-Section BillInfo-PrimeSponsor']/"
                                                     "div[@class='BillInfo-Section-Data']//a")
        # cosponsors
        for sponsor_anchor_tag in sponsors_anchor_tag:
            sponsor = self.get_sponsor_name(sponsor_anchor_tag)
            if sponsor in sponsors_set:  # sponsors set is used to avoid primary sponsor being added as cosponsor
                continue
            else:
                sponsors_set.add(sponsor)
                spon_url = sponsor_anchor_tag.get_attrib('href')
                if 'senate_bio' in spon_url:
                    spon_chamber = 'upper'
                elif 'house_bio' in spon_url:
                    spon_chamber = 'lower'
                else:
                    spon_chamber = None
                if spon_chamber:
                    bill.add_sponsor("cosponsor", sponsor, chamber=spon_chamber)
                else:
                    bill.add_sponsor("cosponsor", sponsor)

        # actions
        action_table = history_page_doc.xpath_single("//div[@class='BillInfo-Section BillInfo-Actions']"
                                                     "/div[@class='BillInfo-Section-Data']/table[@class='DataTable']")
        action_chamber = chamber  # use bill chamber as fallback value
        for row in action_table.xpath(".//tr"):
            action_text_full = row.xpath("./td")[-1].text_content().replace(u"\xa0", " ")
            if action_text_full == 'In the House':
                action_chamber = 'lower'
                continue
            elif action_text_full == 'In the Senate':
                action_chamber = 'upper'
                continue
            elif action_text_full.startswith("(Remarks see"):
                continue
            match = re.findall(r"[A-Za-z.]+ \d{1,2}, \d{4}", action_text_full)
            if not match:
                continue
            date_string = match[0]
            action = action_text_full.split(date_string)[0]
            action = action.strip().strip(",").strip()
            date_string = date_string.replace(".", "")
            # the usage of abbreviation is not consistent, need some normalization (first 3 letters)
            month = re.findall(r"[a-zA-Z]+", date_string)[0]
            date_string = date_string.replace(month, month[:3])
            date = datetime.datetime.strptime(date_string, "%b %d, %Y")
            bill.add_action(action_chamber, action, date)

        # votes
        # this vote page lists "categories" of votes (senate/house floor roll call, committee roll call, etc.)
        # the "Vote" anchor tag leads to all votes that fall into this category (I call it vote summary page)
        # then clicking on a specific vote on the vote summary page leads to all detailed voting data of this vote
        vote_page_url = bill_page_doc.xpath_single("//a[text()='[Votes]']").get_attrib("href")
        vote_page_doc = self.scraper.url_to_lxml(vote_page_url, BRP.bill_votes)

        # floor votes
        floor_roll_call_table = vote_page_doc. \
            xpath_single("//div[@class='Column-OneHalf' and contains(., 'Floor Roll Call')]/table", BRP.test)
        if floor_roll_call_table:  # this table might not exist at all, so this check is necessary
            for row in floor_roll_call_table.xpath(".//tr"):
                vote_chamber = row.xpath_single(".//th").text_content()
                vote_summary_url = row.xpath_single(".//a").get_attrib("href")
                vote_summary_doc = self.scraper.url_to_lxml(vote_summary_url, BRP.bill_votes)
                vote_details_table = vote_summary_doc.xpath_single("//table[@class='DataTable']")
                for details_row in vote_details_table.xpath(".//tr")[1:]:  # first row is header
                    date = details_row.xpath(".//td")[0].text_content()
                    date = datetime.datetime.strptime(date, "%m/%d/%Y")
                    motion = details_row.xpath(".//td")[1].text_content()
                    motion = self.refine_floor_vote_motion(motion, vote_chamber)
                    vote_details_url = details_row.xpath_single(".//td[2]/a").get_attrib("href")
                    vote_details_doc = self.scraper.url_to_lxml(vote_details_url, BRP.bill_votes)
                    voters_div = vote_details_doc. \
                        xpath_single("//div[@class='Column-ThreeFourth Column-Last RollCalls-ListContainer']")

                    yes_voters = []
                    no_voters = []
                    other_voters = []

                    for span in voters_div.xpath(".//span[@class='icon icon-thumbs-up']/parent::div", BRP.test):
                        yes_voters.append(re.split(r"\s{2,}", span.text_content())[0])
                    for span in voters_div.xpath(".//span[@class='icon icon-thumbs-up-2']/parent::div", BRP.test):
                        no_voters.append(re.split(r"\s{2,}", span.text_content())[0])
                    for span in voters_div.xpath(".//span[@class='icon icon-x']/parent::div", BRP.test):
                        other_voters.append(re.split(r"\s{2,}", span.text_content())[0])
                    yes_count = len(yes_voters)
                    no_count = len(no_voters)
                    other_count = len(other_voters)
                    passed = yes_count > no_count
                    vote = Vote("lower" if vote_chamber.lower() == "house" else "upper", date, motion, passed,
                                yes_count, no_count, other_count)
                    vote.add_source(vote_details_url)
                    for voter in yes_voters:
                        vote.yes(voter)
                    for voter in no_voters:
                        vote.no(voter)
                    for voter in other_voters:
                        vote.other(voter)

                    bill.add_vote(vote)

        # committee votes
        committee_roll_call_table = vote_page_doc. \
            xpath_single("//div[@class='Column-OneHalf Column-Last Column-BorderLeft' "
                         "and contains(., 'Committee Roll Call')]/table", BRP.test)
        if committee_roll_call_table:
            for row in committee_roll_call_table.xpath(".//tr"):
                motion_prefix = row.xpath_single(".//td[1]").text_content(). \
                    replace("(H)", "").replace("(S)", "").strip()
                motion_prefix = motion_prefix.title() + " Committee"
                vote_chamber = "lower" if "(H)" in row.text_content() else "upper"
                motion_prefix = "{} {}".format("House" if vote_chamber == "lower" else "Senate", motion_prefix)
                vote_summary_url = row.xpath_single(".//td[2]/a").get_attrib("href")
                vote_summary_doc = self.scraper.url_to_lxml(vote_summary_url, BRP.bill_votes)
                vote_details_table = vote_summary_doc.xpath_single("//table[@class='DataTable']")
                for details_row in vote_details_table.xpath(".//tr"):
                    date_string = details_row.xpath_single(".//td[1]").text_content()
                    date = datetime.datetime.strptime(date_string, "%m-%d-%Y")
                    motion = details_row.xpath_single(".//td[2]/a").text_content()
                    motion = self.refine_committee_vote_motion(motion)
                    motion = motion_prefix + " - " + motion
                    motion = re.sub(r" +", " ", motion)
                    vote_details_url = details_row.xpath_single(".//td[2]/a").get_attrib("href")
                    vote_details_doc = self.scraper.url_to_lxml(vote_details_url, BRP.bill_votes)
                    voters_table = vote_details_doc.xpath_single("//table[@class='DataTable' "
                                                                 "and contains(., 'Majority Members')"
                                                                 "and contains(., 'Minority Members')]", BRP.test)
                    if not voters_table:
                        voters_table = vote_details_doc.xpath("//table[@class='DataTable']")[-1]
                    yes_voters = []
                    no_voters = []
                    other_voters = []
                    for table in voters_table.xpath(".//table"):
                        for voter_row in table.xpath(".//tr"):
                            if len(voter_row.xpath(".//td", BRP.test)) != 2 \
                                    or voter_row.xpath_single(".//td[1]", BRP.test).text_content().endswith(":"):
                                # skipping vote summary and vote result data
                                continue
                            # sometimes, the position of the voter (e.g. chair), is listed after a dash
                            name = voter_row.xpath_single(".//td[1]", BRP.test).text_content().split("-")[0].strip()
                            vote_cast = voter_row.xpath_single(".//td[2]", BRP.test).text_content()
                            if not name:  # taking care of empty cells
                                continue
                            name = re.split(r"\s{2,}", name)[0]
                            if vote_cast == "YEA":
                                yes_voters.append(name)
                            elif vote_cast == "NAY":
                                no_voters.append(name)
                            else:
                                other_voters.append(name)
                    yes_count = len(yes_voters)
                    no_count = len(no_voters)
                    other_count = len(other_voters)
                    passed = yes_count > no_count
                    vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
                    vote.add_source(vote_details_url)
                    for voter in yes_voters:
                        vote.yes(voter)
                    for voter in no_voters:
                        vote.no(voter)
                    for voter in other_voters:
                        vote.other(voter)
                    bill.add_vote(vote)

        self.save_bill(bill)

    @staticmethod
    def get_special_index(session):
        """
        Returns the "sIndex" parameter within the bill list query url
        :param session: string representing the session
        :return: 0 if it is a regular session, and its special session number otherwise (1 for ss1, 2 for ss2 etc.)
        """
        if session.endswith("r"):
            return 0
        else:
            return int(session.split("ss")[1])

    def get_sponsor_name(self, sponsor_page_anchor_tag):
        # TODO: remove the query for full sponsor name to speed up scrper
        sponsor_name_doc = None
        sponsor_page_url = sponsor_page_anchor_tag.get_attrib("href")
        # sponsor_page_doc = self.scraper.url_to_lxml(sponsor_page_url, BRP.bill_sponsors)
        # sponsor_name_doc = sponsor_page_doc.xpath_single("//div[@class='MemberBio-Main']/h1", BRP.test)
        if sponsor_name_doc:
            return sponsor_name_doc.text_content()
        else:
            # logger.info(u"No information found on sponsor page {}. Using only last name as sponsor name."
            #             .format(sponsor_page_url))
            return sponsor_page_anchor_tag.text_content().replace("Representative", "").strip()

    @staticmethod
    def html_parser(element_wrapper):
        '''Extract the primary part of the HTML. Raises ValueError in case of
        invalid page, preventing download and document record creation. '''
        element = element_wrapper.xpath_single("//div[@id='page-container']")
        if not element:
            raise ValueError("Invalid page")
        return [ScraperDocument(element.text_content())]

    @staticmethod
    def version_parser(element_wrapper):
        # this is a minimal parser that ensures parsing only the text body part of the HTML. This does not
        # guarantee the consistency in formatting.
        text = element_wrapper.text_content()
        text = re.sub(r"\s+", " ", text)
        name_buffer = text
        name_buffer_candidates = []
        if "A RESOLUTION" in text:
            name_buffer_candidates.append(text.split("A RESOLUTION")[0])
        if "AN ACT" in text:
            name_buffer_candidates.append(text.split("AN ACT")[0])
        if "A CONCURRENT RESOLUTION" in text:
            name_buffer_candidates.append(text.split("A CONCURRENT RESOLUTION")[0])
        if "A JOINT RESOLUTION" in text:
            name_buffer_candidates.append(text.split("A JOINT RESOLUTION")[0])
        if not name_buffer_candidates:
            logger.critical(u"CANNOT FIND BILL NAME SPLITTER ON PAGE {}".format(element_wrapper.url))
            return [ScraperDocument(text)]

        for name_buffer_candidate in name_buffer_candidates:
            if len(name_buffer_candidate) < len(name_buffer):
                name_buffer = name_buffer_candidate
        splitter_elements = re.split(r"[A-Z]+\s+\d+,\s+\d+", name_buffer)
        if len(splitter_elements) < 2:
            # some documents simply does not have it
            # http://www.legis.state.pa.us/CFDOCS/Legis/PN/Public/btCheck.cfm?txtType=HTM&sessYr=2015&sessInd=0&
            # billBody=S&billTyp=B&billNbr=1071&pn=2202
            return [ScraperDocument(text)]
        splitter = splitter_elements[-2]
        name = (splitter + name_buffer.split(splitter)[1]).strip().strip(",").strip()
        return [ScraperDocument(text, additional_data={"name": name})]

    @staticmethod
    def refine_floor_vote_motion(motion, vote_chamber):
        vote_chamber = vote_chamber.title()
        amd_number_match = re.findall(r"A\d+", motion)
        if not amd_number_match:
            return vote_chamber + " -" + re.sub(r"[SH][BR] \d+ PN \d+,", "", motion)
        else:
            amd_number = amd_number_match[0]
            return "Amendment {}".format(amd_number) + motion.split(amd_number)[-1]

    @staticmethod
    def refine_committee_vote_motion(motion):
        return motion.split(" - ")[1]

    @staticmethod
    def memo_html_parser(element_wrapper):
        text_prefix = element_wrapper.\
            xpath_single("//h2[text()='MEMORANDUM']/following-sibling::table[1]").text_content()
        text_body = element_wrapper.\
            xpath_single("//h2[text()='MEMORANDUM']/following-sibling::table[2]").text_content()
        text = text_prefix + text_body if text_body else text_prefix
        return [ScraperDocument(text)]

    def version_static_content(self, html_file):
        html_text = html_file.read().decode('utf-8')
        root = self.scraper.wrap_html(u"", html_text, BRP.bill_documents)
        elem = root.xpath_single("//div[@id='page-container']")
        if not elem:
            elem = root.xpath_single("body")
        return elem.tostring()

    def memo_static_content(self, html_file):
        html_text = html_file.read().decode('utf-8')
        # There is a dynamically generated id here in the message.
        html_text = re.sub(ur'<div id="Message-\d+"', u'<div id="Message"', html_text)
        root = self.scraper.wrap_html(u"", html_text, BRP.bill_documents)
        elem = root.xpath_single("//div[@class='PageContents']")
        return elem.tostring()

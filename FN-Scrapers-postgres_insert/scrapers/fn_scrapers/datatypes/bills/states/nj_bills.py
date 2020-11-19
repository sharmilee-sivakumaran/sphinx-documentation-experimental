from __future__ import absolute_import

import datetime
import logging
import re

from lxml import html
import mechanize
from requests.exceptions import HTTPError

from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scraperutils.scrape.element_wrapper import ElementWrapper

from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id, normalize_bill_id
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('NJBillScraper')
bill_page_url = "https://www.njleg.state.nj.us/bills/BillView.asp"
set_session_url = "https://www.njleg.state.nj.us/bills/bills0001.asp"


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-NJ", group="fnleg")
class NJBillScraper(BillScraper):
    def __init__(self):
        super(NJBillScraper, self).__init__("nj")
        self._last_set_session = None

    def scrape_bill_ids(self, session):
        if not session.endswith("r"):
            raise Exception("The NJ Bill scraper doesn't support special sessions.")

        bill_ids = []
        url = "https://www.njleg.state.nj.us/legislativepub/sitemap.asp"
        bill_list_page_url = "https://www.njleg.state.nj.us/bills/BillsByNumber.asp"
        br = mechanize.Browser()
        br.open(url)
        br.select_form("GoToBills")
        br.set_all_readonly(False)
        br["DBNAME"] = "LIS" + session[:4]
        br.submit()
        br.select_form("ByNumber")
        source = br.submit().read()
        bill_list_page_doc = self.source_to_lxml(source, bill_list_page_url, BRP.bill_list)
        record_string = bill_list_page_doc.xpath_single("//font[contains(text(), 'Total records:')]").text_content()
        count_string = re.findall(r"Page\s+\d+\s+of\s+(\d+)", record_string)[0]
        max_page_count = int(count_string)
        logger.info("A total of %s pages will scraped to get bill ids", count_string)
        for page in range(1, max_page_count + 1):
            logger.info("Scraping page %s from the search results to get bill ids", str(page))
            br.select_form("Bills")
            br["GoToPage"] = str(page)
            source = br.submit().read()
            bill_list_page_doc = self.source_to_lxml(source, bill_list_page_url, BRP.bill_list)
            for anchor_tag in bill_list_page_doc.xpath(
                    "//td[@valign='top']/a[@title='View Detail Bill Information']"):
                bill_id = anchor_tag.text_content()
                bill_ids.append(bill_id)
                logger.info("Appended {} to bill_ids list".format(bill_id))
        br.close()

        return bill_ids

    def check_page_session(self, bill_page_doc, session):
        # This is a hacky way to check that we got a bill from the session
        # we intended - but I don't know of a better way to do it. One the bill
        # page, there is an image that lists the current session. So, we check to
        # make sure that that very specific image is there. And if its not, we fail.
        # This is likely quite brittle, but, better than scraping bills from the wrong
        # session!
        session_image = "https://www.njleg.state.nj.us/bills/decor/bills{}.gif".format(session[:4])
        session_title = "Bills {}-{}".format(session[:4], session[4:8])
        xpath = "//img[@src='{}' and @title='{}' and @width='544' and @height='33']".format(
            session_image,
            session_title
        )
        bill_page_doc.xpath_single(xpath, BRP.wrong_session)

    def set_session(self, session):
        if self._last_set_session == session:
            return

        # The session is set by posting to the set_session_url with the session that we
        # want to be active. That session is then stored server-side and identified by a
        # cookie that is set.
        data = {"DBNAME": "LIS" + session[:4]}
        self.scraper.http_request(set_session_url, method="POST", request_args={"data": data})
        self._last_set_session = session

    def scrape_bill(self, session, bill_id, **kwargs):
        logger.info("Scraping bill " + bill_id)

        if not session.endswith("r"):
            raise Exception("The NJ Bill scraper doesn't support special sessions.")

        self.set_session(session)

        # We get the bill we are interested in by posting its ID to the bill_page_url
        data = {"BillNumber": bill_id.replace(" ", "").upper()}
        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill, method="POST", request_args={"data": data})

        self.check_page_session(bill_page_doc, session)

        # weird xpath because NJ has no markup. "*" is sometimes appended when it is an emergency bill
        # examine source instead of using "inspect" for this xpath, it has malformed html
        title = bill_page_doc.xpath_single("//b[contains(text(), '{}')]/"
                                           "parent::font/following-sibling::font[@color='maroon']".
                                           format(bill_id.replace(" ", "").upper())).text_content(). \
                                           strip("*").strip("\"").strip("*")

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type_string = bill_id.split()[0]
        if len(bill_type_string) == 1:  # "A" or "S"
            bill_type = "bill"
        else:
            bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)

        # add similar/identical bills
        for anchor_tag in bill_page_doc.xpath("//a[@title='View Detail Bill Information']",
                                              policy=BRP.info):
            bill.add_companion(self.normalize_related_bill_id(anchor_tag.text_content()))

        # sponsors
        anchor_tags = bill_page_doc.xpath("//font[@face='Arial, Helvetica, sans-serif']//font",
                                          policy=BRP.info)
        sponsors = [anchor_tag.text_content() for anchor_tag in anchor_tags]
        sponsor_types_raw = re.findall(r"as (Primary |Co-)Sponsor", bill_page_doc.text_content())
        sponsor_type_map = {"Primary ": "primary",
                            "Co-": "cosponsor"}
        sponsor_types = [sponsor_type_map[sponsor_type] for sponsor_type in sponsor_types_raw]
        assert len(sponsors) == len(sponsor_types)
        for index in range(0, len(sponsor_types)):
            bill.add_sponsor(sponsor_types[index], sponsors[index])

        # actions and documents
        logger.info('scraping action docs')
        font = bill_page_doc.xpath_single("//tr/td[@colspan='5']/font[@face='Times New Roman'][1]")
        action_strings = []
        document_names_raw = []
        document_urls = []
        for text in font.xpath("./text()"):
            text = text.strip()
            if re.findall(r"^\s*(\d+/\d+/\d+)", text):
                action_strings.append(text)
            elif "-" in text:
                document_names_raw.append(text)

        # actions
        logger.info('scraping actions')
        for action_string in action_strings:
            date_string = re.findall(r"^\s*(\d+/\d+/\d+)", action_string)[0]
            date = datetime.datetime.strptime(date_string, "%m/%d/%Y")
            action = action_string.split(date_string)[1].strip()
            actor = chamber
            if "governor" in action.lower():
                actor = "executive"
            elif "assembly" in action.lower() and "senate" in action.lower():
                actor = chamber
            elif "senate" in action.lower():
                actor = "upper"
            elif "assembly" in action.lower():
                actor = "lower"

            bill.add_action(actor, action, date)

        # documents
        logger.info('scraping docs')
        current_document_label = None
        for anchor_tag in font.xpath(".//a"):
            document_label = anchor_tag.get_attrib("href").rsplit(".", 1)[0]
            document_format = anchor_tag.text_content().split()[0].lower()
            document_url = anchor_tag.get_attrib("href")
            if document_label != current_document_label:
                document_urls.append({})
                current_document_label = document_label
            document_urls[-1][document_format] = document_url
        assert len(document_urls) == len(document_names_raw)
        for index in range(0, len(document_names_raw)):
            document_name_raw = document_names_raw[index]
            document_name_raw = document_name_raw.replace("-", "")
            document_name_raw = re.sub(r"\d+\s+pages", "", document_name_raw)
            document_name = re.sub(r"\s+", " ", document_name_raw.replace("-", "")).strip()
            if document_name_raw.startswith("Reprint"):
                try:
                    html_page_doc = self.scraper.url_to_lxml(document_urls[index]["html"], BRP.bill_documents)
                    assert html_page_doc is not None
                    reprint_number_match = re.findall(r"\[([a-zA-Z]+\s+)Reprint\]", html_page_doc.text_content())
                    if not reprint_number_match:
                        logger.warning("Cannot find a reprint number for document located at {}".
                                       format(document_urls[index]["html"]))
                    else:
                        reprint_number = reprint_number_match[0]
                        document_name = reprint_number + document_name
                except AssertionError:
                    logger.warning("Error occurred while querying {}. This is likely because the website "
                                   "provided a bad link.".format(document_urls[index]["html"]))
            document_type, doc_service_type = self.get_doc_type(document_name)
            try:
                if doc_service_type == "complete":
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(document_urls[index]["html"],
                                                                     BRP.bill_documents,
                                                                     self.scraper.extraction_type.html,
                                                                     False,
                                                                     self.html_parser)

                    if len(doc_ids) == 1 and doc_ids[0] is not None:
                        doc_service_document = Doc_service_document(document_name, document_type,
                                                                    "complete",
                                                                    download_id=download_id,
                                                                    doc_id=doc_ids[0])
                        # TODO: removed PDF version download to speed up scraping
                        # pdf_download_id = self.scraper.download_and_register(document_urls[index]["pdf"],
                        #                                                      BRP.bill_documents, True)
                        # doc_service_document.add_alternate_representation(pdf_download_id)
                        bill.add_doc_service_document(doc_service_document)
                    else:
                        logger.warning("Failed to process file at {}. Skip it.".format(document_urls[index]["html"]))
                else:
                    download_id = self.scraper.download_and_register(document_urls[index]["pdf"], BRP.bill_documents,
                                                                     True)
                    doc_service_document = Doc_service_document(document_name, document_type, "partial", download_id)
                    # TODO: removed HTML as alternate format download for "partial" documents
                    # html_download_id = self.scraper.download_and_register(document_urls[index]["html"],
                    #                                                       BRP.bill_documents,
                    #                                                       False)
                    # doc_service_document.add_alternate_representation(html_download_id)
                    bill.add_doc_service_document(doc_service_document)
            except HTTPError:
                logger.warning("HTTP error occurred while trying to download file from {}. This is likely "
                               "because the website provided a bad link".
                               format(document_urls[index]["html"] if doc_service_type == "complete"
                                      else document_urls[index]["pdf"]))

        font = bill_page_doc.xpath_single("//tr/td[@colspan='5']/font[@face='Times New Roman'][2]")
        voting_categories = []
        index = 0
        for element in font.xpath("./*", policy=BRP.info):
            element_text = element.text_content(policy=BRP.info)
            if "voting:" in element_text.lower():
                category = element_text.lower().strip().split()[0] + " Vote"
                category = category.title()
                voting_categories.append((category, [], []))
            if "roll call" in element_text.lower():
                voting_categories[-1][1].append(index)
                index += 1
            if "- Yes" in element_text or \
                            "- No" in element_text or \
                            "- Abstain" in element_text or \
                            "- Not Voting" in element_text:
                voting_categories[-1][2].append(element_text.replace(u"\xa0", " "))
        for voting_category, detail_index, detail_info in voting_categories:
            assert len(detail_info) == len(detail_index)

        vote_info_list = []
        for text in font.xpath("./text()", policy=BRP.info):
            text = text.replace(u"\xa0", " ")
            if re.findall(r"Yes\s+\{\d+\}\s+No\s+\{\d+\}\s+Not\s+Voting\s+\{\d+\}\s+(Abstains\s+\{\d+\})*", text) \
                    and "Voice" not in text:
                vote_info_list.append(text.strip())
        for voting_category, detail_index_list, detail_info_list in voting_categories:
            for i in range(0, len(detail_index_list)):
                vote_info = vote_info_list[detail_index_list[i]]
                detail_info = detail_info_list[i]
                # have to put "Not Voting" before "No", otherwise "Not Voting" will be matched with "No"
                vote_chamber, date_string, motion, yes_count, no_count, not_voting_count = \
                    re.findall(r"^(.*?)\s+(\d+/\d+/\d+)\s+-\s+(.*?)\s+-\s+"
                               r"Yes\s+\{(\d+)\}\s+No\s+\{(\d+)\}\s+Not\s+Voting\s+\{(\d+)\}\s+",
                               vote_info)[0]
                if vote_chamber.startswith("A"):
                    vote_chamber = "lower"
                elif vote_chamber.startswith("S"):
                    vote_chamber = "upper"
                else:
                    vote_chamber = chamber
                # isolating the Abstains because the "Abstains" is not always in the string
                abstain_count = re.findall(r"Abstains\s+\{(\d+)\}", vote_info)
                if not abstain_count:
                    abstain_count = 0
                else:
                    abstain_count = abstain_count[0]
                yes_count = int(yes_count)
                no_count = int(no_count)
                other_count = int(not_voting_count) + int(abstain_count)
                # TODO: we might figure out the vote result from the action text, but action date usually differs
                # TODO: from the voting date on the bill page, falling back to the vote count comparision
                passed = yes_count > no_count
                date = datetime.datetime.strptime(date_string, "%m/%d/%Y")
                display_vote_chamber = "Assembly" if vote_chamber == "lower" else "Senate"
                motion = display_vote_chamber + " " + \
                         voting_category + ": " + motion  # adding vote category before motion, better metadata
                vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)

                # name_and_cast_array is basically a list of [voter1, cast1, voter2, cast2, ...]
                name_and_cast_array = re.split(r"\s+-\s+(Yes|Not\s+Voting|Abstain|No)\s*", detail_info)
                voters = [name_and_cast.strip() for name_and_cast in name_and_cast_array if name_and_cast.strip()]
                index = 0
                yes_voters = []
                no_voters = []
                other_voters = []
                while index < len(voters):
                    voter = voters[index]
                    cast = voters[index + 1]
                    index += 2
                    if cast == "Yes":
                        vote.yes(voter)
                        yes_voters.append(voter)
                    elif cast == "No":
                        vote.no(voter)
                        no_voters.append(voter)
                    else:
                        vote.other(voter)
                        other_voters.append(voter)
                if len(yes_voters) != yes_count or len(no_voters) != no_count or len(other_voters) != other_count:
                    '''
                    In the case that the vote counts do not match what we are parsing from actual voters. We assume
                    that all their data is incorrect and so we remove the actual voters' votes
                    '''
                    vote["no_votes"] = []
                    vote["yes_votes"] = []
                    vote["other_votes"] = []
                    logger.critical("Bill: {}. Motion: {}. Vote count does not match voters and their votes. "
                                    "Vote count listed, "
                                    "Yes:{},No:{},Other:{}. Actual voters' votes, Yes:{},NO:{},Other:{}"
                                    .format(bill_id, motion, yes_count, no_count, other_count,
                                            len(yes_voters), len(no_voters), len(other_voters)))
                bill.add_vote(vote)
        self.save_bill(bill)

    def source_to_lxml(self, page_source, url, policy):
        """
        Converts HTML source into ElementWrapper object
        :param page_source: HTML page source
        :param url: url of the HTML, only for the purpose of the ElementWrapper constructor
        :param policy: reporting policy for the ElementWrapper
        :return: ElementWrapper object for the HTML source
        """
        lxml_page = html.fromstring(page_source)
        lxml_page.make_links_absolute(url)
        return ElementWrapper(lxml_page, policy, url, self.scraper.process_data)


    @staticmethod
    def html_parser(element_wrapper):
        return [ScraperDocument(element_wrapper.xpath_single("//body").text_content())]

    @staticmethod
    def get_doc_type(document_name):
        document_name = document_name.lower()
        # no need to worry about stripping or removing extra empty space, this is done before this function is called
        if "version" in document_name or "reprint" in document_name \
                or "substitute" in document_name or document_name == "introduced" \
                or "advance law" in document_name or "pamphlet law" in document_name:
            document_type = "version"
            doc_service_type = "complete"
        elif document_name.startswith("veto"):
            document_type = "summary"
            doc_service_type = "partial"
        elif "statement" in document_name:
            document_type = "committee_document"
            doc_service_type = "complete"
        elif "fiscal" in document_name:
            document_type = "fiscal_note"
            doc_service_type = "partial"
        else:
            document_type = "other"
            doc_service_type = "partial"
        return document_type, doc_service_type

    @staticmethod
    def normalize_related_bill_id(related_bill_id):
        related_bill_id = related_bill_id.strip().upper()
        related_bill_id = re.sub(r"\s+", " ", related_bill_id)
        related_bill_id = re.findall(r"[A-Z]+\s*\d+", related_bill_id)[0]
        related_bill_id = normalize_bill_id(related_bill_id)
        return related_bill_id

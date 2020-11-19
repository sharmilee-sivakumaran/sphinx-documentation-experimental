from __future__ import absolute_import

import datetime
import logging
import re

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document

from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id

from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger("INBillScraper")

SPONSOR_TYPE_MAP = {
    "author": "primary",
    "co-author": "primary",
    "sponsor": "primary",
    "co-sponsor": "cosponsor"
}

BILL_TYPE_MAP = {
    "B": "bill",
    "R": "resolution",
    "JR": "joint_resolution",
    "JRCA": "constitutional_amendment",
    "AM": "resolution",
    "JSR": "joint_resolution",
    "CR": "concurrent_resolution"
}

ROLL_CALL_ID_PATTERN = re.compile(r"Roll Call \d+")
NUMBERED_READING_PATTERN = re.compile(r"\w+ reading")


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-IN", group="fnleg")
class INBillScraper(BillScraper):
    def __init__(self):
        super(INBillScraper, self).__init__("in", retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

    def scrape_bill_ids(self, session):
        bill_ids = {}
        urls = self.build_urls_for_new_legislation_list(session)
        for url in urls:
            doc = self.scraper.url_to_lxml(url, BRP.bill_list)
            for bill_span in doc.xpath("//strong/ancestor::span", BRP.warning):
                elements = [element.strip() for element in bill_span.text_content().split(u":")]
                bill_id = elements[0]
                bill_title = elements[1]
                bill_ids[bill_id] = {"title": bill_title}
        logger.info("A total of {} bill ids scraped for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        # basic bill info
        bill_info = kwargs.get("bill_info")
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        title = bill_info["title"]
        bill_url = self.build_url_for_new_legislation_page(session, bill_id)

        logger.info("Scraping bill {} at {}".format(bill_id, bill_url))
        doc = self.scraper.url_to_lxml(bill_url, BRP.bill)

        summary = doc.xpath_single("//span[@id='bill-digest']").text_content()
        remaining_summary = doc.xpath_single("//span[@id='digest-remaining-words']", BRP.debug)
        if remaining_summary:
            remaining_summary = remaining_summary.text_content()
            summary = "{} {}".format(summary, remaining_summary)

        bill = Bill(session, chamber, bill_id, title, bill_type, summary=summary)
        bill.add_source(bill_url)

        # scrape sponsor and sponsor types
        for div in doc.xpath("//div[contains(@class, 'bill-author-info')]"):
            name = div.xpath_single("./em").text_content()
            scraped_sponsor_type = div.xpath_single("./p").text_content().lower()
            if scraped_sponsor_type not in SPONSOR_TYPE_MAP:
                logger.warning(r"Cannot determine sponsor type with keyword \"{}\". Falling back to primary sponsor".
                               format(scraped_sponsor_type))
                sponsor_type = "primary"
            else:
                sponsor_type = SPONSOR_TYPE_MAP[scraped_sponsor_type]
            bill.add_sponsor(sponsor_type, name)

        # scrape actions
        table = doc.xpath_single("//table[contains(@class, 'actions-table')]")
        for row in table.xpath("./tbody/tr[contains(@class, 'action-item')]/td/dl/dd"):
            if row.text_content() == "None currently available.":
                logger.warning("No action available for bill {} on page {}".format(bill_id, bill_url))
                continue
            chamber_abbr = row.xpath_single("./b/span[1]").text_content()
            action_date = row.xpath_single("./b/span[2]").text_content()
            action_text_whole = row.text_content()
            action_text_splitter = row.xpath_single("./b").text_content()
            action_text = action_text_whole.split(action_text_splitter)[1]
            action_text = re.sub(r"\s+", " ", action_text).strip()
            if not action_text:
                logger.warning("No effective action text found in action line \"{}\"".format(action_text_whole))
                continue
            # Some resolution actions have no dates, we'll skip these actions
            if not action_date:
                logger.warning("No action date found in action text \"{}\". Skip it".format(action_text_whole))
                continue
            date = datetime.datetime.strptime(action_date.strip(), "%m/%d/%Y")
            actor = "lower" if chamber_abbr.lower() == "h" else "upper"
            action = action_text
            bill.add_action(actor, action, date)

            if row.xpath_single(".//a[contains(text(), 'Roll Call')]", BRP.debug):
                # there is a vote in this action line
                vote_anchor = row.xpath_single(".//a[contains(text(), 'Roll Call')]")
                vote_view_url = vote_anchor.get_attrib("href")
                motion = action_text.split("Roll Call", 1)[0].strip().strip(";").strip().title()
                vote_external_id = re.search(r"^.+?/documents/([a-z0-9]+)$", vote_view_url).group(1)
                vote_pdf_url = self.get_pdf_url(vote_external_id)
                vote_chamber = actor
                vote_date = date
                _, scraper_docs, _ = \
                    self.scraper.register_download_and_documents(vote_pdf_url, BRP.bill_votes,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, self.vote_parser,
                                                                 parser_args={"motion": motion,
                                                                              "chamber": vote_chamber,
                                                                              "date": vote_date},
                                                                 content_type="application/pdf")
                if len(scraper_docs) == 1 and scraper_docs[0] is not None:
                    vote = scraper_docs[0].additional_data["vote"]
                    vote.add_source(vote_pdf_url)
                    bill.add_vote(vote)
                else:
                    logger.warning("Failed to process document at url {}".format(vote_pdf_url))

        # versions
        bill_version_divs = doc.xpath("//div[contains(@id, 'bill-version-')]")
        for bill_version_div in bill_version_divs:
            version_anchor = bill_version_div.xpath_single(".//a[1]")
            version_name = version_anchor.get_attrib("title")
            version_external_id = version_anchor.get_attrib("data-myiga-actiondata")
            version_pdf_url = self.get_pdf_url(version_external_id)
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(version_pdf_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf, True,
                                                             content_type="application/pdf")
            if len(doc_ids) == 1 and doc_ids is not None:
                doc_service_document = \
                    Doc_service_document(version_name, "version", "complete", download_id, doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
            else:
                logger.warning("Failed to process document at {}".format(version_pdf_url))
            fn_subdiv = bill_version_div.xpath_single("./div[2]", policy=BRP.debug)
            if fn_subdiv is not None:
                fn_ul = fn_subdiv.xpath_single("./div[@id='js-head-fiscal-notes']//ul[@class='fiscalnotes-list']")
                if fn_ul is None:
                    logger.warning("No Fiscal Note for \"{}\"".format(version_name))
                    continue
                for fn_li in fn_ul.xpath("./li"):
                    fn_anchor = fn_li.xpath_single("./a[1]")
                    fn_name = fn_anchor.text_content()
                    fn_name = "{} - {}".format(fn_name, version_name)
                    fn_external_id = fn_anchor.get_attrib("data-myiga-actiondata")
                    fn_pdf_url = self.get_pdf_url(fn_external_id)
                    download_id = self.scraper.download_and_register(fn_pdf_url, BRP.bill_documents, True)
                    doc_service_document = Doc_service_document(fn_name, "fiscal_note", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        for amdt_chamber in ["House", "Senate"]:
            amendment_id = "{}-amendments".format(amdt_chamber.lower())
            amdt_div = doc.xpath_single("//div[@id='{}']".format(amendment_id))
            amdt_ul = amdt_div.xpath_single(".//ul[@class='{}-amendments-list']".format(amdt_chamber.lower()))
            for amdt_anchor in amdt_ul.xpath("./li/a[1]", policy=BRP.debug):
                amdt_name = amdt_anchor.text_content()
                amdt_external_id = amdt_anchor.get_attrib("data-myiga-actiondata")
                amdt_pdf_url = self.get_pdf_url(amdt_external_id)
                name = "{} {}".format(amdt_chamber, amdt_name)
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(amdt_pdf_url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, content_type="application/pdf")
                if len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc_service_document = Doc_service_document(name, "amendment", "complete", download_id, doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
                else:
                    logger.warning("Failed to process document at url {}".format(amdt_pdf_url))

        for dom_id in ["ccrconcurrencedissent", "committee-reports"]:
            div = doc.xpath_single("//div[@id='{}']".format(dom_id), policy=BRP.debug)
            if div is not None:
                for anchor in div.xpath(".//ul/li/div/div/a[@data-myiga-action='pdfviewer.loadpdf'][1]", policy=BRP.debug):
                    name = anchor.text_content()
                    external_id = anchor.get_attrib("data-myiga-actiondata")
                    pdf_url = self.get_pdf_url(external_id)
                    download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
                    doc_service_document = Doc_service_document(name, "committee_document", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        div = doc.xpath_single("//div[@id='v-ccrconcurrencedissent-motions']", policy=BRP.debug)
        if div is not None:
            for anchor in div.xpath(".//ul/li//a[@data-myiga-action='pdfviewer.loadpdf'][1]", policy=BRP.debug):
                name = anchor.text_content()
                external_id = anchor.get_attrib("data-myiga-actiondata")
                pdf_url = self.get_pdf_url(external_id)
                download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
                doc_service_document = Doc_service_document(name, "other", "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    @staticmethod
    def get_ending_year(session):
        # return the ending year of a session as a string, e.g. u"2016"
        # the session must follow the format of "20162016r"
        valid_pattern = re.compile(r"^\d{8}(r|ss\d+?)$")
        match = valid_pattern.match(session)
        if not match:
            raise Exception("Session string '{}' from metadata doesn't follow a proper format like ".format(session) +
                            "'20162016r' or '20182018ss1'")
        return session[4:8], match.group(1)

    def build_urls_for_new_legislation_list(self, session):
        # returns urls for both bills and resolutions list
        # note that session is in the format of "20162016r", and we need the ending year for the url
        ending_year, session_type = self.get_ending_year(session)
        if session_type == 'r':
            session_type = ''
        res_url = "http://iga.in.gov/legislative/{}{}/resolutions".format(ending_year, session_type)
        bill_url = "http://iga.in.gov/legislative/{}{}/bills/".format(ending_year, session_type)
        return [res_url, bill_url]

    def build_url_for_new_legislation_page(self, session, bill_id):
        ending_year, s_type = self.get_ending_year(session)
        if s_type == 'r':
            s_type = ''
        doc_type = bill_id.split()[0]
        doc_num = bill_id.split()[1]
        bill_type = "bills" if doc_type[1:] == "B" else "resolutions"
        bill_chamber = "senate" if doc_type[0] == "S" else "house"

        if bill_type == "resolutions":  # resolution type will be added before the bill number
            prefix_dict = {"R": "simple/", "CR": "concurrent/", "JR": "joint/"}
            prefix = prefix_dict[doc_type[1:]]
            doc_num = prefix + doc_num
        return "http://iga.in.gov/legislative/{}{}/{}/{}/{}".format(ending_year, s_type, bill_type,
                                                                    bill_chamber, doc_num)

    @staticmethod
    def vote_parser(entities, parser_args=None):
        last_entity = entities[-1]
        """
        Some vote PDFs have a version string in the bottom of the PDFs, which is always extracted as
        a separate entity. That is sometimes being sent as a voter which is not good. So deleting that
        entity if it exists

        Example: http://iga.in.gov/static-documents/1/d/d/d/1ddd0d22/HB1116.50.pdf
        """
        if re.search(r'^\s*v\.', entities_text_content([last_entity])):
            del entities[-1]
        text = entities_text_content(entities)
        text = re.sub(r"\s+", " ", text)
        if not text:
            logger.error("Failed to download vote file")
            return []
        voter_info = \
            re.search(r"(?:Y\s*E\s*A)\s+-\s+\d+(.*)"
                      r"(?:N\s*A\s*Y)\s+-\s+\d+(.*)"
                      r"(?:E\s*X\s*C\s*U\s*S\s*E\s*D)\s+-\s+\d+(.*)"
                      r"(?:N\s*O\s*T\s*V\s*O\s*T\s*I\s*N\s*G)\s+-\s+\d+(.*)", text).groups()
        voter_info = [("YEA", voter_info[0]), ("NAY", voter_info[1]),
                      ("EXCUSED", voter_info[2]), ("NOT VOTING", voter_info[3])]
        yes_voters = []
        no_voters = []
        other_voters = []
        for cast, voter_names_text in voter_info:
            voter_names = re.split(r"(?<![,.])\s+", voter_names_text)
            if cast == "YEA":
                voters = yes_voters
            elif cast == "NAY":
                voters = no_voters
            else:
                voters = other_voters
            for voter in voter_names:
                if voter:
                    voters.append(voter)
        motion = parser_args["motion"]
        yes_count = len(yes_voters)
        no_count = len(no_voters)
        other_count = len(other_voters)
        display_motion = motion
        motion = motion.lower()
        if "pass" in motion or "adopt" in motion or "approve" in motion:
            passed = True
        elif "reject" in motion or "fail" in motion:
            passed = False
        else:
            passed = yes_count > no_count  # fallback value for vote result
        vote_chamber = parser_args["chamber"]
        vote_date = parser_args["date"]
        vote = Vote(vote_chamber, vote_date, display_motion, passed, yes_count, no_count, other_count)
        for method, voters in [(vote.yes, yes_voters), (vote.no, no_voters), (vote.other, other_voters)]:
            for voter in voters:
                method(voter)

        return [ScraperDocument(text, additional_data={"vote": vote})]

    def get_pdf_url(self, doc_external_id):
        api_url = "https://iga.in.gov/documents/{}".format(doc_external_id)
        request_args = {"headers": {"Accept": "application/json, text/javascript, */*"}}
        data = self.scraper.url_to_json(api_url, BRP.json, method="GET",\
                                       request_args=request_args)

        return 'http://iga.in.gov/static-documents/{}/{}/{}/{}/{uid}/{name}'.format(
            data['uid'][0],
            data['uid'][1],
            data['uid'][2],
            data['uid'][3],
            uid=data['uid'],
            name=data['name']
        )

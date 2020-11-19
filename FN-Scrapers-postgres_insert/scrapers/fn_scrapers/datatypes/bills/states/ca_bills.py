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
from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('CABillScraper')
bill_type_map = {"CA": "constitutional_amendment",
                 "CR": "concurrent_resolution",
                 "JR": "joint_resolution",
                 "R": "resolution",
                 "B": "bill"}
version_post_url = "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml"
analysis_post_url = "https://leginfo.legislature.ca.gov/faces/billAnalysisClient.xhtml"


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-CA", group="fnleg")
class CABillScraper(BillScraper):
    def __init__(self):
        # California goes offline for about 15 minutes every morning
        retry_policy = Retry500RequestsRetryPolicy(
            max_attempts=None, max_retry_time=12000)
        super(CABillScraper, self).__init__("ca", retry_policy=retry_policy)

    def scrape_bill_ids(self, session):
        session_year = session[:8]
        bill_list_url = "https://leginfo.legislature.ca.gov/faces/billSearchClient.xhtml?" \
                        "session_year={}&house=Both&author=All&lawCode=All".format(session_year)
        logger.info("Start scraping CA bill ids for session {} on {}".format(session, bill_list_url))
        bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        table = bill_list_doc.xpath_single("//table[@id='bill_results']")
        bill_ids = []
        for row in table.xpath("./tbody/tr"):
            cells = row.xpath("./td")
            bill_id = cells[0].text_content().replace("-", " ")
            if self.is_bill_id_valid(bill_id, session):
                bill_ids.append(self.normalize_bill_id(bill_id))
            else:
                logger.info("Bill id {} does not belong to session {}. Skipping.".format(bill_id, session))
        logger.info("A total of {} bill ids were scraped.".format(len(bill_ids)))

        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        session_index = self.get_session_index(session)
        url_bill_id = session[0:8] + session_index + bill_id.replace(" ", "")
        bill_text_url = "https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?" \
                        "bill_id={}".format(url_bill_id)
        logger.info("Scraping {} from {}".format(bill_id, bill_text_url))
        bill_text_doc = self.scraper.url_to_lxml(bill_text_url, BRP.bill)

        # basic bill info
        title_div = bill_text_doc.xpath_single("//div[@id='bill_title']")
        if not title_div:
            logger.critical("Cannot find bill title for {} on {}. Skipping this bill".format(bill_id, bill_text_url))
            return
        title_string = title_div.xpath_single("./h2").text_content()
        title_string = title_string.replace(title_div.xpath_single(".//span").text_content(), "")
        title = title_string.replace(bill_id.replace(" ", "-"), "").strip()
        if not title:
            title = bill_text_doc.xpath_single("//div[@id='title']").text_content()
        if "null-0" in title.lower():
            logger.warning("The bill gets a title of \"null-0\". This happens if a bill id is queried for a session "
                           "it does not belong to, which usually happens when dealing with special session. This "
                           "should have already been handled while scraping bill ids.")
            return

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        bill = Bill(session, chamber, bill_id, title, bill_type)

        # subjects
        subject_string = title
        subjects = [subject.strip().strip(".").strip().lower() for subject in subject_string.split(":")]
        for subject in subjects:
            bill.add_subject(subject)

        # sponsors
        sponsors_page_url = "https://leginfo.legislature.ca.gov/faces/billStatusClient.xhtml?" \
                            "bill_id={}".format(url_bill_id)
        bill.add_source(sponsors_page_url)
        sponsors_page_doc = self.scraper.url_to_lxml(sponsors_page_url, BRP.bill_sponsors)
        for primary_sponsor_id in ["leadAuthors", "principalAuthors"]:
            primary_sponsor_span = sponsors_page_doc.xpath_single(
                "//span[@id='{}']".format(primary_sponsor_id), BRP.warning)
            primary_sponsors_string = primary_sponsor_span.text_content(BRP.info)
            if primary_sponsors_string:
                primary_sponsors = [primary_sponsor.split("(")[0].strip()
                                    for primary_sponsor in primary_sponsors_string.split(", ")]
                for primary_sponsor in primary_sponsors:
                    bill.add_sponsor("primary", primary_sponsor)
        cosponsors_span = sponsors_page_doc.xpath_single("//span[@id='coAuthors']",
                                                         BRP.critical)
        cosponsors_string = cosponsors_span.text_content(BRP.info)
        if cosponsors_string:
            cosponsors = [cosponsor.split("(")[0].strip()
                          for cosponsor in cosponsors_string.split(", ")]
            for cosponsor in cosponsors:
                bill.add_sponsor("cosponsor", cosponsor)

        # bill actions
        history_url = "https://leginfo.legislature.ca.gov/faces/billHistoryClient.xhtml?" \
                      "bill_id={}".format(url_bill_id)

        history_doc = self.scraper.url_to_lxml(history_url, BRP.bill_actions)
        prev = None
        for row in history_doc.xpath("//table[@id='billhistory']/tbody/tr")[::-1]:
            cells = row.xpath("./td")
            date = datetime.datetime.strptime(cells[0].text_content(), "%m/%d/%y")
            action = re.sub(r" +", " ", cells[1].text_content())
            action_lower = action.lower()
            if not action:
                continue
            if "ordered to the senate." in action_lower:
                actor = 'lower'
                prev = 'upper'
            elif "ordered to the assembly." in action_lower:
                actor = 'upper'
                prev = 'lower'
            elif "read third time." in action_lower and "passed." in action_lower:
                actor = prev
                if prev == "upper":
                    prev = "lower"
                else:
                    prev = "upper"
            elif "senate" in action_lower:
                actor = "upper"
                prev = actor
            elif "assembly" in action_lower:
                actor = "lower"
                prev = actor
            elif "governor" in action_lower:
                actor = "executive"
            elif prev is not None:
                actor = prev
            else:
                actor = chamber
                prev = actor
            bill.add_action(actor, action, date)


        # votes
        vote_url = "https://leginfo.legislature.ca.gov/faces/billVotesClient.xhtml?" \
                   "bill_id={}".format(url_bill_id)
        vote_doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
        index = 0
        offset = 5
        rows = vote_doc.xpath("//table[@id='billvotes']/tbody/tr")
        assert len(rows) % 5 == 0
        while index < len(rows):
            date_string = rows[index].xpath_single("./th").text_content()
            date = datetime.datetime.strptime(date_string, "%m/%d/%y")
            passed_string = rows[index].xpath_single("./td[1]").text_content()
            if "PASS" in passed_string:
                passed = True
            elif "FAIL" in passed_string:
                passed = False
            else:
                logger.critical("Cannot get vote result on page {}".format(vote_url))
                index += offset
                continue
            location = rows[index].xpath_single("./td[2]").text_content()
            yes_count = int(rows[index].xpath_single("./td[3]").text_content())
            no_count = int(rows[index].xpath_single("./td[4]").text_content())
            other_count = int(rows[index].xpath_single("./td[5]").text_content())
            motion = rows[index].xpath_single("./td[6]").text_content()
            motion = u"{} - {}".format(location, motion)
            if "Senate" in location or "Sen" in location:
                vote_chamber = "upper"
            elif "Assembly" in location or "Asm" in location:
                vote_chamber = "lower"
            else:
                logger.warning("Cannot determine vote chamber for {} on {}. Using \"other\" as fallback value"
                               .format(location, vote_url))
                vote_chamber = "other"

            vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
            for voter in rows[index + 1].xpath_single(
                    ".//td[2]/span[2]", BRP.warning).text_content(BRP.info).split(", "):
                if not voter:
                    continue
                vote.yes(voter)
            for voter in rows[index + 2].xpath_single(
                    ".//td[2]/span[2]", BRP.warning).text_content(BRP.info).split(", "):
                if not voter:
                    continue
                vote.no(voter)
            for voter in rows[index + 3].xpath_single(
                    ".//td[2]/span[2]", BRP.warning).text_content(BRP.info).split(", "):
                if not voter:
                    continue
                vote.other(voter)
            index += offset
            vote.add_source(vote_url)
            bill.add_vote(vote)

        # get version text from new website
        version_select = bill_text_doc.xpath_single("//select[@id='version']")
        version_info = [(option.get_attrib("value"), option.text_content())
                        for option in version_select.xpath("./option")]
        bid = bill_text_doc.xpath_single("//input[@id='bid']").get_attrib("value")
        view_state = bill_text_doc.xpath_single("//div[@id='content']//input[@name='javax.faces.ViewState']"). \
            get_attrib("value")

        for version_id, version_name in version_info:
            post_data = {"billDetailTopNavBarId": "billDetailTopNavBarId",
                         "bid": bid,
                         "vid": version_id,
                         "version": version_id,
                         "javax.faces.ViewState": view_state,
                         "ddbill_version": "ddbill_version",
                         "bill_id": bid}
            version_doc_response = self.scraper.http_request(version_post_url, method="POST",
                                                             request_args={"data": post_data})
            version_doc_source = version_doc_response.content
            version_doc_source = re.sub(r"<strike.*?</strike>", "", version_doc_source)
            version_doc = self.scraper.wrap_html(version_post_url, version_doc_source, BRP.bill_documents)
            version_text_xpath = "//div[@id='bill_all']/div[@id='bill']" if bill_type == "bill" \
                                 else "//div[@id='bill_all']/span[@class='Resolution']"
            version_text = version_doc.xpath_single(version_text_xpath).text_content()
            pdf_download_url = "https://leginfo.legislature.ca.gov/faces/billPdf.xhtml?bill_id={}&version={}".\
                format(bid, version_id)

            # The pdf url gets us to an HTML page that triggers a Javascript call, which downloads the PDF
            version_download_page = self.scraper.url_to_lxml(pdf_download_url, BRP.bill_documents)
            form_inputs = version_download_page.xpath(u"//input")
            form_data = {form_input.get_attrib(u"name"): form_input.get_attrib(u"value") for form_input in form_inputs}
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(pdf_download_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True, self.version_parser,
                                                             content_type=u"application/pdf",
                                                             download_method=u"POST",
                                                             download_args={u"data": form_data},
                                                             parser_args={u"text": version_text},
                                                             should_download=True,
                                                             should_skip_checks=True)

            assert download_id is not None and len(doc_ids) == 1
            doc_id = doc_ids[0]
            doc_service_document = Doc_service_document(version_name, "version", "complete", download_id, doc_id=doc_id)
            bill.add_doc_service_document(doc_service_document)

        # get bill analysis from new website
        bill_analysis_url = \
            "https://leginfo.legislature.ca.gov/faces/billAnalysisClient.xhtml?bill_id={}".format(url_bill_id)
        bill_analysis_doc = self.scraper.url_to_lxml(bill_analysis_url, BRP.bill_documents)
        bill_analysis_table = bill_analysis_doc.xpath_single("//table[@id='billanalysis']")
        anchor_tags = bill_analysis_table.xpath("./tbody/tr/td/a")
        view_state = bill_analysis_doc. \
            xpath_single("//form[@id='billanalysisform']/input[@name='javax.faces.ViewState']").get_attrib("value")
        for anchor_tag in anchor_tags:
            onclick_code = anchor_tag.get_attrib("onclick")
            key_value_pairs = re.findall(r"'([^']+)':\s?'([^']+)'", onclick_code)
            post_data = {"billanalysisform": "billanalysisform",
                         "javax.faces.ViewState": view_state}
            for key, value in key_value_pairs:
                post_data[key] = value
            name = anchor_tag.text_content()
            download_id = self.scraper.download_and_register(url=analysis_post_url,
                                                             download_args={
                                                                 'data': post_data
                                                             },
                                                             policy=BRP.bill_documents,
                                                             serve_from_s3=True,
                                                             should_download=True,
                                                             download_method="POST",
                                                             should_skip_checks=True)
            assert download_id is not None
            doc_service_document = Doc_service_document(name, "summary", "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)


    @staticmethod
    def get_session_index(session):
        """
        returns the session index used for building bill urls
        :param session: session string in the format of "20152016r" or "20132014ss1"
        :return: "0" if regular session and index of special session otherwise
        """
        if session.endswith("r"):
            return "0"
        else:
            return session.split("ss")[1]

    @staticmethod
    def is_bill_id_valid(bill_id, session):
        """
        determines if the raw bill id should be included within this session
        :param bill_id: bill id as appears on the bill id search page, could be in the format of
                        "SB 1", "AB 1", "ABX1 1", "ABX2 1"
        :param session: session string in the format of "20152016r" or "20132014ss1"
        :return: True if both bill id and session are regular or special, and False otherwise
        """
        if session.endswith("r"):
            return "X" not in bill_id
        else:
            special_session_suffix = "X" + session.split("ss")[1]
            return special_session_suffix in bill_id

    @staticmethod
    def normalize_bill_id(bill_id):
        """
        removes the "X" part of the bill id, which facilitates bill page url construction
        :param bill_id: raw bill id, could be in the form of "AB 1", "ABX1 1" or "ABX2 1"
        :return: all examples above should return "AB 1"
        """
        return bill_id.split()[0].split("X")[0] + " " + bill_id.split()[1]

    @staticmethod
    def version_parser(entities, parser_args=None):

        if parser_args is None or "text" not in parser_args:
            logger.warning("A \"text\" field is required in order to parse version text! Falling back "
                           "to using entities content text. This SHOULD NOT happen if the parser function "
                           "is properly used though...")
            text = entities_text_content(entities)
        else:
            text = parser_args["text"]
        text = text.replace("\r\n", "\n")
        lines = [re.sub("\s+", " ", line).strip() for line in text.split("\n")]
        text = "\n".join(lines)
        return [ScraperDocument(text)]

from __future__ import absolute_import

import re
import logging
import datetime

import requests

from fn_scraperutils.doc_service.util import ScraperDocument
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id,
    normalize_bill_id)

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('VABillScraper')


actor_map = {"House": "lower",
             "Senate": "upper",
             "Governor": "executive",
             "Conference": "other"}

class VARetryPolicy(Retry500RequestsRetryPolicy):
    '''Adds support for HTTP400 status code response (DI-1176)'''
    def is_recoverable(self, err):
        '''Retry's on HTTP400'''
        if isinstance(err, requests.exceptions.HTTPError) and err.response.status_code == 400:
            return True
        return super(VARetryPolicy, self).is_recoverable(err)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-VA", group="fnleg")
class VABillScraper(BillScraper):
    def __init__(self):
        super(VABillScraper, self).__init__("va", retry_policy=VARetryPolicy())

    def scrape_bill_ids(self, session):
        bill_ids = {}
        session_external_ids = self.get_session_external_ids(session)
        for session_external_id in session_external_ids:
            bill_list_url = "http://lis.virginia.gov/cgi-bin/legp604.exe?{}+lst+ALL".format(session_external_id)
            reached_last_page = False
            while not reached_last_page:
                bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
                if "Sorry, your request could not be processed at this time" in bill_list_doc.text_content():
                    logger.warning("External id {} currently does not exist. This is likely because it is in "
                                   "the future... Skip this bill list for now.".format(session_external_id))
                    break
                elif "There are currently no bills matching this query." in bill_list_doc.text_content():
                    logger.warning("No bills currently for session id {}".format(session_external_id))
                    break

                # If many requests are called quickly, VA will sometimes be unable to handle it and show an error page.
                # This error page is a 200 though so we can't use a standard retry policy. It will cause occasional
                # failures on this xpath function.
                for anchor_tag in bill_list_doc.xpath("//div[@id='mainC']/ul[@class='linkSect'][1]/li/a"):
                    bill_id = anchor_tag.text_content()
                    url = anchor_tag.get_attrib("href")
                    # we only scrape the same bill id ONCE, and in the LATEST year
                    bill_ids[bill_id] = {"url": url}
                next_page_anchor_tags = bill_list_doc.xpath("//b[text()='More...']/parent::a[1]", BRP.test)
                if next_page_anchor_tags:
                    bill_list_url = next_page_anchor_tags[0].get_attrib("href")
                else:
                    reached_last_page = True
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):

        bill_info = kwargs.get("bill_info")
        bill_page_url = bill_info["url"]
        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        title_raw = bill_page_doc.xpath_single("//h3[@class='topLine']").text_content()
        title = re.split(r"\d+", title_raw, 1)[1].strip()
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_page_url)

        descr = bill_page_doc.xpath_single("//h4[contains(text(), 'SUMMARY AS')]/following-sibling::p").text_content()
        bill.add_summary(descr)

        # sponsors:
        sponsors_url = bill_page_doc.xpath_single("//a[text()='all patrons']").get_attrib("href")
        sponsors_doc = self.scraper.url_to_lxml(sponsors_url, BRP.bill_sponsors)
        sponsor_anchor_tags = sponsors_doc.xpath("//div[@id='mainC']//ul[@class='linkSect']/li/a")
        for sponsor_anchor_tag in sponsor_anchor_tags:
            sponsor = sponsor_anchor_tag.text_content().replace(u"\xa0", u" ")
            sponsor_name = re.sub(r"\(.*\)", "", sponsor).strip()
            if sponsor.endswith("(chief patron)"):
                bill.add_sponsor("primary", sponsor_name)
            else:
                bill.add_sponsor("cosponsor", sponsor_name)

        # summaries
        if bill_page_doc.xpath_single("//a[text()='(all summaries)']", BRP.test):
            summary_url = bill_page_doc.xpath_single("//a[text()='(all summaries)']", BRP.test).get_attrib("href")
            download_id, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(summary_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html,
                                                             False, self.summary_parser,
                                                             parser_args={"bill_id": bill_id})
            assert len(scraper_docs) == len(doc_ids)
            for index in range(0, len(scraper_docs)):
                scraper_doc = scraper_docs[index]
                doc_id = doc_ids[index]
                summary_title = scraper_doc.additional_data["summary_title"]
                doc_service_document = Doc_service_document(summary_title, "summary", "complete",
                                                            download_id, doc_id=doc_id)
                bill.add_doc_service_document(doc_service_document)

        # versions
        if "budget bill" in title.lower():
            # separate handling for budget bills
            budget_bill_info = self.build_budget_bill_info(session)
            for category in budget_bill_info[bill_id]:
                for name in budget_bill_info[bill_id][category]:
                    for file_format in budget_bill_info[bill_id][category][name]:
                        if name.startswith("Governor's"):
                            doc_type = "summary"
                        elif category == "bill":
                            doc_type = "version"
                        else:
                            doc_type = "amendment"
                        if file_format == "pdf":
                            # it does not make sense to me to extract any text from budget bills
                            # most of them are more than 100 pages and have a lot of tables, which
                            # can be messy after text extraction
                            url = budget_bill_info[bill_id][category][name][file_format]
                            download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                            doc_service_document = Doc_service_document(name,
                                                                        doc_type,
                                                                        "partial", download_id)
                            bill.add_doc_service_document(doc_service_document)
                        else:
                            url = budget_bill_info[bill_id][category][name][file_format]
                            download_id, _, doc_ids = \
                                self.scraper.register_download_and_documents(url, BRP.bill_documents,
                                                                             self.scraper.extraction_type.html,
                                                                             False, self.budget_bill_html_parser)
                            assert len(doc_ids) == 1
                            doc_id = doc_ids[0]
                            doc_service_document = \
                                Doc_service_document(name, doc_type, "complete", download_id, doc_id=doc_id)
                            bill.add_doc_service_document(doc_service_document)

        else:
            # ordinary bills
            # documents
            for li in bill_page_doc.xpath("//h4[text()='AMENDMENTS']/following-sibling::ul[@class='linkSect'][1]/li"):
                html_anchor_tag = li.xpath_single("./a[1]")
                if not html_anchor_tag:
                    logger.warning("A text title is provided on {} but there is no link provided. This is likely to be "
                                   "because the website has not updated yet. Skip it for now.".format(bill_page_url))
                    continue
                name = html_anchor_tag.text_content().strip()
                if "amendment" in name.lower():
                    try:
                        amendment_download_id, _, amendment_document_ids = self.scraper.register_download_and_documents(html_anchor_tag.get_attrib("href"), BRP.bill_documents,
                                                                                                                   self.scraper.extraction_type.html,
                                                                                                                   False, self.html_parser)
                    except:
                        logger.warning("Unable to parse amendment for link: {}".format(html_anchor_tag.get_attrib("href")))
                    else:
                        amendment_document = Doc_service_document(name, "amendment", "complete", amendment_download_id, doc_id=amendment_document_ids[0])
                        bill.add_doc_service_document(amendment_document)

            for li in bill_page_doc.xpath("//h4[text()='FULL TEXT']/following-sibling::ul[@class='linkSect'][1]/li"):
                html_anchor_tag = li.xpath_single("./a[1]")
                if not html_anchor_tag:
                    logger.warning("A text title is provided on {} but there is no link provided. This is likely to be "
                                   "because the website has not updated yet. Skip it for now.".format(bill_page_url))
                    continue
                name = html_anchor_tag.text_content().replace(u"\xa0", u" ").split(":", 1)[1].strip()
                html_url = html_anchor_tag.get_attrib("href")
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(html_url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.html,
                                                                 False, self.html_parser)
                assert len(doc_ids) == 1
                if name.startswith("Governor's"):
                    doc_type = "summary"
                else:
                    doc_type = "version"
                doc_service_document = Doc_service_document(name, doc_type, "complete", download_id, doc_id=doc_ids[0])
                pdf_download_id = self.scraper.download_and_register(li.xpath_single("./a[2]").get_attrib("href"),
                                                                     BRP.bill_documents, True)
                doc_service_document.add_alternate_representation(pdf_download_id)
                if li.xpath_single("./a[3]"):
                    impact_statement_url = li.xpath_single("./a[3]").get_attrib("href")
                    impact_statement_download_id = self.scraper.download_and_register(impact_statement_url,
                                                                                      BRP.bill_documents, True)
                    impact_statement_name = "Impact Statement - " + name
                    impact_statement_doc_service_document = Doc_service_document(impact_statement_name, "fiscal_note",
                                                                                 "partial",
                                                                                 impact_statement_download_id)
                    impact_statement_index = bill.add_doc_service_document(impact_statement_doc_service_document)
                    doc_service_document.add_child(impact_statement_index)
                bill.add_doc_service_document(doc_service_document)

        # actions
        for li in bill_page_doc.xpath("//h4[text()='HISTORY']/following-sibling::ul[@class='linkSect'][1]/li"):
            action_string = li.text_content().replace(u"\xa0", u" ")
            # History should have a format similar to
            # "01/25/18  House: Failed to report (defeated) in General Laws (11-Y 11-N)"
            # which has Date, Actor, Action.
            # Sometimes there will be "02/05/18  House:" which doesn't have an action and thus shouldn't be recorded.
            try:
                date_string, actor_string = re.search(r"(\d+/\d+/\d+)\s+([a-zA-Z]+):\s+", action_string).groups()
            except AttributeError:
                logger.warning("Could not parse action out of: {}".format(action_string))
                continue
            date = datetime.datetime.strptime(date_string, "%m/%d/%y")
            actor = actor_map[actor_string]
            action = action_string.split(":", 1)[1].strip()
            bill.add_action(actor, actor_string + ": " + action, date)
            if re.findall(r"\((\d+)-Y\s+(\d+)-N\)", action_string):
                vote_chamber = actor
                motion = re.sub(r"\(\d+-Y\s+\d+-N\)", "", action).strip()
                motion = "{}: {}".format(actor_string, motion)
                yes_voters = []
                no_voters = []
                other_voters = []
                yes_count, no_count = re.findall(r"(\d+)-Y\s+(\d+)-N", action)[0]
                yes_count = int(yes_count)
                no_count = int(no_count)
                other_count = 0
                passed = yes_count > no_count
                action_lower = action.lower()
                if "pass" in action_lower or "adopt" in action_lower or "agree" in action_lower:
                    passed = True
                elif "fail" in action_lower or "reject" in action_lower:
                    passed = False
                else:
                    logger.warning("Vote info for motion {} on {} does not clearly indicate if it has passed or not. "
                                   "Guessing result basing on the vote counts, but this might not be correct as some "
                                   "votes require a certain majority in order to pass".format(motion, bill_page_url))

                if not li.xpath_single(".//a"):
                    # no link for vote details, have to construct vote object immediately
                    # other count has to fall back to zero
                    vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
                    logger.warning("Vote for motion {} on {} does not have a link for vote details. This vote is "
                                   "build simply basing on the action text, and fallback value of \"0\" used for "
                                   "\"other_vote\" parameter".format(motion, bill_page_url))
                    bill.add_vote(vote)
                    continue

                vote_url = li.xpath_single(".//a").get_attrib("href")
                vote_doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
                for p_tag in vote_doc.xpath("//div[@id='mainC']/p"):
                    text = p_tag.text_content()
                    text = re.sub("\s+", " ", text)
                    if not re.findall(r"--(.+)--", text):
                        # this means this voter line is empty
                        continue
                    name_elements = [name_element.strip() for name_element
                                     in re.findall(r"--(.+)--", text)[0].split(",")
                                     if name_element.strip()]
                    names = []
                    for name_element in name_elements:
                        # if there is a Middle Initial, it indicates that the last name before it belongs to it
                        if name_element.endswith("."):
                            # example: http://lis.virginia.gov/cgi-bin/legp604.exe?161+vot+H0701V0001+HB0014
                            names[-1] = names[-1] + ", " + name_element
                        else:
                            names.append(name_element)

                    count = int(re.findall(r"--(\d+)", text)[0])
                    if count != len(names):
                        # verification on name and vote count
                        logger.warning("Voter/Vote_count discrepancy occurred on vote at {}.".format(vote_url))
                    if text.startswith("YEAS"):
                        for name in names:
                            yes_voters.append(name)
                        yes_count = count
                    elif text.startswith("NAYS"):
                        for name in names:
                            no_voters.append(name)
                        no_count = count
                    elif text.startswith("RULE"):
                        continue
                    else:
                        for name in names:
                            other_voters.append(name)
                        other_count += count

                # vote chamber has the same value as the action
                # but it shouldn't be executive
                assert vote_chamber != "executive"
                vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
                for voters, add_voter in [(yes_voters, vote.yes),
                                          (no_voters, vote.no),
                                          (other_voters, vote.other)]:
                    for voter in voters:
                        add_voter(voter)
                vote.add_source(vote_url)
                bill.add_vote(vote)
        self.save_bill(bill)

    @staticmethod
    def get_session_external_ids(session):
        """
        return all external session ids of this session
        this function does not guarantee that we get the external ids in DESCENDING order, which
        is needed to make sure we scrape the same bill id only in the latest year.
        The reversed sorting is done in the scrape_bill function for clarity.
        :param session: e.g. "20152016r"
        :return: "20152016r"==>["151", "161"], "20162016ss2"==>["163"]
        """
        external_ids = set()
        for year_abbr in [session[:4][2:], session[4:8][2:]]:
            external_id = "{}{}".format(year_abbr, 1 if "r" in session else (1 + int(session.split("ss")[1])))
            external_ids.add(external_id)
        return list(external_ids)

    @staticmethod
    def summary_parser(element_wrapper, parser_args=None):
        """
        parser function for bill summaries, multiple summaries live on the same webpage, so
        we return a list of scraper document and store the summary titles in additional_data
        :param parser_args: extra info for parser function
        :param element_wrapper: element_wrapper for summary webpage
        :return: list of scraper documents where the additional_data field stores the summary titles
        """
        summary_doc = element_wrapper
        scraper_docs = []
        bill_id = parser_args["bill_id"]
        for summary_title_doc in summary_doc.xpath(
                "//h4[contains(text(), 'SUMMARY AS') and contains(text(), ':')]"):
            summary_title = summary_title_doc.text_content().strip(":").strip().lower()
            summary_title = summary_title[0].upper() + summary_title[1:]
            summary_text = summary_title_doc.xpath_single("./following-sibling::p[1]").text_content()
            scraper_doc = ScraperDocument(summary_text, scraper_id="{} {}".format(bill_id, summary_title),
                                          additional_data={"summary_title": summary_title})
            scraper_docs.append(scraper_doc)
        return scraper_docs

    @staticmethod
    def html_parser(element_wrapper):
        text = element_wrapper.xpath_single("//div[@id='mainC']").text_content()
        # This element appears in almost all version/amendments and holds useless info like 'print version'
        if element_wrapper.xpath_single("//ul[@id='rtNav']"):
            text_splitter = element_wrapper.xpath_single("//ul[@id='rtNav']").text_content()
            text = text.split(text_splitter, 1)[1].strip()
        return [ScraperDocument(text)]

    def build_budget_bill_info(self, session):
        budget_bill_info = {}
        years = set()
        years.add(session[:4])
        years.add(session[4:8])
        categories = ["bill", "amendments"]
        session_index = "1" if session.endswith("r") else str(1 + int(session.split("ss")[1]))
        for year in years:
            for category in categories:
                budget_bill_page_url = "https://budget.lis.virginia.gov/{}/{}/{}/".format(category, year, session_index)
                response = requests.get(budget_bill_page_url)
                if response.url.split("://", 1)[1].lower().strip() != \
                        budget_bill_page_url.split("://", 1)[1].lower().strip():
                    logger.warning("Request to budge bill page {} has been redirected. This is likely to be because "
                                   "there is no budget bill for the specified year yet.".format(budget_bill_page_url))
                    # we have been redirected. This happens when a new session has no budget bills
                    # we will be redirected to the homepage, e.g. https://budget.lis.virginia.gov/default/2017/1/
                    continue
                budget_bill_page_doc = self.scraper.url_to_lxml(budget_bill_page_url, BRP.bill_documents)
                rows = budget_bill_page_doc. \
                    xpath("//div[@id='ctl00_ctl00_MainContent_MainContent_divBudgetBill']/table/tr")
                bill_id = None
                for index in range(0, len(rows)):
                    row = rows[index]
                    if row.get_attrib("class") == "colhdr":
                        pass
                    elif row.get_attrib("class") == "table-header":
                        bill_id = row.xpath_single("./th[1]/h4").text_content()
                        bill_id = normalize_bill_id(bill_id)
                    else:
                        assert bill_id is not None
                        if bill_id not in budget_bill_info:
                            budget_bill_info[bill_id] = {}
                        if category not in budget_bill_info[bill_id]:
                            budget_bill_info[bill_id][category] = {}
                        document_name = row.xpath_single("./td[1]").text_content()
                        if not row.xpath_single("./td[2]"):  # no pdf available
                            url = row.xpath_single("./td[1]/a").get_attrib("href")
                            if document_name not in budget_bill_info[bill_id][category]:
                                budget_bill_info[bill_id][category][document_name] = {}
                            budget_bill_info[bill_id][category][document_name]["html"] = url
                        elif row.xpath_single("./td[2]").get_attrib("onclick") is None:  # we have a drop down here
                            for anchor_tag in row.xpath("./td[2]//a"):
                                name_suffix = anchor_tag.text_content()
                                url = anchor_tag.get_attrib("href")
                                if document_name + " " + name_suffix not in budget_bill_info[bill_id][category]:
                                    budget_bill_info[bill_id][category][document_name + " " + name_suffix] = {}
                                budget_bill_info[bill_id][category][document_name + " " + name_suffix]["pdf"] = url
                        else:
                            button = row.xpath_single("./td[2]/button")
                            url = "http://budget.lis.virginia.gov{}". \
                                format(button.get_attrib("onclick").split(".open('")[1].split("');")[0])
                            if document_name not in budget_bill_info[bill_id][category]:
                                budget_bill_info[bill_id][category][document_name] = {}
                            budget_bill_info[bill_id][category][document_name]["pdf"] = url
        return budget_bill_info

    @staticmethod
    def budget_bill_html_parser(element_wrapper):
        text = element_wrapper. \
            xpath_single("//h5[@id='ctl00_ctl00_MainContent_MainContent_hSession']/parent::div").text_content()
        return [ScraperDocument(text)]

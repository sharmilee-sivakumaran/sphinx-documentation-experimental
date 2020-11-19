from __future__ import absolute_import

import datetime
import logging
import re
from collections import defaultdict
from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ScraperDocument

from requests.exceptions import HTTPError

from fn_scrapers.datatypes.bills.common import (
    Bill, BillScraper, Vote, BillReportingPolicy as BRP, Doc_service_document)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id)

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('IABillScraper')

HOST = "https://www.legis.iowa.gov"
BILL_PAGE_URL_TEMPLATE = "{}/legislation/BillBook?ga={}&ba={}"
PDF_API_TEMPLATE = "{host}/docs/publications/{doc_type}{version_abbr}/" \
                   "{ga}/{id}.{format}"
API_TEMPLATE = "{host}/docs/publications/{doc_type}{version_abbr}" \
               "/{ga}/attachments/{id}.{format}"
DONE = 1

BILL_ID_PATTERN = re.compile(r"\s*([A-Z\.]+\s+\d+)\s*")
DAY_PATTERN = re.compile(r"MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY|\d{1,2}\w{2} Day")
PAGE_NUMBER_PATTERN = re.compile(r'\d{1,3}')
HEADLINE_PATTERN = re.compile(r'JOURNAL OF THE')


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-IA", group="fnleg")
class IABillScraper(BillScraper):
    def __init__(self):
        super(IABillScraper, self).__init__("ia")

    def scrape_bill_ids(self, session):
        bill_ids = []
        general_assembly = self.get_general_assembly(session)
        bill_list_url = "https://www.legis.iowa.gov/legislation/BillBook?ga={}".format(general_assembly)
        logger.info("Scraping IA bill ids for session {} on {}".format(session, bill_list_url))
        doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        senate_select = doc.xpath_single("//select[@id='senateSelect']")
        house_select = doc.xpath_single("//select[@id='houseSelect']")
        for select in [senate_select, house_select]:
            options = select.xpath(".//option")
            for option in options:
                if option.text_content().lower() == "pick one":
                    continue
                bill_id = option.text_content()
                bill_ids.append(bill_id)
        logger.info("Finishing bill id scraping. A total of {} bill ids collected.".format(len(bill_ids)))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        general_assembly = self.get_general_assembly(session)

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        normalized_bill_id = bill_id
        bill_id = self.denormalize_bill_id(bill_id)
        new_bill_page_url = BILL_PAGE_URL_TEMPLATE.format(HOST, general_assembly, bill_id.replace(" ", ""))
        logger.info("Scraping bill {} on {}".format(bill_id, new_bill_page_url))
        new_bill_page_doc = self.scraper.url_to_lxml(new_bill_page_url, BRP.bill)
        new_history_url = new_bill_page_doc.xpath_single("//a[contains(., 'Bill History')]").get_attrib("href")
        new_history_url = new_history_url.replace(" ", "")
        logger.info("Grabbing history %s", new_history_url)
        new_history_doc = self.scraper.url_to_lxml(new_history_url, BRP.bill_actions)

        # study bills and regular bills have different markup on bill history page
        title = new_history_doc.xpath_single(
            "//table[contains(@class, 'billActionTable')]/preceding-sibling::div/div[last()]/div[2]").text_content()
        if not title:
            logger.warning("Bill {} on {} gives an empty bill title. Will skip it.".
                           format(normalized_bill_id, new_bill_page_url))
            return
        title = re.sub("\s+", " ", title)
        title = title[0].upper() + title[1:]

        bill = Bill(session, chamber, normalized_bill_id, title, bill_type)
        bill.add_source(new_bill_page_url)

        # Scrape all documents
        version_select = new_bill_page_doc.xpath_single("//select[@id='billVersions']")
        versions = [option.text_content()
                    for option in version_select.xpath(".//option")
                    if option.text_content().lower() != "pick one"]

        # get all version text
        for version in versions:
            version_abbr = version[0].lower()
            logger.info("Grabbing version %s", version_abbr)
            html_url = API_TEMPLATE.format(host=HOST, doc_type="LG", version_abbr=version_abbr, ga=general_assembly,
                                           id=bill_id.replace(" ", ""), format="html")
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(html_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html, False,
                                                             self.version_html_parser)
            version_doc_service_document = Doc_service_document(version, "version", "complete",
                                                                download_id, doc_ids[0])
            # TODO: remove alternate format download for speed
            # pdf_url = PDF_API_TEMPLATE.format(doc_type="LG", version_abbr=version_abbr, ga=ga,
            #                                   id=bill_id.replace(" ", ""), format="pdf")
            # rtf_url = API_TEMPLATE.format(doc_type="LG", version_abbr=version_abbr, ga=ga,
            #                               id=bill_id.replace(" ", ""), format="rtf")

            # TODO: remove alternate format download for speed
            # try:
            #     pdf_download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, False)
            #     rtf_download_id = self.scraper.download_and_register(rtf_url, BRP.bill_documents, False)
            #     version_doc_service_document.add_alternate_representation(pdf_download_id)
            #     version_doc_service_document.add_alternate_representation(rtf_download_id)
            # except requests.exceptions.HTTPError:
            #     logger.warning("Failed to download alternate representations for {} version text".format(bill_id))
            version_page_url = "{}&v={}".format(new_bill_page_url, version_abbr)
            version_page_doc = self.scraper.url_to_lxml(version_page_url, BRP.bill)

            logger.info("Completed scraping version.")

            amendment_select = version_page_doc.xpath_single("//select[@name='amendments']", BRP.bill_versions)

            if not amendment_select:
                # some bills may not have amendments at all
                bill.add_doc_service_document(version_doc_service_document)
                continue
            for option in amendment_select.xpath(".//option"):
                if option.text_content().lower().startswith("pick one"):
                    continue
                else:
                    # possible option text formats:
                    # S-1234
                    # S-1234 Obama
                    # S-1234 Obama, B.
                    # S-1234 (2nd)
                    # S-1234 (2nd) Obama
                    # S-1234 (2nd) Obama, B.
                    option_text = option.text_content()
                    if "(" in option_text and ")" in option_text:
                        amendment_id = option_text.split()[0]
                        text_parts = option_text.split(")")
                        amendment_display_name = text_parts[0] + ")"
                        supported_by = text_parts[1].strip()
                    else:
                        text_parts = option_text.split(" ", 1)
                        amendment_id = text_parts[0]
                        amendment_display_name = amendment_id
                        if len(text_parts) > 1:
                            supported_by = text_parts[1].strip()
                        else:
                            supported_by = None

                    if supported_by:
                        amendment_full_name = "Amendment {} ({}) as {}" \
                            .format(amendment_display_name, supported_by, version)
                    else:
                        amendment_full_name = "Amendment {} as {}".format(amendment_display_name, version)

                    # both reprinted and introduced use "I", and enrolled will use "E"
                    amdt_html_url = API_TEMPLATE.format(host=HOST, doc_type="AMD",
                                                        version_abbr="E" if version.lower == "enrolled" else "I",
                                                        ga=general_assembly, id=amendment_id.replace("-", ""),
                                                        format="html")
                    logger.info("Downloading html amendment %s", amdt_html_url)
                    # TODO: remove alternate format download for speed
                    # amdt_pdf_url = PDF_API_TEMPLATE.format(doc_type="AMD",
                    #                                        version_abbr="E" if version.lower == "enrolled" else "I",
                    #                                        ga=ga, id=amendment_id.replace("-", ""), format="pdf")
                    # amdt_rtf_url = API_TEMPLATE.format(doc_type="AMD",
                    #                                    version_abbr="E" if version.lower == "enrolled" else "I",
                    #                                    ga=ga, id=amendment_id.replace("-", ""), format="rtf")
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(amdt_html_url,
                                                                     BRP.bill_versions,
                                                                     self.scraper.extraction_type.html,
                                                                     False, self.version_html_parser)
                    amdt_doc_service_document = Doc_service_document(amendment_full_name, "amendment",
                                                                     "complete", download_id, doc_ids[0])
                    logger.info("Grabbed amendment")
                                    
                    # TODO: remove alternate format download for speed
                    # try:
                    #     amdt_pdf_download_id = self.scraper.download_and_register(amdt_pdf_url,
                    #                                                                BRP.bill_documents, False)
                    #     amdt_rtf_download_id = self.scraper.download_and_register(amdt_rtf_url,
                    #                                                               BRP.bill_documents, False)
                    #     amdt_doc_service_document.add_alternate_representation(amdt_html_download_id)
                    #     amdt_doc_service_document.add_alternate_representation(amdt_rtf_download_id)
                    # except requests.exceptions.HTTPError:
                    #     logger.warning("Failed to download alternate representations for {} to {}".
                    #                    format(amendment_id, bill_id))
                    amdt_index = bill.add_doc_service_document(amdt_doc_service_document)
                    version_doc_service_document.add_child(amdt_index)
            bill.add_doc_service_document(version_doc_service_document)
        # get CCR, if any
        ccr_row = new_bill_page_doc.xpath_single("//tr[contains(., 'CCR:')]", BRP.info)
        if ccr_row:
            ccr_cell = ccr_row.xpath_single(".//a")
            ccr_name = ccr_cell.text_content()
            # html_url = API_TEMPLATE.format(doc_type="AMD", version_abbr="I",
            #                                ga=ga, id=ccr_name.replace("-", ""), format="html")
            pdf_url = PDF_API_TEMPLATE.format(host=HOST, doc_type="AMD", version_abbr="I",
                                              ga=general_assembly, id=ccr_name.replace("-", ""), format="pdf")
            # rtf_url = API_TEMPLATE.format(doc_type="AMD", version_abbr="I",
            #                               ga=ga, id=ccr_name.replace("-", ""), format="rtf")
            logger.info("Registering PDF Amendment %s", pdf_url)
            pdf_download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
            # TODO: remove alternate format download for speed
            # html_download_id = self.scraper.download_and_register(html_url, BRP.bill_documents, False)
            # rtf_download_id = self.scraper.download_and_register(rtf_url, BRP.bill_documents, False)
            doc_service_document = Doc_service_document(ccr_name, "committee_document", "partial", pdf_download_id)
            # TODO: remove alternate format download for speed
            # doc_service_document.add_alternate_representation(html_download_id)
            # doc_service_document.add_alternate_representation(rtf_download_id)
            bill.add_doc_service_document(doc_service_document)

        has_sponsor = True
        # Scrape the sponsors of the bill from the by-line, if it exists.

        # if there are floor managers, those are the sponsors that we want. Otherwise we go for the other xpath
        # which will match sponsors or committees for other bills
        # https://www.legis.iowa.gov/legislation/billTracking/billHistory?billName=HF2236&ga=87
        sponsor_xpath = "//div[contains(text(), 'Floor Managers')]"
        # https://www.legis.iowa.gov/legislation/billTracking/billHistory?billName=HR116&ga=87
        sponsor2_xpath = "//table[contains(@class, 'billActionTable')]/preceding-sibling::div/div[last()]/div"
        if 'HSB' in bill_id or 'SSB' in bill_id:
            # Study Bills are sponsored by a whole committee.
            # sometimes, at least in the new version of the website, a study bill does not show sponsor
            # this is an example for HSB589:
            # https://www.legis.iowa.gov/legislation/billTracking/billHistory?billName=HSB589&ga=86
            try:
                sponsors = new_history_doc.xpath_single(sponsor_xpath) or new_history_doc.xpath_single(sponsor2_xpath)
                sponsors = sponsors.text_content()
                sponsor_pattern = r'.+'
            except AttributeError:
                has_sponsor = False
        else:

            sponsors = new_history_doc.xpath_single(sponsor_xpath) or new_history_doc.xpath_single(sponsor2_xpath)
            sponsors = sponsors.text_content()
            # Match any word or dash followed by comma, ' and', or '.'{endline}.
            # Possibly has a ", [A-Z]." in between.
            sponsor_pattern = r'[\w-]+(?:, [A-Z]\.)?(?:,|(?: and)|\.?$)'
        # add the sponsors to the bill.
        if has_sponsor:
            for sponsor in re.findall(sponsor_pattern, sponsors):
                sponsor = sponsor.replace('In ', '').replace(' and', '').replace('By ', '').strip(' .,').title()
                # a few sponsors get mangled by our regex
                sponsor = {
                    'Means': 'Ways & Means',
                    'Iowa': 'Economic Growth/Rebuild Iowa',
                    'Safety': 'Public Safety',
                    'Resources': 'Human Resources',
                    'Affairs': 'Veterans Affairs',
                    'Protection': 'Environmental Protection',
                    'Government': 'State Government',
                    'Boef': 'De Boef'}.get(sponsor, sponsor)
                bill.add_sponsor('primary', sponsor)
        else:
            logger.warning("No sponsor added to bill {}. This might be the result of faluty xpath or change of website "
                           "markup".format(bill_id))

        # actions
        action_table = new_history_doc.xpath_single("//table[contains(@class, 'billActionTable')]")
        action_rows = action_table.xpath("./tbody/tr")
        for action_row in action_rows:
            date_doc = action_row.xpath_single("./td[1]", BRP.bill_actions)
            if not date_doc or \
                    not date_doc.text_content() or \
                            "no history is recorded at this time" in date_doc.text_content().lower() or \
                            "data not available for the selected bill" in date_doc.text_content().lower():
                logger.warning("Missing action date on action table at {}. Skipping this action for now.".
                               format(new_history_url))
                continue
            else:
                date_string = date_doc.text_content()
            date = datetime.datetime.strptime(date_string, "%B %d, %Y")
            action = action_row.xpath_single("./td[3]")
            if not action:
                logger.warning(
                    "Missing action text on action table at %s. Skipping.",
                    new_history_url
                )
                continue
            action = re.sub(r'\s+', ' ', action.text_content())
            if 'Sent to Governor' in action or 'signed by' in action.lower():
                actor = 'executive'
            elif ('S.J.' in action or 'SCS' in action or 'senate' in action.lower()) and "house" not in action.lower():
                actor = 'upper'
            elif ('H.J.' in action or 'HCS' in action or "house" in action.lower()) and "senate" not in action.lower():
                actor = 'lower'
            else:
                actor = chamber
            journal_link = action_row.xpath_single("./action_row/td[2]/a")
            if journal_link:
                action = action.split(journal_link.text_content())[0].strip()
            if not action.strip():
                logger.warning("Action string is empty. Skipping it for now. {}".format(new_history_url))
                continue
            bill.add_action(actor, action, date)

            # check if vote info exists in the action
            raw_action_string = action_row.xpath_single("./td[2]").text_content()
            vote_outcome = re.findall(r"ayes\s+(\d+|none),\s+nays\s+(\d+|none)", raw_action_string)
            if not vote_outcome:
                continue
            if actor == 'executive':
                vote_actor = 'joint'
            else:
                vote_actor = actor
            yes_count = 0 if vote_outcome[0][0] == "none" else int(vote_outcome[0][0])
            no_count = 0 if vote_outcome[0][1] == "none" else int(vote_outcome[0][1])
            motion = raw_action_string.split("ayes")[0].strip().strip(",").strip()
            vote_result = yes_count > no_count  # fallback value
            if "pass" in raw_action_string.lower() or "agree" in raw_action_string.lower():
                vote_result = True
            elif "fail" in raw_action_string.lower():
                vote_result = False
            else:
                logger.warning("The action text for vote \"{}\" on {} did not clearly indicate whether the vote "
                               "has passed or not. Falling back to comparing yes vs. no counts, but this "
                               "might not be correct if the vote requires a certain majority.".
                               format(raw_action_string, new_history_url))

            vote_journal_link = action_row.xpath_single("./td[2]/a")
            if not vote_journal_link:
                logger.warning("vote info found in action \"{}\" on {}, but the website did not provide a link "
                               "that leads to the detailed voting info. We will just use the simple vote count. "
                               "No voter info could be provided and \"other\" votes are assumed to be zero.")
                
                vote = Vote(vote_actor, date, motion, vote_result, yes_count, no_count, other_count=0)
                vote.add_source(new_history_url)
                bill.add_vote(vote)
                continue
            vote_journal_url = vote_journal_link.get_attrib("href")
            logger.info("Scraping Vote Journal %s", vote_journal_url)
            vote_dict = self.scrape_journal(vote_journal_url, vote_actor, date)
            if bill_id not in vote_dict:
                logger.warning("On page {}. The action text contains a vote with motion \"{}\", but the journal "
                               "link it provides {} does not contain the vote information. We will just use the "
                               "simple vote count. No voter info could be provided and \"other\" votes are assumed to "
                               "be zero.".format(new_history_url, motion, vote_journal_url))
                vote = Vote(vote_actor, date, motion, vote_result, yes_count, no_count, other_count=0)
                vote.add_source(new_history_url)
                bill.add_vote(vote)
                continue
            elif len(vote_dict[bill_id]) > 1:
                vote_found = False
                for vote_candidate in vote_dict[bill_id]:
                    if vote_candidate["yes_count"] == yes_count and vote_candidate["no_count"] == no_count:
                        vote = vote_candidate
                        vote_found = True
                        break
                if not vote_found:
                    logger.warning(
                        "On page {}. The action text contains a vote in action text \"{}\", but the journal "
                        "link it provides {} contains more than one vote with the same bill id and none of them "
                        "has yes/no vote count that matches the action string. It's hard "
                        "for the scraper to determine which one is correct. Using the first one as a fallback "
                        "for now but it might be incorrect and results in duplicate vote info".
                            format(new_history_url, raw_action_string, vote_journal_url))
                    vote = vote_dict[bill_id][0]
            else:
                vote = vote_dict[bill_id][0]

            vote_keys = [("yes_count", "yes_votes"), ("no_count", "no_votes"), ("other_count", "other_votes")]
            should_skip = False
            for vote_key in vote_keys:
                if not isinstance(vote[vote_key[0]], int):
                    logger.warning("The vote count with motion \"{}\" ends up as an empty list. This indicates that "
                                   "the PDF parsing has failed. All voter info will the removed and all vote count "
                                   "falls back to the vote info from the action text".format(motion))
                    vote = Vote(vote_actor, date, motion, vote_result, yes_count, no_count, other_count=0)
                    vote.add_source(new_history_url)
                    bill.add_vote(vote)
                    should_skip = True
                    continue
            if should_skip:
                continue

            # validate vote count and overwrite vote motion
            if yes_count != vote["yes_count"]:
                logger.warning("Action text \"{}\" provided a yes count of {} but the corresponding journal "
                               "link gives {} yes count. Will use the one from action text.".format(raw_action_string,
                                                                                                    yes_count,
                                                                                                    vote["yes_count"]))
                vote["yes_count"] = yes_count
            if no_count != vote["no_count"]:
                logger.warning("Action text \"{}\" provided a no count of {} but the corresponding journal "
                               "link gives {} no count. Will use the one from action text.".format(raw_action_string,
                                                                                                   no_count,
                                                                                                   vote["no_count"]))
                vote["no_count"] = no_count
            if vote_result != vote["passed"]:
                logger.warning("Action text \"{}\" indicates the vote has {}, but info from journal link indicates "
                               "the vote {}. Using the result from action text".
                               format(raw_action_string,
                                      "passed" if vote_result else "not passed",
                                      "passed" if vote["passed"] else "not passed"))
                vote["passed"] = vote_result
            # overwrite motion and date
            vote["motion"] = motion
            vote["date"] = date

            bill.add_vote(vote)

        # related bill info
        post_data = {"ga": str(general_assembly),
                     "billName": normalized_bill_id,
                     "action": "getBillRelatedInfo",
                     "bl": "false",
                     "billVersion": "i"}

        logger.info("Grabbing related: %s", post_data)
        related_info_doc = self.scraper.url_to_lxml("https://www.legis.iowa.gov/legislation/BillBook",
                                                    BRP.bill_documents, method="POST",
                                                    request_args={"data": post_data})
        for row in related_info_doc.xpath("//table/tbody/tr[contains(., 'Track Versions')]/following-sibling::tr", policy=BRP.debug):
            if row.xpath(".//strong", policy=BRP.info) or not row.text_content():
                break
            related_bill_id = self.normalize_bill_id(row.xpath_single(".//a").text_content())
            if related_bill_id != normalized_bill_id:
                bill.add_companion(related_bill_id)
        for row in related_info_doc.xpath("//table/tbody/tr[contains(., 'Related Documents')]/following-sibling::tr"):
            if row.xpath(".//strong", policy=BRP.info) or not row.text_content():
                break
            name = row.text_content()
            # documents that we don't want. making it a set such that we can extend it.
            if name in ['Summary of Legislation', 'Bill Subject Index']:
                continue
            url = row.xpath_single(".//a").get_attrib("href")
            document_label = self.get_document_label(name)
            logger.info("Scraping %s: %s", document_label, url)
            if document_label == "version":
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf, True)
                assert download_id is not None and len(doc_ids) == 1
                doc_service_document = Doc_service_document(name, document_label, "complete",
                                                            download_id, doc_id=doc_ids[0])
            else:
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                doc_service_document = Doc_service_document(name, document_label, "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    def scrape_journal(self, journal_url, chamber, date):
        # this dictionary maps a bill id to its vote objects
        # half of these code is legacy code which handles the vote fine.
        vote_dict = {}
        extract_type = self.scraper.extraction_type.text_pdf
        starting_page_match = re.findall(r"page=(\d+)", journal_url.lower())
        if not starting_page_match:
            starting_page = None
        else:
            starting_page = starting_page_match[0]
        journal_url = re.sub(r'#page=\d+', '', journal_url)
        try:
            scraper_docs, doc_ids = self.scraper.handle_file(journal_url, BRP.bill_votes, extract_type, False,
                                                             self.journal_parser,
                                                             parser_args={"starting_page": starting_page})
        except HTTPError:
            logger.warning("Failed to extract journal text on {}. The vote data will not be collected.".
                           format(journal_url))
            return vote_dict
        text = scraper_docs[0].text
        lines = [re.sub("\s+", " ", line).strip()
                 for line in text.replace("\r\n", "").split("\n")
                 if re.sub("\s+", " ", line).strip()]
        for index in range(0, len(lines)):
            line = lines[index]
            # check if this line indicates the begging of a vote section
            if not self.is_motion_line(line):
                continue
            else:
                motion_line = line

            if BILL_ID_PATTERN.findall(motion_line):
                bill_id = BILL_ID_PATTERN.findall(motion_line)[0]
            else:
                # we want to go to the next line to find the bill_id
                index += 1
                line = lines[index]
                # there might be empty lines we need to skip
                while line == "":
                    index += 1
                    if index >= len(lines):
                        break
                    line = lines[index]
                # now we try to extract a bill_id from this line
                bill_id_found = BILL_ID_PATTERN.findall(line)
                if bill_id_found:
                    bill_id = bill_id_found[0]
                else:
                    logger.warning("Found vote motion but not bill id on line {} in {}, "
                                   "skipping".format(motion_line, journal_url))
                    # we simply skip it and start over the process
                    continue

            # extract motion
            motion = self.normalize_motion(motion_line)
            bill_id = bill_id.replace('.', '').replace(' ', '')

            # now get the vote yes/no counts
            counts = defaultdict(list)
            while index + 1 < len(lines):
                index += 1
                line = lines[index]
                text = line
                if self.is_boundary(text):
                    break

            while index + 1 < len(lines):
                # first check if this section of vote info has ended
                key = self.is_boundary(text)
                if key is DONE:
                    # if ended, break out of the loop, vote info should have already been collected
                    break
                # if we are not "DONE" yet, we try to find a number in the line representing the vote count
                m = re.search(r'\d+', text)
                # if we cannot find a vote count, there are a few things we can try
                if not m:
                    # if a "none" is found, it means the vote count is zero
                    if "none" in text.lower():
                        vote_count = 0
                    # if cannot find "none" either, try skipping lines until a vote count pattern is found
                    else:
                        while index + 1 < len(lines):
                            index += 1
                            line = lines[index]
                            text = line
                            if text.strip() in {"", ","}:
                                continue
                        m = re.search(r"\d+", text)
                        if not m:
                            logger.warning("Failed to read vote number - {} - {}".format(journal_url, bill_id))
                            raise
                        else:
                            vote_count = int(m.group())
                else:
                    vote_count = int(m.group())
                if key != 'skip':
                    counts['%s_count' % key] = vote_count
                # get to the next line to capture voters' names
                while index + 1 < len(lines):
                    index += 1
                    line = lines[index]
                    text = line
                    # if we get to the end of the vote section, everything should have been done
                    if self.is_boundary(text):
                        break
                    elif not text.strip() or text.strip().isdigit() or text.strip() == ':':
                        continue
                    elif DAY_PATTERN.search(text) or PAGE_NUMBER_PATTERN.search(
                            text) or HEADLINE_PATTERN.search(text):
                        continue
                    else:
                        for name in self.split_names(text):
                            counts['%s_votes' % key].append(name.strip())

            totals = filter(lambda x: isinstance(x, int), counts.values())
            passed = (1.0 * counts['yes_count'] / sum(totals)) >= 0.5
            yes_count = counts["yes_count"]
            no_count = counts["no_count"]
            other_count = counts["other_count"]
            vote = Vote(chamber, date, motion, passed, yes_count, no_count, other_count)
            vote.add_source(journal_url)
            for legislator in counts["yes_votes"]:
                vote.yes(legislator)
            for legislator in counts["no_votes"]:
                vote.no(legislator)
            for legislator in counts["other_votes"]:
                vote.other(legislator)
            if bill_id not in vote_dict:
                vote_dict[bill_id] = []
            vote_dict[bill_id].append(vote)
        return vote_dict

    @staticmethod
    def get_general_assembly(session):
        # get the general assembly from session string (e.g. get 86 from "20152016r")
        # reference: https://www.legis.iowa.gov/, in IOWA ACTS box
        # this can also be resolved by simply building a static dictionary
        end_year = int(session[4:8])
        return end_year/2 - 922

    @staticmethod
    def denormalize_bill_id(bill_id):
        # convert "HB 123" to "HB123"
        return bill_id.replace(" ", "")

    @staticmethod
    def normalize_bill_id(bill_id):
        # convert "SB123" to "SB 123"
        numbers = re.findall(r"\d+", bill_id)[0]
        bill_id = bill_id.replace(numbers, " {}".format(numbers))
        bill_id = re.sub(r"\s+", " ", bill_id)
        return bill_id

    @staticmethod
    def is_boundary(text):
        DONE = 1
        boundaries = [
            # Senate journal.
            ('Yeas', 'yes'),
            ('Nays', 'no'),
            ('Absent', 'other'),
            ('Present', 'skip'),
            ('Amendment', DONE),
            ('Resolution', DONE),
            ('Bill', DONE),
            ('Amen', DONE),
            # House journal.
            ('The ayes were', 'yes'),
            ('The yeas were', 'yes'),
            ('The nays were', 'no'),
            ('Absent or not voting', 'other'),
            ('The bill', DONE),
            ('The committee', DONE),
            ('The resolution', DONE),
            ('The motion', DONE),
            ('The joint resolution', DONE),
            ('Under the', DONE)]
        for blurb, key in boundaries:
            if text.strip().startswith(blurb):
                return key

    @staticmethod
    def is_motion_line(line):
        return ("Shall" in line) \
               and (("bill pass?" in line) or ("resolution" in line) or ("amendment" in line))

    @staticmethod
    def normalize_motion(motion_line):
        motion = motion_line.strip()
        motion = re.sub(r'\s+', ' ', motion)
        if "(" in motion:
            motion, _ = motion.rsplit('(', 1)
        motion = motion.replace('"', '')
        motion = motion.replace(u'"', '')
        motion = motion.replace(u'\u201d', '')
        motion = motion.replace(u"\u201c", "")
        motion = motion.replace(u"\u2013", "-")
        motion = motion.replace(u' ,', ',')
        motion = motion.strip()
        motion = re.sub(r'[SH].\d+', lambda m: ' %s ' % m.group(), motion)
        motion = re.sub(r'On the question\s*', '', motion, flags=re.I)

        return motion

    @staticmethod
    def split_names(text):
        # legacy code for name processing. Reusing it as it works fine.
        junk = ['Presiding', 'Mr. Speaker', 'Spkr.', '.']
        text = text.strip()
        chunks = text.split()[::-1]
        name = [chunks.pop()]
        names = []
        while chunks:
            chunk = chunks.pop()
            if len(chunk) < 3:
                name.append(chunk)
            elif name[-1] in ('Mr.', 'Van', 'De', 'Vander'):
                name.append(chunk)
            else:
                name = ' '.join(name).strip(',')
                if name and (name not in names) and (name not in junk):
                    names.append(name)

                # Seed the next loop.
                name = [chunk]

        # Similar changes to the final name in the sequence.
        name = ' '.join(name).strip(',')
        if names and len(name) < 3:
            names[-1] += ' %s' % name
        elif name and (name not in names) and (name not in junk):
            names.append(name)
        return names

    @staticmethod
    def version_html_parser(element_wrapper):
        text = element_wrapper.xpath_single("//body").text_content()
        return [ScraperDocument(text)]

    def journal_parser(self, entities, parser_args=None):
        if not parser_args["starting_page"] \
                or self.get_entity_page_number(entities[0]) == parser_args["starting_page"]:
            return [ScraperDocument(entities_text_content(entities))]
        for index in range(0, len(entities)):
            entity = entities[index]
            if self.get_entity_page_number(entity) == int(parser_args["starting_page"]) - 1:
                entities = entities[index:]
                break
        return [ScraperDocument(entities_text_content(entities))]

    @staticmethod
    def get_entity_page_number(entity):
        if entity.textEntity:
            page_number = entity.textEntity.pageNum
        elif entity.headerEntity:
            page_number = entity.headerEntity.pageNum
        elif entity.tableEntity:
            page_number = entity.tableEntity.pageNum
        else:
            page_number = entity.errorEntity.pageNum
        return page_number

    @staticmethod
    def get_document_label(name):
        name = name.lower()
        if "acts chapter" in name:
            return "version"
        elif "summary" in name:
            return "summary"
        else:
            return "other"

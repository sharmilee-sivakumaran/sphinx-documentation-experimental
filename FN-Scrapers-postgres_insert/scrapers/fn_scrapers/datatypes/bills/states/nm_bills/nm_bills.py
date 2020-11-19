from __future__ import absolute_import
import datetime
import logging
import re
import sys

from fn_document_service.blocking.ttypes import ColumnSpec
from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scrapers.datatypes.bills.common import (
    Bill, BillReportingPolicy as BRP, BillScraper, Doc_service_document, Vote)


from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id)
from .nm_legislator_day_mapping import dates
from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('NMBillScraper')

tab_title_map = {"Analysis": "Analysis",
                 "Committee Reports & Amendments": "Reports",
                 "Proposed Amendment": "Amendments",
                 "Gov. Vetoes": "Vetoes",
                 "Floor Amendments": "FloorReports",
                 "Votes": "Votes"}


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-NM", group="fnleg")
class NMBillScraper(BillScraper):
    sheet_vote_pattern = re.compile(
        r"([A-Z])?\s+((?:[^\s]{2,}(?:,?\s*(?:[^\s.]{2,}))*(?:,?\s*(?:[A-Z]\.))*))"
    )

    def __init__(self):
        super(NMBillScraper, self).__init__('nm')

    # Matches the legacy "[1] foo" as well as new style "Legislative Day: 2 bar"
    day_pattern = re.compile(r"^(?:\[(?=\d+\])|Legislative Day:\s*)(\d+)\]?\s*")
    calendar_day_pattern = re.compile(r"Calendar Day:?\s*(\d{2}/\d{2}/\d{4})")

    def scrape_bill_ids(self, session):
        search_page_url = "https://www.nmlegis.gov/Legislation/Legislation_List"
        search_page_doc = self.scraper.url_to_lxml(search_page_url, BRP.bill_list)
        start = end = self.get_session_external_id(session)
        page_count = 1
        current_page = 1
        checked_page_count = False
        bill_list_page_doc = search_page_doc
        bill_ids = {}
        while current_page <= page_count:
            form = {"__EVENTTARGET": "ctl00$MainContent$gridViewLegislation",
                    "__EVENTARGUMENT": "" if current_page == 1 else "Page${}".format(current_page),
                    "__VIEWSTATE": bill_list_page_doc.xpath_single("//input[@id='__VIEWSTATE']").get_attrib("value"),
                    "__VIEWSTATEGENERATOR":
                        bill_list_page_doc.xpath_single("//input[@id='__VIEWSTATEGENERATOR']").get_attrib("value"),
                    "__EVENTVALIDATION":
                        bill_list_page_doc.xpath_single("//input[@id='__EVENTVALIDATION']").get_attrib("value"),
                    "ctl00$MainContent$ddlSessionStart": start,
                    "ctl00$MainContent$ddlSessionEnd": end,
                    "ctl00$MainContent$chkSearchBills": "on",
                    "ctl00$MainContent$chkSearchMemorials": "on",
                    "ctl00$MainContent$chkSearchResolutions": "on",
                    "ctl00$MainContent$ddlResultsPerPage": 1000}
            if current_page == 1:
                form["ctl00$MainContent$btnSearch"] = "Go"
            bill_list_page_doc = self.scraper.url_to_lxml(search_page_url,
                                                          BRP.bill_list,
                                                          method="POST",
                                                          request_args={"data": form})
            if not checked_page_count:
                span = bill_list_page_doc.xpath_single("//span[@id='MainContent_lblRecordCount']")
                text = span.text_content()
                total_result_count = int(re.search(r"Displaying (?:\d+) of (\d+) result", text).group(1))
                page_count = total_result_count / 1000
                if total_result_count % 1000 != 0:
                    page_count += 1
                checked_page_count = True
            logger.info("Scraping bill ids on page {}/{}".format(current_page, page_count))
            rows = bill_list_page_doc.xpath("//table[@id='MainContent_gridViewLegislation']//tr")
            for row in rows:
                tds = row.xpath("./td", BRP.test)
                if tds and re.search(r"[A-Z]+ \d+", tds[0].text_content()):
                    bill_id = tds[0].text_content().replace("*", "").strip()
                    url = tds[0].xpath_single(".//a").get_attrib("href")
                    bill_ids[bill_id] = {"url": url}
            current_page += 1
        logger.info("A total of {} bill ids scraped for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        # basic bill info
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        bill_page_url = kwargs.get("bill_info")["url"]
        logger.info("Scraping bill {} at {}".format(bill_id, bill_page_url))
        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        if "No results found" in bill_page_doc.text_content():
            logger.warning("It seems there is no content on bill page {}. Skipping it for now".format(bill_page_url))
            return
        title = bill_page_doc.xpath_single("//span[@id='MainContent_formViewLegislation_lblTitle']").text_content()
        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_page_url)

        # scrape sponsors
        primary_sponsors = []
        primary_sponsor_links = bill_page_doc.xpath("//a[contains(@id,'MainContent_formViewLegislation_linkSponsor')]",
                                                    BRP.test)
        if primary_sponsor_links:
            for primary_sponsor_link in primary_sponsor_links:
                primary_sponsor_name = primary_sponsor_link.text_content()
                if primary_sponsor_name:
                    primary_sponsors.append(primary_sponsor_name)
                    bill.add_sponsor("primary", primary_sponsor_name)

        # scrape cosponsors
        sponsor_count_doc = bill_page_doc.xpath_single("//span[@id='MainContent_tabContainerLegislation"
                                                       "_tabPanelSponsors_lblSponsorsCount']",
                                                       BRP.bill_sponsors)
        if sponsor_count_doc and sponsor_count_doc.text_content() != "0":
            sponsors = bill_page_doc.xpath("//span[contains(@id,'MainContent_tabContainerLegislation"
                                           "_tabPanelSponsors_dataListSponsors_lblSponsorName_')]", BRP.bill_sponsors)
            for sponsor in sponsors:
                sponsor_name = sponsor.text_content()
                if sponsor_name not in primary_sponsors:
                    bill.add_sponsor("cosponsor", sponsor_name)

        # DOM element id template for version and amendment document scraping
        version_id_template = "MainContent_formView{}"

        # scrape introduced version
        introduced_pdf_url = None
        introduced_date = None
        introduced_version_id = version_id_template.format("LegislationTextIntroduced")
        introduced_version_table = bill_page_doc.xpath_single("//table[@id='{}']".format(introduced_version_id))
        anchor_tags = introduced_version_table.xpath(".//a", BRP.test)

        if anchor_tags:
            introduced_pdf_url = anchor_tags[0].get_attrib("href")
            introduced_date_raw = anchor_tags[0].xpath_single("./span[2]").text_content()
            introduced_date = datetime.datetime.strptime(introduced_date_raw, "%m/%d/%y")
            introduced_version_name = self.normalize_dated_doc_name(anchor_tags[0]).replace(" (PDF)", "")
            introduced_html_url = anchor_tags[1].get_attrib("href")
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(introduced_html_url, BRP.bill_versions,
                                                             self.scraper.extraction_type.html,
                                                             False, self.html_parser)
            if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                doc_service_document = Doc_service_document(introduced_version_name, "version",
                                                            "complete", download_id, doc_ids[0])
                # TODO: removed pdf download for faster scraping
                # pdf_download_id = self.scraper.download_and_register(introduced_pdf_url, BRP.bill_versions, True)
                # doc_service_document.add_alternate_representation(pdf_download_id)
                bill.add_doc_service_document(doc_service_document)
            else:
                logger.warning("Failed to process file at url {}".format(introduced_html_url))

        # actions
        action_table = bill_page_doc.xpath_single(
            "//table[@id='MainContent_tabContainerLegislation_tabPanelActions_dataListActions']")
        if action_table is not None:
            rows = action_table.xpath("./tr", BRP.debug)
            rows = [row for row in rows if row.text_content()]
            # sometimes, the first action does not have a date, in this case we fall back to the introduced version
            # date, if the introduced version date is not available for any reason, we fall back to the date of the
            # next available action date, if there is no next action, then, we simply fallback to
            # the first legislative date

            # meanwhile, the last action sometimes ends up not having an action date, but instead has a date
            # append to the end of the action, which needs special handling
            year = session[:4]


            max_lower = max([int(d) for d in dates[year]["lower"].keys()])
            max_upper = max([int(d) for d in dates[year]["upper"].keys()])
            max_dict = {"upper": max_upper, "lower": max_lower}

            # If no day is provided, we have to assume that the date of an action is the date of the previous action.
            previous_date = None

            # Additionally, for executive actions we need to assume that the date if not provided is simply the previous
            # date from either chamber.
            prev_actor = None

            actor = chamber

            for index in range(0, len(rows)):
                action_string_raw = rows[index].text_content()
                # first thing, determine actor
                # first check if we can find a committee abbreviation that starts with either H or S
                # if not, try to look if the word "senate" or "house" is in the action string, if both word
                # appears, fallback to the chamber of the chamber of the bill
                if 'Signed by Governor' in action_string_raw or 'Pocket Veto' in action_string_raw or \
                                'Vetoed by' in action_string_raw:
                    actor = 'executive'
                # If signed by the legislature, the actor is the chamber where the bill was introduced
                elif 'Signed by one or both houses' in action_string_raw or \
                                'Motion to reconsider adopted' in action_string_raw:
                    actor = prev_actor
                elif re.findall(r"[SH][A-Z]*C", action_string_raw):
                    committee_match = re.findall(r"[SH][A-Z]*C", action_string_raw)
                    actor = "upper" if committee_match[0][0] == "S" else "lower"
                elif "House" in action_string_raw and "Senate" not in action_string_raw:
                    actor = "lower"
                elif "Senate" in action_string_raw and "House" not in action_string_raw:
                    actor = "upper"
                elif "by the Senate" in action_string_raw:
                    actor = 'upper'
                elif "by the House" in action_string_raw:
                    actor = 'lower'

                matched_date_pattern = (
                    self.day_pattern.search(action_string_raw) or
                    self.calendar_day_pattern.search(action_string_raw)
                )

                # it is needed for cases where action string does not have an action date
                # e.g. https://www.nmlegis.gov/Legislation/Legislation?chamber=S&legType=B&legNo=26&year=16
                # the "action postponed indefinitely", in this case, has a fallback date same as its previous action [2]
                if not matched_date_pattern:
                    if index == 0:
                        # check the first action to see if special handling is needed
                        if introduced_date is not None:
                            date = introduced_date
                        elif len(rows) >= 2 and self.day_pattern.search(rows[1].text_content()):
                            next_action_string = rows[1].text_content()
                            next_action_date = self.day_pattern.search(next_action_string).group(1)
                            date = dates[year][actor][next_action_date][-1]
                        else:
                            date = dates[year][actor]["1"][-1]
                    else:
                        # check the last action to see if special handling is needed
                        date_string = action_string_raw.split("-")[-1].strip().replace(".", "")
                        date_string += ", {}".format(year)
                        try:
                            date = datetime.datetime.strptime(date_string, "%b %d, %Y")
                        except ValueError:
                            date = previous_date
                elif self.calendar_day_pattern.search(action_string_raw):
                    # NM now provides actual calendar days. This was added for the 2018
                    # session, so we need to preserve the complex legislative day logic
                    # for older sessions. Additionally, we can fall back to legislative
                    # days if no calendar day is provided.
                    calendar_day_str = self.calendar_day_pattern.search(action_string_raw).group(1)
                    date = datetime.datetime.strptime(calendar_day_str, u"%m/%d/%Y").date()
                else:
                    # ordinary action string handling
                    date_index = self.day_pattern.search(action_string_raw).group(1)

                    # If the actor is the executive, but there is a day provided, we need to use the previous actor's
                    # calendar.
                    if actor == 'executive' and prev_actor:
                        date_actor = prev_actor
                    else:
                        date_actor = actor
                    # Before we try and get the date, do a sanity check to make sure it isn't outside the scope of the
                    # calendar for that chamber.
                    if int(date_index) > max_dict[date_actor]:
                        date_actor = 'lower' if date_actor == 'upper' else 'upper'
                    try:
                        date = dates[year][date_actor][date_index][-1]
                    except KeyError as e:
                        # append the keys to the message while preserving traceback
                        # https://stackoverflow.com/questions/6062576/adding-information-to-an-exception
                        raise type(e), type(e)(e.message + ' [{}][{}][{}]'.format(
                            year, date_actor, date_index
                        )), sys.exc_info()[2]
                action = self.day_pattern.sub("", action_string_raw)
                action = self.calendar_day_pattern.sub("", action)
                bill.add_action(actor, action, date)

                previous_date = date
                prev_actor = actor

                # If we can't get the actor from the next action, but we know that this action was the bill passing one
                # chamber, we make the actor for the next action the other chamber.
                if 'Passed' in action:
                    actor = 'lower' if actor == 'upper' else 'upper'

        # scrape if there is "final" version text
        final_version_id = version_id_template.format("LegislationTextFinal")
        final_version_table = bill_page_doc.xpath_single("//table[@id='{}']".format(final_version_id))
        if final_version_table and final_version_table.xpath_single(".//a", BRP.test):
            anchor_tag = final_version_table.xpath_single(".//a", BRP.test)
            # we have to build final version url using introduced version url
            # because the button for final version actually triggers a javascript function
            if introduced_pdf_url is None:
                logger.critical("Cannot get final version url because introduced version url is not "
                                "available on page {}. We have to build the final version url using the "
                                "introduced version url".format(bill_page_url))
            else:
                url_pieces = introduced_pdf_url.rsplit("/", 3)
                final_pdf_url = url_pieces[0] + "/final/" + url_pieces[-1]
                final_version_name = self.normalize_dated_doc_name(anchor_tag).replace(" (PDF)", "")
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(
                        final_pdf_url, BRP.bill_versions,
                        self.scraper.extraction_type.text_pdf, True)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc_service_document = Doc_service_document(
                        final_version_name, "version", "complete", download_id,
                        doc_id=doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
                else:
                    logger.warning("Failed to process file at url {}".format(final_pdf_url))

        committee_sub_table = bill_page_doc.xpath(
            "//table[@id='MainContent_dataListLegislationCommitteeSubstitutes']//a",
            BRP.debug)
        for comm_row in committee_sub_table:
            comm_url = comm_row.get_attrib('href')
            comm_span = comm_row.xpath('./span')
            comm_name = comm_span[0].text_content()
            comm_date = comm_span[1].text_content()
            comm_name = "%s (%s)" % (comm_name, comm_date)
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(comm_url, BRP.bill_versions,
                                                             self.scraper.extraction_type.text_pdf, True)
            if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                doc_service_document = Doc_service_document(comm_name, "version", "complete", download_id, doc_ids[0])
                bill.add_doc_service_document(doc_service_document)


        # scrape if there is amendment
        amendment_id = version_id_template.format("AmendmentsInContext")
        amendment_table = bill_page_doc.xpath_single("//table[@id='{}']".format(amendment_id), BRP.test)
        if amendment_table and amendment_table.xpath_single(".//a", BRP.test):
            anchor_tag = amendment_table.xpath_single(".//a", BRP.test)
            url = anchor_tag.get_attrib("href")
            name = self.normalize_dated_doc_name(anchor_tag)
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf, True)
            if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                doc_service_document = Doc_service_document(name, "amendment", "complete", download_id, doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
            else:
                logger.warning("Failed to process file at url {}".format(url))

        # scrape committee reports
        committee_report_tab_title = "Committee Reports & Amendments"
        if self.tab_item_exists(bill_page_doc, committee_report_tab_title):
            anchor_tags = self.get_document_anchor_tags(bill_page_doc, committee_report_tab_title)
            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                name = self.normalize_dated_doc_name(anchor_tag)
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                if download_id is not None:
                    doc_service_document = Doc_service_document(name, "committee_document", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        # scrape analysis documents
        analysis_tab_title = "Analysis"
        if self.tab_item_exists(bill_page_doc, analysis_tab_title):
            anchor_tags = self.get_document_anchor_tags(bill_page_doc, analysis_tab_title)
            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                name = anchor_tag.text_content()
                doc_type = "fiscal_note" if "Fiscal Impact Report" in name else "other"
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                if download_id is not None:
                    doc_service_document = Doc_service_document(name, doc_type, "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        # scrap proposed amendments
        proposed_amendment_tab_title = "Proposed Amendment"
        if self.tab_item_exists(bill_page_doc, proposed_amendment_tab_title):
            # I never saw a single example where this tab contains text
            # therefore the following code may break

            anchor_tags = self.get_document_anchor_tags(bill_page_doc, proposed_amendment_tab_title)
            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                name = anchor_tag.text_content()
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf, True)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc_service_document = Doc_service_document(name, "amendment", "complete",
                                                                download_id, doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
                else:
                    logger.warning("Failed to process file at url {}".format(url))

        # scrape vetoes
        veto_tab_title = "Gov. Vetoes"
        if self.tab_item_exists(bill_page_doc, veto_tab_title):
            anchor_tags = self.get_document_anchor_tags(bill_page_doc, veto_tab_title)
            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                name = self.normalize_dated_doc_name(anchor_tag)
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                if download_id is not None:
                    doc_service_document = Doc_service_document(name, "other", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        # scrape floor reports
        floor_report_tab_title = "Floor Amendments"
        if self.tab_item_exists(bill_page_doc, floor_report_tab_title):
            anchor_tags = self.get_document_anchor_tags(bill_page_doc, floor_report_tab_title)

            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                name = self.normalize_dated_doc_name(anchor_tag)
                if not name:
                    name = anchor_tag.text_content()
                suffix_doc = anchor_tag.getparent().getparent().getparent()
                if suffix_doc:
                    suffix_id = suffix_doc.get_attrib("id").lower()
                    if "proposed" in suffix_id:
                        suffix = " - Proposed"
                    elif "adopted" in suffix_id:
                        suffix = " - Adopted"
                    elif "failed" in suffix_id:
                        suffix = " - Not Adopted"
                    else:
                        # fallback, if no identifier found in the id
                        suffix = ""
                    name = "{}{}".format(name, suffix)

                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.text_pdf, True)
                if len(doc_ids) == 1 and doc_ids[0] is not None:
                    # for text pdf amendments, this works
                    doc_service_document = Doc_service_document(name, "amendment", "complete",
                                                                download_id, doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
                elif download_id is not None:
                    # for image pdf, download will succeed but extraction will fail, so download id is not none
                    doc_service_document = Doc_service_document(name, "amendment", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)
                else:
                    logger.warning("Failed to process file at url {}".format(url))

        # scrape votes
        vote_tab_title = "Votes"
        if self.tab_item_exists(bill_page_doc, vote_tab_title):
            anchor_tags = self.get_document_anchor_tags(bill_page_doc, vote_tab_title)
            for anchor_tag in anchor_tags:
                url = anchor_tag.get_attrib("href")
                motion = self.normalize_dated_doc_name(anchor_tag)
                vote_chamber = "upper" if "svote" in url.lower() else "lower"
                vote_date_str = re.findall(r"\(([^)]+)\)", motion)[-1]
                vote_date = datetime.datetime.strptime(vote_date_str, "%m/%d/%y")
                download_id, scraper_docs, doc_ids = \
                    self.scraper.register_download_and_documents(
                        url, BRP.bill_votes, self.scraper.extraction_type.text_pdf,
                        True, self.vote_parser, column_spec=ColumnSpec.NONE,
                        parser_args={"motion": motion, "vote_date": vote_date,
                                     "vote_chamber": vote_chamber})
                if download_id is not None and doc_ids and doc_ids[0]:
                    vote = scraper_docs[0].additional_data["vote"]
                    vote.add_source(url)
                    bill.add_vote(vote)
                else:
                    logger.warning("failed to process vote document at url {}".format(url))
        self.save_bill(bill)


    def get_session_external_id(self, session):
        """
        Returns the external id NM legislature used to represent sessions.
        Example: 2016 regular session will map to "52"
        :param session: in format of "20162016r" or "20112011ss1"
        :return: a string representing external id of a session
        """
        session_map = {}
        ss_count_map = {}
        example_page_url = "https://www.nmlegis.gov/Legislation/Legislation_List"
        example_page_doc = self.scraper.url_to_lxml(example_page_url, BRP.bill_list)
        session_select = example_page_doc.xpath_single("//select[@id='MainContent_ddlSessionStart']")
        options = [option for option in session_select.xpath("./option")]
        options.reverse()
        for option in options:
            external_id = option.get_attrib("value")
            raw_session_string = option.text_content()
            session_elements = raw_session_string.split()
            start_year = session_elements[0]
            end_year = start_year
            session_type = "r" if session_elements[-1].lower() == "regular" else "ss"
            # this assumes a special session number of no more than 9, which is fine ...
            if len(session_elements) <= 2:
                special_session_number = ""
            else:
                if start_year in ss_count_map:
                    ss_count_map[start_year] += 1
                    special_session_number = ss_count_map[start_year]
                else:
                    ss_count_map[start_year] = 1
                    special_session_number = 1
            session_string = start_year + end_year + session_type + str(special_session_number)
            session_map[session_string] = external_id
        return session_map[session]

    @staticmethod
    def normalize_date(date_string):
        return "/".join([element.zfill(2) for element in date_string.split("/")])

    @staticmethod
    def tab_item_exists(bill_page_doc, tab_title):
        """
        check if a tab with certain title exists and whether the item count > 0
        :param bill_page_doc: element_wrapper for the bill page
        :param tab_title: title of the tab we want to check
        :return: True if tab exists and count > 0 else False
        """
        # sometimes the tab title is different from what it appears in dom ids, build a map to resolve this

        if tab_title not in tab_title_map:
            return False
        tab_title = tab_title_map[tab_title]
        tab_item_count_id_template = "MainContent_tabContainerLegislation_tabPanel{tab_title}_lbl{tab_title}Count"
        tab_item_count_id = tab_item_count_id_template.format(tab_title=tab_title)
        tab_item_count_doc = bill_page_doc.xpath_single("//span[@id='{}']".format(tab_item_count_id), BRP.test)
        if tab_item_count_doc and tab_item_count_doc.text_content() != "0":
            return True
        else:
            return False

    def normalize_dated_doc_name(self, doc_anchor_tag):
        spans = doc_anchor_tag.xpath(".//span")
        if not spans:
            name = doc_anchor_tag.text
        else:
            name = spans[0].text_content()
            if len(spans) > 1:
                raw_date = spans[1].text_content()
                date = self.normalize_date(raw_date)
                name = "{} ({})".format(name, date)
        return name

    @staticmethod
    def get_document_anchor_tags(bill_page_doc, title):
        if title not in tab_title_map:
            return []
        else:
            title = tab_title_map[title]
        div_id = "MainContent_tabContainerLegislation_tabPanel{}".format(title)
        div = bill_page_doc.xpath_single("//div[@id='{}']".format(div_id), BRP.test)

        return div.xpath(".//a")

    # TODO: doc service current does not correctly parse leg dates pdf. Will do manual updates until
    # TODO: the issue gets resolved
    # def get_legislative_dates(self, session):
    #     year_abbr = session[:4][2:]
    #     if "ss" in session:
    #         year_abbr += "s"
    #     url = "https://www.nmlegis.gov/Publications/Session/{}/legis_day_chart_{}.pdf". \
    #         format(year_abbr, year_abbr)
    #     download_id, _, doc_ids = self.scraper.register_download_and_documents(url, BRP.bill_documents,
    #                                                                            self.scraper.extraction_type.text_pdf,
    #                                                                            True, self.legislative_date_parser,
    #                                                                            column_spec=ColumnSpec.QUARTERS)

    # def legislative_date_parser(self, entities):
    #     return [ScraperDocument("dummy")]

    @staticmethod
    def html_parser(element_wrapper):
        text = element_wrapper.xpath_single("//body").text_content()
        return [ScraperDocument(text)]

    @classmethod
    def sheet_vote_parser(cls, entities, parser_args=None):
        # example pdf: https://www.nmlegis.gov/Sessions/15%20regular/votes/HB0083HVOTE.pdf
        text = entities_text_content(entities)
        lines = text.replace("\r\n", "\n").split("\n")
        lines = [re.sub(r"\s+", " ", line).strip() for line in lines if line.strip()]
        text = "\n".join(lines)
        index = 0
        found_stat_line = False
        yes_count = 0
        no_count = 0
        other_count = 0
        yes_voter = []
        no_voters = []
        other_voter = []
        while index < len(lines):
            line = lines[index]
            index += 1
            if not found_stat_line:
                state_line_match = re.findall(r"([A-Za-z]+)\s*:\s*(\d+)", line)
                if not state_line_match or len(state_line_match) < 3:
                    # the "< 3" is added because sometimes the first few lines of the vote pdf
                    # contains date and time that messes up with the regex, but in the
                    # vote stat line we know we at least have yes, no and other, so 3 is used here
                    continue
                for cast, count in state_line_match:
                    cast = cast.lower()
                    count = int(count)
                    if "yea" in cast or "yes" in cast:
                        yes_count += count
                    elif "nay" in cast or "no" in cast:
                        no_count += count
                    else:
                        other_count += count
                found_stat_line = True
                continue
            # Single letter (maybe) followed by space, followed by two or 
            voter_match = cls.sheet_vote_pattern.findall(line)
            if not voter_match:
                break
            for cast, voter in voter_match:
                # if more than five words, we're in the footer 
                # (certified correct to the best of our knowledge)
                if voter.count(' ') > 4:
                    continue
                if cast == "Y":
                    yes_voter.append(voter)
                elif cast == "N":
                    no_voters.append(voter)
                else:
                    other_voter.append(voter)

        motion = parser_args["motion"]
        date = parser_args["vote_date"]
        chamber = parser_args["vote_chamber"]
        vote = Vote(chamber, date, motion, yes_count > no_count, yes_count, no_count, other_count)
        for method, voters in [(vote.yes, yes_voter), (vote.no, no_voters), (vote.other, other_voter)]:
            for voter in voters:
                method(voter)
        scraper_doc = ScraperDocument(text, additional_data={"vote": vote})
        return [scraper_doc]

    @staticmethod
    def table_vote_parser(entities, parser_args=None):
        # example vote pdf: https://www.nmlegis.gov/Sessions/15%20regular/votes/HB0083SVOTE.pdf
        # basically, doc service can parse the table correctly, and we basically associate the voter's name
        # and the vote cast corresponding to the following "X".

        # search for the table entity
        table_entity = None
        for entity in entities:
            if entity.tableEntity is not None:
                table_entity = entity.tableEntity
                break
        assert table_entity is not None

        # column_map maps xPos/column number towards the vote cast value
        column_map = {}
        rows = table_entity.rows
        header = rows[0]
        # name indexes are xPos/columns that correspond to voter names instead of vote casts
        name_indexes = []
        for cell in header.cells:
            key = cell.xPos
            if not cell.textContainers or cell.textContainers[0].text.lower().strip() == u"representative":
                # if a cell within the header row does not have text content, it means this xPos/Column
                # corresponds to voter names, so we add such xPos/Columns to name_indexes list
                name_indexes.append(cell.xPos)
                continue
            # otherwise, it means this xPos/Column corresponds to a vote cast
            value = cell.textContainers[0].text.lower()
            column_map[key] = value
        yes_voters = []
        no_voters = []
        other_voters = []

        yes_votes = None
        no_votes = None
        other_votes = None
        # reached_last_line represents whether we have reached the very last voter in the table
        # this is needed because, the vote pdf also puts the vote state at the end of the table, and we
        # do not want to parse them as voters or vote casts
        reached_last_line = False
        for row in rows[1:]:
            if reached_last_line:
                break
            cells = row.cells
            for name_index in name_indexes:
                name_cell = cells[name_index]
                if not name_cell.textContainers:
                    # if a cell that was supposed to represent voter name ends up being blank
                    # it means we have reached the end of the table
                    reached_last_line = True
                    break
                if name_cell.textContainers[0].text.startswith('TOTAL'):
                    reached_last_line = True
                    index = name_index
                    while True:
                        index += 1
                        if index == len(cells) or index in name_indexes:
                            break
                        if not cells[index].textContainers:
                            continue
                        text = cells[index].textContainers[0].text
                        if text.isdigit() and index in column_map:
                            cast = column_map[index]
                            if cast in ("yes", "yea"):
                                yes_votes = int(text)
                            elif cast in ("no", "nay"):
                                no_votes = int(text)
                            else:
                                if other_votes is None:
                                    other_votes = 0
                                other_votes += int(text)
                    break

                # search for the first subsequent cell which has a text value of "X" that represents a vote cast
                index = name_index + 1
                while index < len(cells) and not cells[index].textContainers:
                    index += 1
                if index == len(cells):
                    continue
                cast_cell = cells[index]
                name = name_cell.textContainers[0].text

                cast_key = cast_cell.xPos
                cast = column_map[cast_key]
                if cast in ("yes", "yea"):
                    yes_voters.append(name)
                elif cast in ("no", "nay"):
                    no_voters.append(name)
                else:
                    other_voters.append(name)
        motion = parser_args["motion"]
        chamber = parser_args["vote_chamber"]
        date = parser_args["vote_date"]

        yes_votes = len(yes_voters) if yes_votes is None else yes_votes
        no_votes = len(no_voters) if no_votes is None else no_votes
        other_votes = len(other_voters) if other_votes is None else other_votes

        vote = Vote(chamber, date, motion, yes_votes > no_votes, yes_votes,
                    no_votes, other_votes)
        for method, voters in [(vote.yes, yes_voters), (vote.no, no_voters), (vote.other, other_voters)]:
            for voter in voters:
                method(voter)
        text = entities_text_content(entities)
        lines = text.replace("\r\n", "\n").split("\n")
        lines = [re.sub(r"\s+", " ", line).strip() for line in lines if line.strip()]
        text = "\n".join(lines)
        scraper_doc = ScraperDocument(text, additional_data={"vote": vote})
        return [scraper_doc]

    @classmethod
    def vote_parser(cls, entities, parser_args=None):
        if any(entity.tableEntity is not None for entity in entities):
            return cls.table_vote_parser(entities, parser_args)
        return cls.sheet_vote_parser(entities, parser_args)

from __future__ import absolute_import

import re
import logging
import datetime
from lxml import html

from fn_scraperutils.doc_service.util import ScraperDocument

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(u"SDBillScraper")


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-SD", group="fnleg")
class SDBillScraper(BillScraper):
    def __init__(self):
        super(SDBillScraper, self).__init__("sd")

    def scrape_bill_ids(self, session):
        bill_ids = {}
        session_year = session[:4]
        bill_list_url = u"http://legis.sd.gov/Legislative_Session/Bills/default.aspx?Session={}".format(session_year)
        bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        tables = bill_list_doc.xpath(u"//div[@id='ctl00_ContentPlaceHolder1_CtlBillList1_divBillTables']/section/"
                                     u"table")
        fn_map = self.build_fn_map(session)
        fiscal_impact_map = self.build_fiscal_impact_map(session)
        committee_map = self.build_committee_map(session)
        legislators_map = self.build_legislator_meta_map(session)
        for table in tables:
            for row in table.xpath(u".//tr")[1:]:  # no tbody in page source, skip table header
                bill_id = row.xpath_single(u"./td[1]").text_content().replace(u'\xa0', u' ')
                url = row.xpath_single(u"./td[1]/a[1]").get_attrib(u"href")
                title = row.xpath_single(u"./td[2]").text_content()
                # We capitalize the first letter of the title, as it is usually
                # all lower case.
                title = title[:1].upper() + title[1:]

                bill_ids[bill_id] = {u"url": url, u"title": title,
                                     u"legislators_map": legislators_map,
                                     u"committee_map": committee_map}
                if bill_id in fn_map:
                    bill_ids[bill_id][u"fiscal_note"] = fn_map[bill_id]
                if bill_id in fiscal_impact_map:
                    bill_ids[bill_id][u"fiscal_impact"] = fiscal_impact_map[bill_id]
        logger.info(u"A total of {} bill ids scraped for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        bill_info = kwargs.get(u"bill_info")
        bill_page_url = bill_info[u"url"]
        legislators_map = bill_info[u"legislators_map"]
        committee_map = bill_info[u"committee_map"]
        fn_map = {bill_id: bill_info[u"fiscal_note"]} if u"fiscal_note" in bill_info else {}
        fiscal_impact_map = {bill_id: bill_info[u"fiscal_impact"]} if u"fiscal_impact" in bill_info else {}

        # basic bill info
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        logger.info(u"Scraping bill {} at url {}".format(bill_id, bill_page_url))
        title = bill_info[u"title"]
        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_page_url)

        # sponsors
        for anchor_tag in bill_page_doc.xpath(u"//a[contains(@href, '/Legislators/Legislators/MemberBills.aspx?Member')]"):
            url = anchor_tag.get_attrib(u"href")
            if re.findall(r"Member=(\d+)", url):
                legislator_id = re.findall(r"Member=(\d+)", url)[0]
                legislator_info = legislators_map[legislator_id]
                legislator_name = legislator_info[u"name"]
                if bill_id in legislator_info[u"primary_sponsor"]:
                    bill.add_sponsor(u"primary", legislator_name)
                elif bill_id in legislator_info[u"cosponsor"]:
                    bill.add_sponsor(u"cosponsor", legislator_name)
                else:
                    logger.warning(u"Cannot determine sponsorship type for sponsor %s. "
                                   u"Assigning \"primary_sponsor\" as fallback", legislator_name)
                    bill.add_sponsor(u"primary", legislator_name)
            elif re.findall(r"Committee=(\d+)", url):
                committee_id = re.findall(r"Committee=(\d+)", url)[0]
                committee_name = committee_map[committee_id]
                bill.add_sponsor(u"primary", committee_name)
            else:
                logger.warning(
                    u"Cannot map sponsor info on page %s to either legislator or committee. Skipping for now",
                    bill_page_url)

        # subjects
        keywords_anchor_tags = bill_page_doc.xpath(u"//tr[contains(@id, '_trKeywords') and "
                                                   u"contains(@id, '_ContentPlaceHolder')]//a")
        for anchor_tag in keywords_anchor_tags:
            subject = anchor_tag.text_content()
            bill.add_subject(subject)
        
        # actions
        action_table_header_row = bill_page_doc.xpath("//table[contains(@id, 'BillActions')]/tbody/tr")

        for row in action_table_header_row:
            cells = row.xpath(u"./td")
            if len(cells) == 4 and cells[0].xpath_single(u"./a") \
                    and re.match(r"\d{2}/\d{2}/\d{4}", cells[0].text_content()):
                date = datetime.datetime.strptime(cells[0].text_content(), u"%m/%d/%Y")
                action = cells[1].text_content()
                actor = chamber
                if u"house" in action.lower():
                    actor = u"lower"
                if u"senate" in action.lower():
                    actor = u"upper"
                if u"house" in action.lower() and u"senate" in action.lower():
                    logger.warning(u"Cannot determine action actor because both word \"house\" and \"senate\" "
                                   u"appear within the action text. Using the bill chamber as fallback value.")
                    actor = chamber
                if re.search(r"signed by(?: the)? governor", action, flags=re.I):
                    actor = u'executive'
                bill.add_action(actor, action, date)

                # check if vote or amendment information is available
                if cells[1].xpath(u".//a", BRP.test):
                    for vote_or_amdt_anchor in cells[1].xpath(u".//a"):
                        if u"Vote" in vote_or_amdt_anchor.get_attrib(u"href"):
                            vote_url = vote_or_amdt_anchor.get_attrib(u"href")
                            vote_doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
                            motion = vote_doc.xpath_single(u"//h3[contains(@id, '_hdVote')]"). \
                                text_content().split(u",", 1)[1].strip()
                            vote_chamber = chamber
                            if u"joint" in motion.lower():
                                vote_chamber = u"joint"
                            elif u"house" in motion.lower():
                                vote_chamber = u"lower"
                            elif u"senate" in motion.lower():
                                vote_chamber = u"upper"
                            vote_table = vote_doc.xpath_single(u"//table[contains(@id, 'tblVoteTotals')]")
                            yes_voters = []
                            no_voters = []
                            other_voters = []
                            for vote_row in vote_table.xpath(u".//tr"):
                                cells = vote_row.xpath(u".//td")
                                assert len(cells) % 2 == 0
                                index = 0
                                while index < len(cells):
                                    name = cells[index].text_content()
                                    cast = cells[index + 1].text_content()
                                    if cast.startswith(u"Y"):
                                        yes_voters.append(name)
                                    elif cast.startswith(u"N"):
                                        no_voters.append(name)
                                    else:
                                        other_voters.append(name)
                                    index += 2
                            yes_count = int(vote_doc.xpath_single(u"//span[contains(@id, '_tdAyes')]").text_content())
                            no_count = int(vote_doc.xpath_single(u"//span[contains(@id, '_tdNays')]").text_content())
                            other_count = int(
                                vote_doc.xpath_single(u"//span[contains(@id, '_tdExcused')]").text_content())
                            other_count += int(
                                vote_doc.xpath_single(u"//span[contains(@id, '_tdAbsent')]").text_content())
                            # the website screw up counting people sometimes:
                            # http://sdlegislature.gov/Legislative_Session/Bills/RollCall.aspx?
                            # Vote=21505&Session=2016
                            for vote_cast, voters, count in [(u"YES", yes_voters, yes_count),
                                                             (u"NO", no_voters, no_count),
                                                             (u"OTHER", other_voters, other_count)]:
                                if len(voters) > count:
                                    logger.warning(u"Number of {} voters on page {} is larger than the vote count in "
                                                   u"vote summary table. Using the previous value "
                                                   u"for this vote because "
                                                   u"we have all the voters' names.".format(vote_cast, vote_url))
                                elif len(voters) < count:
                                    logger.critical(u"Number of {} voters on page {} is smaller than the vote count in "
                                                    u"vote summary table. Using the previous value for now, but this "
                                                    u"could be the result of our scraper not being able to pick up "
                                                    u"all voter information. Further investigation is needed.")
                            yes_count = len(yes_voters)
                            no_count = len(no_voters)
                            other_count = len(other_voters)
                            passed = u"Passed" in action
                            if passed != (yes_count > no_count):
                                logger.warning(u"Vote on {} receives {} Ayes and {} Nays, but \"passed\" value is {} "
                                               u"basing on action text \"{}\". This is probably because this vote needs "
                                               u"a certain ratio of majority (two-thirds for example) in order to pass."
                                               .format(vote_url, yes_count, no_count, passed, action))
                            vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
                            for voter in yes_voters:
                                vote.yes(voter)
                            for voter in no_voters:
                                vote.no(voter)
                            for voter in other_voters:
                                vote.other(voter)
                            vote.add_source(vote_url)
                            bill.add_vote(vote)
                        elif u"Amend" in vote_or_amdt_anchor.get_attrib(u"href"):
                            amdt_url = vote_or_amdt_anchor.get_attrib(u"href")
                            amdt_name = vote_or_amdt_anchor.text_content()
                            download_id, _, doc_ids = \
                                self.scraper.register_download_and_documents(amdt_url, BRP.bill_documents,
                                                                             self.scraper.extraction_type.html,
                                                                             False, self.html_amdt_parser,
                                                                             get_static_content=
                                                                             self.version_html_static_content)
                            if len(doc_ids) == 1 and doc_ids[0] is not None:
                                doc_service_document = Doc_service_document(amdt_name, u"amendment", u"complete",
                                                                            download_id, doc_ids[0])
                                bill.add_doc_service_document(doc_service_document)
                            else:
                                logger.warning(u"Failed to process document at url {}".format(amdt_url))
                        else:
                            logger.warning(u"An anchor link on page %s does not point to either amendment or vote. "
                                           u"Skipping it for now but it might be some information we want in the "
                                           u"future.", bill_page_url)
            else:
                # this indicates all rows that represent action has been scraped
                continue

        # version
        version_table = bill_page_doc.xpath_single(u"//table[contains(@id, 'tblBillVersions')]")
        for row in version_table.xpath(u".//tr")[1:]:
            cells = row.xpath(u"./td")
            name = cells[1].text_content()
            html_anchor = cells[1].xpath_single(u"./a")
            if html_anchor is None:
                # in some cases, they list a "coming soon" where the html link should be placed
                # which breaks the logic.
                pdf_url = cells[2].xpath_single(u"./a").get_attrib(u"href")
                primary_url = pdf_url
                parser = None
                get_static_content = None
                serve_from_s3 = True
                extraction_type = self.scraper.extraction_type.text_pdf
                name_suffix = u"pdf"
            else:
                html_url = html_anchor.get_attrib(u"href")
                primary_url = html_url
                parser = self.html_version_parser
                get_static_content = self.version_html_static_content
                serve_from_s3 = False
                extraction_type = self.scraper.extraction_type.html
                name_suffix = u"html"
            name = u"{} ({})".format(name, name_suffix)
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(primary_url,
                                                             BRP.bill_documents,
                                                             extraction_type,
                                                             serve_from_s3,
                                                             parse_function=parser,
                                                             get_static_content=get_static_content)
            if len(doc_ids) == 1 and doc_ids is not None:
                doc_service_document = Doc_service_document(name, u"version", u"complete", download_id, doc_ids[0])
                doc_service_document.add_alternate_representation(download_id)
                bill.add_doc_service_document(doc_service_document)
            else:
                logger.warning(u"Failed to process document at url {}".format(html_url))
        if bill_id in fn_map:
            for fn_info in fn_map[bill_id]:
                url = fn_info[u"url"]
                name = fn_info[u"name"]
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                doc_service_document = Doc_service_document(name, u"fiscal_note", u"partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        if bill_id in fiscal_impact_map:
            for fiscal_impact_info in fiscal_impact_map[bill_id]:
                url = fiscal_impact_info[u"url"]
                name = fiscal_impact_info[u"name"]
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                doc_service_document = Doc_service_document(name, u"fiscal_note", u"partial", download_id)
                bill.add_doc_service_document(doc_service_document)
        self.save_bill(bill)

    def build_legislator_meta_map(self, session):
        legislators_map = {}
        url = "http://sdlegislature.gov/Legislators/ContactLegislator.aspx?Session={}".format(session[:4])
        doc = self.scraper.url_to_lxml(url, BRP.bill_sponsors)
        for anchor_tag in doc.xpath(".//a[contains(text(), 'View Profile')]"):
            member_id = re.findall(r"Member=(\d+)", anchor_tag.get_attrib("href"))[0]
            member_dict = {}
            sponsorship_details_url = "http://sdlegislature.gov/Legislators/Legislators/MemberBills.aspx?" \
                                      "Member={}&Session={}".format(member_id, session[:4])
            sponsorship_details_doc = self.scraper.url_to_lxml(sponsorship_details_url, BRP.bill_sponsors)
            name = sponsorship_details_doc.xpath_single("//h3[@id='ctl00_ContentPlaceHolder1_hdMember']").text_content()
            name = re.match(ur"(Representative|Senator)(.*)\u2014", name).group(2).strip()
            member_dict["name"] = name
            member_dict["primary_sponsor"] = []
            member_dict["cosponsor"] = []
            sponsorship_div = sponsorship_details_doc. \
                xpath_single("//div[@id='divBills']")
            primary_sponsorship_table = sponsorship_div. \
                xpath_single(".//h4[text()='Prime Sponsor']/following-sibling::section/table")
            if primary_sponsorship_table:
                for row in primary_sponsorship_table.xpath(".//tr")[1:]:
                    member_dict["primary_sponsor"].append(
                        row.xpath_single("./td[1]").text_content().replace(u'\xa0', u' '))
            cosponsorship_table = sponsorship_div.xpath_single(".//h4[text()='Co Sponsor']/following-sibling::section/table")
            if cosponsorship_table:
                for row in cosponsorship_table.xpath(".//tr")[1:]:
                    member_dict["cosponsor"].append(row.xpath_single("./td[1]").text_content().replace(u'\xa0', u' '))
            legislators_map[member_id] = member_dict
        return legislators_map

    def build_committee_map(self, session):
        committee_map = {}
        year = session[:4]
        committee_home_url = "http://sdlegislature.gov/Legislative_Session/Committees/default.aspx?" \
                             "Session={}".format(year)
        committee_home_doc = self.scraper.url_to_lxml(committee_home_url, BRP.bill_sponsors)
        house_committees_url = committee_home_doc.xpath_single("//div[@id='ctl00_ContentPlaceHolder1_BlueBoxLeft']"
                                                               "//a[text()='House of Representatives']").get_attrib("href")
        senate_committees_url = committee_home_doc.xpath_single("//div[@id='ctl00_ContentPlaceHolder1_BlueBoxLeft']"
                                                                "//a[text()='Senate']").get_attrib("href")

        # house committees
        house_committees_doc = self.scraper.url_to_lxml(house_committees_url, BRP.bill_sponsors)
        house_committee_anchor_tags = house_committees_doc. \
            xpath("//div[@id='ctl00_ContentPlaceHolder1_BlueBoxLeft']"
                  "//a[contains(@href, 'Default.aspx?Committee=')]")
        for anchor_tag in house_committee_anchor_tags:
            name = anchor_tag.text_content()
            committee_id = re.findall(r"Committee=(\d+)", anchor_tag.get_attrib("href"))[0]
            committee_map[committee_id] = name

        # senate committees
        senate_committees_doc = self.scraper.url_to_lxml(senate_committees_url, BRP.bill_sponsors)
        senate_committee_anchor_tags = senate_committees_doc. \
            xpath("//div[@id='ctl00_ContentPlaceHolder1_BlueBoxLeft']"
                  "//a[contains(@href, 'Default.aspx?Committee=')]")
        for anchor_tag in senate_committee_anchor_tags:
            name = anchor_tag.text_content()
            committee_id = re.findall(r"Committee=(\d+)", anchor_tag.get_attrib("href"))[0]
            committee_map[committee_id] = name

        return committee_map

    def build_fn_map(self, session):
        fn_map = {}
        url = "http://sdlegislature.gov/Legislative_Session/Bill_Reports/FiscalNotes.aspx?Session={}".format(session[:4])
        doc = self.scraper.url_to_lxml(url, BRP.bill_documents)
        table = doc.xpath_single("//table[contains(@id, 'tblFiscalNotes')]")
        for anchor_tag in table.xpath(".//a"):
            fn_name = anchor_tag.text_content().replace(u'\xa0', u' ')
            fn_url = anchor_tag.get_attrib("href")
            bill_id = re.findall(r"[A-Z]+ \d+", fn_name)[0]
            if bill_id not in fn_map:
                fn_map[bill_id] = []
            fn_name = "Fiscal Note " + fn_name
            fn_info = {"url": fn_url, "name": fn_name}
            fn_map[bill_id].append(fn_info)
        return fn_map

    def build_fiscal_impact_map(self, session):
        fiscal_impact_map = {}
        main_url = "http://sdlegislature.gov/Legislative_Session/Bill_Reports/FiscalImpact.aspx?Session=".format(
            session[:4])
        doc = self.scraper.url_to_lxml(main_url, BRP.bill_documents)
        table = doc.xpath_single("//table[contains(@id, 'tblFiscalImpacts')]")
        for anchor_tag in table.xpath(".//a"):
            name = anchor_tag.text_content().replace(u'\xa0', u' ')
            url = anchor_tag.get_attrib("href")
            bill_id = re.findall(r"[A-Z]+ \d+", name)[0]
            if bill_id not in fiscal_impact_map:
                fiscal_impact_map[bill_id] = []
            name = "Prison/Jail Population Cost Estimates " + name
            info = {"url": url, "name": name}
            fiscal_impact_map[bill_id].append(info)
        return fiscal_impact_map

    @staticmethod
    def html_version_parser(element_wrapper):
        raw_html = html.tostring(element_wrapper.xpath_single("//div[contains(@id, '_divBottom')]").element)
        refined_html = re.sub(r" *<strike>.*?</strike> *", " ", raw_html)
        text = html.fromstring(refined_html).text_content().strip()
        return [ScraperDocument(text)]

    @staticmethod
    def html_amdt_parser(element_wrapper):
        text = element_wrapper.xpath_single("//div[contains(@id, '_divAmendment')]").text_content()
        return [ScraperDocument(text)]

    def version_html_static_content(self, html_file):
        source = html_file.read().decode('utf-8')
        root = self.scraper.wrap_html(u"", source, BRP.bill_documents)
        elem = root.xpath_single("//div[@id='textcolumn']")
        if not elem:
            elem = root.xpath_single("body")
        return elem.tostring()


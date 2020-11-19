from __future__ import absolute_import

import re
import logging
import datetime
import json
from requests.exceptions import HTTPError
import injector

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from fn_scraperutils.doc_service.util import ScraperDocument
from ..common.normalize import normalize_bill_id

from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.api.resources import ScraperConfig

logger = logging.getLogger('NYBillScraper')

bill_info_map = {
    'S': ('upper', 'bill'),
    'R': ('upper', 'resolution'),
    'J': ('upper', 'resolution'),
    'B': ('upper', 'concurrent_resolution'),
    'C': ('lower', 'concurrent_resolution'),
    'A': ('lower', 'bill'),
    'E': ('lower', 'resolution'),
    'K': ('lower', 'resolution'),
    'L': ('lower', 'joint_resolution')}

YES_VOTE_KEYS = ["AYE", "AYEWR", "Y", "YES"]
NO_VOTE_KEYS = ["NAY", "NO", "N"]
OTHER_VOTE_KEYS = ["ABS", "EXC", "ER", "NV", "AB"]


class NYURL:
    senate_base = "http://legislation.nysenate.gov/api/3/"

    @staticmethod
    def bills_list_url(api_key, year, offset, limit):
        f = [year, api_key, offset, limit]
        return NYURL.senate_base + "bills/{}?key={}&offset={}&limit={}&full=false".format(*f)

    @staticmethod
    def bill_detail_url(api_key, year, bill_id):
        f = [year, bill_id, api_key]
        return NYURL.senate_base + "bills/{}/{}?detail=true&key={}".format(*f)

    @staticmethod
    def pdf_url(api_key, year, version_name):
        f = [year, version_name, api_key]
        return NYURL.senate_base + "bills/{}/{}.pdf?key={}".format(*f)

    @staticmethod
    def assembly_bill_url(session, bill_id):
        f = [bill_id.replace(" ", ""), session[0:4]]
        return "http://assembly.state.ny.us/leg/?default_fld=&" \
               "leg_video=&bn={}&term={}&Summary=Y&Actions=Y&" \
               "Committee%26nbspVotes=Y&Floor%26nbspVotes=Y&" \
               "Memo=Y&Text=Y".format(*f)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-NY", group="fnleg")
class NYBillScraper(BillScraper):
    """
    NYBillScraper requires an api key to run

    There are two sources used for ingestion of each bill:
    1. NY Senate API - http://legislation.nysenate.gov/api/3/bills/2017/A 3729?detail=true&key=HYWwld2AbSwHlQVfcLbxhkmpSNr3kQ3d
    2. NY Assembly website - http://assembly.state.ny.us/leg/?default_fld=&leg_video=&bn=A3729&term=2017&Summary=Y&Actions=Y&Committee%26nbspVotes=Y&Floor%26nbspVotes=Y&Memo=Y&Text=Y
    Because we do not want to serve an api as a source to the user, we specify as a source
    another site that directly uses the NY Senate API:
    https://www.nysenate.gov/legislation/bills/2017/A3729

    scrape_bill_ids
    Bill IDs are retrieved exclusively from NY Senate API in groups of 'limit'(1000)

    scrape_bill
    API is first called to receive all information except Assembly memo and votes.
    Assembly memo and votes are then retrieved from Assembly website
        Memo xpath - //pre[contains(., 'MEMORANDUM IN SUPPORT OF LEGISLATION')]/pre
        Assembly Floor Vote xpath - //table[contains(., 'Assembly Vote') or contains(., 'YEA/NAY')]
        Assembly Committee Vote xpath - //table[contains(., 'Committee:')]

    NOTE: Requires NYBillScraper config file entry.
    """
    @injector.inject(
        config=ScraperConfig
    )
    def __init__(self, config):
        super(NYBillScraper, self).__init__("ny")
        self.api_key = config.get("api_key")
        if not self.api_key or self.api_key == 'NY_API_KEY':
            raise ValueError("Update API key in config.yaml.")

    def scrape_bill_ids(self, session):
        bill_ids = set()
        years = set()
        years.add(session[0:4])
        years.add(session[4:8])

        for year in years:
            limit = 1000
            offset = 1
            while True:
                api_url = NYURL.bills_list_url(self.api_key, year, offset, limit)
                data = self.scraper.url_to_json(api_url, BRP.bill_list)
                logger.info("Successfully fetch the next {limit} bills starting from {offset}.".
                            format(limit=limit, offset=offset))
                # behaviors of the NY senate API:
                # if there are a total of 15 bills, offset set to 1, limit set to 10
                # a call with offset = 1, limit = 10, returns bill 1~10
                # a call with offset = 11, limit = 10, returns bill 11~15 (not a full size  )
                # a call with offset = 21, limit = 10, returns a json w/ "result"-"items" field as empty lis
                if data["responseType"] == "empty list":
                    logger.info("API call returned an empty list. Finished scraping bill ids for year {}".format(year))
                    break
                for item in data["result"]["items"]:
                    bill_id = item["printNo"]
                    bill_id = re.findall("[A-Z]+\d+", bill_id)[0]
                    # remove trailing letters because they represent bill versions
                    # e.g. S123, S123A, S123B all return the same JSON, and all different amendment
                    # versions are displayed within the "amendments" field
                    bill_ids.add(bill_id)
                offset += limit

        bill_ids = list(bill_ids)
        logger.info("A total of {} bill ids were scraped".format(len(bill_ids)))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        logger.info("Scraping bill {}".format(bill_id))
        year = session[0:4]
        api_url = NYURL.bill_detail_url(self.api_key, year, bill_id)
        data = self.scraper.url_to_json(api_url, BRP.bill)
        chamber, bill_type = bill_info_map[bill_id[0]]
        title = data["result"]["title"]
        bill = Bill(session, chamber, bill_id, title, bill_type)

        # primary sponsor
        primary_sponsor_info = data["result"]["sponsor"]
        # There are cases when the sponsor field is a null. This might be because the bill is still
        # at a very early stage.
        if primary_sponsor_info is not None:
            if primary_sponsor_info["member"]:
                primary_sponsor = primary_sponsor_info["member"]["fullName"]
                bill.add_sponsor("primary", primary_sponsor)
            else:
                # in some cases, the primary sponsor might be a committee
                # http://assembly.state.ny.us/leg/?default_fld=&bn=S05953&term=2015&Summary=Y&Actions=Y&Text=Y&Votes=Y
                # in such cases, the "member" field will be None, and there will be other keys with the name of
                # committees in the "sponsor" field, the corresponding boolean value represents whether
                # the committee sponsors the bill
                for (key, value) in primary_sponsor_info.iteritems():
                    if key == "member" or not value:
                        continue
                    primary_sponsor = "{} Committee".format(key.title())
                    bill.add_sponsor("primary", primary_sponsor)

        # cosponsors
        # active version could be empty string or "A", "B", "C"
        # all of these versions are under the "amendments" field, each having a "coSponsors" section
        # for now we use the "coSponsors" field of only the "activeVersion"
        active_version = data["result"]["activeVersion"]
        cosponsor_keys = ["coSponsors", "multiSponsors"]
        for key in cosponsor_keys:
            cosponsors = data["result"]["amendments"]["items"][active_version][key]["items"]
            for cosponsor in cosponsors:
                bill.add_sponsor("cosponsor", cosponsor["fullName"])

        # action
        actions = data["result"]["actions"]["items"]
        for action in actions:
            action_text = action["text"]
            date = datetime.datetime.strptime(action["date"], "%Y-%m-%d")
            actor = "lower" if action["chamber"].upper() == "ASSEMBLY" else "upper"
            bill.add_action(actor, action_text, date)

        # add description
        summary_text = data["result"]["summary"]
        if summary_text.strip():
            bill.add_summary(summary_text)

        # handles all documents from this API call: versions and memos
        has_memo = 0
        for key, item in data["result"]["amendments"]["items"].iteritems():
            has_memo += len(item["memo"])

        if has_memo > 0:
            download_id, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(api_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text,
                                                             True, self.version_memo_parser,
                                                             content_type='text/html',
                                                             should_download=True,
                                                             get_static_content=self.memo_static_content)

            doc_service_document = Doc_service_document("Sponsor's Memorandum (Senate)", "summary", "complete",
                                                        download_id=download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        # TODO: removed alternate format download for latency test
        # if "pdf_url" in scraper_doc.additional_data:
        #     pdf_url = scraper_doc.additional_data["pdf_url"]
        #     logger.info("downloading pdf document for bill {} version {} from {}".
        #                 format(bill_id, doc_name, pdf_url))
        #     pdf_download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
        #     doc_service_document.add_alternate_representation(pdf_download_id)

        # handles similar bill, alternate bill id, and subjects
        version_info = data["result"]["amendments"]["items"]
        version_keys = sorted(version_info.keys())
        similar_bill_set = set()
        subject_set = set()

        for version_key in version_keys:
            version_text = version_info[version_key]["fullText"]
            version_name = version_info[version_key]["printNo"]
            version_name = normalize_bill_id(version_name)
            lines = version_text.strip().split("\n")
            lines = [re.sub(r"\s+", " ", line) for line in lines]
            version_text = "\n".join(lines)
            if version_text.strip():
                # some version actually has empty text, we skip them
                try:
                    pdf_url = NYURL.pdf_url(self.api_key, year, version_name.replace(" ", ""))

                    # PDFs are dynamically generated with a different ID every time. We need to pass in a
                    # get_static_content which strips these out so that we don't always reupload the file.
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(pdf_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, should_download=True,
                                                                     get_static_content=self.pdf_static_content,
                                                                     extracted_text=version_text)
                    assert len(doc_ids) == 1
                    version_doc_service_document = Doc_service_document(version_name, "version", "complete",
                                                                        download_id=download_id,
                                                                        doc_id=doc_ids[0])
                    bill.add_doc_service_document(version_doc_service_document)
                except HTTPError:
                    logger.warning("Download failure while trying to download bill version {} for bill id {}. Skipping "
                                   "it for now".format(version_name, bill_id))
            name = version_name
            if name != bill_id:
                bill.add_alternate_id(name)

            similar_bill_info = version_info[version_key]["sameAs"]
            for item in similar_bill_info["items"]:
                similar_bill_id = item["printNo"]
                if not similar_bill_id.strip():
                    # avoid passing empty strings
                    continue
                similar_bill_id = normalize_bill_id(similar_bill_id)
                if similar_bill_id not in similar_bill_set:
                    similar_bill_set.add(similar_bill_id)
                    bill.add_companion(similar_bill_id, "other")

            if version_info[version_key]["lawSection"]:
                subject = version_info[version_key]["lawSection"]
                if not subject.strip():
                    # avoid empty strings
                    continue
                if subject not in subject_set:
                    subject_set.add(subject)
                    bill.add_subject(subject)

        # senate votes
        votes_info = data["result"]["votes"]["items"]
        for vote_info in votes_info:
            vote_chamber = "upper"
            date = datetime.datetime.strptime(vote_info["voteDate"], "%Y-%m-%d")
            motion = "Senate Floor Vote" if vote_info["voteType"].upper() == "FLOOR" \
                else "{} Committee Vote".format(vote_info["committee"]["name"])

            yes_count = 0
            yes_voters = []
            for yes_vote_key in YES_VOTE_KEYS:
                if yes_vote_key in vote_info["memberVotes"]["items"]:
                    yes_count += vote_info["memberVotes"]["items"][yes_vote_key]["size"]
                    for member in vote_info["memberVotes"]["items"][yes_vote_key]["items"]:
                        yes_voters.append(member["fullName"])
            no_count = 0
            no_voters = []
            for no_vote_key in NO_VOTE_KEYS:
                if no_vote_key in vote_info["memberVotes"]["items"]:
                    no_count += vote_info["memberVotes"]["items"][no_vote_key]["size"]
                    for member in vote_info["memberVotes"]["items"][no_vote_key]["items"]:
                        no_voters.append(member["fullName"])
            other_count = 0
            other_voters = []
            for other_vote_key in OTHER_VOTE_KEYS:
                if other_vote_key in vote_info["memberVotes"]["items"]:
                    other_count += vote_info["memberVotes"]["items"][other_vote_key]["size"]
                    for member in vote_info["memberVotes"]["items"][other_vote_key]["items"]:
                        other_voters.append(member["fullName"])
            passed = yes_count > (no_count + other_count)
            vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)

            for voter in yes_voters:
                vote.yes(voter)
            for voter in no_voters:
                vote.no(voter)
            for voter in other_voters:
                vote.other(voter)

            bill.add_vote(vote)

        # assembly data
        # assembly votes and memo are only available on the assembly website, not from the senate API
        assembly_bill_page_url = NYURL.assembly_bill_url(session, bill_id)

        assembly_bill_page_doc = self.scraper.url_to_lxml(assembly_bill_page_url, BRP.bill)

        # This senate page uses the same api source that we do, so even though we don't directly use this page,
        # we set this as a source for users
        # On this Senate page, every bill is under ".../bills" and everything else is ".../resolutions"
        api_bill_type = "bills" if bill_type == "bill" else "resolutions"
        senate_page_url = "https://www.nysenate.gov/legislation/{}/{}/{}"\
            .format(api_bill_type, year, bill_id.replace(" ", ""))
        bill.add_source(senate_page_url, "senate")
        bill.add_source(assembly_bill_page_url, "assembly")

        # assembly floor votes
        floor_vote_tables = assembly_bill_page_doc. \
            xpath("//table[contains(., 'Assembly Vote') or contains(., 'YEA/NAY')]", BRP.test)
        for floor_vote_table in floor_vote_tables:
            date = datetime.datetime. \
                strptime(floor_vote_table.xpath_single("./caption/span[2]").text_content(), "%m/%d/%Y")
            yes_voters = []
            no_voters = []
            other_voters = []
            for row in floor_vote_table.xpath(".//tr"):
                cells = row.xpath(".//td", BRP.test)
                assert len(cells) % 2 == 0
                index = 0
                while index < len(cells):
                    name = cells[index].text_content()
                    vote = cells[index + 1].text_content()
                    index += 2
                    if not name:
                        continue  # taking care of empty cells
                    if vote in YES_VOTE_KEYS:
                        yes_voters.append(name)
                    elif vote in NO_VOTE_KEYS:
                        no_voters.append(name)
                    elif vote in OTHER_VOTE_KEYS:
                        other_voters.append(name)
                    else:
                        raise ValueError(u'Unrecognized vote status: ' + vote)
            vote_chamber = "lower"
            motion = "Assembly Floor Vote"
            raw_vote_table_text = floor_vote_table.text_content()
            better_motion = re.findall(r"(MOTION:.*)YEA/NAY", raw_vote_table_text)
            if better_motion:
                better_motion = better_motion[0]
                better_motion = re.sub(r"\s+", " ", better_motion).strip()
                motion = better_motion

            yes_count = len(yes_voters)
            no_count = len(no_voters)
            other_count = len(other_voters)
            passed = (yes_count > no_count + other_count)
            vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
            for voter in yes_voters:
                vote.yes(voter)
            for voter in no_voters:
                vote.no(voter)
            for voter in other_voters:
                vote.other(voter)

            bill.add_vote(vote)

        # assembly memo
        assembly_memo_doc = assembly_bill_page_doc. \
            xpath_single("//pre[contains(., 'MEMORANDUM IN SUPPORT OF LEGISLATION')]/pre", BRP.test)
        if assembly_memo_doc:
            download_id, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(assembly_bill_page_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html,
                                                             True, self.assembly_memo_parser)
            assert len(scraper_docs) == 1 and len(doc_ids) == 1
            doc_id = doc_ids[0]
            name = scraper_docs[0].additional_data["name"]
            doc_service_document = Doc_service_document(name, "summary", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_id)
            bill.add_doc_service_document(doc_service_document)

        # assembly committee vote
        assembly_committee_vote_tables = assembly_bill_page_doc.xpath("//table[contains(., 'Committee:')]", BRP.test)
        if assembly_committee_vote_tables:
            for table in assembly_committee_vote_tables:
                committee_name = table.xpath_single("./caption/span[1]/span[1]/b").text_content(). \
                    split(" ", 1)[0].strip().title()
                motion = "{} Committee Vote".format(committee_name)
                date_string = table.xpath_single("./caption/span[3]").text_content()
                date = datetime.datetime.strptime(date_string, "%m/%d/%Y")
                vote_chamber = "lower"
                yes_voters = []
                no_voters = []
                other_voters = []
                for row in table.xpath(".//tr"):
                    cells = row.xpath(".//td", BRP.test)
                    index = 0
                    while index < len(cells):
                        name = cells[index].text_content()
                        vote = cells[index + 1].text_content()
                        index += 2
                        if not name:
                            continue
                        if vote == "Aye":
                            yes_voters.append(name)
                        elif vote == "Nay":
                            no_voters.append(name)
                        else:
                            other_voters.append(name)
                yes_count = len(yes_voters)
                no_count = len(no_voters)
                other_count = len(other_voters)
                passed = yes_count > no_count + other_count
                vote = Vote(vote_chamber, date, motion, passed, yes_count, no_count, other_count)
                for voter in yes_voters:
                    vote.yes(voter)
                for voter in no_voters:
                    vote.no(voter)
                for voter in other_voters:
                    vote.other(voter)

                bill.add_vote(vote)

        self.save_bill(bill)

    @staticmethod
    def version_memo_parser(text):
        return [ScraperDocument(text)]

    @staticmethod
    def assembly_memo_parser(element_wrapper):
        assembly_memo_doc = element_wrapper. \
            xpath_single("//pre[contains(., 'MEMORANDUM IN SUPPORT OF LEGISLATION')]/pre", BRP.test)
        memo_text = assembly_memo_doc.text_content().replace("&nbsp", "")
        memo_name = "Sponsor's Memorandum (Assembly)"
        additional_data = {"name": memo_name}
        return [ScraperDocument(memo_text, additional_data=additional_data)]

    @staticmethod
    def memo_static_content(html_file):
        raw_text = html_file.read()
        data = json.loads(raw_text)
        version_info = data["result"]["amendments"]["items"]
        version_keys = version_info.keys()
        content = ""
        for version_key in version_keys:
            if "memo" in version_info[version_key] and version_info[version_key]["memo"]:
                memo_text = version_info[version_key]["memo"]
                content += memo_text
        return content

    @staticmethod
    def pdf_static_content(pdf_file):
        content = pdf_file.read()
        static_content, _ = re.subn(r"/ID \[<[A-Z0-9]+> <[A-Z0-9]+>\]", "", content)
        return static_content

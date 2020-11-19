"""
:class NEBillScraper: scrapes Nebraska Bills
"""
from __future__ import absolute_import

import re
import logging
import datetime

from ..common.normalize import normalize_bill_id
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ExtractionType
from fn_document_service.blocking.ttypes import ColumnSpec

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('NEBillScraper')


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-NE", group="fnleg")
class NEBillScraper(BillScraper):
    def __init__(self):
        super(NEBillScraper, self).__init__("ne")

    def scrape_bill_ids(self, session):
        bill_ids = {}
        start_year = session[0:4]
        end_year = session[4:8]
        for year in {start_year, end_year}:
            url = "http://nebraskalegislature.gov/bills/search_by_date.php?SessionDay={}".format(year)
            logger.info("Scraping bill ids from {}".format(url))
            page_doc = self.scraper.url_to_lxml(url, BRP.bill_list)
            bill_id_rows = page_doc.xpath("//table[@class='table table-condensed']//tr", BRP.test)[1:]
            for bill_id_row in bill_id_rows:
                bill_id_anchor = bill_id_row.xpath(".//a")[0]
                bill_id = bill_id_anchor.text_content()
                bill_id = normalize_bill_id(bill_id)
                bill_url = bill_id_anchor.get_attrib("href")
                bill_title = bill_id_row.xpath(".//td")[3].text_content()
                bill_ids[bill_id] = {"title": bill_title,
                                     "url": bill_url}
        logger.info("A total of {} bill ids scraped for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        bill_type = 'resolution' if 'LR' in bill_id else 'bill'
        bill_info = kwargs.get("bill_info")
        bill_page_url = bill_info["url"]
        logger.info("Scraping bill {} on {}".format(bill_id, bill_page_url))
        bill_title = bill_info["title"]
        bill = Bill(session, 'upper', bill_id, bill_title, bill_type)
        bill.add_source(bill_page_url)
        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)

        # primary sponsor
        sponsor_set = set()
        primary_sponsor_anchor_tag = bill_page_doc.xpath_single("//li[contains(text(), 'Introduced By:')]/a")
        if primary_sponsor_anchor_tag:
            primary_sponsor_text = primary_sponsor_anchor_tag.text_content()
            primary_sponsor = self.sponsor_name_normalizer(primary_sponsor_text)
            bill.add_sponsor("primary", primary_sponsor)
            sponsor_set.add(primary_sponsor)

        # scrape actions
        actions_url = bill_page_doc.xpath_single("//h3[@id='view-bill-history']//a").get_attrib("href")
        actions_page_doc = self.scraper.url_to_lxml(actions_url, BRP.bill_actions)
        for action_row in actions_page_doc.xpath('//div[@class="table-responsive"]/table/tr'):
            action_info = action_row.xpath(".//td")
            date = action_info[0].text_content()
            date = datetime.datetime.strptime(date, '%b %d, %Y')
            action = action_info[1].text_content()
            if not action:
                continue
            if 'Governor' in action:
                actor = 'executive'
            elif 'Speaker' in action:
                actor = 'upper'
            else:
                logger.info("No clear indication of actor in action text \"{}\", using \"upper\" as default "
                            "value.".format(action))
                actor = 'upper'
            bill.add_action(actor, action, date)

        pdf_extraction_type = ExtractionType.text_pdf
        # scrape version texts
        for version in bill_page_doc.xpath('//div[@class="col-sm-3"]//div[@class="list-group"]/a'):
            version_name = version.text_content()
            version_url = version.get_attrib("href")
            download_id, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(version_url, BRP.bill_documents, pdf_extraction_type,
                                                             True, self.version_parser, content_type="application/pdf")
            assert len(doc_ids) == 1
            assert len(scraper_docs) == 1
            assert download_id is not None
            doc_id = doc_ids[0]
            sponsors = scraper_docs[0].additional_data["sponsors"]
            # if a sponsor is not in the sponsors set yet, it is considered a cosponsor
            for sponsor in sponsors:
                if sponsor not in sponsor_set:
                    bill.add_sponsor("cosponsor", sponsor)
                    sponsor_set.add(sponsor)
            doc_service_document = Doc_service_document(version_name, "version", "complete",
                                                        download_id, doc_id=doc_id)
            bill.add_doc_service_document(doc_service_document)

        # scrape amendment texts
        amdt_tables = bill_page_doc.xpath("//div[@class='col-sm-4'][1]//"
                                          "table[@class='table table-condensed table-striped']")[:2]
        for amdt_table in amdt_tables:
            for amdt_row in amdt_table.xpath(".//tr", BRP.test):

                amdt_cells = amdt_row.xpath(".//td")
                amdt_name = amdt_cells[0].text_content()
                if len(amdt_name.split()) == 2:
                    amdt_name_pieces = amdt_name.split()
                    amdt_name_pieces.reverse()
                    amdt_name = " by ".join(amdt_name_pieces)
                amdt_name = "Amendment " + amdt_name
                amdt_url = amdt_cells[0].xpath_single("./a").get_attrib("href")
                amdt_result = amdt_cells[1].text_content()
                if amdt_result:
                    amdt_name = "{} ({})".format(amdt_name, amdt_result)
                download_id, scraper_docs, doc_ids = \
                    self.scraper.register_download_and_documents(amdt_url, BRP.bill_documents, pdf_extraction_type,
                                                                 True)
                assert len(scraper_docs) == 1
                assert len(doc_ids) == 1
                assert download_id is not None
                doc_id = doc_ids[0]
                doc_service_document = Doc_service_document(amdt_name, "amendment", "complete",
                                                            download_id, doc_id=doc_id)
                bill.add_doc_service_document(doc_service_document)

        # scrape related transcripts
        transcript_table = bill_page_doc.xpath_single("//div[@class='main-content']/"
                                                      "div[5]/div[2]/div[@class='hidden-xs']/table")
        for transcript in transcript_table.xpath(".//tr")[1:]:  # skip the first row because it is the header
            transcript_name = transcript.text_content()
            transcript_url = transcript.xpath_single(".//a").get_attrib("href")
            download_id = self.scraper.download_and_register(transcript_url, BRP.bill_documents, True)
            assert download_id is not None
            doc_service_document = Doc_service_document(transcript_name, "other", "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        # scrape additional info
        for document in bill_page_doc.xpath('//div[@class="col-sm-4"][1]/div[@class="hidden-xs"]//a'):
            name = document.text_content()
            url = document.get_attrib("href")
            if name.lower() == "fiscal note":
                # fiscal notes need special handling
                # if only one fiscal note, the page redirects to a PDF, although the href does not show the pdf link
                # if multiple fiscal notes, the page leads to another page with multiple PDF links
                redirected_url = self.scraper.http_request(url).url
                if redirected_url.lower().strip().endswith("pdf"):
                    date_string = redirected_url.lower().rsplit("_")[1].replace(".pdf", "")
                    submit_date = datetime.datetime.strptime(date_string, "%Y%m%d-%H%M%S")
                    download_id = self.scraper.download_and_register(redirected_url, BRP.bill_documents, True)
                    assert download_id is not None
                    fn_name = "Fiscal Note {}".format(submit_date.strftime("%m-%d-%Y"))
                    doc_service_document = Doc_service_document(fn_name, "fiscal_note", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)
                else:
                    redirected_page_doc = self.scraper.url_to_lxml(redirected_url, BRP.bill_documents)
                    for anchor_tag in redirected_page_doc.xpath("//div[@class='main-content']//a"):
                        name = anchor_tag.text_content()
                        submit_date = datetime.datetime. \
                            strptime(name.split("submitted")[1].strip(), "%B %d, %Y %I:%M%p")
                        url = anchor_tag.get_attrib("href")
                        fn_name = "Fiscal Note {}".format(submit_date.strftime("%m-%d-%Y"))
                        download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                        assert download_id is not None
                        doc_service_document = Doc_service_document(fn_name, "fiscal_note", "partial", download_id)
                        bill.add_doc_service_document(doc_service_document)
            else:
                doc_type = "other"
                if "committee" in name.lower():
                    doc_type = "committee_document"
                elif "statement of intent" == name.lower():
                    doc_type = "summary"
                else:
                    # TODO: we might want to add more as we see necessary
                    logger.info("Cannot find a good label for document {} at {}. "
                                "Labelling it as \"other\".".format(name, url))
                    pass

                download_id = self.scraper.download_and_register(url, BRP.bill_documents, True)
                assert download_id is not None
                doc_service_document = Doc_service_document(name, doc_type, "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        # scrape votes
        if actions_page_doc.xpath('//div[@class="table-responsive"]/table//th[contains(text(), "Vote")]'):
            self.scrape_votes_from_table(bill, actions_page_doc)
        else:
            self.scrape_votes_from_journal(bill, session, actions_page_doc)

        self.save_bill(bill)

    def download_journals(self, session):
        journal_entities = {}
        leg_number = self.get_legislature_number(session)
        url = "http://www.nebraskalegislature.gov/session/view_archives.php?leg={}".format(leg_number)
        doc = self.scraper.url_to_lxml(url, BRP.bill_votes)
        journal_div = doc.xpath("//div[@class='panel panel-leg']")[0]
        for anchor_tag in journal_div.xpath(".//a"):
            name = anchor_tag.text_content()
            url = anchor_tag.get_attrib("href")
            logger.info("Downloading {} from {}".format(name, url))
            _, scraper_docs, doc_ids = \
                self.scraper.register_download_and_documents(url, BRP.bill_votes,
                                                             self.scraper.extraction_type.text_pdf,
                                                             False, self.journal_parser,
                                                             column_spec=ColumnSpec.NONE)
            logger.info("Finished downloading {}".format(name))
            entities = scraper_docs[0].additional_data["entities"]
            journal_entities[url.replace("www.", "")] = entities
        return journal_entities

    @staticmethod
    def get_legislature_number(session):
        end_year = int(session[4:8])
        return end_year / 2 - 904

    def journal_parser(self, entities):
        text = entities_text_content(entities)
        return [ScraperDocument(text, additional_data={"entities": entities})]

    def get_page_entities(self, start_page, end_page, entities):
        start_page = int(start_page)
        end_page = int(end_page)
        result = []
        for entity in entities:
            page_number = self.get_page_number(entity)
            if page_number < start_page:
                continue
            elif page_number > end_page:
                break
            else:
                result.append(entity)
        return result

    @staticmethod
    def get_page_number(entity):
        if entity.textEntity:
            return entity.textEntity.pageNum
        elif entity.headerEntity:
            return entity.headerEntity.pageNum
        elif entity.tableEntity:
            return entity.tableEntity.pageNum

    def version_parser(self, entities):
        text = entities_text_content(entities)
        sponsors_text_raw = \
            re.findall(r"Introduced\s+by\s+(.*?)\.", text, re.DOTALL)[0]
        sponsors_text_raw = sponsors_text_raw.strip().strip(".").strip()
        sponsors_list = sponsors_text_raw.split(";")
        sponsors = [self.sponsor_name_normalizer(sponsor) for sponsor in sponsors_list]
        return [ScraperDocument(text, additional_data={"sponsors": sponsors})]

    def scrape_votes_from_journal(self, bill, session, actions_page_doc):
        bill_page_url = bill["sources"][0]["url"]
        bill_id = bill["id"]
        journal_entities = {}
        roll_call_vote_pattern = re.compile(r"\d+-\d+-\d+")
        for action_row in actions_page_doc.xpath('//div[@class="table-responsive"]/table/tr'):
            action_cells = action_row.xpath(".//td")
            action_text = action_cells[1].text_content()
            if re.findall(roll_call_vote_pattern, action_text):
                if not journal_entities:
                    journal_entities = self.download_journals(session)
                journal_page = int(action_cells[2].text_content())
                journal_url = action_cells[2].xpath_single(".//a").get_attrib("href")
                journal_url = journal_url.rsplit("#", 1)[0].replace("www.", "")
                vote_date = datetime.datetime.strptime(action_cells[0].text_content(), "%b %d, %Y")
                # example action text: "Passed on Final Reading with Emergency Clause 44-0-5"
                # example motion: "Final Reading with Emergency Clause"
                vote_pattern_match = re.findall(roll_call_vote_pattern, action_text)
                assert len(vote_pattern_match) == 1
                numbers = vote_pattern_match[0].split("-")
                yes_count = int(numbers[0])
                no_count = int(numbers[1])
                other_count = int(numbers[2])
                if "passed" in action_text.lower():
                    passed = True
                elif "failed" in action_text.lower():
                    passed = False
                else:
                    logger.warnning("Cannot guess vote outcome from action text {} on {} "
                                    "falling back to comparing yes-no counts".format(action_text, bill_page_url))
                    passed = yes_count > no_count
                entities = self.get_page_entities(journal_page - 1, journal_page + 10, journal_entities[journal_url])
                if "notwithstanding" in action_text.lower():
                    start_line_pattern = re.compile(r"MOTION - .+?{}".format(bill_id.replace(" ", "")))
                else:
                    start_line = "LEGISLATIVE BILL {}." if "LB" in bill_id else "LEGISLATIVE RESOLUTION {}."
                    start_line_pattern = re.compile(r"{}".format(start_line.format(bill_id.split()[1])))
                end_line_pattern = re.compile(r"(Having\s+.*?receive[d]?\s+a\s+constitutional|A\s+constitutional)")
                vote_text = re.split(start_line_pattern, entities_text_content(entities), 1)[1]
                action_text_vote_motion = None
                if "notwithstanding" in action_text.lower():
                    motion_pattern = start_line_pattern
                    motion = re.findall(motion_pattern, entities_text_content(entities))[0]
                else:
                    action_text_vote_motion = re.split(roll_call_vote_pattern, action_text)[0].split(" on ")[1].strip()
                    motion_pattern = re.compile(r"Shall\s+the.*?\?", re.DOTALL)
                    motion = re.findall(motion_pattern, vote_text)[0]
                if action_text_vote_motion:
                    motion = action_text_vote_motion
                motion = re.sub(r"\s+", " ", motion)
                # need to remove header/footer from the vote text, otherwise they may mix into voters' names
                header_pattern_even = re.compile(r"\d+\s+LEGISLATIVE JOURNAL")
                vote_text = re.sub(header_pattern_even, "", vote_text)
                header_pattern_odd = re.compile(r"[A-Z\-,0-9 ]{5,} +\d+")
                vote_text = re.sub(header_pattern_odd, "", vote_text)

                vote = Vote("upper", vote_date, motion, passed, yes_count, no_count, other_count)
                vote.add_source(journal_url)
                yes_vote_start_line_pattern = re.compile(r"Voting in the affirmative, \d+[.:]")
                no_vote_start_line_pattern = re.compile(r"Voting in the negative, \d+[.:]")
                other_vote_start_line_pattern = re.compile(r"[a-zA-Z]+ and not voting, \d+[.:]")

                yes_vote_names_text = re.split(no_vote_start_line_pattern,
                                               re.split(yes_vote_start_line_pattern, vote_text)[1])[0]
                yes_vote_name_lines = yes_vote_names_text.replace("\r", "\n").split("\n")
                for yes_vote_name_line in yes_vote_name_lines:
                    if not yes_vote_name_line.strip():
                        continue
                    names = self.get_voter_names(yes_vote_name_line)
                    for name in names:
                        vote.yes(name)

                no_vote_names_text = re.split(other_vote_start_line_pattern,
                                              re.split(no_vote_start_line_pattern, vote_text)[1], 1)[0]
                no_vote_name_lines = no_vote_names_text.replace("\r", "\n").split("\n")
                for no_vote_name_line in no_vote_name_lines:
                    if not no_vote_name_line.strip():
                        continue
                    names = self.get_voter_names(no_vote_name_line)
                    for name in names:
                        vote.no(name)

                other_vote_names_text = "\n".join(re.split(other_vote_start_line_pattern, vote_text)[1:])
                other_vote_names_text = re.split(end_line_pattern, other_vote_names_text)[0]
                other_vote_name_lines = other_vote_names_text.replace("\r", "\n").split("\n")
                for other_vote_name_line in other_vote_name_lines:
                    if not other_vote_name_line.strip():
                        continue
                    names = self.get_voter_names(other_vote_name_line)
                    for name in names:
                        vote.other(name)
                bill.add_vote(vote)

    def scrape_votes_from_table(self, bill, actions_page_doc):
        for action_row in actions_page_doc.xpath('//div[@class="table-responsive"]/table/tr'):
            vote_details_anchor = action_row.xpath_single("./td[4]/a")
            if not vote_details_anchor:
                continue
            vote_details_url = vote_details_anchor.get_attrib("href")
            date_string = action_row.xpath_single("./td[1]").text_content()
            fallback_motion = action_row.xpath_single("./td[2]").text_content()
            date = datetime.datetime.strptime(date_string, "%b %d, %Y")
            vote_details_doc = self.scraper.url_to_lxml(vote_details_url, BRP.bill_votes)
            vote_table = vote_details_doc.xpath_single("//table[@class='table table-condensed calendar-table']")
            table_headers = vote_table.xpath_single("./thead")
            motion = table_headers.xpath_single("./tr[1]/th/span").text_content()
            if not motion:
                motion = fallback_motion
            stat_string = table_headers.xpath_single("./tr[3]/th/span").text_content()
            yes_count = int(re.search(r"Yes:\s+(\d+)", stat_string).group(1))
            no_count = int(re.search(r"No:\s+(\d+)", stat_string).group(1))
            other_count = 0
            for match in re.findall(r"Not Voting:\s+(\d+)", stat_string):
                other_count += int(match)
            vote = Vote("upper", date, motion, yes_count > no_count, yes_count, no_count, other_count)
            vote.add_source(vote_details_url)
            for voter_row in vote_table.xpath("./tr"):
                cells = voter_row.xpath("./td")
                index = 0
                while index < len(cells):
                    voter_name = cells[index].text_content()
                    vote_cast = cells[index + 1].text_content()
                    index += 2
                    if not voter_name.strip():
                        continue
                    if vote_cast.startswith("Yes"):
                        vote.yes(voter_name)
                    elif vote_cast.startswith("No"):
                        vote.no(voter_name)
                    else:
                        vote.other(voter_name)

            bill.add_vote(vote)

    @staticmethod
    def sponsor_name_normalizer(sponsor_name_text):
        sponsor_name_text = re.split(r",?\s+\d+\s*", sponsor_name_text)[0]
        sponsor_name_text = sponsor_name_text.split(":")[-1].strip()
        sponsor_name_text = re.sub(r"\s*Senator\s+(.*?:)?\s*", "", sponsor_name_text)
        sponsor_name_text = re.sub(r",?\s+Chairperson\s*", "", sponsor_name_text)
        return sponsor_name_text

    @staticmethod
    def get_voter_names(vote_name_line):
        vote_name_line = vote_name_line.strip()
        names = []
        for name in re.findall(r"\w+,\s+\w+\.?", vote_name_line):
            names.append(name)
        vote_name_line = re.sub(r"\w+,\s+\w+\.?", "", vote_name_line)
        for name in re.findall(r"\w+", vote_name_line):
            names.append(name)
        return names

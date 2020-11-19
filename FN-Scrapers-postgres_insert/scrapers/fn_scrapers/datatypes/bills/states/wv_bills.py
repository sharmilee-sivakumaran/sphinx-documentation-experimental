from __future__ import absolute_import

import re
import logging
import datetime
from requests.exceptions import HTTPError
import urlparse

from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scraperutils.doc_service.fn_extraction import entities_text_content, entity_text_content
from fn_document_service.blocking.ttypes import ColumnSpec

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import (
    get_chamber_from_ahs_type_bill_id,
    normalize_bill_id,
    get_bill_type_from_normal_bill_id
)

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(u"WVBillScraper")
bill_type_map = {u"B": u"bill",
                 u"R": u"resolution",
                 u"JR": u"joint_resolution",
                 u"CR": u"concurrent_resolution"}


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-WV", group="fnleg")
class WVBillScraper(BillScraper):
    root = "http://www.wvlegislature.gov/Bill_Status/"
    search_url = root + "{}.cfm?year={}&sessiontype={}"
    bill_url = root + ("Bills_history.cfm?input={bnum}&year={year}&sessiontype="
                       "{stype}&btype=bill")
    res_url = root + ("Resolution_History.cfm?year={year}&sessiontype={stype}&"
                      "input4={bnum}&billtype={btype}&houseorig={chm}&btype=res")

    def __init__(self):
        super(WVBillScraper, self).__init__("wv")

    def scrape_bill_ids(self, session):
        url_template = u"http://www.legis.state.wv.us/Bill_Status/{}.cfm?year={}&sessiontype={}"
        session_type = self.get_session_type(session)
        year = session[:4]
        bill_ids = {}
        for list_type in [u"Bills_all_bills", u"res_list"]:
            bill_list_url = url_template.format(list_type, year, session_type)
            logger.info(u"Scraping bill/res ids for {} session on {}".format(
                session, bill_list_url))
            bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
            table = bill_list_doc.xpath_single(u"//table[@class='tabborder']")
            if table is not None:
                for row in table.xpath(u".//tr")[1:]:
                    bill_id = row.xpath_single(u".//td[1]").text_content()
                    bill_page_url = row.xpath_single(u"./td[1]/a").get_attrib(u"href")
                    bill_ids[bill_id] = {u"url": bill_page_url}
        logger.info(u"A total of {} bill ids scraped".format(len(bill_ids)))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        if kwargs and kwargs.get('bill_info'):
            bill_page_url = kwargs['bill_info'][u"url"]
        else:
            year = session[:4]
            session_type = self.get_session_type(session)
            bill_type = get_bill_type_from_normal_bill_id(bill_id)
            bill_parts = bill_id.split(' ')
            if bill_type == 'bill':
                bill_page_url = self.bill_url.format(
                    bnum=bill_parts[1], stype=session_type, year=year)
            else:
                bill_page_url = self.res_url.format(
                    year=year, stype=session_type, bnum=bill_parts[1],
                    btype=bill_parts[0][1:], chm=bill_parts[0][0]
                )
        logger.info(u"Scraping bill {} at {}".format(bill_id, bill_page_url))
        bill_page_doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        title = bill_page_doc.xpath_single(u"//div[@id='bhistcontent']//strong[text()='SUMMARY:']/"
                                           u"parent::td/following-sibling::td[1]").text_content()
        bill = Bill(session, chamber, bill_id, title, bill_type)

        bill.add_source(bill_page_url)
        # primary sponsors
        for td in bill_page_doc.xpath(u"//div[@id='bhistcontent']//strong[text()='LEAD SPONSOR:']"
                                      u"/parent::td/following-sibling::td"):

            for primary_sponsor in self.sponsor_extractor(td):
                bill.add_sponsor(u'primary', **primary_sponsor)

        # cosponsors
        for td in bill_page_doc.xpath(u"//div[@id='bhistcontent']//strong[text()='SPONSORS:']"
                                      u"/parent::td/following-sibling::td"):

            for cosponsor in self.sponsor_extractor(td):
                bill.add_sponsor(u'cosponsor', **cosponsor)

        # versions
        version_label = u"BILL TEXT:" if bill_type == u"bill" else u"RESOLUTION TEXT:"
        td = bill_page_doc.xpath_single(u"//div[@id='bhistcontent']//strong[text()='{}']"
                                        u"/parent::td/following-sibling::td[1]".format(version_label))
        # titles are names of all the version texts
        titles = [re.sub(r"\s+", u" ", text.strip().strip("-").strip())
                  for text in td.xpath(u"./text()")
                  if text.strip() and text.strip() != u"|"]
        url_dict = []
        # url dict is a list of dictionaries, where each dictionary maps a file format (html, pdf, etc) to
        # the corresponding url.
        # ideally, the length of "titles" and the length of "url_dict" are the same, where titles[x] is the version
        # text name for all urls in url_dict[x]. And that's why we have the assertion check.
        for anchor_tag in td.xpath(u".//a", policy=BRP.debug):
            if anchor_tag.text_content() == u"html":
                url_dict.append({})
                url_dict[-1][u"html"] = anchor_tag.get_attrib(u"href")
            else:
                url_dict[-1][anchor_tag.text_content()] = anchor_tag.get_attrib(u"href")

        assert len(url_dict) == len(titles)
        # for the same document, fnleg uses the first name assigned to this document,
        # and ignore all subsequent appearances
        # we loop over the original titles, and when we see a document with bad title, such as "SB330 SUB1 ENR.pdf",
        # we remove it from the titles list, and store it, temporarily, in bad_titles, and after the looping,
        # we combine the two lists, appending the "bad"
        # ones to the end. url_dict will have a parallel behavior
        bad_titles = []
        bad_url_dict = []
        good_titles = []
        good_url_dict = []
        for index in range(len(titles)):
            if titles[index].strip().lower().endswith(u".pdf"):
                bad_titles.append(titles[index].strip(u".pdf"))
                bad_url_dict.append(url_dict[index])
            else:
                good_titles.append(titles[index])
                good_url_dict.append(url_dict[index])

        titles = good_titles + bad_titles
        url_dict = good_url_dict + bad_url_dict
        index = 0
        while index < len(url_dict):
            title = titles[index]
            urls = url_dict[index]
            html_url = urls[u"html"]
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(html_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html,
                                                             False, self.version_parser)
            if len(doc_ids) == 1 and doc_ids[0] is not None and download_id is not None:
                doc_service_document = Doc_service_document(title, u"version", u"complete", download_id, doc_ids[0])
                del urls[u"html"]
                for key in urls:
                    download_id = self.scraper.download_and_register(urls[key], BRP.bill_documents, True)
                    if download_id:
                        doc_service_document.add_alternate_representation(download_id)
                bill.add_doc_service_document(doc_service_document)
            else:
                logger.warning(u"Doc service failed to process document at {}. This is likely to be because the link "
                               u"is not valid".format(html_url))
            index += 1

        # subjects
        for anchor_tag in bill_page_doc.xpath(
                u"//strong[text()='SUBJECT(S):']/parent::td/following-sibling::td[1]/a",
                policy=BRP.info):
            subject = anchor_tag.text_content()
            bill.add_subject(subject)

        # related bills
        for anchor_tag in bill_page_doc.xpath(
                u"//strong[text()='SIMILAR TO:']/parent::td/following-sibling::td[1]/a",
                policy=BRP.info):
            similar_bill_id = normalize_bill_id(anchor_tag.text_content())
            bill.add_companion(similar_bill_id)

        # actions
        action_table = bill_page_doc.xpath_single(u"//table[@class='tabborder' and contains(., 'Description') "
                                                  u"and contains(., 'Date') and contains(., 'Journal Page')]")
        for row in action_table.xpath(u"./tr")[2:]:
            actor = u"lower" if row.xpath_single(u"./td[1]").text_content() == u"H" else u"upper"
            action = row.xpath_single(u"./td[2]").text_content()
            date_string = row.xpath_single(u"./td[3]").text_content()
            date = datetime.datetime.strptime(date_string, u"%m/%d/%y")
            bill.add_action(actor, action, date)
            # check if a vote pdf is present
            if row.xpath_single(u"./td[2]//a[contains(text(), 'Roll No')]", BRP.debug):
                assert len(row.xpath(u"./td[2]//a[contains(text(), 'Roll No')]", BRP.debug)) == 1
                anchor_tag = row.xpath_single(u"./td[2]//a[contains(text(), 'Roll No')]")
                vote_url = anchor_tag.get_attrib(u"href")
                motion = re.sub(r"\s*\(.+?\)\s*", u"", action).strip()
                passed = None
                lowercase_motion = motion.lower()
                if u"pass" in lowercase_motion or u"adopt" in lowercase_motion:
                    passed = True
                elif u"reject" in lowercase_motion or u"fail" in lowercase_motion:
                    passed = False
                parser = self.house_vote_parser if actor == u"lower" else self.senate_voter_parser
                try:
                    download_id, vote_scraper_docs, doc_ids = \
                        self.scraper.register_download_and_documents(
                            vote_url,
                            BRP.bill_votes,
                            self.scraper.extraction_type.text_pdf,
                            True,
                            parser,
                            parser_args={
                                u"date": date,
                                u"motion": motion,
                                u"passed": passed},
                            column_spec=ColumnSpec.NONE)
                    if len(doc_ids) == 1 and doc_ids[0] is not None and download_id is not None:
                        vote = vote_scraper_docs[0].additional_data[u"vote"]
                        vote.add_source(vote_url)
                        bill.add_vote(vote)
                    else:
                        logger.warning(u"Doc service failed to process document at {}. "
                                       u"This is likely to be because the link "
                                       u"is not valid".format(vote_url))
                except HTTPError:
                    logger.warning(u"Doc service failed to process document at {}. "
                                   u"This is likely to be because the link "
                                   u"is not valid".format(vote_url))


        # veto message
        for td in bill_page_doc.xpath(
                u"//strong[text()='VETO MESSAGE:']/parent::td/following-sibling::td",
                policy=BRP.info):
            veto_name = td.text_content()
            veto_name = u"Veto Message: " + veto_name
            veto_name = re.sub(r"\s+", u" ", veto_name)
            veto_url = td.xpath_single(u"./a").get_attrib(u"href")
            assert len(td.xpath(u"./a")) == 1
            download_id = self.scraper.download_and_register(veto_url, BRP.bill_documents, True)
            doc_service_document = Doc_service_document(veto_name, u"summary", u"partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        # amendments
        for amdt_label, amdt_name_prefix in [(u"FLOOR AMENDMENTS:", u"Floor Amendment: "),
                                             (u"COM. AMENDMENTS:", u"Committee Amendment: ")]:
            for td in bill_page_doc.xpath(
                    u"//strong[text()='{}']/parent::td/following-sibling::td".format(amdt_label),
                    policy=BRP.info):
                for anchor_tag in td.xpath(u"./a", policy=BRP.info):
                    name = amdt_name_prefix + anchor_tag.text_content().strip(u".htm")
                    url = anchor_tag.get_attrib(u"href")
                    try:
                        download_id, _, doc_ids = self.scraper.register_download_and_documents(
                            url, BRP.bill_documents, self.scraper.extraction_type.html,
                            False, self.amdt_parser)
                    except ValueError:
                        logging.warning(
                            'Unable to extract text from amendment: %s', url)
                        continue
                    assert len(doc_ids) == 1 and download_id is not None, (
                        'Could not extract document: {}'.format(url)
                    )
                    doc_service_document = Doc_service_document(
                        name, u"amendment", u"complete", download_id, doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)

        # similar bills
        for td in bill_page_doc.xpath(
                u"//string[text()='SIMILAR TO:']/parent::td/following-sibling::td",
                policy=BRP.info):
            for anchor_tag in td.xpath(u"./a", policy=BRP.info):
                similar_bill_id = normalize_bill_id(anchor_tag.text_content())
                bill.add_companion(similar_bill_id)

        # fiscal note
        for td in bill_page_doc.xpath(
                u"//strong[text()='FISCAL NOTES:']/parent::td/following-sibling::td",
                policy=BRP.info):
            for anchor_tag in td.xpath(u"./a", policy=BRP.info):
                name = u"Fiscal Note: " + anchor_tag.text_content()
                url = anchor_tag.get_attrib(u"href")
                download_id = self.scraper.download_and_register(url, BRP.bill_documents, False)
                doc_service_document = Doc_service_document(name, u"fiscal_note", u"partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    @staticmethod
    def get_session_type(session):
        assert session.endswith(u"r") or u"ss" in session
        if session.endswith(u"r"):
            return u"rs"
        else:
            return session.split(u"ss")[1] + u"x"

    @staticmethod
    def version_parser(element_wrapper):
        text = element_wrapper.xpath_single(u"//div[@id='wrapper']").text_content()
        return [ScraperDocument(text)]

    @classmethod
    def house_vote_parser(cls, entities, parser_args=None):
        vote = None
        motion = None
        date = None
        passed = None
        if parser_args is not None:
            motion = parser_args[u"motion"]
            date = parser_args[u"date"]
            passed = parser_args[u"passed"]

        header_pattern = re.compile(
            r"YEAS:\s+(\d+)\s+NAYS:\s+(\d+)\s+NOT\s+VOTING:\s+(\d+)\s+(?:PAIRED:\s+\d+\s+)?(\w+)"
        )
        found_header = False
        section_pattern = re.compile(
            r"^\s*(YEAS|NAYS|NOT\s+VOTING|PAIRED):\s+(\d+)\s*(.*)"
        )

        sections = {'yeas': [], 'nays': []}
        section = None # current key
        for index in range(0, len(entities)):
            entity = entities[index]
            line = entity_text_content(entity)
            if not found_header:
                header = header_pattern.search(line)
                if header:
                    found_header = True
                    yes_count = int(header.group(1))
                    no_count = int(header.group(2))
                    other_count = int(header.group(3))
                    result = header.group(4).lower()
                    if u"pass" in result or u"adopt" in result:
                        passed = True
                    if u"reject" in result or u"fail" in result:
                        passed = False
                    if passed is None:
                        passed = yes_count > no_count
                continue
            section_header = section_pattern.search(line)

            if section_header: # new section
                section = section_header.group(1).lower()
                sections[section] = []
                extra_names = section_header.group(3).strip()
                if extra_names:
                    sections[section] += cls.get_voters(extra_names)
                continue
            assert section is not None
            sections[section] += cls.get_voters(line)

        other_voters = []
        for key in sections:
            if key == 'paired':
                for i in range(0, len(sections['paired']), 2):
                    if sections['paired'][i+1] == '(YEA)':
                        sections['yeas'].append(sections['paired'][i])
                    elif sections['paired'][i+1] == '(NAY)':
                        sections['nays'].append(sections['paired'][i])
                    else:
                        other_voters.append(sections['paired'][i])
            elif key not in ['yeas', 'nays']:
                other_voters += sections[key]

        vote = Vote(u"lower", date, motion, passed, yes_count, no_count,
                    other_count)

        for method, voters in [(vote.yes, sections['yeas']),
                               (vote.no, sections['nays']),
                               (vote.other, other_voters)]:
            for name in voters:
                if name:
                    method(name)
        assert vote is not None
        scraper_doc = ScraperDocument(entities_text_content(entities))
        scraper_doc.additional_data = {u"vote": vote}
        return [scraper_doc]

    @staticmethod
    def senate_voter_parser(entities, parser_args=None):
        vote = None
        for index in range(0, len(entities)):
            entity = entities[index]
            line = entity_text_content(entity)
            if re.search(r"Yea:\s+\d+\s+Nay:\s+\d+", line):
                yes_count = 0
                no_count = 0
                other_count = 0
                for cast, count in re.findall(r"(\w+)\s*:\s+(\d+)", line):
                    if cast == u"Yea":
                        yes_count += int(count)
                    elif cast == u"Nay":
                        no_count += int(count)
                    else:
                        other_count += int(count)

                index += 1
                voters = entity_text_content(entities[index])

                passed = None
                date = None
                motion = None
                if parser_args is not None:
                    passed = parser_args[u"passed"]
                    date = parser_args[u"date"]
                    motion = parser_args[u"motion"]
                if passed is None:
                    passed = yes_count > no_count
                vote = Vote(u"upper", date, motion, passed, yes_count, no_count, other_count)
                for cast, voter in re.findall(r"([A-Z])\s+((?:MR|MS|MRS)?\s*[^\s]{2,})", voters):
                    if cast == u"Y":
                        vote.yes(voter)
                    elif cast == u"N":
                        vote.no(voter)
                    else:
                        vote.other(voter)
                break
        assert vote is not None
        scraper_doc = ScraperDocument(entities_text_content(entities))
        scraper_doc.additional_data = {u"vote": vote}
        return [scraper_doc]

    @staticmethod
    def amdt_parser(element_wrapper):
        text = element_wrapper.xpath_single(u"//body").text_content()
        if not text.strip():
            raise ValueError("No text content")
        return [ScraperDocument(text)]

    @staticmethod
    def get_voters(voters_line):
        voters = []
        voters_line = re.sub(r"\s+", u" ", voters_line)
        for name in re.findall(r"[^\s]+,\s\w+\.", voters_line):
            voters.append(name)
        voters_line = re.sub(r"[^\s]+,\s\w+\.", u"", voters_line)
        for name in re.split(r"\s+", voters_line.strip()):
            if name.lower() == u"speaker":
                continue
            voters.append(name.strip())
        return voters

    def sponsor_extractor(self, element):
        '''Find (co)sponsors a set of links, returning a list containing dicts
        of known properties (name, possibly chamber).

        Expects an html element containing one or more child elements of the
        format:
            <a href="http://www.legis.state.wv.us/Bill_Status/Bills_Sponsors.cfm?
                     year=2017&sessiontype=RS&btype=bill&senmem=Hall"
               title="View bills Senator Hall sponsored during 2017 Regular
                      Session."
            >Hall</a><br>
        '''
        sponsors = []

        # "Mr. Speaker (Mr. Armstead)"
        speaker_pattern = re.compile(r"Speaker\s+\(M[a-z.]+\s+([^)]+)\)")

        for sponsor in element.xpath('.//a', policy=BRP.debug):
            spons = {'name': sponsor.text_content().strip()}

            speaker_match = speaker_pattern.search(spons['name'])
            if speaker_match:
                spons['name'] = speaker_match.group(1)
            query = urlparse.parse_qs(
                urlparse.urlparse(sponsor.get_attrib('href')).query
            )

            if "senmem" in query:
                spons['chamber'] = 'upper'
            elif "hsemem" in query:
                spons['chamber'] = 'lower'

            sponsors.append(spons)

        return sponsors

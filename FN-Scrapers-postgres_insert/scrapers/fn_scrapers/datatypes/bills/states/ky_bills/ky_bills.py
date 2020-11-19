from __future__ import absolute_import

import re
from dateutil.parser import parse
import datetime
import logging
from fn_scrapers.datatypes.bills.common import (
    BillScraper, Bill, BillReportingPolicy as BRP, Doc_service_document, Vote)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id)
from fn_scrapers.datatypes.bills.common.metadata import get_session
from requests.exceptions import HTTPError

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(__name__)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-KY", group="fnleg")
class KYBillScraper(BillScraper):
    action_pattern = re.compile(
        r"(Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec) "
        r"(\d+, \d{4}) - (.*?)[\r\n]+")

    def __init__(self):
        super(KYBillScraper, self).__init__("ky")

    def scrape_bill_ids(self, session):
        slug = session[2:4]
        url_list = ["http://www.lrc.ky.gov/record/%srs/bills_S.htm" % slug,
                    "http://www.lrc.ky.gov/record/%srs/bills_H.htm" % slug,
                    "http://www.lrc.ky.gov/record/%srs/res_S.htm" % slug,
                    "http://www.lrc.ky.gov/record/%srs/res_H.htm" % slug
                    ]

        # Prefile Function
        self.scrape_prefile_list(session)

        session_data = get_session(
            self.scraper.metadata_client,
            self.locality,
            session)
        session_start = parse(session_data.startDate).date()
        # If the session hasn't started yet, we don't throw
        # a critical if we get a 404 when trying to request
        # the bill list pages.
        if session_start > datetime.date.today():
            reporting_policy = BRP.bill_list.set_severity(self.scraper.severity.info)
        else:
            reporting_policy = BRP.bill_list
        bill_ids = set()
        for url in url_list:
            list_doc = self.scraper.url_to_lxml(url, reporting_policy)
            if not list_doc:
                continue
            bill_id_table = list_doc.xpath("//p[@class='StandardText leftDivMargin']/a")
            for bill_id_ele in bill_id_table:
                bill_id = bill_id_ele.get_attrib('href')
                bill_id = re.findall(r"rs/(.*)\.htm", bill_id)[0]
                bill_ids.add(bill_id)

        return list(bill_ids)

    def scrape_bill(self, session, bill_id, **kwargs):

        bid = re.sub(r'\s+', '', bill_id)
        slug = session[2:4]
        bill_url = "http://www.lrc.ky.gov/record/%sRS/%s.htm" % (slug, bid)
        list_doc = self.scraper.url_to_lxml(bill_url, BRP.bill)
        list_doc_text = list_doc.text_content()

        if list_doc is None:
            logger.error('Failed to scrape bill page for %s' % bill_id)
            return
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        text_pattern = "%s.*?- (.*?)\r\n(.*?)\r\n(.*?)\r\n" % bill_id
        sponsor_str, title, description = re.findall(text_pattern, list_doc_text, re.S)[0]

        bill = Bill(session, chamber, bill_id, title.strip(), bill_type)
        bill.add_source(bill_url)

        sponsor_list = sponsor_str.split(',')
        for sponsor in sponsor_list:
            sponsor = sponsor.strip()
            bill.add_sponsor('primary', sponsor)

        description = description.strip()
        if description:
            bill.add_summary(description)
        for action in self.parse_actions(list_doc_text):
            bill.add_action(*action)

        doc_list = []
        doc_table = list_doc.xpath('//a[contains(@href, ".pdf")]', BRP.bill_documents)
        for doc_row in doc_table:
            doc_name = doc_row.text_content()
            doc_name = re.sub('/', '', doc_name).strip()
            doc_url = doc_row.get_attrib('href')
            if doc_url in doc_list:
                continue
            doc_list.append(doc_url)
            if 'bill.pdf' in doc_url:
                doc_name = re.findall(r'\((.*)\)', doc_name)
                if doc_name:
                    doc_name = doc_name[0]
                else:
                    doc_name = "Current Version"
                download_id, _, doc_ids = self.scraper.register_download_and_documents(
                    doc_url, BRP.bill_versions, self.scraper.extraction_type.unknown_new,
                    True, content_type='application/pdf')

                doc_service_document = Doc_service_document(
                    doc_name, "version", "complete", download_id=download_id,
                    doc_id=doc_ids[0])

            elif 'LM.pdf' in doc_url or 'CI' in doc_url or 'FN.pdf' in doc_url or 'AA' in doc_url:
                try:
                    doc_download_id = self.scraper.download_and_register(
                        doc_url, BRP.bill_documents, False)
                except HTTPError:
                    logger.warning("Bad Document Link %s" % doc_url)
                    continue

                if 'LM.pdf' in doc_url:
                    name = "Fiscal Impact"
                elif 'FN.pdf' in doc_url or 'AA' in doc_url:
                    name = "FISCAL NOTE"
                else:
                    name = 'CORRECTIONS IMPACT'
                doc_service_document = Doc_service_document(name, 'fiscal_note', "partial", doc_download_id)
            elif 'Vote History' in doc_name:
                _, vote_page, _ = self.scraper.register_download_and_documents(
                    doc_url, BRP.bill_votes, self.scraper.extraction_type.unknown_new,
                    True, content_type="application/pdf")

                vote_pages = u'\n'.join([v.text for v in vote_page])
                for vote in self.parse_votes(vote_pages, doc_url):
                    if vote:
                        bill.add_vote(vote)
                    else:
                        logger.warning('Unable to scrape vote: {}'.format(bill_id))

            elif 'veto.pdf' in doc_url:
                try:
                    doc_download_id = self.scraper.download_and_register(
                        doc_url, BRP.bill_documents, False)
                except HTTPError:
                    logger.warning("Bad Document Link %s" % doc_url)
                    continue

                doc_service_document = Doc_service_document("Veto Message", 'summary', "partial", doc_download_id)

            else:
                doc_name = "Amendment %s" % doc_name
                download_id, _, doc_ids = self.scraper.register_download_and_documents(
                    doc_url, BRP.bill_documents, self.scraper.extraction_type.unknown_new,
                    True, content_type='application/pdf')
                if len(doc_ids) != 1 or not doc_ids[0]:
                    logger.warning("Bad Amendment Link %s" % doc_url)
                    continue
                doc_service_document = Doc_service_document(
                    doc_name, "amendment", "complete", download_id=download_id,
                    doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    def scrape_prefile_list(self, session):
        bill_abbr = None

        url = "http://www.lrc.ky.gov/record/%srs/prefiled/prefiled_bills.htm" % session[2:4]
        page = self.scraper.url_to_lxml(url, BRP.bill)

        for link in page.xpath("//a"):
            if re.search(r"\d{1,4}\.htm", link.get_attrib('href')):
                bill_id = link.text
                match = re.match(r'([A-Z]+)\s?\d+', link.text)
                if match:
                    bill_abbr = match.group(1)
                else:
                    bill_id = bill_abbr + " " + bill_id

                try:
                    self.parse_prefile(session, bill_id, link.get_attrib('href'))
                except Exception as e:
                    logger.warning("Error parsing %s: %s" % (bill_id, e.message))

    def parse_prefile(self, session, prefile_id, url):
        chamber_re = r'http://www\.lrc\.ky\.gov/record/.*?/(.*)\.htm'
        try:
            bill_id = re.findall(chamber_re, url)[0]
        except:
            logger.warning('Failed to scrape bill id')
            return

        page = self.scraper.url_to_lxml(url, BRP.bill)
        try:
            version_link = page.xpath("//a[contains(@href, 'bill.pdf')]")[0]
            sponsor_link = page.xpath("//a[contains(@href, 'legislator')]/@href")[0]
        except IndexError:
            # Bill withdrawn
            logger.warning('Failed to scrape version link')
            return

        chamber = 'lower' if '/H' in sponsor_link else 'upper'

        pars = version_link.getparent().text_content().splitlines()
        if len(pars) < 3:
            logger.error("Error with %s Couldn't get title, bailing." % bill_id)
            return
        title = pars[1].strip()

        sponsors = pars[0].split('-')[1]
        sponsor = sponsors.split(',')

        if 'CR' in bill_id:
            bill_type = 'concurrent_resoluition'
        elif 'JR' in bill_id:
            bill_type = 'joint_resolution'
        elif 'R' in bill_id:
            bill_type = 'resolution'
        else:
            bill_type = 'bill'

        bill = Bill(session, chamber, prefile_id, title, bill_type)

        # The first two lines are the bill link, and the last line is the bill action so grab everything in between
        summary = "\n".join(pars[2:-1]).strip()
        if not summary:
            summary = title
        bill['summary'] = summary

        # The final line of the prefile is the one action, which is the prefiling date
        action_text = pars[-1]
        action_data = action_text.split(u" - ")
        if len(action_data) == 2:
            action_date = parse(action_data[0])
            action_string = action_data[1].strip()
            bill.add_action(chamber, action_string, action_date)

        bill.add_source(url)

        download_id, _, doc_ids = self.scraper.register_download_and_documents(
            version_link.get_attrib('href'), BRP.bill_versions,
            self.scraper.extraction_type.unknown_new, True,
            content_type='application/pdf')

        doc_service_document = Doc_service_document(
            "Prefile %s" % prefile_id, "version", "complete",
            download_id=download_id, doc_id=doc_ids[0])

        bill.add_doc_service_document(doc_service_document)

        for spon in sponsor:
            spon = re.sub(r'(Representative|Senator)', '', spon).strip()
            bill.add_sponsor('primary', spon)
        self.save_bill(bill)

    @classmethod
    def parse_votes(cls, vote_text, url):
        vote_pages = re.split(r"\s+?COMMONWEALTH OF KENTUCKY\s+?(?=house|senate)", vote_text.strip(), flags=re.I)
        for vote_page in vote_pages:
            if re.search(r'SENATE', vote_page, re.I):
                chamber = 'upper'
            elif re.search(r'House', vote_page, re.I):
                chamber = 'lower'
            else:
                chamber = 'joint'

            try:
                motion = re.search(r"(R(?:SN|CS)# \d+)", vote_page).group(1)
            except AttributeError:
                logger.warning("Unable to parse vote motion")
                continue

            try:
                date = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", vote_page).group(1)
            except AttributeError:
                logger.warning("Unable to extract date")
                continue

            try:
                time = re.search(r"(\d{1,2}:\d{1,2}:\d{1,2}\s*?[AP]M)", vote_page).group(1)
            except AttributeError:
                logger.warning("Unable to extract time")
                time = ''

            if date:
                try:
                    date = parse("{} {}".format(date, time))
                except ValueError:
                    try:
                        date = parse(date)
                    except ValueError:
                        logger.warning("Unable to parse date from {!r}".format(date))
                        continue

            try:
                yes_count = int(re.search(r"YEAS:\s+?(\d+)", vote_page).group(1))
            except AttributeError:
                logger.warning("Unable to extract yes count")
                continue
            try:
                no_count = int(re.search(r"NAYS:\s+?(\d+)", vote_page).group(1))
            except AttributeError:
                logger.warning("Unable to extract no count")
                continue
            try:
                nv_count = int(re.search(r"NOT VOTING:\s+?(\d+)", vote_page).group(1))
            except AttributeError:
                logger.warning("Unable to extract NOT VOTING count")
                continue

            try:
                other_count = int(re.search(r"(?:PASSES|ABSTAINED):\s+?(\d+)", vote_page).group(1))
                other_count += nv_count
            except AttributeError:
                logger.warning("Unable to extract other count")
                continue

            vote = Vote(chamber, date, motion, yes_count > no_count, yes_count, no_count, other_count)
            vote.add_source(url)

            yes_people = re.search(r"YEAS\s+?:\s+?\d+(.+?)NAYS", vote_page, re.DOTALL)
            no_people = re.search(r"NAYS\s+?:\s+?\d+(.+?)(?:PASSES|ABSTAINED)", vote_page, re.DOTALL)
            nv_people = re.search(r"NOT VOTING\s+?:\s+?\d+(.+?)$", vote_page, re.DOTALL)
            other_people = re.search(r"(?:PASSES|ABSTAINED)\s+?:\s+?\d+(.+?)NOT ", vote_page, re.DOTALL)

            for people, func in [(yes_people, vote.yes),
                                 (no_people, vote.no),
                                 (nv_people, vote.other),
                                 (other_people, vote.other)]:
                if people:
                    people = people.group(1).strip()
                    people = re.sub(r"\n", '  ', people).strip()
                    people_list = re.split(r"\s{2,}", people)
                    for person in people_list:
                        if person.strip():
                            func(person.strip())

            yield vote

    @classmethod
    def parse_actions(cls, text):
        action_table = cls.action_pattern.findall(text)
        cur_actor = None
        for (month, date, action_list) in action_table:
            date = datetime.datetime.strptime(
                '{} {}'.format(month[0:3], date), '%b %d, %Y')
            for action in action_list.split(';'):
                action = action.strip()
                if not action:
                    continue
                if 'House' in action or '(H)' in action:
                    actor = 'lower'
                    cur_actor = actor
                elif 'Senate' in action or '(S)' in action:
                    actor = 'upper'
                    cur_actor = actor
                elif cur_actor:
                    actor = cur_actor
                else:
                    actor = 'other'
                yield actor, action, date


    """
    #Subject Function
    def scrape_subjects(self, session, bill_ids):
        url = "http://www.lrc.ky.gov/record/%sRS/indexhd.htm" % session[2:4]
        doc = self.scraper.url_to_lxml(url, BRP.bill_subjects)
        for subj_ele in doc.xpath('//a[contains(@href, ".htm")]'):
            # subject links are 4 numbers
            if re.findall(r'\d{4}', subj_link):
                subject_name = subj_ele.text_content()
                sdoc = self.scraper.url_to_lxml(subj_link, BRP.bill_subjects)
                for bill in sdoc.xpath('//div[@class="StandardText leftDivMargin"]//a/text()'):
                    if bill not in bill_ids:
                        continue
                    sub_list = bill_ids[bill]
                    if subject_name not in sub_list:
                        bill_ids[bill].append(subject_name)
    """

from __future__ import absolute_import

import logging
import re
import urllib

from dateutil.parser import parse

from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.datatypes.bills.common import (
    Bill, BillScraper, Vote, BillReportingPolicy as BRP, Doc_service_document)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id)
from fn_scraperutils.doc_service.util import ScraperDocument

logger = logging.getLogger(__name__)

name_re = re.compile((
    ur"(?P<last>[\w'-]+(?: [\w'-]+)?), (?P<first_mi>[\w'-]+\.*(?:\s\w\.)?)(?:, "
    ur"(?P<honorific>Jr\.|I+|Sr\.))?"), flags=re.U)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-MA", group="fnleg")
class MABillScraper(BillScraper):
    def __init__(self):
        super(MABillScraper, self).__init__("ma")

    def scrape_bill_ids(self, session):
        bill_list_url = "https://malegislature.gov/Bills/Search?SearchTerms=&Page=1"
        bills_url = "https://malegislature.gov/Bills/Search?Page={}&Refinements%5Blawsgeneralcourt%5D={}"
        session_id = self.get_session_id(session)
        bill_list_page = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        session_list = bill_list_page.xpath("//div[@data-refinername='lawsgeneralcourt']//label")
        session_code = None
        for session in session_list:
            session_name = session.text_content()
            ses_id = int(re.findall(r'^(\d+)', session_name)[0])
            if session_id != ses_id:
                continue
            session_code = session.xpath_single("./input").get_attrib('data-refinertoken')
            session_code = re.sub(r"\"", "", session_code)
            break
        if not session_code:
            logger.warning("No Bill List for session %s" % session)
            return []

        session_code = session_code.encode('utf8')
        session_code = urllib.quote(session_code)

        bill_ids = set()
        index = 1
        max_index = 1
        while index <= max_index:
            url = bills_url.format(index, session_code)
            bills_page = self.scraper.url_to_lxml(url, BRP.bill_list)
            index += 1
            if max_index == 1:
                last_str = bills_page.xpath("//ul[@class='pagination pagination-sm']/li/a", BRP.test)
                if last_str:
                    last_str = last_str[-1].get_attrib('onclick')
                    max_index = int(re.findall(r'changePage\((\d+)\);', last_str)[0])

            bill_list = bills_page.xpath("//table[@id='searchTable']//tr/td[2]/a[contains(@href, '/Bills/')]")
            for bill in bill_list:
                bill_id = bill.text_content()
                bill_ids.add(bill_id)
        return list(bill_ids)

    def scrape_bill(self, session, bill_id, **kwargs):
        """
        scrape individual bill
        """
        url_bill_id = re.sub(r'\s+', '', bill_id)
        bill_base_url = "https://malegislature.gov/Bills/%s/%s"

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        session_id = self.get_session_id(session)
        bill_url = bill_base_url % (session_id, url_bill_id)
        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)

        # MA will occasionally post bills without titles and without descriptions. Because of this, we switch the
        # title and description fields when there is only a description, and we use the bill_id_title otherwise
        title = bill_page.xpath('//div[@id="contentContainer"]//h2/text()')
        summary = bill_page.xpath_single('//p[@id="pinslip"]/text()')
        bill_id_title = bill_page.xpath('//div[contains(@class, "titleBar")]//h1/text()')

        if title and title[0] != "Similar Bills":
            title = title[0]
        elif summary:
            title = summary
            summary = None
        elif bill_id_title and bill_id_title[0].strip():
            title = bill_id_title[0].strip()
        else:
            raise ValueError("No title found for bill: {} url: {}".format(bill_id, bill_url))

        header = bill_page.xpath_single("//h1").text_content()
        if 'bill' in header.lower() or 'order' in header.lower() or 'reorg' in header.lower():
            bill_type = 'bill'
        elif 'resolution' in header.lower() or 'resolve' in header.lower():
            bill_type = 'resolution'
        elif 'constitutional amendment' in header.lower():
            bill_type = 'constitutional_amendment'
        else:
            bill_type = 'bill'

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)

        if summary:
            bill.add_summary(summary)

        sponsor_list = []
        sponsor = bill_page.xpath_single('//dt[text()="Sponsor:"]/following-sibling::dd', BRP.test)
        if not sponsor:
            sponsor = bill_page.xpath_single('//dt[text()="Presenters:"]/following-sibling::dd', BRP.test)
        if not sponsor:
            sponsor = bill_page.xpath_single('//dt[text()="Presenter:"]/following-sibling::dd', BRP.test)
        if sponsor:
            more_sponsors = sponsor.xpath('./a', BRP.test)
            if more_sponsors:
                for sponsor_name in more_sponsors:
                    sponsor_name = sponsor_name.text_content()
                    bill.add_sponsor('primary', sponsor_name)
                    sponsor_list.append(sponsor_name)
            else:
                sponsor_name = sponsor.text_content()
                bill.add_sponsor('primary', sponsor_name)
                sponsor_list.append(sponsor_name)

        cosponsor_url = bill_url + '/CoSponsor'
        cosponsor_page = self.scraper.url_to_lxml(cosponsor_url, BRP.test)
        cosponsor_table = cosponsor_page.xpath("//div[@id='searchResults']//tr//a", BRP.test)
        for cosponsor in cosponsor_table:
            cosponsor_name = cosponsor.text_content()
            if cosponsor_name not in sponsor_list:
                sponsor_list.append(cosponsor_name)
                bill.add_sponsor('cosponsor', cosponsor_name)

        action_table = bill_page.xpath("//div[@id='searchResults']//tr", BRP.test)
        for action_line in action_table[1:]:
            action_ele = action_line.xpath('./td')
            action_date = action_ele[0].text_content()
            action_date = parse(action_date)
            action_chamber = {'Senate': 'upper',
                              'House': 'lower',
                              'Joint': 'other',
                              'Executive': 'executive',
                              '': 'other'}[action_ele[1].text_content()]
            action_str = action_ele[2].text_content()
            bill.add_action(action_chamber, action_str, action_date)

        index_ele = bill_page.xpath("//div[@id='searchResults']//ul[@class='pagination pagination-sm']/li", BRP.test)
        if index_ele:
            index_str = index_ele[-1].xpath_single('./a').get_attrib('onclick')
            index = re.findall(r'return reloadAjaxContent\((\d+)\);', index_str)[0]
            index = int(index)
            cur = 2
            while cur <= index:
                action_page_url = bill_url + '/BillHistory?pageNumber=' + str(cur)
                action_page = self.scraper.url_to_lxml(action_page_url, BRP.test)
                action_table = action_page.xpath("//div[@id='searchResults']//tr", BRP.test)
                cur += 1
                for action_line in action_table[1:]:
                    if not action_line.text_content():
                        continue
                    action_ele = action_line.xpath('./td')
                    action_date = action_ele[0].text_content()
                    action_date = parse(action_date)
                    action_chamber = {'Senate': 'upper',
                                      'House': 'lower',
                                      'Joint': 'other',
                                      '': 'other',
                                      'Executive': 'executive'}[action_ele[1].text_content()]
                    action_str = action_ele[2].text_content()
                    bill.add_action(action_chamber, action_str, action_date)

        version_link = bill_page.xpath_single("//a[contains(text(), 'Download PDF')]").get_attrib('href')
        if version_link:
            try:
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(version_link, BRP.bill_versions,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, should_download=True,
                                                                 should_skip_checks=True,
                                                                 content_type='application/pdf')

                doc_service_document = Doc_service_document("Current Version", "version", "complete",
                                                            download_id, doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
            except:
                logger.warning("Bad Document Link %s" % version_link)

        self.scrape_amendment('House', bill, bill_url)
        self.scrape_amendment('Senate', bill, bill_url)

        if bill_page.xpath("//*[@id='RollCall']"):
            vote_url = bill_url + '/RollCall'
            vote_page = self.scraper.url_to_lxml(vote_url, BRP.test)
            if vote_page:
                vote_table = vote_page.xpath("//div[contains(@class, 'tab-content')]//table", BRP.test)
                for vote in vote_table:
                    self.scrape_vote(vote, bill)
        self.save_bill(bill)

    def scrape_vote(self, vote_table, bill):
        vote_name = vote_table.xpath_single('./caption').text

        date = vote_table.xpath_single('./caption/span').text_content()
        formed_date = parse(date)
        motion = vote_table.xpath_single('.//td[@class="titleCol"]').text_content()
        motion = u"{} - {}".format(vote_name.strip(), motion.strip())
        summary = self.vote_summary_parse(
            vote_table.xpath_single('.//td[@class="statusCol"]').text_content())

        if 'Senate' in vote_name:
            vote_chamber = 'upper'
        elif 'House' in vote_name:
            vote_chamber = 'lower'
        else:
            vote_chamber = bill['chamber']

        vote = Vote(
            vote_chamber, formed_date, motion, summary['yeas'] > summary['nays'],
            summary['yeas'], summary['nays'], summary['other'],
            external_id=vote_name)

        link = vote_table.xpath_single('.//td[@class="downloadCol"]/a')
        if link:
            link = link.get_attrib('href')
            vote.add_source(link)
            _, vote_page, _ = self.scraper.register_download_and_documents(
                link, BRP.bill_votes, self.scraper.extraction_type.text_pdf,
                False, should_download=True, content_type='application/pdf',
                column_spec=2)

            pdf_text = vote_page[0].text
            pdflines = re.split(r'\n', pdf_text)

            flag = None
            for line in pdflines:
                """
                When a particular section is empty, it is separated by a unicode dash character with a trailing digit
                and a period. Since the category of the voter is being determined by this line, it is required that we
                separate/remove the sesction that has not votes.
                """
                if re.search(ur'YEAS|NAYS|ABSENT OR NOT VOTING|ANSWERED', line):
                    line = re.sub(ur"(YEAS|NAYS|ABSENT OR NOT VOTING|ANSWERED.+?)\s\u2212\s\d+.", u"", line).strip()
                if 'YEAS' in line:
                    flag = 'Y'
                elif 'NAYS' in line:
                    flag = 'N'
                elif 'ABSENT OR NOT VOTING' in line or 'ANSWERED' in line:
                    # Sometimes other votes are being categorized into section that looks like ANSWERED "PRESENT", etc.
                    flag = 'A'
                elif flag:
                    voter_li = re.split(r'\s{3,}', line)
                    for name in voter_li:
                        name = name.strip()
                        name = re.sub(ur"\s+\u2212\s.+$", u"", name, flags=re.U)
                        names = []
                        match = name_re.findall(name.strip())
                        if len(match) < 3:
                            # There can only be at most two names in one string
                            # This is because of the way we get the text from the document service
                            # It is a 2 column separated text PDF file.
                            for last, first_mi, honorific in match:
                                if honorific:
                                    names.append(u"{}, {}, {}".format(last.strip(), first_mi.strip(),
                                                                      honorific.strip()))
                                else:
                                    names.append(u"{}, {}".format(last.strip(), first_mi.strip()))
                        if not names:
                            continue
                        if flag == 'Y':
                            for name in names:
                                vote.yes(name)
                        elif flag == 'N':
                            for name in names:
                                vote.no(name)
                        elif flag == 'A':
                            for name in names:
                                vote.other(name)
        bill.add_vote(vote)

    def scrape_amendment(self, chamber, bill, bill_url):
        """
        Scrape amendement from amendment pages
        """
        amend_url = bill_url + '/Amendments/' + chamber
        amend_page = self.scraper.url_to_lxml(amend_url, BRP.test)
        if not amend_page:
            logger.warning("Failed to scrape amendment table %s" % amend_url)
            return
        amend_table = amend_page.xpath("//div[@id='searchResults']//tr", BRP.test)
        self.scrape_amend_table(amend_table, bill, chamber)
        index_ele = amend_page.xpath("//div[@id='searchResults']//ul[@class='pagination pagination-sm']/li", BRP.test)
        if index_ele:
            index_str = index_ele[-1].xpath_single('./a').get_attrib('onclick')
            index = re.findall(r'return reloadAjaxContent\((\d+)\);', index_str)[0]
            index = int(index)
            cur = 2
            while cur <= index:
                amend_page_url = amend_url + '?pageNumber=' + str(cur)
                amend_page = self.scraper.url_to_lxml(amend_page_url, BRP.test)
                amend_table = amend_page.xpath("//div[@id='searchResults']//tr", BRP.test)
                self.scrape_amend_table(amend_table, bill, chamber)
                cur += 1

    def scrape_amend_table(self, amend_table, bill, chamber):
        """
        Scrape amendment tables
        """
        for amend in amend_table[1:]:
            if not amend.text_content():
                continue
            amend_file_url = amend.xpath_single('.//a[contains(@href, "/GetAmendmentContent")]')
            if not amend_file_url:
                continue
            clerk_no = amend_file_url.text_content()
            amend_file_url = amend_file_url.get_attrib('href')
            amend_name = chamber + " Clerk #" + clerk_no
            if 'Preview' not in amend_file_url:
                continue
            amend_file_url = re.sub('Preview', 'Content', amend_file_url)
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(amend_file_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.html,
                                                             False, self.amend_parser)

            if not doc_ids:
                logger.warning("Could not download amendment {}".format(amend_file_url))
                continue
            doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                        download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

    @staticmethod
    def get_session_id(session):
        session_year = int(session[:4])
        session_id = (session_year - 2015) / 2 + 189
        return session_id

    @staticmethod
    def amend_parser(element_wrapper):
        text = element_wrapper.xpath_single("//body").text_content()
        return [ScraperDocument(text)]

    @classmethod
    def vote_summary_parse(cls, status_text):
        '''
        Parse a vote summary string into vote counts. 
        
        NOTE: MA's vote count seems to be off for some reason (see H2424: 
        37/0/1 summary but 37/0/2 votes).
        '''
        summary = {'yeas': 0, 'nays': 0, 'other': 0}
        status_text = re.sub(r'\s+', ' ', status_text)
        for field, count in re.findall(r'(\w+) - (\d+)', status_text):
            if field == 'Yea':
                summary['yeas'] = int(count)
            elif field == 'Nay':
                summary['nays'] = int(count)
            else:
                summary['other'] += int(count)
        return summary

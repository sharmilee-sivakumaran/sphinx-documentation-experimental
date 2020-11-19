# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import logging
from dateutil.parser import parse

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from ..common import metadata

from fn_scrapers.api.scraper import scraper, tags

from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy

post_data = {
            "BillSponsorIndex": -1,
            "BillReferredIndex": -1,
            "BillTypeIndex": -1,
            "BillListIndex": -1,
            "Chamber": 'B'
            }


bill_list_url = "http://www.myfloridahouse.gov/Sections/Bills/bills.aspx"


logger = logging.getLogger('FLBillScraper')


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-FL", group="fnleg")
class FLBillScraper(BillScraper):
    def __init__(self):
        retry_policy = Retry500RequestsRetryPolicy(
            max_attempts=None, max_retry_time=300)
        super(FLBillScraper, self).__init__("fl", retry_policy=retry_policy)

    def scrape_bill_ids(self, session):

        session_name = metadata.get_session_name(self.scraper.metadata_client, 'fl', session)
        req = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
        session_id = None
        session_list = req.xpath("//select[@id='ddlSession']/option", policy=BRP.debug)
        for session_option in session_list:
            session_title = session_option.text_content()
            if session_name in session_title:
                session_id = session_option.get_attrib('value')
                break
        if not session_id:
            logger.error("Bad Session ID %s" % session)
            return
        post_data['SessionId'] = session_id

        page = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list, method="POST",
                                        request_args={"data": post_data})

        bill_list = {}
        # get bill page range
        bill_page_list = page.xpath("//select[@id='ddlPaging']/option/@value", policy=BRP.debug)
        for bill_range in bill_page_list:
            bill_list.update(self.scrape_range(session, bill_range))
        return bill_list

    def scrape_range(self, session, bill_range):
        bill_list = {}
        post_data['PagingInfo'] = bill_range
        page = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list, method="POST",
                                        request_args={"data": post_data})

        for bill in page.xpath("//a[contains(@href,'/Sections/Bills/billsdetail.aspx?BillId=')]", policy=BRP.debug):
            bill_text = bill.text_content()
            bill_id = bill_text.split('-')[0].strip()
            bill_id = re.sub(r"CS/?", '', bill_id)
            bill_url = bill.get_attrib('href')
            bill_list[bill_id] = bill_url
        return bill_list

    def scrape_bill(self, session, bill_id, **kwargs):
        bill_url = kwargs['bill_info']
        page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        title_text = page.xpath("//h1[@class='bd_title']")[0].text_content()
        title_list = title_text.split('-')
        index = 1
        if title_list[1].startswith('A\r\n'):
            index += 1
        title = '-'.join(title_list[index:]).strip()
        title = re.sub('\r\n', ' ', title)
        title = re.sub(r'\s+', ' ', title)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)

        descr = page.xpath("//span[@id='lblShortTitle']")[0].text_content().strip()
        bill['summary'] = descr

        sponsor_text = page.xpath("//span[@id='lblSponsors']")[0].text_content().strip()
        sponsor_text = re.sub(r'^by ', '', sponsor_text).strip()
        sponsors = sponsor_text.split("CO-SPONSORS")

        # 6/29/2016 Confirmed with Josh that sponsors can only be from chamber
        # of origin so to help with matching, we can add the chamber to sponsors

        sponsors[0] = re.sub(r'[\(\)]', '', sponsors[0])
        sponsors[0] = re.sub(r'(?<!,) and ', ';', sponsors[0])
        primary_sponsors = sponsors[0].split(';')
        for name in primary_sponsors:
            if name.strip() == "":
                continue
            bill.add_sponsor("primary", name.strip())

        if len(sponsors) > 1:
            sponsors[1] = re.sub(r'[\(\)]', '', sponsors[1])
            sponsors[1] = re.sub(r'(?<!,) and ', ';', sponsors[1])
            co_sponsors = sponsors[1].split(';')
            for name in co_sponsors:
                if name.strip() == "":
                    continue
                bill.add_sponsor("cosponsor", name.strip())

        for related_bills in page.xpath("//table[@id='dgRelatedBills']/tr", policy=BRP.debug)[1:]:
            bill_id_td = related_bills.xpath_single(".//a")
            related_bill_id = bill_id_td.text_content().strip()
            related_bill_id = re.split('/', related_bill_id)[-1].strip()
            bill.add_companion(related_bill_id)

        for version_row in page.xpath("//div[@id='divBillTextDisplay']//td/a[contains(@href, 'Documents')]",
                                      policy=BRP.debug):
            version_title = version_row.get_attrib('title').strip()
            version_title = re.sub(r'.*?-', '', version_title).strip()
            state = version_row.xpath("./ancestor::tr/td")[-1]
            if state:
                state = state.text_content()
                if state not in version_title:
                    state = re.sub(r'[\r\n\s]+', ' ', state)
                    version_title = version_title + ' - ' + state
            version_link = version_row.get_attrib('href')
            if 'DocumentType=Amendments' in version_link:
                doc_type = 'amendment'
            else:
                doc_type = 'version'
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(version_link,
                                                             BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True)

            if len(doc_ids) == 0 or doc_ids[0] is None:
                logger.warning("Bad Version Link %s" % version_link)
                continue
            doc_service_document = Doc_service_document(version_title, doc_type,
                                                        "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])

            bill.add_doc_service_document(doc_service_document)

        for doc_row in page.xpath("//div[@id='divAnalysis']//td/a[contains(@href, 'Documents')]", policy=BRP.debug):
            doc_title = doc_row.text_content().strip()
            doc_link = doc_row.get_attrib('href')
            download_id = self.scraper.download_and_register(doc_link, BRP.bill_documents, True)
            doc_service_document = Doc_service_document(doc_title, 'summary', 'partial', download_id)
            bill.add_doc_service_document(doc_service_document)

        vote_dict = {}
        committee_vote = page.xpath("//a[contains(@href, 'billvote.aspx')]", policy=BRP.debug)
        for vote in committee_vote:
            vote_url = vote.get_attrib('href')
            vote_chamber = vote.xpath("./ancestor::tr/preceding-sibling::tr//h1", policy=BRP.debug)
            if vote_chamber:
                vote_chamber = vote_chamber[-1].text_content()
                if 'House' in vote_chamber:
                    vote_chamber = 'lower'
                elif 'Senate' in vote_chamber:
                    vote_chamber = 'upper'
                else:
                    vote_chamber = 'joint'
            else:
                vote_chamber = chamber

            self.scrape_vote(bill, vote_url, vote_chamber, vote_dict)

        floor_vote = page.xpath(
            "//table[@id='ctl00_ContentPlaceHolder1_ctrlContentBox_ctrlPageContent_ctl00_dgHistory']/tbody/tr",
            policy=BRP.debug)
        for vote in floor_vote[1:]:
            vote_url = vote.xpath_single(".//a[contains(@href, 'floorvote.aspx')]").get_attrib('href')
            vote_chamber = vote.xpath("./td")[0].text_content()
            if 'House' in vote_chamber:
                vote_chamber = 'lower'
            elif 'Senate' in vote_chamber:
                vote_chamber = 'upper'
            else:
                vote_chamber = 'joint'
            self.scrape_vote(bill, vote_url, vote_chamber, vote_dict)

        action_table = page.xpath("//table[@id='dgBillHistory']/tbody/tr", policy=BRP.debug)
        for action_row in action_table[1:]:
            action_elem = action_row.xpath('./td')
            event = action_elem[0].text_content().strip()
            date = action_elem[1].text_content().strip()

            form_date = parse(date)
            if event.startswith('S '):
                action_chamber = 'upper'
                event = re.findall(r'S\s+(.*)', event)[0]
            elif event.startswith('H '):
                action_chamber = 'lower'
                event = re.findall(r'H\s+(.*)', event)[0]
            elif 'Signed by' in event or 'presented to' in event or event.startswith('Chapter')\
                    or 'by Governor' in event:
                action_chamber = 'executive'
            else:
                action_chamber = 'other'
            bill.add_action(action_chamber, event, form_date)

            vote_string = re.findall(r'YEAS\s+(\d+)\s+NAYS\s+(\d+)', event)
            if len(vote_string) > 0:
                if form_date in vote_dict:
                    continue
                yes_count = vote_string[0][0]
                nay_count = vote_string[0][1]
                passed = yes_count > nay_count
                vote = Vote(action_chamber, form_date, event, passed, int(yes_count), int(nay_count), 0)
                bill.add_vote(vote)
        self.save_bill(bill)

    def scrape_vote(self, bill, vote_url, vote_chamber, vote_dict):

        page = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
        date = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblDate']").text_content()
        action_date = parse(date)

        committee = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblCommittee']", policy=BRP.debug)
        if committee:
            committee = committee.text_content()
            action = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblAction']").text_content()
            motion = "%s: %s" % (committee, action)
        else:
            floor_action = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblFloorActions']").text_content()
            motion = "Floor Vote: %s" % "Passage"

        yes_count = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblYeas']").text_content()
        yes_count = int(yes_count)

        nay_count = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblNays']").text_content()
        nay_count = int(nay_count)

        other_count = 0

        missed_count = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblMissed']").text_content()
        missed_count = int(missed_count)
        other_count += missed_count

        abstained_count = page.xpath_single("//span[@id='ctl00_ContentPlaceHolder1_lblAbstained']")
        if abstained_count:
            abstained_count = abstained_count.text_content()
            abstained_count = int(abstained_count)
            other_count += abstained_count

        passed = yes_count > (missed_count+nay_count)

        vote_dict[action_date] = True
        vote = Vote(vote_chamber, action_date, motion, passed, yes_count, nay_count, other_count)
        vote.add_source(vote_url)

        vote_table = page.xpath("//table[@id='ctl00_ContentPlaceHolder1_dlVoteMember']//td", policy=BRP.debug)
        for vote_cell in vote_table:
            if vote_cell.text_content():
                vote_result = vote_cell.xpath_single("./span[contains(@id, 'lblVote')]").text_content()
                vote_name = vote_cell.xpath_single("./span[contains(@id, 'lblVoterName')]").text_content()
                if 'Y' in vote_result:
                    vote.yes(vote_name)
                elif 'N' in vote_result:
                    vote.no(vote_name)
                else:
                    vote.other(vote_name)

        bill.add_vote(vote)

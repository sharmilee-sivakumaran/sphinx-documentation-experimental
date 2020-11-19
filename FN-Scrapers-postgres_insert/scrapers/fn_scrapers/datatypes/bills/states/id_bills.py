# -*- coding: utf-8 -*-

"""
id.bills
:class IDBillScraper: scrapes Idaho Bills
"""
from __future__ import absolute_import

import re
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id

from fn_scrapers.api.scraper import scraper, tags

import logging
from dateutil.parser import parse


logger = logging.getLogger('IDBillScraper')

BASE_URL = 'https://legislature.idaho.gov/sessioninfo'

def get_session_id(session):
    return (int(session[:4]))


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-ID", group="fnleg")
class IDBillScraper(BillScraper):
    """
    IDBillScraper
    Scrape ID Bills
    """
    def __init__(self):
        super(IDBillScraper, self).__init__("id", retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

    def scrape_bill_ids(self, session):
        session_id = get_session_id(session)
        url = '%s/%s/legislation/' % (BASE_URL, session_id)
        bill_list_page = self.scraper.url_to_lxml(url, BRP.bill_list, request_args={"verify":False})

        bill_ids = []
        for link in bill_list_page.xpath("//tr[contains(@id, 'bill')]", BRP.test):
            bill_id = link.xpath("td/a/text()")[0]
            bill_id = bill_id[:-1] if bill_id[-1] == 'a' else bill_id
            bill_ids.append(bill_id)
        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):        
        # Validate Bill
        bill_id_rgx = re.compile(r'^[HS][RCJ]?[RBM]? ?\d+a?$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.warning("Invalid Bill Id %s" % bill_id)
            return

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        if re.match(r'[SH] \d+', bill_id):
            bill_type = "bill"
        else:
            bill_type = get_bill_type_from_normal_bill_id(bill_id)

        session_id = get_session_id(session)
        url_bill_id = bill_id[:-1] if bill_id[-1] == 'A' else bill_id
        prefix_url_bill_id, suffix_url_bill_id = url_bill_id.split(' ')
        if len(prefix_url_bill_id) > 1:
           url_bill_id = prefix_url_bill_id+suffix_url_bill_id.zfill(3)
        else:
           url_bill_id = prefix_url_bill_id+suffix_url_bill_id.zfill(4)
        bill_url = '%s/%s/legislation/%s' % (BASE_URL, session_id, url_bill_id)
        # check if the URL is good otherwise raise a NoBillDataForPeriod
        try:
            bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill, request_args={"verify":False})
        except:
            logger.warning("No Data for Bill %s" % bill_id)
            return
        # Get title
        title = bill_page.xpath("//table")[1].text_content().strip()

        bill = Bill(session, chamber, bill_id, title, bill_type)

        bill.add_source(bill_url)
        sponsors = re.sub(r'by', '', bill_page.xpath("//table")[0].xpath("tr/td")[-1].text_content().strip())
        bill.add_sponsor("primary", sponsors.strip())
        actor = chamber
        for actions in bill_page.xpath("//table")[2].xpath("tr"):
            action = actions.text_content().strip().split("\n")
            if len(action) > 1:
                try:
                    date = parse(action[0].strip()+'/%s' % session[:4])
                except:
                    continue
                action_text = u"\n".join(action[1:]).strip()
            else:
                action_text = action[0].strip()

            action_text = re.sub("\s+", " ", action_text, flags=re.U)
            bill.add_action(actor, action_text, date)

            if u"AYES" in action_text and u"NAYS" in action_text:
                ayes = actions.xpath_single(
                    "td/span[contains(text(), 'AYES')]/following-sibling::text()").strip()
                if ayes.startswith(u"–"):
                    ayes = ayes[1:]
                aye_voters = [aye.strip() for aye in ayes.split(u',')]

                nays = actions.xpath_single(
                    "td/span[contains(text(), 'NAYS')]/following-sibling::text()").strip()
                if nays.startswith(u"–"):
                    nays = nays[1:]
                nay_voters = [nay.strip() for nay in nays.split(u',')]

                absents = actions.xpath_single(
                    "td/span[contains(text(), 'Absent')]/following-sibling::text()").strip()
                if absents.startswith(u"–"):
                    absents = absents[1:]
                absent_voters = [absent.strip() for absent in absents.split(u',')]
                if aye_voters == ['None']:
                    aye_voters = []
                if nay_voters == ['None']:
                   nay_voters = []
                if absent_voters == ['None']:
                   absent_voters = []
                yes_count = len(aye_voters)
                no_count = len(nay_voters)
                other_count = len(absent_voters)

                # If the vote string ends with the bill getting moved to
                # a chamber, we can assume the vote was in the other chamber
                if re.search(ur"to\sHouse", action_text, re.U):
                    vote_chamber = u"upper"
                elif re.search(ur"to\sSenate", action_text, re.U):
                    vote_chamber = u"lower"
                elif actor == u"executive":
                    vote_chamber = u"joint"
                else:
                    vote_chamber = actor

                vote = Vote(vote_chamber, date, action_text, yes_count>no_count, yes_count, no_count, other_count)
                if aye_voters:
                    vote['yes_votes'] = aye_voters
                if nay_voters:
                    vote['no_votes'] = nay_voters
                if absent_voters:
                    vote['other_votes'] = absent_voters
                bill.add_vote(vote)

            # If an action text includes that it was sent to a different chamber, or to the governor, we change it here
            if re.search(ur"to\sHouse", action_text, re.U):
                actor = u"lower"
            elif re.search(ur"to\sSenate", action_text, re.U):
                actor = u"upper"
            elif re.search(ur"to\sGovernor", action_text, re.U):
                actor = u"executive"

        ver_url = bill_page.xpath_single("//a[contains(text(), 'Bill Text')]/@href")
        if ver_url:
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(ver_url, BRP.bill_versions,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True, should_download=True,
                                                             should_skip_checks=True)

            doc_service_document = Doc_service_document("Bill Text", "version", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        amd_urls = bill_page.xpath("//a[contains(text(), 'Amendment')]", BRP.bill_partial_documents)
        for amd_url in amd_urls:
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(amd_url.get_attrib("href"), BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, should_download=True,
                                                                     should_skip_checks=True)
            doc_name = re.findall("\w+.pdf", amd_url.get_attrib("href"))[0]
            doc_service_document = Doc_service_document("Amendment - %s" % doc_name, "amendment", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)

        engross_urls = bill_page.xpath("//a[contains(text(), 'Engrossment')]",
                                       BRP.bill_partial_documents)
        for engross_url in engross_urls:
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(engross_url.get_attrib("href"), BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, should_download=True,
                                                                     should_skip_checks=True)

            doc_service_document = Doc_service_document(engross_url.text_content(), "version", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)
         
        fiscal_url = bill_page.xpath("//a[contains(text(), 'Statement of Purpose')]/@href",
                                     BRP.bill_partial_documents)
        if fiscal_url:
            fiscal_url = fiscal_url[0]
            download_id = self.scraper.download_and_register(fiscal_url, BRP.bill_partial_documents,
                                                             False, should_skip_checks=True)

            doc_service_document = Doc_service_document("Fiscal Note", "fiscal_note", "partial",
                                                        download_id=download_id)
            bill.add_doc_service_document(doc_service_document)

        sponsor_url = bill_page.xpath("//a[contains(text(), 'Legislative Co-sponsors')]/@href",
                                      BRP.bill_partial_documents)
        if sponsor_url:
            sponsor_url = sponsor_url[0]
            download_id = self.scraper.download_and_register(sponsor_url, BRP.bill_partial_documents,
                                                             False, should_skip_checks=True)
            if download_id:
                doc_service_document = Doc_service_document("Legislative Co-sponsors", "other", "partial",
                                                            download_id=download_id)
                bill.add_doc_service_document(doc_service_document)
        self.save_bill(bill)


"""
ak.bills
:class AKBillScraper: scrapes Alaska Bills
"""
from __future__ import absolute_import
import re
import socket
import datetime
import htmlentitydefs

import lxml.html
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from fn_scraperutils.doc_service.util import ScraperDocument
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy
from fn_scrapers.api.scraper import scraper, tags

import logging
from dateutil.parser import parse

logger = logging.getLogger('AKBillScraper')

BASE_URL = 'http://www.legis.state.ak.us/basis/Bill'

def get_session_id(session):
    return (int(session[:4])-1993)/2 + 18

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-AK", group="fnleg")
class AKBillScraper(BillScraper):
    """
    AKBillScraper
    Scrape AK Bills
    """
    def __init__(self, *args, **kwargs):
        retry_policy = Retry500RequestsRetryPolicy(
            max_attempts=None, max_retry_time=300)
        super(AKBillScraper, self).__init__(
            'ak', *args, retry_policy=retry_policy, **kwargs)

    def scrape_bill_ids(self, session):
        session_id = get_session_id(session)
        url = '%s/Subject/%s' % (BASE_URL, session_id)
        bill_list_page = self.scraper.url_to_lxml(url, BRP.bill_list)

        bill_ids = []
        for link in bill_list_page.xpath('//a[contains(@href, "basis/Bill/Detail")]/text()'):
            bill_ids.append(link)
        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):        
        # Validate Bill
        bill_id_rgx = re.compile(r'^[HS][RCJ]?[RB] \d+$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.warning("Invalid Bill Id %s" % bill_id)
            return
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        session_id = get_session_id(session)

        bill_url = '%s/Detail/%s?Root=%s' % (BASE_URL, session_id, bill_id)
        # check if the URL is good otherwise raise a NoBillDataForPeriod
        try:
            bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        except:
            logger.warning("No Data for Bill %s" % bill_id)
            return
        # Get title
        title = re.sub("Title", '', bill_page.xpath("//span[text()='Title']/..")[0].text_content()).strip()
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        if not title:
            title = re.sub("Short Title", '', bill_page.xpath("//span[contains(text(),'Short Title')]/..")[0].text_content()).strip()
       
        if title.upper() == title:
            title = title.title()

        bill = Bill(session, chamber, bill_id, title, bill_type)

        bill.add_source(bill_url)
        sponsors = re.sub(r"Sponsor\(S\)", '', bill_page.xpath("//span[contains(text(),'Sponsor(S)')]/..")[0].text_content()).strip()
        chamber_sponsors = sponsors.split(u"\n")
        for ch_spon in chamber_sponsors:
            if u"REPRESENTATIVE" in ch_spon:
                spon_chamber = u"lower"
                ch_spon = re.sub(r"REPRESENTATIVES?", "", ch_spon)
            elif u"SENATOR" in ch_spon:
                spon_chamber = u"upper"
                ch_spon = re.sub(r"SENATORS?", "", ch_spon)
            else:
                spon_chamber = None
            for sponsor in ch_spon.split(","):
                if sponsor == sponsor.upper():
                    spon_type = u"primary"
                else:
                    spon_type = u"cosponsor"

                if spon_chamber:
                    bill.add_sponsor(spon_type, sponsor, chamber=spon_chamber)
                else:
                    bill.add_sponsor(spon_type, sponsor)

        for actions in bill_page.xpath("//div[@class='actions']/table/tbody/*"):
            action = actions.xpath("td")[2].text_content()
            actor = "lower" if action.startswith("(H)") else "upper"
            date = parse(actions.xpath("td")[0].text_content())
            attrs = dict(action=action,
                         date=date,
                         actor=actor
                        )
            bill.add_action(**attrs)
            if re.findall(r"\w+ Y(\d+)", action):
                vote_href = actions.xpath("td/span/a/@href")
                if vote_href:
                    self.parse_vote(bill, action, actor, date,
                                    vote_href[0])

        fiscal_note = bill_page.xpath("//td[@data-label='Fiscal Note']/span/a")
        fiscal_list = []
        for fiscal_link in fiscal_note:
            if fiscal_link.xpath_single("@href") in fiscal_list:
                continue
            fiscal_list.append(fiscal_link.xpath_single("@href"))
            fiscal_name = fiscal_link.xpath("..")[0].text_content()
            fiscal_name = re.sub("pdf", "", fiscal_name).strip()
           
            download_id = self.scraper.download_and_register(fiscal_link.xpath_single("@href"), BRP.bill_documents, False)
            doc_service_document = Doc_service_document(fiscal_name, "fiscal_note", "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        bill_versions = bill_page.xpath("//a[@class='pdf' and contains(@href, 'Bills')]/@href")
        for bill_version in bill_versions:
            version_name = re.findall("(\w+)\.PDF", bill_version)[0].strip()
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(bill_version, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, should_download=True,
                                                                     content_type='application/pdf')
            if doc_ids:
                doc_service_document = Doc_service_document(version_name, "version", "complete", download_id,
                                                            doc_id=doc_ids[0])
            else:
                doc_service_document = Doc_service_document(version_name, "version", "partial", download_id)
                logger.warning('Unable to extract text from: {}'.format(bill_version))

            bill.add_doc_service_document(doc_service_document)
        
        self.save_bill(bill)

    def parse_vote(self, bill, action, act_chamber, act_date, url,
        re_vote_text = re.compile(r'The question (?:being|to be reconsidered):\s*"(.*?\?)"', re.S),
        re_header=re.compile(r'\d{2}-\d{2}-\d{4}\s{10,}\w{,20} Journal\s{10,}\d{,6}\s{,4}')):
        vote_doc = self.scraper.url_to_lxml(url, BRP.bill_votes)
        # Find all chunks of text representing voting reports.
        votes_text = vote_doc.xpath_single('//pre').text_content()
        votes_text = re_vote_text.split(votes_text)
        votes_data = zip(votes_text[1::2], votes_text[2::2])

        # Process each.
        for motion, text in votes_data:

            yes = no = other = 0

            tally = re.findall(r'\b([YNEA])[A-Z]+:\s{,3}(\d{,3})', text)
            for vtype, vcount in tally:
                vcount = int(vcount) if vcount != '-' else 0
                if vtype == 'Y':
                    yes = vcount
                elif vtype == 'N':
                    no = vcount
                else:
                    other += vcount

            vote = Vote(act_chamber, act_date, motion, yes > no, yes, no, other)
       
            # In lengthy documents, the "header" can be repeated in the middle
            # of content. This regex gets rid of it.
            vote_lines = re_header.sub('', text)
            vote_lines = vote_lines.split('\r\n')

            vote_type = None
            for vote_list in vote_lines:
                if vote_list.startswith('Yeas: '):
                    vote_list, vote_type = vote_list[6:], vote.yes
                elif vote_list.startswith('Nays: '):
                    vote_list, vote_type = vote_list[6:], vote.no
                elif vote_list.startswith('Excused: '):
                    vote_list, vote_type = vote_list[9:], vote.other
                elif vote_list.startswith('Absent: '):
                    vote_list, vote_type = vote_list[9:], vote.other
                elif vote_list.strip() == '':
                    vote_type = None
                if vote_type:
                    for name in vote_list.split(','):
                        name = name.strip()
                        if name:
                            vote_type(name)

            bill.add_vote(vote)

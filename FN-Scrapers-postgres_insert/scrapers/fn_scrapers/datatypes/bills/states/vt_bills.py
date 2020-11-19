"""
vt.bills
:class VTBillScraper: scrapes Vermont Bills
"""
from __future__ import absolute_import

import re

from ..common.bill_scraper import BillScraper, ExpectedError
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
import logging
from dateutil.parser import parse

from fn_scrapers.api.scraper import scraper, tags
from fn_scraperutils.events.reporting import ScrapeError


logger = logging.getLogger('VTBillScraper')

def get_session_id(session):
    if 'ss' not in session:
        return session[4:8]
    else:
        year = session[4:8]
        ss_num = re.findall(r'ss(\d+)', session)[0]
        session_id = "%s.%s" % (year, ss_num)
        return session_id

class VTUrl:
    base = 'http://legislature.vermont.gov/'

    @staticmethod
    def introduced_url_for_session(session_id):
        return VTUrl.base + 'bill/loadBillsIntroduced/' + session_id

    @staticmethod
    def prefiled_url_for_session(session_id):
        return VTUrl.base + 'bill/loadBillsReleased/' + session_id
    
    @staticmethod
    def introduced_resolution_for_session(session_id):
        return VTUrl.base + 'bill/loadAllResolutionsByChamber/%s/both'  % session_id
    
    @staticmethod
    def bill_url_for_session(modded_bill_id, session_id):
        return VTUrl.base + 'bill/status/%s/%s' % (session_id, modded_bill_id)

    @staticmethod
    def bill_detail_url_for_internal_id(session_id, internal_id):
         return VTUrl.base + 'bill/loadBillDetailedStatus/%s/%s' % (session_id, internal_id)

    @staticmethod
    def vote_list_url_for_bill(session_id, internal_id):
        return VTUrl.base + 'bill/loadBillRollCalls/%s/%s' % (session_id, internal_id)

    @staticmethod
    def vote_detail_url(session_id, roll_call_id):
        return VTUrl.base + 'bill/loadBillRollCallDetails/%s/%s' % (session_id, roll_call_id)

class VTActionActor:
    @staticmethod
    def get_actor(action, chamber):
        if "Signed by Governor" in action:
            actor = 'executive'
        elif chamber == 'H':
            actor = 'lower'
        elif chamber == 'S':
            actor = 'upper'
        return actor

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-VT", group="fnleg")
class VTBillScraper(BillScraper):
    """
    VTBillScraper
    Scrape VT Bills
    """
    def __init__(self):
        super(VTBillScraper, self).__init__("vt")

    expected_errors = [
        ("20172018r", "HCR 350", "No title")  # DI-2181
    ]

    def scrape_bill_ids(self, session):
        bill_ids = set()
        session_id = get_session_id(session)
        urls = [
            VTUrl.introduced_url_for_session(session_id),
            VTUrl.introduced_resolution_for_session(session_id),
            VTUrl.prefiled_url_for_session(session_id)
        ]
        for url in urls:
            try:
                page = self.scraper.url_to_json(url, BRP.bill_list)
                for bill in page['data']:
                    bill_ids.add(bill['BillNumber'])
            except ScrapeError:
                pass

        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):
        # Validate Bill
        bill_id_rgx = re.compile(r'^[JHS].*\d+$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.warning("Invalid Bill Id %s" % bill_id)
            return
        if bill_id[2] == 'H' or bill_id[2] == 'S':
            fake_bill_id = bill_id[2] + bill_id[:2] + bill_id[3:]
        else:
            fake_bill_id = bill_id

        chamber = get_chamber_from_ahs_type_bill_id(fake_bill_id)
        if len(bill_id.split()[0]) == 1:
            bill_type = 'bill'
        else:
            bill_type = get_bill_type_from_normal_bill_id(fake_bill_id)


        session_id = get_session_id(session) 
        modded_bill_id = re.sub(" ", ".", bill_id)
        bill_url = VTUrl.bill_url_for_session(modded_bill_id, session_id)
        # check if the URL is good otherwise raise a NoBillDataForPeriod

        bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        if not bill_page:
            logger.warning("No Data for Bill %s" % bill_id)
            return
        # Get title
        try:
            title = re.sub(modded_bill_id, '', bill_page.xpath_single("//div[@class='bill-title']").text_content()).strip()
        except AttributeError:
            raise ExpectedError("No title")

        bill = Bill(session, chamber, bill_id, title, bill_type)

        bill.add_source(bill_url)
        sponsors = bill_page.xpath("//dt[contains(text(), 'Sponsor')]/../dd[1]/ul/li")
        sponsor_type = 'primary'
        for sponsor in sponsors:
            sponsor = sponsor.text_content()
            # If Additional Sponsor is in the current line, every sponsor after is considered an additional sponsor
            if "Additional Sponsor" in sponsor:
                sponsor_type = 'cosponsor'
                continue
            elif 'Rep' not in sponsor and 'Sen' not in sponsor:
                continue
            bill.add_sponsor(sponsor_type, sponsor)

        # get internal id for actions/votes
        try:
            bill_internal_id = re.findall(r'loadBill(?:DetailedStatus|WitnessList)/.*/(\d+)', bill_page.text_content())[0]
        except IndexError:
            logger.warning("No internal id for %s", bill_id)
            bill_internal_id = None
        if bill_internal_id:
            # actions
            detail_url = VTUrl.bill_detail_url_for_internal_id(session_id, bill_internal_id)
            action_data = self.scraper.url_to_json(detail_url, BRP.bill_actions)
            actions = action_data['data']



            for action in actions:
                action = {k:v.strip() for k, v in action.iteritems()}
                action_text = re.sub(r'<.*?>', "", action['FullStatus'])
                action_date = parse(action['StatusDate'])
                actor = VTActionActor.get_actor(action_text, action['ChamberCode'])
                action_dict = dict(
                    actor=actor,
                    action=action_text,
                    date=action_date
                )
                bill.add_action(**action_dict) #pylint: disable=W0142

            #votes
            votes_url = VTUrl.vote_list_url_for_bill(session_id, bill_internal_id)
            vote_data = self.scraper.url_to_json(votes_url, BRP.bill_votes)
            votes = vote_data['data']

            for vote in votes:
                roll_call_no = vote['VoteHeaderID']
                vote_url = VTUrl.vote_detail_url(session_id, roll_call_no)
                sigle_vote_data = self.scraper.url_to_json(vote_url, BRP.bill_votes)
                vote_json = sigle_vote_data['data']

                # get counts with filtering
                yes_count = len([x for x in vote_json if x['MemberVote'] == 'Yea'])
                no_count = len([x for x in vote_json if x['MemberVote'] == 'Nay'])
                other_count = len([x for x in vote_json if x['MemberVote'] not in ('Nay', 'Yea')])

                # parse chamber, date and motion, try to get chamber, default to bill chamber
                vote_chamber = {'H':'lower', 'S':'upper'}.get(vote['ChamberCode'], chamber)
                vote_date = parse(vote['StatusDate'])
                motion = re.sub(r'<.*?>', "", vote['FullStatus']).strip()

                # get passed from motino, otherwise counts
                if 'Passed' in motion:
                    did_pass = True
                elif 'Failed' in motion:
                    did_pass = False
                else:
                    # presume
                    did_pass = yes_count > (no_count + other_count)

                the_vote = Vote(vote_chamber, vote_date, motion, did_pass, yes_count, no_count, other_count)

                for voter in vote_json:
                    voter_name, _ = voter['MemberName'].split(" of ")
                    voter_name = voter_name.strip()
                    vote_cast = voter['MemberVote'] if voter['MemberVote'] in ('Yea', 'Nay') else 'other'
                    add_vote = {'Yea': the_vote.yes,
                                'Nay': the_vote.no,
                                'other': the_vote.other}
                    add_vote[vote_cast](voter_name)

                the_vote.add_source(votes_url)
                the_vote.add_source(vote_url)
                bill.add_vote(the_vote)

        bill_versions = bill_page.xpath("//a[contains(@href,'As Introduced') or contains(@href, ' Official') "
                                        "or contains(@href, ' As Enacted') or contains(@href, ' Act Summary')"
                                        "or contains(@href, 'As Adopted')]")
        seen_versions = set([])
        for version in bill_versions:
            ref = version.get_attrib("href")
            if ref in seen_versions:
                continue
            seen_versions.add(ref)

            if u"Official" in version.text or u"Unofficial" in version.text:
                name = version.xpath_single("..").text_content().strip()
                name = name.split("Official", 1)[0].strip()
            else:
                name = version.text.strip()
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(ref, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, content_type="application/pdf",
                                                                     update_flag="DI-2073")
            doc_type = "summary" if "summary" in name.lower() else "version"
            doc_serv_document = Doc_service_document(name, doc_type, "complete", download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_serv_document)

        self.save_bill(bill)

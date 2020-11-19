"""
de.bills
:class DEBillScraper: scrapes Delaware Bills
"""
from __future__ import absolute_import

from datetime import datetime
import logging
import re
import time

from dateutil.parser import parse
import pytz
from fn_scrapers.datatypes.bills.common import (
    Bill, BillScraper, Vote, BillReportingPolicy as BRP, Doc_service_document, ExpectedError)
from fn_scrapers.datatypes.bills.common.normalize import get_bill_type_from_normal_bill_id

from fn_scrapers.common.time import sleep
from fn_scrapers.api.scraper import scraper, tags

from fn_scraperutils.events.reporting import ScrapeError


logger = logging.getLogger('DEBillScraper')

DE_BASE_URL = 'http://www.legis.delaware.gov/'
base_json_url = "http://www.legis.delaware.gov/json/BillDetail/"

json_post_data = {'sort': '',
                  'group': '',
                  'filter': ''
                  }

chamber_dict = {'House': 'lower',
                'Senate': 'upper'
                }


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-DE", group="fnleg")
class DEBillScraper(BillScraper):
    # blackout periods in 24H format, UTC timezone.
    blackout_periods = ([('22:27', '22:33')], 'US/Eastern')

    def __init__(self):
        super(DEBillScraper, self).__init__("de")

    expected_errors = [
        ("20172018r", "SB 144", "No original text link")  # DI-2179
    ]

    def scrape_bill_ids(self, session):
        sleep(*self.blackout_periods)
        session_id = self.get_session_id(session)
        bill_ids = set()
        base_list_url = 'http://www.legis.delaware.gov/json/AllLegislation/GetAllLegislation'
        """
        The legislation type mapping is as follows:
        1: Bills
        2: Resolutions
        3: Concurrent Resolutions
        4: Joint Resultions
        5: Amendments
        6: Substitutions

        We skip amendments, as we don't treat them as separate bills, and we normalize the ids for Substitutions
        """
        post_date = {'sort': '',
                     'page': 1,
                     'pageSize': 10,
                     'group': '',
                     'filter': '',
                     'selectedGA[0]':  '',
                     'sponsorName': '',
                     'selectedLegislationTypeId[0]': 1,
                     'selectedLegislationTypeId[1]': 2,
                     'selectedLegislationTypeId[2]': 3,
                     'selectedLegislationTypeId[3]': 4,
                     'selectedLegislationTypeId[4]': 6,
                     'fromIntroDate': '',
                     'toIntroDate': '',
                     'coSponsorCheck': ''
                     }
        #set session id
        post_date['selectedGA[0]'] = session_id
        bill_id_json = self.scraper.url_to_json(base_list_url, BRP.bill_list, method="POST", request_args={"data":post_date})


        #set the page size to the bill number 
        post_date['pageSize'] = int(bill_id_json['Total'])
        
        bill_id_json = self.scraper.url_to_json(base_list_url, BRP.bill_list, method="POST", request_args={"data":post_date})

        for dat in bill_id_json['Data']:
            bill_id = dat['LegislationNumber']
            # If there is a substitute parent id, we need to form a substitute id by adding a hyphen and the sub num
            if dat.get(u"SubstituteParentLegislationDisplayCode"):
                sub_num = bill_id.split()[1]
                parent_id = dat[u"SubstituteParentLegislationDisplayCode"]
                bill_id = u"{}-{}".format(parent_id, sub_num)
            bill_ids.add(bill_id)

        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):
        sleep(*self.blackout_periods)
        sub_num = 0
        if u"-" in bill_id:
            sub_num = bill_id.split(u'-')[1]

        session_id = self.get_session_id(session)
        search_id = bill_id.split(u"-")[0]
        bill_search_url = "http://www.legis.delaware.gov/json/Search/GetLegislation?searchTerm=%s" % search_id
        post_date = {'sort': '',
                     'page': 1,
                     'pageSize': 10,
                     'group': '',
                     'filter': '',
                     'selectedGA[0]':  '',
                     'sponsorName': '',
                     'fromIntroDate': '',
                     'toIntroDate': '',
                     'coSponsorCheck': False
                     }

        post_date['selectedGA[0]'] = session_id
        bill_json = self.scraper.url_to_json(bill_search_url, BRP.bill, method="POST", request_args={"data":post_date})
        bill_data = bill_json['Data']
        # If the bill is a substitute, we expect multiple versions, and want to grab the one with a substitute id.
        index = 0
        if len(bill_data) > 1:
            leg_nums = [b[u'LegislationNumber'] for b in bill_data]
            for leg_num in leg_nums:
                match = re.match(u"[HS]S (\d+)", leg_num)
                # If the bill id is just the legislation number, then we use that one
                if leg_num == bill_id:
                    index = leg_nums.index(leg_num)
                    break
                elif match and match.group(1) == sub_num:
                    index = leg_nums.index(leg_num)
                    break
            else:
                raise ScrapeError(u"Could not find bill to match id in list of bills returned by search")

        bill_data = bill_data[index]

        if bill_data.get(u"ChamberName"):
            chamber = chamber_dict[bill_data['ChamberName']]
        # For some reason, substitutes have no chamber name, so we need to get it a different way
        else:
            chamber = u"lower" if bill_id.startswith(u"H") else u"upper"

        title = bill_data['LongTitle']
        # Append Substitute to the front of substitute bills
        if sub_num:
            title = u"Substitute {}: {}".format(sub_num, title)

        leg_id = bill_data['LegislationId']
        bill_url = "http://www.legis.delaware.gov/BillDetail?legislationId=%s" % leg_id

        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)

        summary = bill_data['Synopsis']
        if summary:
            bill.add_summary(summary)
        bill_doc = self.scraper.url_to_lxml(bill_url, BRP.bill)
        sponsor = bill_doc.xpath('//label[contains(text(), "Primary Sponsor:")]/following-sibling::div/span/a')
        for spon in sponsor:
            bill.add_sponsor('primary', spon.text_content())

        sponsor = bill_doc.xpath('//label[contains(text(), "Additional Sponsor(s):")]/following-sibling::div//a', BRP.bill_sponsors)
        for spon in sponsor:
            if spon.text_content():
                bill.add_sponsor('primary', spon.text_content())

        sponsor = bill_doc.xpath('//label[contains(text(), "Co-Sponsor(s):")]/following-sibling::div/span/a', BRP.bill_sponsors)
        for spon in sponsor:
            if spon.text_content():
                bill.add_sponsor('cosponsor', spon.text_content())


        version_ele = bill_doc.xpath_single('//label[contains(text(), "Original Text:")]/following-sibling::div//a[text()="View PDF"]')
        if version_ele is not None:
            pdf_url = version_ele.get_attrib('href')
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(pdf_url, BRP.bill_versions,
                                                             self.scraper.extraction_type.text_pdf, True)
        else:
            version_ele = bill_doc.xpath_single('//label[contains(text(), "Original Text:")]/following-sibling::div//a[text()="View MS Word"]')
            try:
                word_url = version_ele.get_attrib('href')
            except AttributeError:
                raise ExpectedError("No original text link")

            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(word_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.msword_docx, True)
        version_doc_service_document = Doc_service_document('Original Text', "version", "complete",
                                                            download_id, doc_ids[0])
        
        bill.add_doc_service_document(version_doc_service_document)


        final_version_ele = bill_doc.xpath_single('//label[contains(text(), "Final/Engrossed:")]/following-sibling::div//a[text()="View PDF"]')
        if final_version_ele:
            pdf_url = final_version_ele.get_attrib('href')
            pdf_content = final_version_ele.text_content()
            if 'MS Word' in pdf_content:
                download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(pdf_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.msword_docx, True)
            else:
                download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(pdf_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf, True)

            version_doc_service_document = Doc_service_document('Final/Engrossed', "version", "complete",
                                                                download_id, doc_ids[0])

            bill.add_doc_service_document(version_doc_service_document)


        fiscal_ele = bill_doc.xpath_single('//div[@class="fiscalNote"]//a')
        if fiscal_ele:
            html_url = fiscal_ele.get_attrib('href')

            download_id = self.scraper.download_and_register(html_url, BRP.bill_documents, False)
            doc_service_document = Doc_service_document('Fiscal Note/Fee Impact', 'fiscal_note', "partial", download_id)
            bill.add_doc_service_document(doc_service_document)


        #Read amendement json
        if bill_data['HasAmendments']:
            amend_json_url = "http://www.legis.delaware.gov/json/BillDetail/"\
                             "GetRelatedAmendmentsByLegislationId?legislationId=%s" % leg_id
            amend_json = self.scraper.url_to_json(amend_json_url, BRP.bill, method="POST",\
                                                 request_args={"data":json_post_data})
            for amend_entry in amend_json['Data']:
                if 'LegislationId' in amend_entry:
                    amend_id = amend_entry['LegislationId']
                elif 'AmendmentLegislationId' in amend_entry:
                    amend_id = amend_entry['AmendmentLegislationId']
                else:
                    continue
                self.scrape_amendment(bill, amend_id)


        #Read Action json
        action_json_url = base_json_url + "GetRecentReportsByLegislationId?legislationId=%s" % leg_id
        action_json = self.scraper.url_to_json(action_json_url, BRP.bill, method="POST",\
                                               request_args={"data":json_post_data})

        for action_entry in action_json['Data']:
            date = action_entry['OccuredAtDateTime']
            fomed_date = parse(date)
            action_str = action_entry['ActionDescription']
            chamber = None
            if 'ChamberName' in action_entry and action_entry['ChamberName']:
                chamber = action_entry['ChamberName']
                if chamber == 'House':
                    chamber = 'lower'
                elif chamber == 'Senate':
                    chamber = 'upper'
            else:
                if 'House' in action_str or ' HA ' in action_str:
                    chamber = 'lower'
                elif 'Senate' in action_str or ' SA ' in action_str:
                    chamber = 'upper'
                elif 'by Governor' in action_str:
                    chamber = 'executive'
                else:
                    chamber = 'other'
            bill.add_action(chamber, action_str, fomed_date)

        
        #Read Vote Json
        vote_json_url = base_json_url + "GetVotingReportsByLegislationId?legislationId=%s" % leg_id
        vote_json = self.scraper.url_to_json(vote_json_url, BRP.bill_votes, method="POST",\
                                             request_args={"data":json_post_data})
        if vote_json:
            for vote_entry in vote_json['Data']:
                if vote_entry['RollCallResultTypeName'] == 'Passed':
                    passed = True
                else:
                    passed = False

                rollcallId = vote_entry['RollCallId']
                vote_detail_url = DE_BASE_URL + "json/RollCall/GetRollCallVoteByRollCallId?RollCallId=%s" % rollcallId
                vote_detail_json = self.scraper.url_to_json(vote_detail_url, BRP.bill_votes, method="POST",\
                                                            request_args={"data":json_post_data})
                vote_data = vote_detail_json['Model']

                yes_count = 0
                no_count = 0
                other_count = 0
                yes_count = int(vote_data['YesVoteCount'])
                no_count = int(vote_data['NoVoteCount'])
                other_count = int(vote_data['AbsentVoteCount']) + int(vote_data['NotVotingCount']) + int(vote_data['VacantVoteCount'])
                date = parse(vote_data['TakenAtDateTime'])
                chamber = chamber_dict[vote_data['ChamberName']]
                motion = "Roll Call #%s" % rollcallId
                vote = Vote(chamber, date, motion, passed, yes_count, no_count, other_count)
                vote.add_source(vote_detail_url)

                for voter in vote_data['AssemblyMemberVotes']:
                    if voter['SelectVoteTypeCode'] == 'Y':
                        vote.yes(voter['ShortName'])
                    elif voter['SelectVoteTypeCode'] == 'N':
                        vote.no(voter['ShortName'])
                    else:
                        vote.other(voter['ShortName'])
                bill.add_vote(vote)

        self.save_bill(bill)


    def scrape_amendment(self, bill, amend_id):
        amend_url = "http://www.legis.delaware.gov/BillDetail?legislationId=%s" % amend_id
        amend_doc = self.scraper.url_to_lxml(amend_url, BRP.bill_documents)
        amend_ele = amend_doc.xpath_single('//label[contains(text(), "Original Text:")]/following-sibling::div/a[text()="View PDF"]')
        if amend_ele is not None:
            pdf_url = amend_ele.get_attrib('href')
            amend_name = amend_doc.xpath_single('//h2').text_content()
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(pdf_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.text_pdf, True)
        else:
            amend_ele = amend_doc.xpath_single('//label[contains(text(), "Original Text:")]/following-sibling::div/a[text()="View MS Word"]')
            word_url = amend_ele.get_attrib('href')
            amend_name = amend_doc.xpath_single('//h2').text_content()
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(word_url, BRP.bill_documents,
                                                             self.scraper.extraction_type.msword_docx, True)
        doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                    download_id, doc_ids[0])

        bill.add_doc_service_document(doc_service_document)

    @classmethod
    def get_session_id(cls, session):
        year = int(session[:4])
        session_id = (year - 2016)/2 + 149
        return str(session_id)

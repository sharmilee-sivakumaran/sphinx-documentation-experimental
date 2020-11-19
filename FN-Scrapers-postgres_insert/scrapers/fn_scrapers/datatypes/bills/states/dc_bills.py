# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
import json
import logging
from dateutil.parser import parse

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('DCBillScraper')


def get_session_dates(session):
    start = session[:4]
    end = session[4:8]
    return ("1/1/%s" % start, "12/31/%s" % end)

def get_session_id(session):
    return str(22 + (int(session[:4]) - 2017)/2)

def recursiveloads(input):
    out = input
    if isinstance(out, str) or isinstance(out, unicode):
        try:
            out = json.loads(input)
        except:
            pass
    if isinstance(out, dict):    
        for k, v in out.iteritems():
            out[k] = recursiveloads(out[k])
    if isinstance(out, list):
        out = [recursiveloads(x) for x in out]
    return out

class DCUrl:
   
    BASE_URL = 'http://lims.dccouncil.us/'
    POST_REQUEST = 'http://lims.dccouncil.us/_layouts/15/uploader/AdminProxy.aspx/GetKeywordSearchWithCategory'
    
    post_data = '''{"request":{"sEcho":4,"iColumns":4,"sColumns":"","iDisplayStart":%s,"iDisplayLength":10,"mDataProp_0":"ShortTitle","mDataProp_1":"Title","mDataProp_2":"LegislationCategories","mDataProp_3":"Modified","iSortCol_0":0,"sSortDir_0":"asc","iSortingCols":0,"bSortable_0":true,"bSortable_1":true,"bSortable_2":true,"bSortable_3":true},"criteria":{"Keyword":"","Category":"%s","SubCategoryId":"","RequestOf":"","CouncilPeriod":"","Introducer":"","CoSponsor":"","ComitteeReferral":"","CommitteeReferralComments":"","StartDate":"%s","EndDate":"%s","QueryLimit":100,"FilterType":"","Phases":"","LegislationStatus":"0","IncludeDocumentSearch":false}}'''

    bill_detail_url = 'http://lims.dccouncil.us/_layouts/15/uploader/AdminProxy.aspx/LegislationDetails'
    info_lookup = 'http://lims.dccouncil.us/_layouts/15/uploader/AdminProxy.aspx/LIMSLookups'
    
    @staticmethod
    def bill_list_url(session):
        start_date, end_date = get_session_dates(session)
        return DCUrl.BASE_URL + 'SearchResults/?Category=1&StartDate=%s&EndDate=%s' % (start_date,end_date)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-DC", group="fnleg")
class DCBillScraper(BillScraper):
    def __init__(self):
        super(DCBillScraper, self).__init__("dc")

    @staticmethod
    def clean_title(title):
        title = re.sub("Short Title:", "", title)
        title = re.sub("\s{2,}", " ", title).strip()
        return title

    @staticmethod
    def get_sponsors_from_page(sponsor_text):
        sponsor = re.sub("Primary Sponsor:", "", sponsor_text)
        sponsor = re.sub("&nbsp", "", sponsor)
        sponsor = sponsor[:sponsor.find('(')]
        return sponsor

    def scrape_bill_ids(self, session):
        start, end = get_session_dates(session)
        bill_ids = [] 
        for category in [1,2]:
            bill_list = self.scraper.url_to_lxml(DCUrl.POST_REQUEST,
                                                 BRP.bill_list, 
                                                 method="POST",
                                                 request_args={"data": DCUrl.post_data % (0, category, start, end), 
                                                               "headers": {
                                                                   "Content-Type":'application/json', 
                                                                   "User-Agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36"
                                                               }
                                                              }
                                                )
        
            bill_list = recursiveloads(bill_list.text_content())['d']
            count = 0
            while len(bill_list['aaData']) > 0:
                for bill in bill_list['aaData']:
                    pillar_bill_id = bill['Title'][0] + " " + bill['Title'][1:]
                    bill_ids.append(pillar_bill_id)
                count += 10 
                bill_list = self.scraper.url_to_lxml(DCUrl.POST_REQUEST, 
                                                     BRP.bill_list, method="POST",
                                                     request_args={"data": DCUrl.post_data % (count, category, start, end), 
                                                                   "headers": {
                                                                       "Content-Type":'application/json', 
                                                                       "User-Agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36"
                                                                   }
                                                                  })
                bill_list = recursiveloads(bill_list.text_content())['d']
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        bill_id = bill_id.strip()
        bill_detail_data = '''{"legislationId": "%s"}''' % bill_id.replace(" ", "")
        bill_page = self.scraper.url_to_lxml(DCUrl.bill_detail_url, 
                                             BRP.bill, 
                                             method="POST", 
                                             request_args={
                                                 "data": bill_detail_data, 
                                                 "headers": {
                                                     "Content-Type": 'application/json', 
                                                     "User-Agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36"
                                                 }
                                             })
        bill_info = recursiveloads(bill_page.text_content())['d']
        title = bill_info['Legislation']['ShortTitle']
        if bill_id[0] == 'B':
            bill_type = 'bill'
        else:
            bill_type = 'resolution'
        bill_prefix, bill_suffix = re.findall("([A-Z]+)(\d+\-\d+)", bill_id.replace(" ", ""))[0]
        external_id = bill_prefix + " " + bill_suffix
        bill = Bill(session, "upper", external_id, title, bill_type)

        if 'AdditionalInformation' in bill_info['Legislation']:
            descr = bill_info['Legislation']['AdditionalInformation']
            bill.add_summary(descr)

        member_info = self.scraper.url_to_lxml(DCUrl.info_lookup, BRP.bill, method="POST", request_args={"headers": {"Content-Type": "application/json"}})
        member_info = recursiveloads(member_info.text_content())['d']

        member_dict = {}
        for member in member_info['Members'][get_session_id(session)]:
            member_dict[member['ID']] = member['Title']

        for sponsor in bill_info["Legislation"]['Introducer']:
            bill.add_sponsor("primary", sponsor['Name'])
       
        for hearing in bill_info["Hearing"]:
            if 'HearingDate' in hearing.keys():
                bill.add_action(date=parse(hearing['HearingDate']), action=('Public Hearing on %s' % bill_id), actor='upper')
            if "AttachmentPath" in hearing.keys():
                attachment_path = hearing['AttachmentPath']
                for attachment in attachment_path:
                    download_id  = self.scraper.download_and_register("http://lims.dccouncil.us/Download/%s/%s" % (hearing['Title'], attachment['Name']), BRP.bill_documents,
                                                                      False)
                    doc_service_document = Doc_service_document(('Public Hearing on %s' % bill_id), "committee_document", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)
                   
            if 'HearingPubDate' in hearing.keys():
                bill.add_action(date=parse(hearing['HearingPubDate']), action='Notice of Public Hearing Published in the District of Columbia Register', actor='upper')        

        if 'PublicationDate' in bill_info['Legislation'].keys():
            bill.add_action(date=parse(bill_info['Legislation']['PublicationDate']), action='Notice of Intent to Act on %s Published in the District of Columbia Register' % bill_id, actor='upper')

        if 'IntroductionDate' in bill_info['Legislation'].keys():
            bill.add_action(date=parse(bill_info['Legislation']['IntroductionDate']), action='%s introduced by %s' % (bill_info['Legislation']['Title'], bill_info["Legislation"]['Introducer'][0]['Name']), actor='upper')

        if bill_info["Legislation"]["CommitteeReferral"][0]['Id'] != '232':
            action_text = 'Referred to %s' % bill_info["Legislation"]["CommitteeReferral"][0]['Name']
        else:
            action_text = bill_info["Legislation"]["CommitteeReferral"][0]['Name']
        if 'CommitteeReferralComments' in bill_info['Legislation'].keys():
            action_text += " with comments from the Committee on %s" % bill_info['Legislation']['CommitteeReferralComments'][0]['Name']

        bill.add_action(date=parse(bill_info['Legislation']['DateRead']), action=action_text, actor='upper')

        if 'MayorReview' in bill_info.keys():
            for mayor_review in bill_info['MayorReview']:
                if 'TransmittedDate' in mayor_review.keys():
                    bill.add_action(date=parse(mayor_review['TransmittedDate']), action='Transmitted to Mayor, Response Due on %s' % mayor_review['ResponseDate'], actor='upper')
                if 'ReturnedDate' in mayor_review.keys():
                    bill.add_action(date=parse(mayor_review['ReturnedDate']), action='Returned from Mayor', actor='upper')
                if 'PublishedDate1' in mayor_review.keys():
                    bill.add_action(date=parse(mayor_review['PublishedDate1']), action='Act %s Published in DC Register Vol %s and Page %s' % (bill_info['Legislation']['ActNumber'], mayor_review['Volume'], mayor_review['Page']), actor='upper')
                if 'SignedDate' in mayor_review.keys():
                    bill.add_action(date=parse(mayor_review['SignedDate']), action='Signed with Act Number %s' % bill_info['Legislation']['ActNumber'], actor='upper')
                if 'EnactedDate' in mayor_review.keys():
                    bill.add_action(date=parse(mayor_review['EnactedDate']), action='Enacted with Act Number %s' % bill_info['Legislation']['ActNumber'], actor='upper')
                if "AttachmentPath" in mayor_review.keys():
                    attachment_path = mayor_review['AttachmentPath']
                    for attachment in attachment_path:
                        download_id, _, docs_id = self.scraper.register_download_and_documents("http://lims.dccouncil.us/Download/%s/%s" % (attachment['RelativePath'], attachment['Name']), BRP.bill_documents,
                                                                                    self.scraper.extraction_type.text_pdf, True)
                        if len(docs_id) == 1 and docs_id[0] is not None:
                            doc_service_document = Doc_service_document(attachment['Type'], "version", "complete", download_id, doc_id=docs_id[0])
                            bill.add_doc_service_document(doc_service_document) 

 
        for comm_markup in bill_info['CommitteeMarkup']:
            if 'ActionDate' not in comm_markup.keys():
                continue
            bill.add_action(date=parse(comm_markup['ActionDate']), action='Committee Mark-up of %s' % bill_id, actor='upper')
            if "AttachmentPath" in comm_markup.keys():
                attachment_path = comm_markup['AttachmentPath']
                for attachment in attachment_path:
                    download_id = self.scraper.download_and_register("http://lims.dccouncil.us/Download/%s/%s" % (attachment['RelativePath'], attachment['Name']), BRP.bill_documents,
                                                                                 False)
                    doc_service_document = Doc_service_document(('Committee Mark-up of %s' % bill_id), "committee_document", "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        if "AttachmentPath" in bill_info['Legislation'].keys():
            attachment_path = bill_info['Legislation']['AttachmentPath']
            for attachment in attachment_path:
                download_id, _, docs_id = self.scraper.register_download_and_documents("http://lims.dccouncil.us/Download/%s/%s" % (attachment['RelativePath'], attachment['Name']), BRP.bill_documents,
                                                                                       self.scraper.extraction_type.unknown, True)
                if len(docs_id) == 1 and docs_id[0] is not None:
                    doc_service_document = Doc_service_document(('Introduction'), "version", "complete", download_id, doc_id=docs_id[0])
                    bill.add_doc_service_document(doc_service_document)

        if "MemoLink" in bill_info['Legislation'].keys():
            attachment_path = bill_info['Legislation']['MemoLink']
            for attachment in attachment_path:
                download_id = self.scraper.download_and_register("http://lims.dccouncil.us/Download/%s/%s" % (attachment['RelativePath'], attachment['Name']), BRP.bill_documents,
                                                                             False)
                doc_service_document = Doc_service_document('Memorandum', "other", "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        for voting_summary in bill_info['VotingSummary']:
            bill.add_action(date=parse(voting_summary['DateOfVote']), action=voting_summary['ReadingDescription'], actor='upper')
            
            if "AttachmentPath" in voting_summary.keys():
                attachment_path = (voting_summary['AttachmentPath'])
                for attachment in attachment_path:
                    download_id, _, docs_id = self.scraper.register_download_and_documents("http://lims.dccouncil.us/Download/%s/%s" % (attachment['RelativePath'], attachment['Name']), BRP.bill_documents,
                                                                                    self.scraper.extraction_type.text_pdf, True)
                    document_name = voting_summary['ReadingDescription']
                    document_type = 'version'
                    if voting_summary['DocumentType'] == 'Engrossment':
                        document_name = 'Engrossment'
                    elif voting_summary['DocumentType'] == 'Enrollment':
                        document_name = 'Enrollment'
                    if  voting_summary['DocumentType'] == 'Amendment':
                        document_type = 'amendment'
                    doc_service_document = Doc_service_document(document_name, document_type, "complete", download_id, doc_id=docs_id[0])
                    bill.add_doc_service_document(doc_service_document)

            yes_votes = []
            no_votes = []
            other_votes = []
            for votes in voting_summary['MemberVotes']:
                if votes['Vote'] == 1:
                    yes_votes.append(member_dict[votes['MemberId']])
                elif votes['Vote'] == 2:
                    no_votes.append(member_dict[votes['MemberId']])
                else:
                    other_votes.append(member_dict[votes['MemberId']])
            vote = Vote('upper', parse(voting_summary['DateOfVote']), voting_summary['ReadingDescription'], len(yes_votes) > len(no_votes), len(yes_votes), len(no_votes), len(other_votes))
            if len(yes_votes) > 0:
                vote['yes_votes'] = yes_votes
            if len(no_votes) > 0:
                vote['no_votes'] = no_votes
            if len(other_votes) > 0:
                vote['other_votes'] = other_votes
            bill.add_vote(vote)

        bill.add_source("http://lims.dccouncil.us/Legislation/%s" % bill_id.replace(" ", ""))
        self.save_bill(bill)

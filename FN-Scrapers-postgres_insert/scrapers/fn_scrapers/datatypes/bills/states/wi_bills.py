# -*- coding: utf-8 -*-
"""
wi.bills
:class WIBillScraper: scrapes Wisconsin Bills
"""
from __future__ import absolute_import
import re
import datetime
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from fn_scraperutils.doc_service.util import ScraperDocument
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from ..common.metadata import get_session_name
import logging
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags

logger = logging.getLogger('WIBillScraper')

def get_session_id(session):
    return session[:4]

class WIUrl:
    
    base = 'https://docs.legis.wisconsin.gov/'

    @staticmethod
    def assembly_bills(ss_code, session_id):
        return WIUrl.base + '%s/proposals/%s/asm/bill' % (session_id, ss_code)

    @staticmethod
    def assembly_joint_resolution(ss_code, session_id):
        return  WIUrl.base + '%s/proposals/%s/asm/joint_resolution' % (session_id, ss_code)

    @staticmethod
    def assembly_resolution(ss_code, session_id):
        return   WIUrl.base + '%s/proposals/%s/asm/resolution' % (session_id, ss_code)

    @staticmethod
    def senate_bills(ss_code, session_id):
        return  WIUrl.base + '%s/proposals/%s/sen/bill' % (session_id, ss_code)

    @staticmethod
    def senate_joint_resolution(ss_code, session_id):
        return  WIUrl.base + '%s/proposals/%s/sen/joint_resolution' % (session_id, ss_code)

    @staticmethod
    def senate_resolution(ss_code, session_id):
        return  WIUrl.base + '%s/proposals/%s/sen/resolution' % (session_id, ss_code)
    
    @staticmethod 
    def get_url_chamber(chamber):
        if chamber == 'lower':
            return 'asm'
        else:
            return 'sen'
 
    @staticmethod 
    def get_url_ext(bill_id):
        if "JR" in bill_id:
            return 'joint_resolution'
        elif "B" in bill_id:
            return 'bill'
        else:
            return 'resolution'

    @staticmethod 
    def get_bill_page(session_id, url_chamber, url_ext, url_bill_id, ss_code):
        return  WIUrl.base + '%s/proposals/%s/%s/%s/%s' % (session_id, ss_code, url_chamber, url_ext, url_bill_id)
  
    @staticmethod
    def get_bill_text(session_id, url_bill_id, ss_code):
        return WIUrl.base + 'document/proposaltext/%s/%s/%s' % (session_id, ss_code, url_bill_id)


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-WI", group="fnleg")
class WIBillScraper(BillScraper):
    """
    """
    def __init__(self):
        super(WIBillScraper, self).__init__("wi")

    def scrape_bill_ids(self, session):
        if 'ss' in session:
            session_name = get_session_name(self.scraper.metadata_client, 'wi', session)
            ss_code = re.findall(r'\((.*)\)', session_name)[0]
        else:
            ss_code = "reg"
        session_id = get_session_id(session)
        a_bills = WIUrl.assembly_bills(ss_code, session_id)
        a_joint_res = WIUrl.assembly_joint_resolution(ss_code, session_id)
        a_res = WIUrl.assembly_resolution(ss_code, session_id)
        s_bills = WIUrl.senate_bills(ss_code, session_id)
        s_joint_res = WIUrl.senate_joint_resolution(ss_code, session_id)
        s_res = WIUrl.senate_resolution(ss_code, session_id)
        
        list_of_urls = [a_bills, a_joint_res, a_res, s_bills, s_joint_res, s_res]
        bill_ids = []
        for url in list_of_urls:
            bill_list = self.scraper.url_to_lxml(url, BRP.test)
            if not bill_list:
                continue
            for bill_id in bill_list.xpath("//ul[@class='infoLinks']/li/div/div/a/@href"):
                mod = bill_id[bill_id.rfind('/') + 1:]
                bill_ids.append(mod)
        return bill_ids
    
    def scrape_bill(self, session, bill_id, **kwargs):
        logger.info("Scraping bill {}".format(bill_id))     
        url_bill_id = re.sub(" ", "", bill_id)

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)      
        url_chamber = WIUrl.get_url_chamber(chamber)
 
        url_ext = get_bill_type_from_normal_bill_id(bill_id)

        session_id = get_session_id(session)

        if 'ss' in session:
            session_name = get_session_name(self.scraper.metadata_client, 'wi', session)
            ss_code = re.findall(r'\((.*)\)', session_name)[0]
        else:
            ss_code = "reg"

        url = WIUrl.get_bill_page(session_id, url_chamber, url_ext, url_bill_id, ss_code)
        bill_page = self.scraper.url_to_lxml(url, BRP.bill)
        title = bill_page.xpath_single("//p[contains(text(), 'Relating to:')]/text()")
        description, title = title.split("Relating to:")
        bill = Bill(session, chamber, bill_id, title, url_ext)      
        bill.add_summary(description)  
        bill.add_source(url)
     
        see_also = bill_page.xpath("//ul[@class='docLinks']")
        if len(see_also) == 2:
            see_also = see_also[1]
            bill['related_bills'] = []
            for li in see_also.xpath("li"): 
                related = re.findall("(?:Senate|Assembly) (?:Bill|Joint Resolution|Resolution) \d+", li.text_content())[0]
                related = re.sub("Senate", "S", related)
                related = re.sub("Assembly", "A", related)
                related = re.sub(" Bill", "B", related)
                related = re.sub(" Joint Resolution", "JR", related)
                related = re.sub(" Resolution", "R", related)
                bill['related_bills'].append({'external_id': related, 'type': 'companion'})          


        action_list = {}
        for action in bill_page.xpath("//table[@class='history']")[1].xpath("tr"):
            td_action = action.xpath("td")
            if not td_action:
                continue
            
            date, action, _ = td_action
            date, actor = date.text_content().split(" ")
            if actor.strip() == 'Asm.':
                actor = 'lower'
            else:
                actor = 'upper'
            date = parse(date)
            action_element = action
            action = action.text_content()
            action_key = (date, action, actor)
            if action_key in action_list:
                continue
            action_list[action_key] = True
            bill.add_action(date=date, action=action, actor=actor)
            if "Introduced by " in action:
                sponsors = re.findall(r"(?:Representative|Senator)s? ([\s\S]+);[\s\S]+?cosponsored by (?:Representative|Senator)s? ([\s\S]+)", action)
                if not sponsors:
                    sponsors = re.findall("(?:Representative|Senator)s? ([\s\S]+),?", action)
                    if len(sponsors) > 0:
                        primary = sponsors[0]
                    else:
                        primary = action
                    cospon = ''

                else:
                    primary, cospon = sponsors[0]
                primary = re.sub(" and ", ",", primary).strip()
                cospon = re.sub(" and ", ",", cospon).strip() 
                for spon in primary.split(","):
                    spon = re.sub(r'by request of', '', spon)
                    if spon.strip():
                        bill.add_sponsor('primary', spon.strip())
                for spon in cospon.split(","): 
                    if spon.strip():
                        spon = re.sub(r'by request of', '', spon)
                        bill.add_sponsor('cosponsor', spon.strip())
            elif "Aye" in action and ("No" in action or "Nay" in action):
                vote_url = action_element.xpath_single('a[contains(@href,"votes")]/@href')
                if not vote_url:
                    votes = re.findall(r'Ayes,? (\d+)[,;]\s+N(?:oes|ays),? (\d+)', action)
                    (yes, no) = int(votes[0][0]), int(votes[0][1])
                    vote = Vote(actor, date, action, yes > no, yes, no, 0)
                    bill.add_vote(vote)
                else:
                    vote_page = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
                    yes = []
                    other = []
                    no = []
                    if actor == 'lower':
                        for voter in vote_page.xpath("//td/table/tbody/tr"):
                            if not voter.text_content() or voter.text_content() == 'ANNVNAME':
                                continue
                            vote, voter = voter.text_content()[0], voter.text_content()[1:-1].strip()
                            if vote == 'Y':
                                yes.append(voter)
                            elif vote == 'N':
                                no.append(voter)
                            else:
                                other.append(voter)
                    else:
                       yes, no, other = re.findall("AYES - \d+([\s\S]+?)NAYS - \d+([\s\S]+?)NOT VOTING - \d+([\s\S]+?)", vote_page.text_content())[0]
                       yes = re.sub("\s{2,}", " ", yes).strip()
                       yes = re.sub("\xc2", "", yes).strip().split(" ")
                       no = re.sub("\s{2,}", " ", no).strip()
                       no = re.sub("\xc2", "", no).strip().split(" ")
                       other = re.sub("\s{2,}", " ", other).strip()
                       other = re.sub("\xc2", "", other).strip().split(" ")

                    vote = Vote(actor, date, action, len(yes) > len(no), len(yes), len(no), len(other))
                    yes_count = 0
                    for yes_vote in yes:
                        if yes_vote.strip():
                            vote.yes(yes_vote.strip())
                            yes_count += 1

                    no_count = 0
                    for no_vote in no:
                        if no_vote.strip():
                            vote.no(no_vote.strip())
                            no_count += 1

                    other_count = 0
                    for other_vote in other:
                        if other_vote.strip():
                            vote.other(other_vote.strip())
                            other_count += 1

                    vote['passed'] = yes_count > no_count + other_count
                    vote['yes_count'] = yes_count
                    vote['no_count'] = no_count
                    vote['other_count'] = other_count
                    bill.add_vote(vote)

        bill_text = WIUrl.get_bill_text(session_id, url_bill_id, ss_code)
        bill_text_header = self.scraper.http_request(bill_text, method=u"HEAD")
        mimetype = bill_text_header.headers[u"content-type"]
        if u"text/html" in mimetype:
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(bill_text, BRP.bill_versions,
                                                             self.scraper.extraction_type.html,
                                                             False, get_static_content=self.html_static_content)
        elif u"application/pdf" in mimetype:
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(bill_text, BRP.bill_versions,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True)
        else:
            logger.error(u"Unknown mimetype '%s' for version text at: %s", mimetype, bill_text)
            download_id, doc_ids = None, []

        if len(doc_ids) == 1 and doc_ids[0] is not None:
            doc_service_document = Doc_service_document("Proposal Text", "version", "complete",
                                                        download_id, doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)
        else:
            logger.warning("Failed to process file at url {}".format(bill_text))

        enroll_url = bill_page.xpath_single("//a[text()='Text as Enrolled']/@href", BRP.bill_partial_documents)
        if enroll_url:
            download_id, _, docs_id = self.scraper.register_download_and_documents(enroll_url, BRP.bill_versions,
                                                                                   self.scraper.extraction_type.html,
                                                                                   False,
                                                                                   get_static_content=self.html_static_content)
            if len(docs_id) == 1 and docs_id[0] is not None:
                doc_service_document = Doc_service_document('Text as Enrolled', "version", "complete", download_id, doc_id=docs_id[0])
                bill.add_doc_service_document(doc_service_document)

        amendments_url = bill_page.xpath_single("//a[text()='Amendment Histories']/@href", BRP.bill_partial_documents)
        if amendments_url:
            amendments_page = self.scraper.url_to_lxml(amendments_url, BRP.bill_versions)
            for links in amendments_page.xpath("//ul[@class='docLinks']/li/p/a"):
                amendment_text_link = links.get_attrib(u"href").replace(u"/session/", u"/amends/")
                download_id, _, docs_id = self.scraper.register_download_and_documents(
                    amendment_text_link, BRP.bill_versions, self.scraper.extraction_type.html,                                                                
                    False, get_static_content=self.html_static_content)
                if docs_id and docs_id[0] != None:
                    amendment_page = self.scraper.url_to_lxml(links.get_attrib("href"), BRP.bill_versions)
                    status = re.findall(r"Status: (.*)", amendment_page.text_content())[0]
                    doc_service_document = Doc_service_document(
                        "{} {}".format(links.text_content(), status).strip(),
                        "amendment", "complete", download_id, doc_id=docs_id[0])
                    bill.add_doc_service_document(doc_service_document)

        summary_url = bill_page.xpath("//a[contains(text(), 'LC A')]", BRP.bill_partial_documents)
        if summary_url:
            for links in summary_url:
                link_url = links.get_attrib(u"href")
                if u".pdf" in link_url:
                    download_id, _, docs_id = self.scraper.register_download_and_documents(
                        link_url, BRP.bill_documents, 
                        self.scraper.extraction_type.text_pdf, True)
                else:
                    download_id, _, docs_id = self.scraper.register_download_and_documents(
                        link_url, BRP.bill_documents,
                        self.scraper.extraction_type.html, False,
                                                                                           get_static_content=self.html_static_content)
                if len(docs_id) == 1 and docs_id[0] is not None:
                    doc_service_document = Doc_service_document(
                        links.text_content(), "summary", "complete",
                                                                download_id, doc_id=docs_id[0])
                    bill.add_doc_service_document(doc_service_document)

        rocp_url = bill_page.xpath("//span/a[contains(text(), 'ROCP')]", BRP.bill_partial_documents)
        if rocp_url:
            for links in rocp_url:
                download_id = self.scraper.download_and_register(links.get_attrib("href"),
                                                                 BRP.bill_partial_documents,
                                                                 False)
                if download_id:
                    doc_service_document = Doc_service_document(links.text_content(), "committee_document",
                                                                "partial", download_id)
                    bill.add_doc_service_document(doc_service_document)

        fiscalnote = bill_page.xpath("//a[contains(text(), 'Fiscal Estimates')]", BRP.bill_partial_documents)
        if fiscalnote:
            fiscalnote_page = self.scraper.url_to_lxml(fiscalnote[0].get_attrib("href"), BRP.bill_documents)
            for links in fiscalnote_page.xpath("//span[@class='hasPdfLink']/a"):
                download_id = self.scraper.download_and_register(links.get_attrib("href"),
                                                                 BRP.bill_partial_documents,
                                                                 False)
                if download_id:
                    doc_service_document = Doc_service_document(links.text_content(), "fiscal_note", "partial",
                                                                download_id)
                    bill.add_doc_service_document(doc_service_document)
 
        final_act = bill_page.xpath_single("//span/a[contains(@href, 'document/acts/')]", BRP.bill_partial_documents)
        if final_act:

            download_id = self.scraper.download_and_register(final_act.get_attrib("href"),
                                                             BRP.bill_partial_documents,
                                                             False)
            if download_id:
                doc_service_document = Doc_service_document(final_act.text_content(), "other", "partial",
                                                            download_id)
                bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    def html_static_content(self, html_file):
        html_text = html_file.read().decode(u'utf-8')
        root = self.scraper.wrap_html(u"", html_text, BRP.bill_documents)
        elem = root.xpath_single(u"//div[@id='document']")
        if not elem:
            elem = root.xpath_single(u"body")
        return elem.tostring()

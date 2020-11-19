'''
# Mississippi Bill Scraper

## Example URLs:

 - Docket List: http://billstatus.ls.state.ms.us/2018/pdf/all_measures/allmsrs.xml
 - Specific bills: http://billstatus.ls.state.ms.us/2018/pdf/history/SN/SN0001.xml

'''

from __future__ import absolute_import

import datetime
import logging
import re
from urlparse import urljoin

from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ScraperDocument
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import normalize_bill_id
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(u'MSBillScraper')

version_tags = {u"intro", u"cmtesub", u"current", u"asg", u"passed"}


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-MS", group="fnleg")
class MSBillScraper(BillScraper):
    def __init__(self):
        super(MSBillScraper, self).__init__("ms")
        self.sponsor_cache = {}

    def scrape_bill_ids(self, session):
        bill_ids = []
        bill_list_url = u"http://billstatus.ls.state.ms.us/{}/pdf/all_measures/allmsrs.xml". \
            format(self.get_url_year(session))
        response = self.scraper.http_request(bill_list_url)
        content = response.content
        source = content.decode(u"iso-8859-1").encode(u"utf8")
        bill_list_doc = self.scraper.wrap_html(bill_list_url, source, BRP.bill_list)
        for item in bill_list_doc.xpath(u"//msrgroup", BRP.debug):
            bill_id = item.xpath_single(u"./measure").text_content()
            bill_id = normalize_bill_id(bill_id)
            bill_ids.append(bill_id)
        logger.info(u"A total of {} bill ids scraped for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        # basic info
        bill_page_url = self.build_bill_page_url(self.get_url_year(session), bill_id)
        logger.info(u"Scraping bill {} at {}".format(bill_id, bill_page_url))
        response = self.scraper.http_request(bill_page_url)
        content = response.content
        source = content.decode(u"iso-8859-1").encode(u"utf8")
        bill_page_doc = self.scraper.wrap_html(bill_page_url, source, BRP.bill)

        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        short_title = bill_page_doc.xpath_single(u"//shorttitle").text_content()

        bill = Bill(session, chamber, bill_id, short_title, bill_type)
        bill.add_source(bill_page_url)

        # summary
        long_title = bill_page_doc.xpath_single(u"//longtitle")
        if long_title and long_title.text_content(BRP.debug):
            bill.add_summary(long_title.text_content().title())
        
        self.scrape_sponsors(bill, bill_page_doc, bill_page_url)

        # actions and votes
        for action_doc in bill_page_doc.xpath(u"//action", policy=BRP.info):
            action_string_full = action_doc.xpath_single(u"./act_desc").text_content()
            date_string, _, action = \
                re.findall(r"^\s*(\d{2}/\d{2})\s+(\([SH]\))?\s*(.+)\s*$", action_string_full)[0]
            date_string = u"{}/{}".format(session[:4], date_string)
            action_date = datetime.datetime.strptime(date_string, u"%Y/%m/%d")
            if re.search(r"\([SH]\)", action_string_full):
                chamber_string = re.search(r"\(([SH])\)", action_string_full).group(1)
                actor = u"lower" if chamber_string == u"H" else u"upper"
            elif u"governor" in action.lower():
                actor = u"executive"
            else:
                actor = chamber
            bill.add_action(actor, action, action_date)
            abridged_vote_url = action_doc.xpath_single(u"./act_vote", policy=BRP.info)
            if abridged_vote_url:
                abridged_vote_url = abridged_vote_url.text_content()
                vote_url = self.get_document_url(abridged_vote_url)
                download_id, scraper_docs, doc_ids = \
                    self.scraper.register_download_and_documents(vote_url, BRP.bill_votes,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, self.vote_parser)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    vote_info = scraper_docs[0].additional_data
                    yes_voters = vote_info[u"yes_voters"]
                    no_voters = vote_info[u"no_voters"]
                    other_voters = vote_info[u"other_voters"]
                    action_string_full = action_string_full.lower()
                    if u"pass" in action_string_full or u"adopt" in action_string_full \
                            or u"approve" in action_string_full:
                        passed = True
                    elif u"reject" in action_string_full or u"faile" in action_string_full:
                        passed = False
                    else:
                        passed = len(yes_voters) > len(no_voters)
                        logger.warning(u"No clear indication whether the vote has passed basing on action "
                                       u"string \"{}\". Falling back to comparing yes and no votes".
                                       format(action_string_full))

                    vote = Vote(actor, action_date, action, passed, len(yes_voters), len(no_voters), len(other_voters))
                    for method, voters in [(vote.yes, yes_voters), (vote.no, no_voters), (vote.other, other_voters)]:
                        for voter in voters:
                            method(voter)
                    vote.add_source(vote_url)
                    bill.add_vote(vote)
                else:
                    logger.warning(u"Doc service failed to process vote file at url {}".format(vote_url))

        # documents
        documents_doc = bill_page_doc.xpath_single(u"//documents", policy=BRP.info)
        if documents_doc:

            version_docs = [version_doc for version_doc in documents_doc.xpath(u"./*")
                            if version_doc.element.tag.lower() in version_tags]
            for index in range(0, len(version_docs)):
                version_doc = version_docs[index]
                if version_doc.element.tag.lower() == u"current":
                    temp = version_doc
                    version_docs.pop(index)
                    version_docs.append(temp)
                    break

            for version_doc in version_docs:
                tag = version_doc.element.tag
                name = self.get_version_name(tag, chamber)
                urls = {}
                for file_format in version_doc.xpath(u"./*"):
                    abridged_url = file_format.text_content()
                    url = self.get_document_url(abridged_url)
                    if url.lower().endswith(u"pdf"):
                        urls[u"pdf"] = url
                    elif url.lower().endswith(u"htm") or url.lower().endswith(u"html"):
                        urls[u"html"] = url
                    else:
                        logger.warning(u"Failed to guess file format from url {}".format(url))
                assert u"html" in urls or not urls
                if urls:
                    html_url = urls[u"html"]
                    if u"docnotfound" in html_url.lower():
                        logger.warning(u"url {} for document {} is not valid. skip it.".format(html_url, name))
                        continue
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(html_url, BRP.bill_versions,
                                                                     self.scraper.extraction_type.html,
                                                                     False, self.version_parser)
                    if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                        doc_service_document = Doc_service_document(name, u"version", u"complete", download_id,
                                                                    doc_ids[0])
                        bill.add_doc_service_document(doc_service_document)
                    else:
                        logger.warning(u"Failed to process document at url {}. This is likely to be because either the "
                                       u"link is invalid causing download failure or there is not content at the link "
                                       u"causing doc_ids[0] to be a None".format(html_url))
                else:
                    logger.warning(u"No url found for version {} at {}".format(name, bill_page_url))

            for amendment_doc in documents_doc.xpath(u"./amendments/*", policy=BRP.info):
                tag = amendment_doc.element.tag
                name = amendment_doc.xpath_single(u"./{}_desc".format(tag)).text_content()
                if amendment_doc.xpath_single(u"./{}_disp".format(tag), policy=BRP.info):
                    name = u"{} -- {}".format(name, amendment_doc.xpath_single(u"./{}_disp".format(tag)).text_content())
                abridged_html_url = amendment_doc.xpath_single(u"./{}_other".format(tag)).text_content()
                html_url = self.get_document_url(abridged_html_url)
                if u"docnotfound" in html_url.lower():
                    logger.warning(u"url {} for document {} is not valid. skip it.".format(html_url, name))
                    continue
                # TODO: removed alternate format download
                # abridged_pdf_url = amendment_doc.xpath_single(u"./{}_pdf".format(tag)).text_content()
                # pdf_url = self.get_document_url(abridged_pdf_url)
                download_id, _, doc_ids = \
                    self.scraper.register_download_and_documents(html_url, BRP.bill_documents,
                                                                 self.scraper.extraction_type.html,
                                                                 False, self.version_parser)
                if download_id is not None and len(doc_ids) == 1 and doc_ids[0] is not None:
                    doc_service_document = Doc_service_document(name, u"amendment", u"complete",
                                                                download_id, doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
                else:
                    logger.warning(u"Failed to process document at url {}. This is likely to be because either the "
                                   u"link is invalid causing download failure or there is not content at the link "
                                   u"causing doc_ids[0] to be a None".format(html_url))

            for fiscal_note_doc in documents_doc.xpath(u"./fiscalnote", policy=BRP.info):
                flag = fiscal_note_doc.xpath_single(u"./fiscalnoteflag").text_content().lower()
                if flag != u"n":
                    logger.critical(u"We found an example where fiscalnot is not null! bill url = {}".
                                    format(bill_page_url))

            for conference_report_doc in documents_doc.xpath(u"./confrpts", policy=BRP.info):
                name = conference_report_doc.xpath_single(u"./cr_desc").text_content()
                pdf_url = conference_report_doc.xpath_single(u"./cr_pdf").text_content()
                pdf_url = self.get_document_url(pdf_url)
                if u"docnotfound" in pdf_url.lower():
                    logger.warning(u"url {} for document {} is not valid. skip it.".format(pdf_url, name))
                    continue
                download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
                doc_service_document = Doc_service_document(name, u"committee_document", u"partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)

    def scrape_sponsors(self, bill, bill_page_doc, bill_page_url):
        '''
        Scrape the sponsors for a bill, adding them to the bill object.

        Important notes:
            - some sponsors do not have links associated with them:
                <AUTHORS><PRINCIPAL>
                    <P_NAME>LT. GOVERNOR</P_NAME>
                </PRINCIPAL></AUTHORS>
            - sponsors may not be listed, uncertain why (DI-1918)
                <AUTHORS><ADDITIONAL>
                    <CO_NAME></CO_NAME>
                    <CO_LINK>../../House_authors/.xml</CO_LINK>
                </ADDITIONAL></AUTHORS>
        '''
        authors_doc = bill_page_doc.xpath_single(u"//authors")
        xpaths = [
            ("primary", "./principal", "./p_name/text()", "./p_link/text()"),
            ("cosponsor", "./additional", "./co_name/text()", "./co_link/text()")
        ]
        for sponsor_type, sponsor_root, name_xpath, url_xpath in xpaths:
            sponsors = []
            for sponsor_link in authors_doc.xpath(sponsor_root):
                name = sponsor_link.xpath_single(
                    name_xpath, policy=BRP.info)
                if not name:
                    continue
                url = sponsor_link.xpath_single(url_xpath, policy=BRP.info)
                if url and url in self.sponsor_cache:
                    name = self.sponsor_cache[url]
                elif url:
                    response = self.scraper.http_request(urljoin(
                        bill_page_url, url))
                    content = response.content
                    source = content.decode("iso-8859-1").encode("utf8")
                    sponsor_page = self.scraper.wrap_html(
                        sponsor_link.text_content(), source, BRP.bill_sponsors)
                    sponsor_page_name = sponsor_page.xpath_single(
                        "//titleline").text_content()
                    if sponsor_page_name:
                        name = sponsor_page_name
                    self.sponsor_cache[url] = name
                sponsors.append(name)
             
            for sponsor in sponsors:
                sponsor = re.sub(r"\(.+?\)", u"", sponsor)
                bill.add_sponsor(sponsor_type, sponsor)

    @staticmethod
    def get_url_year(session):
        year = session[0:4]
        if u"ss" in session:
            year += session.split(u"ss")[-1]
            year += u"E"
        return year

    @staticmethod
    def build_bill_page_url(session, bill_id):
        [bill_type, bill_number] = bill_id.split()
        return u"http://billstatus.ls.state.ms.us/{}/pdf/history/{}/{}.xml". \
            format(session, bill_type, bill_type + bill_number.zfill(4))

    @staticmethod
    def get_version_name(tag, bill_chamber):
        tag = tag.upper()
        version_name_map = {u"INTRO": u"As Introduced",
                            u"CMTESUB": u"Committee Substitute",
                            u"CURRENT": u"Current Version",
                            u"ASG": u"Approved by the Governor"}
        if tag == u"PASSED":
            return u"As Passed the {}".format(u"House" if bill_chamber == u"lower" else u"Senate")
        elif tag in version_name_map:
            return version_name_map[tag]
        else:
            logger.critical(u"Failed to assign a proper version name for document with tag {}. This "
                            u"tag needs to be added.".format(tag))

    @staticmethod
    def get_document_url(abridged_url):

        return re.sub(r".+\.\./", u"http://billstatus.ls.state.ms.us/", abridged_url)

    @staticmethod
    def version_parser(element_wrapper):
        text = element_wrapper.xpath_single(u"//body").text_content()
        return [ScraperDocument(text)]

    def vote_parser(self, entities):
        text = entities_text_content(entities)
        text = re.sub(r"\s+", " ", text)
        name_blocks = re.findall(r"-\s*-\s*(.*?)\.?\s*(?:Total\s*-\s*-?\s*\d+|None)\.?\s*", text)

        other_voters = []
        yes_voters = self.parse_voters(name_blocks[0])
        no_voters = self.parse_voters(name_blocks[1])
        for name_block in name_blocks[2:]:
            other_voters += self.parse_voters(name_block)

        return [ScraperDocument(text, additional_data={u"yes_voters": yes_voters,
                                                       u"no_voters": no_voters,
                                                       u"other_voters": other_voters})]

    @staticmethod
    def parse_voters(voters_block):
        voters = []
        for voter_raw in voters_block.split(","):
            voter = re.sub(r"\s*\(.+?\)\s*", u"", voter_raw).strip()
            if voter.lower() == u"mr. speaker" or not voter:
                # there are voters whose name is Zuber, Mr. Speaker
                # but indeed the name is just Zuber. So we ignore if the name turned out to be only "Mr. Speaker"
                continue
            voters.append(voter)
        return voters

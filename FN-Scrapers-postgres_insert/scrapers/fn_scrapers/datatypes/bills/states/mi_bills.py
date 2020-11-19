# -*- coding: utf-8 -*-

'''
MI Bill Scraper

Initiallyscrapes bills at the RSS index pages:
 - https://legislature.mi.gov/documents/2017-2018/status/house/rss/
 - https://legislature.mi.gov/documents/2017-2018/status/senate/rss/

Uses the bill id (HR 1) as well as the year to identify a bill (MI has a two
year session and bills are unique across the two years, but still uses the year
a bill is introduced to link bills, because reasons).

Past problems with this scraper:
 - Previously blocked for hitting the search page. Uses the RSS Indexes above
   to prevent that.
 - Had an issue with non-numeric bill id's (HJR A). See DI-1719 and DI-2037.
'''

from __future__ import absolute_import

import datetime
import re

import logging
from requests.exceptions import HTTPError
from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY
from fn_scraperutils.doc_service.util import ScraperDocument

from fn_scrapers.api.scraper import scraper, tags


web_url = 'http://www.legislature.mi.gov'
logger = logging.getLogger('MIBillScraper')


chamber_type = {'lower': 'house',
                'upper': 'senate'
                }

# NOTE: Change from /xml to /rss due to /xml missing approx 30% of bills
base_url = "http://legislature.mi.gov/documents/%s/status/%s/rss"


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-MI", group="fnleg")
class MIBillScraper(BillScraper):
    bill_pattern = re.compile(r'/rss/(\d{4})-(.*?)-(.*?)rss\.xml$')

    def __init__(self):
        super(MIBillScraper, self).__init__("mi")

    def scrape_bill_ids(self, session):
        """
        Scrape list of bill ids
        """
        session_slug = "%s-%s" %(session[:4], session[4:8])
        bill_ids = {}
        for chamber in ['house', 'senate']:
            url = base_url % (session_slug, chamber)
            logger.info("Scraping bills at %s", url)
            page = self.scraper.url_to_lxml(
                url, BRP.bill_list, retry_policy=RETRY500_REQUESTS_RETRY_POLICY)
            for bill_url in page.xpath("//a/@href", BRP.test):
                match = self.bill_pattern.search(bill_url)
                if not match:
                    continue
                year, bill_type, bill_num = match.groups()
                bill_id = "%s %s" % (bill_type, bill_num)
                bill_ids[bill_id] = year
        return bill_ids


    def scrape_bill(self, session, bill_id, **kwargs):
        """
        Scrape single bill
        bill_id is in format "bill id_year"
        """
        year = kwargs['bill_info']
        logger.info("Scraping bill %s (%s)", bill_id, year)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        # Validate Bill
        bill_id_rgx = re.compile(r'^[HS][RCBJHG]{1,2}\s?[\d+]?[\w]{1,4}?$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.warning("Invalid bill ID %s", bill_id)
            return

        # try and get bill for first year
        bill_type, bill_num = bill_id.split(' ')
        if bill_num.isdigit():
            bill_num = str("%04d" % int(bill_num))
        bill_id_slug = "%s-%s" % (bill_type, bill_num)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)

        url = 'http://legislature.mi.gov/doc.aspx?%s-%s' % (year, bill_id_slug)
        doc = self.scraper.url_to_lxml(url, BRP.bill, retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

        if doc is None:
            logger.error("Failed to load bill %s page %s" % (bill_id, url))
            return

        # if first page isn't found, try second year
        if 'Page Not Found' in doc.text_content() or 'The bill you are looking for is not available yet.'\
                                                     '  Please try again later.' in doc.text_content():
            logger.error("Failed to load bill %s page %s" % (bill_id, url))
            return

        try:
            title = doc.xpath_single('//span[@id="frg_billstatus_ObjectSubject"]').text_content()
        except AttributeError:
            logger.warning("Failed to find web page for %s" % bill_id)
            return

        # get B/R/JR/CR part and look up bill type

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(url)

        # sponsors
        sp_type = 'primary'
        for sponsor_elem in doc.xpath('//span[@id="frg_billstatus_SponsorList"]/a[@class="personPopupTrigger"]'):
            sponsor = sponsor_elem.text_content().replace(u'\xa0', ' ')
            bill.add_sponsor(sp_type, sponsor)
            sp_type = 'cosponsor'

        for sub in doc.xpath('//span[@id="frg_billstatus_CategoryList"]/a/text()', BRP.bill_subjects):
            bill.add_subject(sub)
        
        previous_date = None
        # actions (skip header)
        for row in doc.xpath('//table[@id="frg_billstatus_HistoriesGridView"]/tr')[1:]:
            tds = row.xpath('td')  # date, journal link, action
            date = tds[0].text_content().strip()
            journal = tds[1].text_content()
            action = tds[2].text_content()
            if date:
                date = datetime.datetime.strptime(date, "%m/%d/%Y")
            else:
                date = previous_date
            
            # instead of trusting upper/lower case, use journal for actor
            actor = 'upper' if 'SJ' in journal else 'lower'
            if date:
                bill.add_action(actor, action, date)

            # check if action mentions a vote
            rcmatch = re.search(r'Roll Call # (\d+)', action, re.IGNORECASE)
            if rcmatch:
                rc_num = rcmatch.groups()[0]
                # in format mileg.aspx?page=getobject&objectname=2011-SJ-02-10-011
                journal_link = tds[1].xpath('a/@href')
                if journal_link:
                    objectname = journal_link[0].rsplit('=', 1)[-1]
                    chamber_name = {'upper': 'Senate', 'lower': 'House'}[actor]
                    session_slug = "%s-%s" %(session[:4], session[4:8])
                    vote_url = 'http://legislature.mi.gov/documents/%s/Journal/%s/htm/%s.htm' % (
                        session_slug, chamber_name, objectname)

                    if 'not adopted' in action:
                        passed = False
                    else:
                        passed = True

                    yays, nays = re.findall(
                        r'Roll Call # \d+ Yeas (\d+) Nays (\d+)', action, re.IGNORECASE)[0]

                    if yays > 0 or nays > 0:
                        vote = Vote(actor, date, action, passed, yays, nays, 0)
                    else:
                        vote = Vote(actor, date, action, False, 0, 0, 0)
                    self.parse_roll_call(vote, vote_url, rc_num)
                    
                    # check the expected counts vs actual
                    count = re.search(r'YEAS (\d+)', action, re.IGNORECASE)
                    count = int(count.groups()[0]) if count else 0
                    if 'yes_votes' not in vote and count != 0  or 'yes_votes' in vote and \
                                                   count != len(vote['yes_votes']):
                        if 'yes_votes' not in vote:
                            yes_num = 0
                        else:
                            yes_num = len(vote['yes_votes'])
                        logger.warning('vote count mismatch for %s %s, %d != %d' %
                                     (bill_id, action, count, yes_num))
                    count = re.search(r'NAYS (\d+)', action, re.IGNORECASE)
                   

                    count = int(count.groups()[0]) if count else 0
                    if 'no_votes' not in vote and count != 0  or 'no_votes' in vote and \
                                                  count != len(vote['no_votes']):
                        if 'no_votes' not in vote:
                            no_num = 0
                        else:
                            no_num = len(vote['no_votes'])
                        logger.warning('vote count mismatch for %s %s, %d != %d' %
                                     (bill_id, action, count, no_num))
                    if 'yes_votes' in vote:
                        vote['yes_count'] = len(vote['yes_votes'])
                    else:
                        vote['yes_count'] = 0
                    
                    if 'no_votes' in vote:
                        vote['no_count'] = len(vote['no_votes'])
                    else:
                        vote['no_count'] = 0
                    
                    if 'other_votes' in vote:
                        vote['other_count'] = len(vote['other_votes'])
                    else:
                        vote['other_count'] = 0

                    vote['passed'] = vote['yes_count'] > vote['no_count']
                    vote.add_source(vote_url)
                    bill.add_vote(vote)
                else:
                    logger.warning("missing journal link for %s %s" %
                                 (bill_id, journal))
            previous_date = date

        # versions
        for row in doc.xpath('//table[@id="frg_billstatus_DocumentGridTable"]/tr'):

            doc_service_document = None
            version_html_name, version_html_url = self.parse_doc_row(row, 'htm')
            if version_html_url:
                download_id, _, doc_ids = self.scraper.register_download_and_documents(
                    version_html_url, BRP.bill_versions, 
                    self.scraper.extraction_type.html, True, self.html_parser)

                if len(doc_ids) != 1:
                        logger.warning(
                            "Document %s get more than one doc id, ignore",
                            version_html_name)
                        continue
                doc_service_document = Doc_service_document(
                    version_html_name, "version", "complete",
                    download_id=download_id, doc_id=doc_ids[0])


            else:
                version_pdf_name, version_pdf_url = self.parse_doc_row(row, 'pdf')
                if version_pdf_url:
                    try:
                        download_id, _, doc_ids = self.scraper.register_download_and_documents(
                            version_pdf_url, BRP.bill_versions,
                            self.scraper.extraction_type.text_pdf, True)

                        if len(doc_ids) != 1:
                            logger.warning(
                                "Document %s get more than one doc id, ignore",
                                version_pdf_name)
                            continue

                        doc_service_document = Doc_service_document(
                            version_pdf_name, "version", "complete",
                            download_id=download_id, doc_id=doc_ids[0])
                    except HTTPError:
                        logger.warning("%s not found" % version_html_url)


            if doc_service_document:
                bill.add_doc_service_document(doc_service_document)

        
        # documents
        for row in doc.xpath('//table[@id="frg_billstatus_HlaTable"]/tr', BRP.bill_documents):
            doc_service_document = None
            document_name, document_url = self.parse_doc_row(row, 'htm')
            if document_url:
                html_download_id = self.scraper.download_and_register(
                    document_url, BRP.bill_documents, False)
                doc_service_document = Doc_service_document(
                    document_name, "summary", "partial", html_download_id)
            else:
                document_pdf_name, document_pdf_url = self.parse_doc_row(row, 'pdf')
                if document_pdf_url:
                    html_download_id = self.scraper.download_and_register(
                        document_pdf_url, BRP.bill_documents, False)
                    doc_service_document = Doc_service_document(
                        document_pdf_name, "summary", "partial", html_download_id)
            if doc_service_document:
                bill.add_doc_service_document(doc_service_document)


        for row in doc.xpath('//table[@id="frg_billstatus_SfaTable"]/tr'):
            document_name, document_url = self.parse_doc_row(row, 'htm')
            doc_service_document = None
            if document_url:
                html_download_id = self.scraper.download_and_register(
                    document_url, BRP.bill_documents, False)
                doc_service_document = Doc_service_document(
                    document_name, "summary", "partial", html_download_id)
            else:
                document_pdf_name, document_pdf_url = self.parse_doc_row(row, 'pdf')
                if document_pdf_url:
                    html_download_id = self.scraper.download_and_register(
                        document_pdf_url, BRP.bill_documents, False)
                    doc_service_document = Doc_service_document(
                        document_pdf_name, "summary", "partial",
                        html_download_id)
            if doc_service_document:
                bill.add_doc_service_document(doc_service_document)
        
        self.save_bill(bill)


    def parse_doc_row(self, row, type):
        """
        scrape document name and url
        """
        a = row.xpath_single('.//a[contains(@href, "%s")]' % type, BRP.test)
        
        if not a:
            return None, None

        if a:
            name = row.xpath_single('.//b/text()')
            if not name:
                name = row.text_content().strip()
            url = a.get_attrib('href').replace('../', '')
            return name, url


    def parse_roll_call(self, vote, url, rc_num):
        """
            Parse vote information from roll call page
        """
        vote_doc = self.scraper.url_to_lxml(url, BRP.bill_votes, retry_policy=RETRY500_REQUESTS_RETRY_POLICY)
        if 'In The Chair' not in vote_doc.text_content():
            logger.warning('"In The Chair" indicator not found, unable to extract vote')
            return

        # split the file into lines using the <p> tags
        pieces = [p.text_content().replace(u'\xa0', ' ')
                  for p in vote_doc.xpath("//*[self::p or self::h5 or self::td]")]

        # Certain files have wrapper p tag that has entirety of text
        pieces = filter(lambda x: len(x) < 20000, pieces)

        # go until we find the roll call
        for i, p in enumerate(pieces):
            if ('Roll Call No. %s' % rc_num) in p:
                break
            elif p.startswith(u'Roll Call No. %s' % rc_num):
                break
            elif p.startswith(u'Roll Call No.%s' % rc_num):
                break
            elif (u'Roll Call No.%s' % rc_num) in p:
                break

        vtype = None
        # once we find the roll call, go through voters
        entire_string = ""

        for p in pieces[i:]:
            # mdash: \xe2\x80\x94 splits Yeas/Nays/Excused/NotVoting
            entire_string += p
            if 'Yeas' in p:
                vtype = vote.yes
            elif 'Nays' in p:
                vtype = vote.no
            elif 'Excused' in p or 'Not Voting' in p:
                vtype = vote.other
            elif 'Roll Call No' in p:
                continue
            elif 'JOURNAL OF THE HOUSE' in p:
                continue
            elif p.startswith('In The Chair:') or 'In The Chair:' in p:
                break
            elif vtype:
                # split on spaces not preceeded by commas
                van = False
                vander = False
                for l in re.split(r'(?<!,)\s+', p):
                    if l:
                        if(l == 'Van'):
                            van = True
                        elif(van):
                            vtype("Van " + l)
                            van = False
                        elif(l == 'Vander'):
                            vander = True
                        elif(vander):
                            vtype("Vander " + l)
                            vander = False
                        else:
                            vtype(l)
            else:
                logger.warning('piece without vtype set: %s' % p)


    @staticmethod
    def html_parser(element_wrapper):
        return [ScraperDocument(element_wrapper.xpath_single("//body").text_content())]

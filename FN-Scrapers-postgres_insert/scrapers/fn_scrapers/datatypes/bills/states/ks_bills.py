from __future__ import absolute_import

import urllib

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags

import re
import os
import datetime
import logging
import ezodf
from dateutil.parser import parse


logger = logging.getLogger('KSBillScraper')


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-KS", group="fnleg")
class KSBillScraper(BillScraper):
    ksleg = 'http://www.kslegislature.org/li%s'
    url = '%s/api/v10/rev-1/' % ksleg

    def __init__(self):
        super(KSBillScraper, self).__init__("ks")

    def scrape_bill_ids(self, session):
        """
        Scrape list of bill ids
        """
        # perhaps we should save this data so we can make one request for both?
        request_url = self.url + 'bill_status/'
        slug = get_session_details(session)

        request_url = request_url % slug

        bill_request_json = self.scraper.url_to_json(request_url, BRP.bill_list)

        bills = bill_request_json['content']

        bill_ids = []
        for bill_data in bills:
            bill_id = bill_data['BILLNO']
            bill_ids.append(bill_id)
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        """
        Scrape single bills
        """
        slug = get_slug(session)

        ses_slug = get_session_details(session)
        base_url = 'http://www.kslegislature.org/li%s/%s/measures/' % (ses_slug, slug)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        if 'CR' in bill_id:
            btype = 'concurrent_resolution'
            base_url = 'http://www.kslegislature.org/li%s/%s/year1/measures/' % (ses_slug, slug)
        elif 'R' in bill_id:
            btype = 'resolution'
            base_url = 'http://www.kslegislature.org/li%s/%s/year1/measures/' % (ses_slug, slug)
        elif 'B' in bill_id:
            btype = 'bill'

        full_id = re.sub(r'\s+', '', bill_id.lower())
        bill_page_url = base_url + full_id + '/'
        doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)
        if not doc:
            base_url = 'http://www.kslegislature.org/li%s/%s/year2/measures/' % (ses_slug, slug)
            bill_page_url = base_url + full_id + '/'
            doc = self.scraper.url_to_lxml(bill_page_url, BRP.bill)


        title = doc.xpath_single('//p[@class="truncated_text"]').text_content()

        bill = Bill(session, chamber, bill_id, title, btype)
        
        bill.add_source(bill_page_url)

        bill_url = (self.url + 'bill_status/' + full_id) % ses_slug

        bill_request_json = self.scraper.url_to_json(bill_url, BRP.bill_sponsors)
        if bill_request_json is None:
            logger.error("Failed to retrieve %s" % bill_url)
            return
        bill_data = bill_request_json['content'][0]
        if bill_data['LONGTITLE'] and bill_data['LONGTITLE'] != bill['title']:
            bill.add_alternate_title(bill_data['LONGTITLE'])

        for sponsor in bill_data['SPONSOR_NAMES']:
            stype = ('primary' if len(bill_data['SPONSOR_NAMES']) == 1
                     else 'cosponsor')
            name = None
            senator = re.match(r"senator (.+)", sponsor, re.I)
            representative = re.match(r"representative (.+)", sponsor, re.I)
            if senator:
                name = senator.group(1)
                chamber = "upper"
            elif representative:
                name = representative.group(1)
                chamber = "lower"

            if name:
                bill.add_sponsor(stype, name, chamber=chamber)
            else:
                bill.add_sponsor(stype, sponsor)

        # history is backwards
        for event in reversed(bill_data['HISTORY']):

            actor = ('upper' if event['chamber'] == 'Senate'
                     else 'lower')

            date = datetime.datetime.strptime(event['occurred_datetime'], "%Y-%m-%dT%H:%M:%S")
            # append committee names if present

            if 'committee_names' in event:
                action = (event['status'] + ' ' +
                          ' and '.join(event['committee_names']))
            else:
                action = event['status']

            if 'governor' in action.lower():
                actor = 'executive'

            bill.add_action(actor=actor, action=action, date=date)

        # versions & notes
        version_rows = doc.xpath('//tbody[starts-with(@id, "version-tab")]/tr')
        for row in version_rows:
            # version, docs, sn, fn
            tds = row.xpath('./td')
            title = tds[0].text_content()
            doc_url = get_doc_link(tds[1])
            if not title:
                if 'enrolled' in doc_url:
                    title = 'Enrolled'
                else:
                    title = 'Current Version'
            if doc_url:
                download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(doc_url, BRP.bill_versions,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         True)

                if len(doc_ids) != 1:
                    logger.warning("Document %s get more than one doc id, ignore" % doc_url)
                    continue
                
                doc_service_document = Doc_service_document(title, "version", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

            if len(tds) > 2:
                sn_url = get_doc_link(tds[2])
                if sn_url:
                    doc_id = self.scraper.download_and_register(sn_url, BRP.bill_documents, False)
                    doc_service_document = Doc_service_document(title + ' - Supplementary Note', "summary", "partial", doc_id)
                    bill.add_doc_service_document(doc_service_document)
            if len(tds) > 3:
                fn_url = get_doc_link(tds[3])
                if sn_url:
                    doc_id = self.scraper.download_and_register(sn_url, BRP.bill_documents, False)

                    doc_service_document = Doc_service_document(title + ' - Fiscal Note', "fiscal_note", "partial", doc_id)
                    bill.add_doc_service_document(doc_service_document)


        history_rows = doc.xpath('//tbody[starts-with(@id, "history-tab")]/tr')
        for row in history_rows:
            row_text = row.xpath('.//td[3]')[0].text_content()

            # votes
            vote_url = row.xpath('.//a[contains(text(), "Yea:")]/@href', BRP.bill_versions)
            if vote_url:
                vote_date = row.xpath('.//td[1]')[0].text_content()
                vote_chamber = row.xpath('.//td[2]')[0].text_content()
                self.parse_vote(bill, vote_date, vote_chamber, row_text, vote_url[0])

            # amendments & reports
            amendment = get_doc_link(row.xpath('.//td[4]', BRP.bill_versions)[0])
            if amendment:
                if 'Motion to Amend' in row_text:
                    _, offered_by = row_text.split('Motion to Amend -')
                    amendment_name = 'Amendment ' + offered_by.strip()
                elif 'Conference committee report now available' in row_text:
                    amendment_name = 'Conference Committee Report'
                else:
                    amendment_name = row_text.strip()
                download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(amendment, BRP.bill_documents,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         True)

                if len(doc_ids) != 1:
                    logger.warning("Document %s get more than one doc id, ignore" % doc_url)
                    continue
                
                doc_service_document = Doc_service_document(amendment_name, "amendment", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
        self.save_bill(bill)


    def parse_vote(self, bill, vote_date, vote_chamber, vote_status, vote_url):
        """
        Parse vote
        """
        vote_chamber = 'upper' if vote_chamber == 'Senate' else 'lower'
        vote_date = parse(vote_date)
        vote_doc, resp = urllib.urlretrieve(vote_url)
        try:
            #Vote in Doc File
            odf_vote = ezodf.opendoc(vote_doc)
        except:
            #Method to handle votes in web page
            try:
                doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
                yeas = 0
                nays = 0
                presents = 0
                absents = 0
                vote = None
                for match in ('Yea', 'Nay', 'Present but Not Voting', 'Absent and Not Voting'):
                    vote_head = doc.xpath_single('//h3[contains(text(), "%s")]' % match)
                    text = vote_head.text_content().strip()
                    #Find the voter numbers
                    Num = int(text[text.index('(')+1: text.index(')')])
                    if match == 'Yea':
                        yeas = Num
                    elif match == 'Nay':
                        nays = Num
                    elif match == 'Present but Not Voting':
                        presents = Num
                    elif match == 'Absent and Not Voting':
                        absents = Num
                vote = Vote(vote_chamber, vote_date, vote_status.strip(),
                            yeas>nays, yeas, nays, presents+absents)

                #Store the voters in list of yea, nay or other
                for match in ('Yea', 'Nay', 'Present but Not Voting', 'Absent and Not Voting'):
                    vote_rows = doc.xpath('//h3[contains(text(), "%s")]/following-sibling::*' % match)
                    for  vote_lines in vote_rows:
                        if vote_lines.tag!='a':
                            break
                        member = vote_lines.text_content().strip()
                        if match == 'Yea':
                            vote.yes(member)
                        elif match == 'Nay':
                            vote.no(member)
                        else:
                            vote.other(member)
                vote.add_source(vote_url)
                bill.add_vote(vote)
                return
            except (IndexError, AttributeError):
                logger.warning("couldn't handle file from: " + vote_url)
                return
        #Method to handle the vote in doc fule
        vote_tree = odf_vote.body.xmlnode
        vote_lines = []
        for match in ('Yea', 'Nay', 'Present', 'Absent', 'On roll', 'The bill', 'the motion'):
            try:
                word = ''
                for line in vote_tree.xpath('//*[contains(text(), "%s")]/text()' % match):
                    if(line.strip() == 'Yeas:' or line.strip() == 'Nays:' or
                          line.strip() == 'Absent or Not Voting:' or
                          'Present but not voting:' == line.strip() or
                          line.strip() == 'Present and Passing:' or
                          line.strip() == 'Absent or not voting:'):

                        word = line.strip()

                    else:
                        if (word + " " + line).strip() not in vote_lines:
                            vote_lines.append((word + " " + line).strip())
            except:
                pass
        os.remove(vote_doc)

        comma_or_and = re.compile(', |\sand\s')
        comma_or_and_jrsr = re.compile(', (?!Sr.|Jr.)|\sand\s')

        vote = None
        passed = True
        for line in vote_lines:
            totals = re.findall('Yeas (\d+)[;,] Nays (\d+)[;,] (?:Present but not voting|Present and Passing):? (\d+)[;,] '\
                                '(?:Absent or not voting|Absent or Not Voting):? (\d+)', line)
            line = line.strip()
            if totals:
                totals = totals[0]
                yeas = int(totals[0])
                nays = int(totals[1])
                nv = int(totals[2])
                absent = int(totals[3])
                # default passed to true
                vote = Vote(vote_chamber, vote_date, vote_status.strip(),
                            True, yeas, nays, nv+absent)
                break

        vote_iter = iter(vote_lines)
        absent = True
        yes = True
        no = True
        pres = True
        for line in vote_iter:
            if vote and line.startswith('Yeas:') and yes:
                line = line.split(':', 1)[1].strip()
                yes = False
                if(line and yeas != 0):
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.yes(member)
                elif(yeas != 0):
                    line = vote_iter.next()
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.yes(member)
            elif vote and line.startswith('Nays:') and no:
                no = False
                line = line.split(':', 1)[1].strip()
                # slightly different vote format if Jr stands alone on a line
                if line and nays != 0:
                    if ', Jr.,' in line:
                        regex = comma_or_and_jrsr
                    else:
                        regex = comma_or_and
                    for member in regex.split(line):
                        if member != 'None.' and member != '':
                            vote.no(member)
                elif nays != 0:
                    line = vote_iter.next()
                    if ', Jr.,' in line:
                        regex = comma_or_and_jrsr
                    else:
                        regex = comma_or_and
                    for member in regex.split(line):
                        if member != 'None.' and member != '':
                            vote.no(member)
            elif vote and line.startswith('Present ') and pres:
                pres = False
                line = line.split(':', 1)[1].strip()
                if(line and nv != 0):
                    pres = False
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.other(member)
                elif(nv != 0):
                    pres = False
                    line = vote_iter.next()
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.other(member)
            elif vote and line.startswith('Absent or') and absent:
                line = line.split(':', 1)[1].strip()
                if line and absent != 0:
                    absent = False
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.other(member)
                elif absent != 0:
                    absent = False
                    line = vote_iter.next()
                    for member in comma_or_and.split(line):
                        if member != 'None.' and member != '':
                            vote.other(member)
            elif 'the motion did not prevail' in line:
                passed = False
                break
            elif 'bill passed' in line:
                passed = True
                break
        if vote:
            vote['passed'] = passed
            vote.add_source(vote_url)
            bill.add_vote(vote)



def get_session_details(session):
    year = int(session[:4])
    if session == '20112012r':
        slug = '_2012'
    elif session == '20132014ss1':
        slug = '_2013s'
    else:
        slug = ''
    return slug


def get_slug(session):
    if int(session[:4]) <= 2009:
        sign = '-'
    else:
        sign = '_'
    slug = "b%s%s%s" % (session[:4], sign, session[6:8])
    return slug

def get_doc_link(elem):
    # try ODT then PDF
    link = elem.xpath_single('.//a[contains(@href, ".odt")]', BRP.bill_versions)
    if link:
        return link.get_attrib('href')
    link = elem.xpath_single('.//a[contains(@href, ".pdf")]', BRP.bill_versions)
    if link:
        return link.get_attrib('href')

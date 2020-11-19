from __future__ import absolute_import

import re
import datetime
import logging

from fn_scraperutils.doc_service.util import ScraperDocument

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('WABillScraper')

LEGISLATION_URL = "http://wslwebservices.leg.wa.gov/legislationservice.asmx/"


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-WA", group="fnleg")
class WABillScraper(BillScraper):
    def __init__(self):
        super(WABillScraper, self).__init__("wa")

    def scrape_bill_ids(self, session):
        bill_ids = []
        year = int(session[0:4])
            # first go through API response and get bill list
        for a_year in (year, year + 1):
            url = LEGISLATION_URL + "GetLegislationByYear?year=%s" % a_year
            page = self.scraper.url_to_lxml(url, BRP.bill_list)
            leg_info = page.xpath('./legislationinfo', BRP.test)
            if not leg_info:
                # If there's no API response, we're in prefile territory.
                url = 'http://apps.leg.wa.gov/billinfo/prefiled.aspx?year={}'.format(a_year)
                prefile_page = self.scraper.url_to_lxml(url, BRP.bill_list)
                prefile_ids = prefile_page.xpath('//tr/td[1]/a/text()', BRP.test)
                if prefile_ids:
                    for prefile_id in prefile_ids:
                        bill_id_norm = re.match(r'(?:S|H)(?:B|CR|JM|JR|R|I) \d+', prefile_id)
                        if not bill_id_norm:
                            logger.warning("bill_id should be in format of chamber doc_type + doc_num: %s" % prefile_id)
                            continue
                        bill_ids.append(bill_id_norm.group(0))
                    break
                else:
                    logger.info(u"No prefiles for year %s", a_year)

            for leg_info in leg_info:
                bill_id = leg_info.xpath_single("billid").text_content()


                # normalize bill_id
                bill_id_norm = re.findall(r'(?:S|H)(?:B|CR|JM|JR|R|I) \d+', bill_id)
                if not bill_id_norm:
                    logger.warning("bill_id should be in format of chamber doc_type + doc_num: %s" % bill_id)
                    continue
                bill_ids += bill_id_norm
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        cham_name = "Senate" if bill_id[0] == "S" else "House"

        num_part = re.search("\d", bill_id)
        if not num_part:
            logger.warning("bill_id should be in format of chamber doc_type + doc_num: %s" % bill_id)
            return

        biennium = "%s-%s" % (session[0:4], session[6:8])
        bill_num = bill_id.split()[1]
        year = session[0:4]

        bill_url = "http://app.leg.wa.gov/billsummary?BillNumber=%s&Year=%s&Chamber=%s" % (bill_num, year, cham_name)
        user_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        url = LEGISLATION_URL + 'GetLegislation?biennium=%s&billNumber=%s&Chamber=%s' % (biennium, bill_num, cham_name)
        page = self.scraper.url_to_lxml(url, BRP.bill)
        page = page.xpath("./legislation")[0]

        title = page.xpath_single("longdescription").text_content()

        bill_type = page.xpath_single(
            "shortlegislationtype/longlegislationtype").text_content()
        bill_type = bill_type.lower()
        bill_type = bill_type.replace(u" ", "_")
        if bill_type == u"initiative":
            bill_type = u"bill"

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)

        chamber_name = {'lower': 'House', 'upper': 'Senate'}[chamber]


        #goes to a seperate page that has all legislative documents
        doc_xml_url = 'http://wslwebservices.leg.wa.gov/LegislativeDocumentService.asmx/GetDocuments?biennium=%s&namedLike=%s' % (biennium, bill_num)
        doc_page = self.scraper.url_to_lxml(doc_xml_url, BRP.bill_documents)
        if doc_page:
            doc_list = doc_page.xpath("./legislativedocument")
        else:
            doc_list = []
        for doc_row in doc_list:
            doc_bill_id = doc_row.xpath_single("billid").text
            if not doc_bill_id or not re.search(ur"[^\d]{}(?:[^\d]|$)".format(bill_num), doc_bill_id):
                logger.warning(u"Document is for %s, not this bill (%s)", doc_bill_id, bill_id)
                continue
            doc_type = doc_row.xpath_single("class").text_content()
            doc_title = doc_row.xpath_single("longfriendlyname").text_content()
            doc_url = doc_row.xpath_single("htmurl").text_content()
            doc_url = re.sub(" ", "%20", doc_url)
            if not doc_url:
                doc_url = doc_row.xpath_single("pdfurl")
                mimetype = 'application/pdf'
            else:
                mimetype = 'text/html'

            # Veto messages appear in the Bill Documents section, even though
            # they contain a message concerning why the bill was vetoed, and
            # not any bill text.
            if (doc_type == 'Bills' or doc_type == 'Amendments') and u'Veto' not in doc_title:
                document_type = "version" if doc_type == "Bills" else "amendment"
                if mimetype == 'text/html':
                    doc_url = doc_url.encode('utf-8')
                    download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(doc_url,
                                                                         BRP.bill_documents,
                                                                         self.scraper.extraction_type.html,
                                                                         False,
                                                                         self.html_parser)
                else:
                    download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(doc_url,
                                                                         BRP.bill_documents,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         True)
                assert len(doc_ids) == 1
                doc_service_document = Doc_service_document(doc_title, document_type,
                                                            "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

            else:
                document_type = self.get_doc_type(doc_title)
                download_id = self.scraper.download_and_register(doc_url, BRP.bill_documents,
                                                                 True)
                doc_service_document = Doc_service_document(doc_title, document_type, "partial", download_id)
                bill.add_doc_service_document(doc_service_document)

        fiscal_note_link = None
        if user_page:
            fiscal_notes = user_page.xpath("//div[@class='row clearfix no-print']/div[@class='']/a",
                                           BRP.bill_partial_documents)
            for note in fiscal_notes:
                if re.match(r'Get Fiscal Note', note.text_content()):
                    fiscal_note_link = note.get_attrib("href")
            if fiscal_note_link:
                self.scrape_fiscal_notes(fiscal_note_link, bill, bill_num)

            self.scrape_actions(bill, user_page)
        else:
            logger.warning("Bad Link %s" % bill_url)

        self.scrape_sponsors(bill)

        self.scrape_votes(bill)
        #self.scrape_videos(bill, user_page)
        self.scrape_subjects(bill, year)
        self.save_bill(bill)

    def scrape_sponsors(self, bill):
        bill_num = bill['id'].split()[1]
        bill_id = bill['id'].replace(' ', '%20')
        session = bill['session']
        biennium = "%s-%s" % (session[0:4], session[6:8])
        chamber = "Senate" if bill_id[0] == "S" else "House"

        url = LEGISLATION_URL + 'GetSponsors?biennium=%s&billId=%s&Chamber=%s' % (biennium, bill_num, chamber)
        page = self.scraper.url_to_lxml(url, BRP.bill_sponsors)

        for sponsor in page.xpath("sponsor"):
            sponsor_name = sponsor.xpath_single("name")
            sponsor_agency = sponsor.xpath_single("agency")
            sponsor_type = sponsor.xpath_single("type")
            if not sponsor_name:
                continue
            name = sponsor_name.text
            chamber = None

            # There should always be a sponsor type on the page, but if not it will default to primary
            if sponsor_type and sponsor_type.text == "Secondary":
                sponsor_type = "cosponsor"
            else:
                sponsor_type = "primary"

            if sponsor_agency:
                sponsor_agency = sponsor_agency.text
                if sponsor_agency == "Senate":
                    chamber = "upper"
                elif sponsor_agency == "House":
                    chamber = "lower"

            if chamber:
                bill.add_sponsor(sponsor_type, name, chamber=chamber)
            else:
                bill.add_sponsor(sponsor_type, name)

    def scrape_actions(self, bill, page):
        session = bill['session']
        year = session[0:4]

        og_chamber = bill['chamber']
        curchamber = og_chamber

        action_row = page.xpath("//div[@class='historytable']/div", BRP.bill_actions)
        curyear = year
        curdate = None

        for action_text in action_row:
            #this is ugly and convoluted, but it wouldn't work if I started with the p tag and moved down
            nearest_p = action_text.xpath("preceding::p")[-1]
            header = nearest_p.text_content()
            text = action_text.text_content()
            #we have two versions of localization because their site uses two different kinds
            if "SENATE" in header:
                curchamber = 'upper'
            if "HOUSE" in header:
                curchamber = 'lower'
            if "SESSION" in header:
                curchamber = og_chamber
            if "OTHER THAN LEGISLATIVE ACTION" in header:
                curchamber = 'executive'
            if re.match(r'\d{4}', header):
                curyear = header[:4]
            if re.match(r'[A-z]{3} \d{1,2}', text):
                curdate = text[:7].strip()
                text = text[8:]
                if curdate[:3] == "Dec":
                    curyear = str(int(curyear) - 1)
            junk = ['(View Original Bill)',
                        '(Committee Materials)',
                        '(View Session Law)',
                        '(View Bill as Passed Legislature)',
                        '(View Roll Calls)',
                        '(Majority Report)',
                        '(View 1st Substitute)'
                        ]

            for string in junk:
                text = text.replace(string, '').strip()

            text = text.replace('\n', ' ').replace('\r', '').strip()
            date = "%s %s" % (curyear, curdate)
            date = datetime.datetime.strptime(date, "%Y %b %d")
            bill.add_action(curchamber, text, date)



    def scrape_votes(self, bill):
        session = bill['session']
        biennium = "%s-%s" % (session[0:4], session[6:8])
        bill_num = bill['id'].split()[1]
        biil_chamber = u"Senate" if bill['id'][0] == u"S" else u"House"


        url = LEGISLATION_URL + "GetRollCalls?billNumber=%s&biennium=%s&Chamber=%s" % (bill_num, biennium, biil_chamber)
        page = self.scraper.url_to_lxml(url, BRP.bill_votes)


        for rc in page.xpath("rollcall"):
            motion = rc.xpath_single("motion").text
            if motion == '':
                motion = "Third reading"

            date = rc.xpath_single("votedate").text_content().split("T")[0]
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()


            yes_count = int(rc.xpath_single("yeavotes/count").text_content())
            no_count = int(rc.xpath_single("nayvotes/count").text_content())
            abs_count = int(
                rc.xpath_single("absentvotes/count").text_content())
            ex_count = int(
                rc.xpath_single("excusedvotes/count").text_content())

            other_count = abs_count + ex_count

            agency = rc.xpath_single("agency").text_content()
            chamber = {'House': 'lower', 'Senate': 'upper'}[agency]

            vote = Vote(chamber, date, motion,
                        yes_count > (no_count + other_count),
                        yes_count, no_count, other_count)

            for sv in rc.xpath("votes/vote"):
                name = sv.xpath_single("name/text()")
                vtype = sv.xpath_single("vote/text()")

                if vtype == 'Yea':
                    vote.yes(name)
                elif vtype == 'Nay':
                    vote.no(name)
                else:
                    vote.other(name)
            bill.add_vote(vote)


    def scrape_subjects(self, bill, year):
        url = 'http://apps.leg.wa.gov/billsbytopic/Results.aspx?year=%s' % year
        doc = self.scraper.url_to_lxml(url, BRP.bill_documents)
        bill_num = bill['id'].split()[1]
        subjects = []

        bill_location = doc.xpath("//div[@id='divContent']//tr/td/a[@href='http://apps.leg.wa.gov/billinfo/summary.aspx?year={}&bill={}']".format(year, bill_num))
        for things in bill_location:
            #parent = doc.xpath_sis[@id='divContent']//tr/td/a[@href='http://apps.leg.wa.gov/billinfo/summary.aspx?year={}&bill={}']/../../../..//preceding-sibling::b[1]".format(year, bill_num))
            parent = things.xpath("preceding::b[(not(contains(., '*'))and(not(contains(., 'See'))))]")

            size = len(parent.elements)
            item = parent[size-1]
            while item.text == None:
                size = size-1
                item = parent[size]
            substr  = "".join(item.text)
            if substr not in subjects:
                subjects.append(substr)

        for subject in subjects:
            bill.add_subject(subject)

    def scrape_videos(self, bill, user_page):
        vid_container = user_page.xpath_single("//div[@id='videoContainer']", BRP.bill_documents)
        if vid_container:
            link_to_data = vid_container.get_attrib("data-url")
            link_to_data = "http://app.leg.wa.gov" + link_to_data.replace("amp;", "").strip()
            data_page = self.scraper.url_to_lxml(link_to_data, BRP.bill_documents)
            rows = data_page.xpath("//div[@class='row']/div/a")
            links = []
            for row in rows:
                links.append( [row.text_content(), row.get_attrib('href')] )
            if len(links) > 0:
                for link in links:
                    doc_title = link[0]
                    doc_url = link[1]
                    document_type = "other"
                    doc_service_type = "partial"
                    download_id = self.scraper.download_and_register(doc_url, BRP.bill_documents,
                                                                     True)
                    doc_service_document = Doc_service_document(doc_title, document_type, doc_service_type, download_id)
                    bill.add_doc_service_document(doc_service_document)



    def scrape_fiscal_notes(self, fiscal_note_link, bill, bill_num):
        bill_search_url = "https://fortress.wa.gov/FNSPublicSearch/DoSearch"
        year_num = str(fiscal_note_link[-2:])
        post_data = {
        "SessionYear" : year_num,
        "BillNumber" : bill_num,
        "BillTitle" : ""
        }
        # This magic cookie header value is required to make the request succeed.
        # It appears to be a static value, so, there doesn't appear to be a point
        # in trying to figure out which page sets it and visiting that page.
        headers = {
            "Cookie": "IV_JCT=%2Fofm%2ffnspublic",
        }

        response_dict = self.scraper.url_to_json(
            bill_search_url,
            BRP.bill_votes,
            method="POST",
            request_args={
                "data": post_data,
                "headers": headers,
            }
        )

        packageIds = []
        for item in response_dict['data']:
            #format = FiscalNote - BillTitle - Type
            doc_id = "FiscalNote - " + item['BillTitle'] + " - " + item['BillType']
            packageIds.append([ doc_id, str(item['packageId'])])

        for package_id in packageIds:
            doc_url = "https://fortress.wa.gov/FNSPublicSearch/GetPDF?packageID=" + package_id[1]
            doc_name = package_id[0]
            # Can't do a head request, so always download the file
            download_id = self.scraper.download_and_register(doc_url, BRP.bill_documents, True,
                                                             download_args={ "headers" : headers},
                                                             should_download=True,
                                                             content_type="application/pdf")
            if not download_id:
                logger.warning(u"Could not register document at %s", doc_url)
                continue
            doc_service_document = Doc_service_document(doc_name, "fiscal_note", "partial", download_id)
            bill.add_doc_service_document(doc_service_document)



    @staticmethod
    def get_doc_type(document_name):
        if 'Analysis' in document_name:
            document_type = 'summary'
        elif 'Report' in document_name or 'Veto' in document_name:
            document_type = 'summary'
        elif 'Digest' in document_name:
            document_type = 'other'
        elif 'Fiscal' in document_name:
            document_type = 'fiscal_note'
        else:
            document_type = "other"
        return document_type

    @staticmethod
    def html_parser(element_wrapper):
        return [ScraperDocument(element_wrapper.xpath_single("//body").text_content())]

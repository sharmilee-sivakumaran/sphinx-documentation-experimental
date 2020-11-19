import re
import datetime as dt
from dateutil.parser import parse

from fn_scrapers.datatypes.bills.common.bill_scraper import BillScraper
from fn_scrapers.datatypes.bills.common.bill import Bill
from fn_scrapers.datatypes.bills.common.vote import Vote
from fn_scrapers.datatypes.bills.common.bill_reporting_policy import BillReportingPolicy as BRP
from fn_scrapers.datatypes.bills.common.doc_service_document import Doc_service_document
import logging
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scrapers.datatypes.bills.common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_scrapers.api.scraper import scraper, tags

logger = logging.getLogger('MOBillScraper')


def senate_url(page, year, session_type):
    """
    Generate senate url
    """
    if re.match("^https?://.*", page):
        return page
    else:
        year2 = "%02d" % (int(year) % 100)
        bill_root = 'http://www.senate.mo.gov/%sinfo/BTS_Web/' % year2
        return bill_root + page + session_type


def senate_get_actor_from_action(text):
    """
    For senate bills, get actor from action string
    """
    if re.search("Prefiled", text):
        return 'upper'

    m = re.search(r"(\bH\b|\bS\b|House)", text)
    if not m:
        if text.endswith('Governor'):
            return 'executive'
        else:
            return 'upper'

    if m.group(1) == 'S':
        return 'upper'
    else:
        return 'lower'


def clean_text(text):
    """
    Clean unnecessary characters
    """
    newtext = re.sub(r"[\r\n]+", " ", text)
    newtext = re.sub(r"\s{2,}", " ", newtext)
    m = re.match(r"(.*)\(.*?\)", newtext)
    if not m:
        return newtext.strip()
    else:
        return m.group(1).strip()


def house_get_actor_from_action(text):
    """
    get actor from action string
    """
    m = re.search(r"\((\bH\b|\bS\b)\)", text)
    if not m:
        if text.endswith('Governor'):
            return 'executive'
        else:
            return 'other'

    abbrev = m.group(1)
    if abbrev == 'S':
        return 'upper'
    return 'lower'


def html_parser(element_wrapper):
    text = element_wrapper.xpath_single("//body").text_content()
    return [ScraperDocument(text)]

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-MO", group="fnleg")
class MOBillScraper(BillScraper):
    """
    Scrape MO Bill Scraper
    """
    house_base_url = 'http://www.house.mo.gov'

    def __init__(self):
        super(MOBillScraper, self).__init__("mo")

    def scrape_bill_ids(self, session):
        """
        Get list of bill ids
        """
        bill_ids = {}
        year = session[:4]
        # We only have data from 2005-present
        if int(year) < 2005 or int(year) > dt.date.today().year + 1:
            logger.warning("Invalid Session %s" % session)
            return
        if 'ss' in session:
            session_num = re.findall(r'ss(\d+)', session)[0]
            session_type = 'S%s' % session_num
        else:
            session_type = 'R'
        upper_url = senate_url('BillList.aspx?SessionType=', year, session_type)

        upper_doc = self.scraper.url_to_lxml(upper_url, BRP.bill_list)

        bill_list = upper_doc.xpath("//a[contains(@href, 'BillID=')]", BRP.debug)
        if not bill_list:
            logger.critical("Unable to get bill IDs from {}".format(upper_url))
        for bill in bill_list:
            bill_id = bill.text_content()
            bill_url = bill.get_attrib('href')
            bill_ids[bill_id] = bill_url
        lower_url = 'http://www.house.mo.gov/Bill.aspx?year=%s&code=%s' % (year, session_type)
        lower_doc = self.scraper.url_to_lxml(lower_url, BRP.bill_list)
        bill_list = lower_doc.xpath("//div[@id='billlisttable']/a[@target='billcon']", BRP.debug)
        if not bill_list:
            logger.critical("Unable to get bill IDs from {}".format(lower_url))
        for bill in bill_list:
            bill_id = bill.text_content()
            bill_url = bill.get_attrib('href')
            bill_ids[bill_id] = bill_url
        return bill_ids

    def _get_post_vars(self, url):
        '''
        get aspx postback variables for a given head url
        '''
        doc = self.scraper.url_to_lxml(url, BRP.bill)
        data = dict(
            __EVENTARGUMENT=None,
            __EVENTTARGET=None
        )
        data.update({obj.element.name: obj.element.value for obj in doc.xpath(".//input") if obj.element.name})
        return data

    def scrape_bill(self, session, bill_id, **kwargs):
        # Validate Bill
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_url = kwargs['bill_info']
        if chamber == 'upper':
            # Get bill_url by making a post request to search page
            self.parse_senate_billpage(bill_url, session)
        elif chamber == 'lower':
            bill = self.parse_house_bill(bill_id, session, bill_url)

    def parse_senate_billpage(self, bill_url, session):

        year = int(session[:4])
        if 'ss' in session:
            session_num = re.findall(r'ss(\d+)', session)[0]
            session_type = 'S%s' % session_num
        else:
            session_type = 'R'

        try:
            bill_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        except:
            logger.warning("Bad Link bill url %s" % bill_url)
            return

        bill_id = bill_page.xpath_single('//*[@id="lblBillNum"]').text_content()

        # Handle case where some bills don't have titles
        bill_title = bill_page.xpath_single('//*[@id="lblBriefDesc"]').text_content()
        if not bill_title:
            bill_desc = bill_page.xpath_single('//*[@id="lblBillTitle"]').text_content()
            if not bill_desc:
                bill_title = 'No Title'
            else:
                bill_title = bill_desc
        else:
            bill_title = bill_title

        # bill type
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        bill = Bill(session, 'upper', bill_id, bill_title, bill_type)
        bill.add_source(bill_url)

        # LR Number
        bill_lr = bill_page.xpath_single('//*[@id="lblLRNum"]').text_content()
        if bill_lr:
            bill.add_alternate_id(bill_lr)

        # Get the primary sponsor
        sponsor = bill_page.xpath_single('//*[@id="hlSponsor"]')
        if sponsor:
            bill_sponsor = sponsor.text_content()
            bill.add_sponsor('primary', bill_sponsor)
        # cosponsors show up on their own page, if they exist
        cosponsor_tag = bill_page.xpath_single('//*[@id="hlCoSponsors"]')
        if cosponsor_tag and cosponsor_tag.element.attrib.has_key('href'):
            self.parse_senate_cosponsors(bill, cosponsor_tag.get_attrib('href'), year, session_type)
        # get the actions
        action_url = bill_page.xpath_single('//*[@id="hlAllActions"]')
        if action_url:
            action_url = action_url.get_attrib('href')
            self.parse_senate_actions(bill, action_url, year, session_type)
        # stored on a separate page
        versions_url = bill_page.xpath_single('//*[@id="hlFullBillText"]')
        if versions_url and versions_url.element.attrib.has_key('href'):
            self.parse_senate_bill_doc(bill, versions_url.get_attrib('href'), year, 'version', session_type)

        amend_url = bill_page.xpath_single('//*[@id="hlAmends"]')
        if amend_url and amend_url.element.attrib.has_key('href'):
            self.parse_senate_bill_amend(bill, amend_url.get_attrib('href'))

        summary_url = bill_page.xpath_single('//*[@id="hlSummaries"]')
        if summary_url and summary_url.element.attrib.has_key('href'):
            self.parse_senate_bill_doc(bill, summary_url.get_attrib('href'), year, 'summary', session_type)
        self.save_bill(bill)

    def parse_senate_bill_amend(self, bill, link):
        amend_page = self.scraper.url_to_lxml(link, BRP.bill_documents)

        hamend_url = amend_page.xpath_single("//a[@id='hlHAmend']")
        if hamend_url:
            hamend_url = hamend_url.get_attrib('href')
            # self.parse_senate_house_amend(bill, hamend_url)

        amend_list = amend_page.xpath("//table[@id='dgList']/tr", BRP.test)
        if len(amend_list) == 0:
            return
        viewstate = amend_page.xpath_single("//input[@id='__VIEWSTATE']").get_attrib('value')
        viewtategenerator = amend_page.xpath_single("//input[@id='__VIEWSTATEGENERATOR']").get_attrib('value')
        eventvalidation = amend_page.xpath_single("//input[@id='__EVENTVALIDATION']").get_attrib('value')
        post_headers = {u'Pragma': u'no-cache',
                        u'Origin': u'http://www.senate.mo.gov',
                        u'Accept-Encoding': u'gzip, deflate',
                        u'Accept-Language': u'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
                        u'Upgrade-Insecure-Requests': '1',
                        u'User-Agent': u'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36',
                        u'Content-Type': u'application/x-www-form-urlencoded',
                        u'Accept': u'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        u'Cache-Control': u'no-cache',
                        u'Connection': u'keep-alive',
                        u'Referer': u''
                        }

        data = {}
        post_headers[u'Referer'] = link
        data['__VIEWSTATE'] = viewstate

        data['__VIEWSTATEGENERATOR'] = viewtategenerator
        data['__EVENTVALIDATION'] = eventvalidation

        link = re.sub('BTS_Amendments/', 'BTS_Amendments', link)

        for amend_row in amend_list[1:]:
            amend_id_ele = amend_row.xpath_single(".//input[@type='submit']")
            if amend_id_ele:
                amend_id = amend_id_ele.get_attrib('name')
                ele_id = amend_id_ele.get_attrib('id')
                post_data = dict(data)
                post_data[amend_id] = 'View'
                download_id, doc, doc_ids = self.scraper.register_download_and_documents(link, BRP.bill_documents,
                                                                                         self.scraper.extraction_type.unknown,
                                                                                         True, download_args={
                        u"headers": post_headers, "data": post_data},
                                                                                         content_type='application/pdf',
                                                                                         download_method="POST",
                                                                                         should_skip_checks=True
                                                                                         )
                if not doc_ids or doc_ids[0] is None:
                    logger.warning("Bad Doucment Like %s with id %s" % (link, amend_id))
                    continue
                name_id = re.sub('btnView', 'lblListAction', ele_id)
                amend_str = ".//span[@id='%s']" % name_id
                amend_name = amend_page.xpath_single(amend_str).text_content()
                doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

    def parse_senate_house_amend(self, bill, link):
        amend_page = self.scraper.url_to_lxml(link, BRP.bill_documents)
        amend_list = amend_page.xpath("//td[@class='amendmentcolumn']")
        for amend_row in amend_list:
            amend_url = amend_row.xpath_single('./a').get_attrib('href')
            amend_name = amend_row.text_content()
            amend_name = "Amendment #%s" % amend_name
            try:
                download_id, _, doc_ids = self.scraper.register_download_and_documents(amend_url, BRP.bill_documents,
                                                                                       self.scraper.extraction_type.text_pdf,
                                                                                       True,
                                                                                       content_type='application/pdf'
                                                                                       )
                doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
            except:
                logger.warning("Bad Amendment link %s" % amend_url)
                continue

    def parse_senate_bill_doc(self, bill, page, year, doc_type, session_type):
        url = senate_url(page, year, session_type)

        if doc_type == 'version':
            policy = BRP.bill_documents
        elif doc_type == 'summary':
            policy = BRP.bill_documents
        documents_page = self.scraper.url_to_lxml(url, policy)
        document_tags = documents_page.xpath('//li/font/a', BRP.test)
        if len(document_tags) == 0:
            document_tags = documents_page.xpath('//table[@id="table1"]//a', BRP.test)
            if len(document_tags) == 0:
                document_tags = documents_page.xpath('//table[@id="Table1"]//a')

        for document_tag in document_tags:
            description = document_tag.text_content()
            if 'Return to Main Bill Page' in description:
                continue
            document_url = document_tag.get_attrib('href')
            if document_url.lower().endswith('pdf'):
                download_id, _, doc_ids = self.scraper.register_download_and_documents(document_url, policy,
                                                                                       self.scraper.extraction_type.text_pdf,
                                                                                       True,
                                                                                       content_type='application/pdf')

                doc_service_document = Doc_service_document(description, doc_type, "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
            else:
                download_id, _, doc_ids = self.scraper.register_download_and_documents(document_url, policy,
                                                                                       self.scraper.extraction_type.html,
                                                                                       False, html_parser)

                doc_service_document = Doc_service_document(description, doc_type, "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

    def parse_pdf_vote(self, vote_url, bill):
        _, vote_page, _ = self.scraper.register_download_and_documents(vote_url, BRP.bill_votes,
                                                                       self.scraper.extraction_type.text_pdf,
                                                                       False, content_type='application/pdf')
        pdf_text = vote_page[0].text

        date = re.findall(r'Date\s+:\s+(\d+/\d+/\d+)', pdf_text, re.M)[0]
        formed_date = parse(date)
        voters_list = re.findall(r'([YN@])\s+-\s+(.*?)\s+', pdf_text, re.M)
        yes_voters = []
        no_voters = []
        other_voters = []
        for vote, name in voters_list:
            if vote == 'Y':
                yes_voters.append(name)
            elif vote == 'N':
                no_voters.append(name)
            else:
                other_voters.append(name)

        yes_count = len(yes_voters)
        no_count = len(no_voters)
        other_count = len(other_voters)

        try:
            motion = re.findall(r'\s+FOR\s+(.*?)\s{2,}', pdf_text)[0]
        except IndexError:
            roll_call = re.findall(r'rollcalls/(.*?)\.pdf', vote_url)[0]
            motion = "Roll Call: %s" % roll_call
        if 'House of Representatives' in pdf_text:
            vote_chamber = 'lower'
        else:
            vote_chamber = 'upper'
        vote = Vote(vote_chamber, formed_date, motion, yes_count > no_count, yes_count, no_count, other_count)
        if len(yes_voters) > 0:
            vote['yes_votes'] = yes_voters
        if len(no_voters) > 0:
            vote['no_votes'] = no_voters
        if len(other_voters) > 0:
            vote['other_votes'] = other_voters
        bill.add_vote(vote)
        return formed_date

    def parse_senate_actions(self, bill, page, year, session_type):
        url = senate_url(page, year, session_type)

        actions_page = self.scraper.url_to_lxml(url, BRP.bill_actions)
        bigtable = actions_page.xpath('/html/body/font/form/table/tr[3]/td/div/table/tr')

        for row in bigtable:
            col = row.xpath("./td")
            action = col[1].text_content()
            if action.strip() == '':
                continue
            date = col[0].text_content()
            date = dt.datetime.strptime(date, '%m/%d/%Y')
            actor = senate_get_actor_from_action(action)
            action_dict = dict(actor=actor, action=action, date=date)
            bill.add_action(**action_dict)

    def parse_senate_cosponsors(self, bill, page, year, session_type):
        """
        Function to parse senate cosponsors
        """
        url = senate_url(page, year, session_type)

        # cosponsors are all in a table
        cosponsors_page = self.scraper.url_to_lxml(url, BRP.bill_sponsors)
        cosponsors = cosponsors_page.xpath('//table[@id="dgCoSponsors"]/tr/td/a')

        for cosponsor_row in cosponsors:
            # cosponsors include district, so parse that out
            cosponsor_string = cosponsor_row.text_content()
            cosponsor = clean_text(cosponsor_string)
            cosponsor = cosponsor.split(',')[0]
            if cosponsor:
                bill.add_sponsor('cosponsor', cosponsor.strip())

    def parse_house_bill(self, bill_id, session, bill_content_url):
        year = session[:4]
        bid = bill_id.replace(" ", "")
        if 'ss' in session:
            session_num = re.findall(r'ss(\d+)', session)[0]
            session_type = 'S%s' % session_num
        else:
            session_type = 'R'
        bill_url = "http://www.house.mo.gov/Bill.aspx?bill=%s&year=%s&code=%s" % (bid, year, session_type)
        bill_page = self.scraper.url_to_lxml(bill_content_url, BRP.bill)
        if not bill_page:
            logger.warning("Failed to scrape Bill Page %s" % bid)
            return
        bill_desc = bill_page.xpath_single('//*[@class="BillDescription"]')
        if bill_desc:
            bill_desc = bill_desc.text_content()
            bill_desc = clean_text(bill_desc)

        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        if not bill_desc:
            bill_desc = 'No Title'

        bill = Bill(session, 'lower', bill_id, bill_desc, bill_type)
        bill.add_source(bill_url)

        sponsor = bill_page.xpath_single("//th[text()='Sponsor:']")
        if sponsor:
            sponsor = sponsor.getnext().text_content()
            sponsor = re.sub(r'\(.*\)', '', sponsor).strip()
            bill.add_sponsor('primary', sponsor)

        lr_number = bill_page.xpath_single("//th[text()='LR Number:']")
        if lr_number:
            lr_number = lr_number.getnext().text_content()
            if lr_number:
                bill.add_alternate_id(lr_number)

        # Fiscal Note
        try:
            fiscal_ele = bill_page.xpath_single('//a[contains(text(), "Fiscal Notes")]',
                                                policy=BRP.test)
            if fiscal_ele:
                fiscal_url = fiscal_ele.get_attrib('href')
                fiscal_page = self.scraper.url_to_lxml(fiscal_url, BRP.bill_documents)
                for fiscal_doc in fiscal_page.xpath("//a[contains(@href, 'fispdf')]", BRP.test):
                    fiscal_name = fiscal_doc.text_content()
                    fiscal_name = 'Fiscal Notes - ' + fiscal_name
                    fiscal_url = fiscal_doc.get_attrib('href')
                    doc_download_id = self.scraper.download_and_register(fiscal_url, BRP.bill_documents, False)
                    doc_service_document = Doc_service_document(fiscal_name, 'fiscal_note', "partial", doc_download_id)
                    bill.add_doc_service_document(doc_service_document)
        except:
            logger.warning("Bad fiscal note page")

        vote_list = []
        doc_table = bill_page.xpath("//div[@id='DocRows']//a")
        for doc_row in doc_table:
            doc_url = doc_row.get_attrib('href')
            if '.pdf' not in doc_url:
                continue
            title = doc_row.text_content()
            doc_id = re.findall(r'/([^/]+?)\.pdf', doc_url)[0]
            if '/billpdf/' in doc_url or '/hlrbillspdf/' in doc_url:
                title = "Version - %s - %s" % (title, doc_id)
                download_id, _, doc_ids = self.scraper.register_download_and_documents(doc_url, BRP.bill_versions,
                                                                                       self.scraper.extraction_type.unknown,
                                                                                       True,
                                                                                       content_type='application/pdf')

                doc_service_document = Doc_service_document(title, 'version', "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
            elif '/amendpdf/' in doc_url:
                title = "Amendment - %s" % doc_id
                download_id, _, doc_ids = self.scraper.register_download_and_documents(doc_url, BRP.bill_documents,
                                                                                       self.scraper.extraction_type.unknown,
                                                                                       True,
                                                                                       content_type='application/pdf')

                doc_service_document = Doc_service_document(title, 'amendment', "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
            else:
                if '/sumpdf/' in doc_url:
                    title = "Summary - %s - %s" % (title, doc_id)
                    doc_type = 'summary'
                elif '/rollcalls/' in doc_url:
                    title = "%s - %s" % (title, doc_id)
                    doc_type = 'committee_document'
                    date = self.parse_pdf_vote(doc_url, bill)
                    vote_list.append(date)
                else:
                    title = "Other - %s - %s" % (title, doc_id)
                    doc_type = 'other'
                doc_download_id = self.scraper.download_and_register(doc_url, BRP.bill_documents, False)
                doc_service_document = Doc_service_document(title, doc_type, "partial", doc_download_id)

            bill.add_doc_service_document(doc_service_document)

        cosponsor_url = "http://www.house.mo.gov/CoSponsors.aspx?bill=%s&year=%s&code=%s" % (bid, year, session_type)
        cosponsor_page = self.scraper.url_to_lxml(cosponsor_url, BRP.bill_sponsors)
        assert cosponsor_page, "Unable to fetch cosponsors."
        if 'his bill has no Co-Sponsors.' not in cosponsor_page.text_content():
            cosponsor_table = cosponsor_page.xpath("//table[@id='CoSponsorTable']/tr", BRP.test)
            for cosponsor_row in cosponsor_table:
                cosponsor_name = cosponsor_row.xpath_single('./td').text_content()
                cosponsor = clean_text(cosponsor_name)
                bill.add_sponsor('cosponsor', cosponsor)

        action_url = "http://www.house.mo.gov/BillActions.aspx?bill=%s&year=%s&code=%s" % (bid, year, session_type)
        action_page = self.scraper.url_to_lxml(action_url, BRP.bill_actions)
        assert action_page, "Unable to fetch action page."
        action_table = action_page.xpath("//table[@id='actionTable']/tr", BRP.test)
        for action_row in action_table:
            action_col = action_row.xpath('./td')
            action_date = parse(action_col[0].text_content())
            action_str = action_col[-1].text_content().strip()
            actor = house_get_actor_from_action(action_str)
            if actor == 'other':
                jour_link = action_col[1].text_content()
                if 'S ' in jour_link:
                    actor = 'upper'
                elif 'H ' in jour_link:
                    actor = 'lower'

            action_dict = dict(actor=actor, action=action_str, date=action_date)
            bill.add_action(**action_dict)

            if 'AYES:' in action_str and action_date not in vote_list:
                self.scrape_vote_from_action(action_str, actor, action_date, bill)

        self.save_bill(bill)

    def scrape_vote_from_action(self, action, actor, date, bill):
        """
        scrape vote from action information
        """
        try:
            motion, yes_count, no_count, other_count = re.search(r"(.*?\S.*?) AYES: (\d+) NOES: (\d+) PRESENT: (\d+)",
                                                                 action, flags=re.DOTALL).groups()
        except AttributeError:
            logger.warning("Could not scrape votes from action - {}".group(action))
            return
        motion = re.sub(r'-', ' ', motion).strip()
        yes_count = int(yes_count)
        no_count = int(no_count)
        other_count = int(other_count)
        passed = yes_count > no_count
        if 'Do Pass' in motion:
            passed = True
        elif 'Defeated' in motion:
            passed = False
        if actor == 'other' or actor == 'executive':
            actor = 'joint'
        vote = Vote(actor, date, motion, passed, yes_count, no_count, other_count)
        bill.add_vote(vote)
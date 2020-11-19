"""
me.bills
:class MEBillScraper: scrapes Maine Bills
"""
from __future__ import absolute_import

import re
import socket
import htmlentitydefs

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id
from fn_scraperutils.doc_service.util import ScraperDocument

import logging
from dateutil.parser import parse

from lxml.etree import XMLSyntaxError

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('MEBillScraper')


base_bill_url = "http://www.mainelegislature.org/legis/bills/display_ps.asp?paper=%s&snum=%s"

def unescape(text):
    '''
    Removes HTML or XML character references and entities
    from a text string.
    :param text The HTML (or XML) source text.
    :return The plain text, as a Unicode string, if necessary.
    Source: http://effbot.org/zone/re-sub.htm#unescape-html
    '''
    def fixup(text_group):
        """
        fix up character that mess up
        """
        text = text_group.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:#pylint: disable=pointless-except
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:#pylint: disable=pointless-except
                pass
        return text  # leave as is
    return re.sub(r"&#?\w+;", fixup, text)

def get_session_id(session):
    year = int(session[:4])
    session_id = (year - 2001)/2 + 120
    return str(session_id)

def session_abbr_format(session):
    """
    rewrite session in number to word format
    """
    if session[-1] == "1":
        session_abbr = session + "st"
    elif session[-1] == "2":
        session_abbr = session + "nd"
    elif session[-1] == "3":
        session_abbr = session + "rd"
    else:
        session_abbr = session + "th"

    return session_abbr


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-ME", group="fnleg")
class MEBillScraper(BillScraper):
    """
    MEBillScraper
    Scrape ME Bills
    """
    # Dict mapping location to list of names representing location
    # Used to get full name of sponsors for bills
    # Currently does not match when there is someone with another's last name in the same location
    senate_legislator_table = {}
    house_legislator_table = {}
    # Dynamic table mapping (name, loc) to full name
    senate_dyn_table = {}
    house_dyn_table = {}

    def __init__(self):
        super(MEBillScraper, self).__init__("me")

    def make_sponsors_table(self):
        url = 'http://legislature.maine.gov/senate/find-your-state-senator/9392'
        try:
            senator_page = self.scraper.url_to_lxml(url, BRP.legislators)
        except XMLSyntaxError as err:
            self.error("Failed to scrape legislators list %s, %s" % (url, err))
            return
        location_info_list = senator_page.xpath('//div[@id = "content"]/p')
        for location_info in location_info_list:
            self.parse_names(location_info.text_content())

    def parse_names(self, location_info):
        match = re.match(".+-(.+)\([D|R]-(.+)\).+-(.+)\([D|R]-(.+)\)", location_info)
        try:
            senate_sponsor_name = match.group(1).strip()
            senate_loc = match.group(2).lower()
            house_sponsor_name = match.group(3).strip()
            house_loc = match.group(4).lower()
        except AttributeError:
            return

        self.senate_legislator_table[senate_loc] = self.senate_legislator_table.get(senate_loc, set())
        self.senate_legislator_table[senate_loc].add(senate_sponsor_name)

        self.house_legislator_table[house_loc] = self.house_legislator_table.get(house_loc, set())
        self.house_legislator_table[house_loc].add(house_sponsor_name)

    def scrape_bill_ids(self, session):
        self.make_sponsors_table()
        session_id = get_session_id(session)
        session_abbr = session_abbr_format(session_id)

        url = ('http://www.mainelegislature.org/legis/bills/bills_%s'
               '/billtexts/' % session_abbr)

        bill_list_page = self.scraper.url_to_lxml(url, BRP.bill_list)

        bill_ids = set()

        # This scrapes "LD 0" which for some reason isn't listed on the site
        link = url + "contents0.asp"
        bill_ids.update(self.scrape_session_directory(session, link))

        for link in bill_list_page.xpath('//a[contains(@href, "contents")]/@href', BRP.test):
            bill_ids.update(self.scrape_session_directory(session, link))
        return list(bill_ids)


    def scrape_session_directory(self, session, url):
        """
        scraper bills based on session directory
        :param session: session of bills
        :param url: url of bill page
        """
        try:
            bill_sub_list_page = self.scraper.url_to_lxml(url, BRP.bill_list)
        except XMLSyntaxError as err:
            self.error("Failed to scrape bill list %s, %s" % (url, err))
            return
        bill_ids = []

        session_id = get_session_id(session)

        for bill in bill_sub_list_page.xpath("//a[contains(@href, '?paper=')]"):
            bill_id = bill.text_content()
            bill_ids.append(bill_id)

        return bill_ids



    def scrape_bill(self, session, bill_id, **kwargs):
        bill_id_code = re.sub(r'\s+', '', bill_id)
        # Validate Bill
        bill_id_rgx = re.compile(r'^[HSI][PBOR] \d+$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.warning("Invalid Bill Id %s" % bill_id)
            return

        url = 'http://www.mainelegislature.org/LawMakerWeb/summary.asp?ID=%s'
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        session_id = get_session_id(session)

        bill_url = base_bill_url % (bill_id_code, session_id)
        # check if the URL is good otherwise raise a NoBillDataForPeriod
        try:
            old_page = self.scraper.url_to_lxml(bill_url, BRP.bill)
        except:
            logger.warning("No Data for Bill %s" % bill_id)
            return

        # Get title
        try:
            title = old_page.xpath_single('//h2[@class="ldTitle"]').text_content()
        except AttributeError:
            raise ValueError("No Title for Bill %s" % bill_id)

        if re.match(r'(joint )?(order|resolution)', title.lower()):
            bill_type = 'joint_resolution'
        elif re.match(r'(house|senate) (order|resolution)', title.lower()):
            bill_type = 'resolution'
        else:
            bill_type = 'bill'

        if title.upper() == title:
            title = title.title()

        new_bill_link = old_page.xpath_single("//a[contains(text(), 'Chamber Status,')]").get_attrib('href')

        # check if the URL is good otherwise raise a NoBillDataForPeriod
        try:
            page = self.scraper.url_to_lxml(new_bill_link, BRP.bill)
        except:
            logger.warning("No Data for Bill %s" % bill_id)
            return


        bill = Bill(session, chamber, bill_id, title, bill_type)

        sub_download_id = self.scraper.download_and_register(new_bill_link, BRP.bill_documents, False)
        sub_service_document = Doc_service_document("Summary", "summary", "partial", sub_download_id)
        bill.add_doc_service_document(sub_service_document)

        # Add the LD number in.
        for ld_num in page.xpath("//b[contains(text(), 'LD ')]/text()"):
            if re.search(r'LD \d+', ld_num):
                bill.add_alternate_id(ld_num)

        bill.add_source(new_bill_link)
        # Add bill sponsors.
        try:
            xpath = '//a[contains(@href, "sponsors")]/@href'
            sponsors_url = page.xpath_single(xpath)
        except IndexError:
            msg = ('Page didn\'t contain sponsors url with expected '
                   'format. Page url was %s' % url)
            logger.warning(msg)

        if sponsors_url:
            sponsors_page = self.scraper.url_to_lxml(sponsors_url, BRP.bill_sponsors)
            tr_text = sponsors_page.xpath('//tr')
            tr_text = [tr.text_content() for tr in tr_text]
            rgx = r'(Speaker|President|Senator|Representative)(.*?)(?:\s{2,}|$)'
            for text in tr_text:
                if 'the Majority' in text:
                    # At least one bill was sponsored by 'the Majority'.
                    bill.add_sponsor('primary', 'the Majority', chamber=bill['chamber'])
                    continue
                if text.lower().startswith('sponsored by:'):
                    type_ = 'primary'
                elif 'introduc' in text.lower():
                    type_ = 'primary'
                elif text.lower().startswith('cosponsored by:'):
                    type_ = 'cosponsor'
                else:
                    continue

                sponsors = re.findall(rgx, text)
                for (title, name) in sponsors:
                    chamber = None
                    if title == "Senator":
                        chamber = 'upper'
                    elif title == "Representative":
                        chamber = "lower"
                    if chamber:
                        bill.add_sponsor(type_.lower(), self.get_name(name.strip(), chamber), chamber=chamber)
                    else:
                        bill.add_sponsor(type_.lower(), name.strip())



        docket_link = page.xpath("//a[contains(@href, 'dockets.asp')]")[0]
        self.scrape_actions(bill, docket_link.get_attrib('href'))

        # Add signed by guv action.
        governor_action = page.xpath_single('//td[contains(text(), "Governor Action")]/following-sibling::td/b', BRP.test)
        if governor_action:
            action_str = governor_action.text_content()
            date = page.xpath_single('//td[contains(text(), "Date")]/'
                                     'following-sibling::td/b').text_content()
            dt = parse(date)
            attrs = dict(action=action_str,
                         date=dt,
                         actor="executive"
                        )
            bill.add_action(**attrs)

        xpath = "//a[contains(@href, 'rollcalls.asp')]"
        votes_link = page.xpath_single(xpath)
        self.scrape_votes(bill, votes_link.get_attrib('href'))

        spon_link = page.xpath_single("//a[contains(@href, 'subjects.asp')]")
        spon_url = spon_link.get_attrib('href')

        sdoc = self.scraper.url_to_lxml(spon_url, BRP.bill_sponsors)
        xpath = '//table[@class="sectionbody"]/tr[2]/td/text()'
        srow = sdoc.xpath(xpath)[1:]
        if srow:
            for sub in srow:
                if sub.strip():
                    bill.add_subject(sub.strip())


        doc_link = page.xpath_single("//a[contains(@href, 'display_ps.asp')]")
        doc_url = doc_link.get_attrib('href')
        try:
            doc_page = self.scraper.url_to_lxml(doc_url, BRP.bill_versions)
        except socket.timeout:
            pass

        ver_blocks = doc_page.xpath("//span[contains(@class, 'tlnk-bill')]")
        ver_list = []
        for ver_table in ver_blocks:
            ver_span = ver_table.xpath("./span")
            ver_name = ver_span[0].text_content()
            ver_name = re.sub(r'Text', '', ver_name)
            ver_name = re.sub(r'Change Title', '', ver_name).strip()
            if 'LD' in ver_name:
                ver_name = re.findall(r'(LD \d+)', ver_name)[0]
            ver_pdf_link = ver_span[1].xpath(".//a")[0].get_attrib('href')
            if ver_pdf_link in ver_list:
                continue
            ver_list.append(ver_pdf_link)
            if 'item=1' in ver_pdf_link:
                ver_name = "%s - Version" % ver_name

            try:
                ver_html_link = ver_span[2].xpath(".//a")[0].get_attrib('href')
                download_id, _, doc_ids = self.scraper.register_download_and_documents(ver_html_link, BRP.bill_versions,
                                                                                       self.scraper.extraction_type.html,
                                                                                       True, self.html_parser)
            except IndexError:
                download_id, _, doc_ids = self.scraper.register_download_and_documents(ver_pdf_link, BRP.bill_versions,
                                                                                       self.scraper.extraction_type.text_pdf,
                                                                                       True)
            doc_service_document = Doc_service_document(ver_name, "version", "complete",
                                                        download_id=download_id,
                                                        doc_id=doc_ids[0])
            bill.add_doc_service_document(doc_service_document)


        fiscal_note = doc_page.xpath('//a[contains(@href, "fiscalnotes/")]/@href')
        fiscal_list = []
        for fiscal_link in fiscal_note:
            if fiscal_link in fiscal_list:
                continue
            fiscal_list.append(fiscal_link)
            fiscal_name = re.findall(r'/fiscalnotes/(.*)\.', fiscal_link)[0]

            download_id = self.scraper.download_and_register(fiscal_link, BRP.bill_documents, False)
            doc_service_document = Doc_service_document(fiscal_name, "fiscal_note", "partial", download_id)
            bill.add_doc_service_document(doc_service_document)

        self.save_bill(bill)


    def scrape_votes(self, bill, url):
        """
        scrape votes for bill
        :param bill: bill that need to scrape votes for
        :param url: url of votes
        """
        page = self.scraper.url_to_lxml(url, BRP.bill_votes)
        path = "//div/a[contains(@href, 'rollcall.asp')]"
        for link in page.xpath(path):
            if link.text:
                motion = link.text.strip()
                url = link.get_attrib('href')
                self.scrape_vote(bill, motion, url)


    def scrape_vote(self, bill, motion, url):
        """
        scapre vote by motion and url
        :param bill: bill that the vote belong to
        :param motion: motion of vote
        :param url: url of this url
        """
        page = self.scraper.url_to_lxml(url, BRP.bill_votes)

        yeas_cell = page.xpath_single("//td[text() = 'Yeas (Y):']")
        yes_count = int(yeas_cell.getnext().text_content())
        nays_cell = page.xpath_single("//td[text() = 'Nays (N):']")
        no_count = int(nays_cell.getnext().text_content())
        abs_cell = page.xpath_single("//td[text() = 'Absent (X):']")
        abs_count = int(abs_cell.getnext().text_content())
        ex_cell = page.xpath_single("//td[text() = 'Excused (E):']")
        ex_count = int(ex_cell.getnext().text_content())
        other_count = abs_count + ex_count

        if 'chamber=House' in url:
            chamber = 'lower'
        elif 'chamber=Senate' in url:
            chamber = 'upper'

        roll_calls = page.xpath_single("//font[contains(text(), 'Roll-call')]").text_content()


        motion_str = re.findall(r'Roll-call #\d+', roll_calls)
        if motion_str:
            motion = motion_str[0]

        date_cell = page.xpath_single("//td[text() = 'Date:']")
        date = date_cell.getnext().text_content()
        date = parse(date)

        outcome_cell = page.xpath_single("//td[text()='Outcome:']")
        outcome = outcome_cell.getnext().text_content()
        vote = Vote(chamber, date, motion,
                    outcome == 'PREVAILS',
                    yes_count, no_count, other_count)
        vote.add_source(url)

        member_cell = page.xpath_single("//td[text() = 'Member']")
        for row in member_cell.xpath("../../tr")[1:]:
            name = self.get_name(row.xpath("./td")[1].text_content(), chamber)
            vtype = row.xpath_single("string(td[4])")
            if vtype == 'Y':
                vote.yes(name)
            elif vtype == 'N':
                vote.no(name)
            elif vtype == 'X' or vtype == 'E':
                vote.other(name)

        bill.add_vote(vote)


    def scrape_actions(self, bill, url):
        """
        scrape actions for bill
        :param bill: the bill that actions belong to
        :param url: url of bill
        """
        page = self.scraper.url_to_lxml(url, BRP.bill_actions)

        path = "//b[. = 'Date']/../../../following-sibling::tr"
        for row in page.xpath(path):
            cols = row.xpath("./td")
            date = cols[0].text_content().strip()
            date = parse(date)
            chamber = cols[1].text_content().strip()
            if chamber == 'Senate':
                chamber = 'upper'
            elif chamber == 'House':
                chamber = 'lower'

            action = cols[2].text_content().strip()
            action = unescape(action).strip()
            actions = []
            for action in action.splitlines():
                action = re.sub(r'\s+', ' ', action)
                if not action or 'Unfinished Business' in action:
                    continue
                actions.append(action)
            for action in actions:
                attrs = dict(actor=chamber, action=action, date=date)
                bill.add_action(**attrs)

    # Method to get name from the sponsor text. If no match or duplicates just returns back the text
    def get_name(self, text, chamber):
        match = re.match("(.+) of (.+)", text)
        if not match:
            return text
        name = match.group(1).strip().lower()
        loc = match.group(2).strip().lower()
        key = (name, loc)
        if chamber == "lower":
            table = self.house_legislator_table
            dyn_table = self.house_dyn_table
        else:
            table = self.senate_legislator_table
            dyn_table = self.senate_dyn_table
        # if mapping of (name, loc) has already been done before, return it
        # otherwise determine mapping and put into table
        if key in dyn_table:
            return dyn_table[key]
        else:
            full_name_list = table.get(loc)

            if full_name_list is None:
                return text

            num_match = 0
            # Checks if last name is in any part of full name
            # If only one match will return that full name otherwise will just return last name
            for full_name in full_name_list:
                if name in full_name.lower():
                    num_match += 1
                    name_match = full_name

            if num_match == 1:
                dyn_table[key] = name_match
                return name_match
            elif num_match == 0:
                logging.warning("No match for name " + text)
            else:
                logging.warning("Multiple matches for name " + text)
            dyn_table[key] = text
            return text

    @staticmethod
    def html_parser(element_wrapper):
        return [ScraperDocument(element_wrapper.xpath_single("//body").text_content())]


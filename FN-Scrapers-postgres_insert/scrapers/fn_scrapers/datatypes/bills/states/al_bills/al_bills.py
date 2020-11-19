from __future__ import absolute_import

import os
import re
import datetime
import logging
import socket

from fn_scrapers.datatypes.bills.common import (
    Bill, BillScraper, Vote, BillReportingPolicy as BRP, Doc_service_document
)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
)

from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY
from fn_scraperutils.doc_service.fn_extraction import entities_text_content
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scraperutils.events.reporting import ScrapeError
from fn_scraperutils.scrape.element_wrapper import ElementWrapper

from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.api.resources import files, http, injector

logger = logging.getLogger('ALBillScraper')

AL_BASE_URL = 'http://alisondb.legislature.state.al.us/Alison/'
BODY_CODES = dict(lower=1755, upper=1753, joint=999999)
BILL_ID_CHAMBER = dict(H='lower', S='upper')

ORDINAL_NUMBERS = ["First", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth", "Ninth", "Tenth"]


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-AL", group="fnleg")
class ALBillScraper(BillScraper):
    """
    ALBillScraper

    Scrape Alabama Bills

    The source uses POST requests to get into a particular state. The code below uses a lot of these requests to get
    the information we need. The process is somewhat as follows:

    For getting the BillIDs:

    1. The scraper goes to this URL for House Bills
       http://alisondb.legislature.state.al.us/Alison/SESSBillsByHouseSponsorSelect.aspx
    2. Saves the information available in the
        //input[@type="image"]/@name <-- This is needed for a POST request
        //input[@type="image"]/@alt <-- This contains the ID for that sponsor
        //span[@class="label label-default"] <-- This contains the name of that sponsor
    3. Then for each sponsor, clicks the Sponsor, Co-Sponsor and Both button on top of the page, and clicks
       the same sponsor for all the categories using POST
    4. This enables the scraper to GET the page for the URL (For Ainsworth)
       http://alisondb.legislature.state.al.us/Alison/SESSBillsList.aspx?NAME=Ainsworth&SPONSOROID=85941&BODY=1755&SESSNAME=Regular%20Session%202018
    5. Here Bills are listed in a Tabular format where the 'Ainsworth' is a sponsor depending on what button was clicked
       previously.
    6. The scraper gets
        //tr/td[1]/font/input[@type='button'] <-- This is the Bill ID/Number
        //tr/td[2]/font/input[@type='button'] <-- This is the Primary Sponsor
        //tr/td[3] <-- This is the subject of that bill
        //tr/td[6] <-- This is the Committee this bill is assigned to
       for all the rows in that table.
    7. A dict is created for each bill ID where the corresponding information in 6. is stored

    For scraping the bill:

    1. The scraper GETs the following URL for HB 100 ('Ainsworth' is the Primary sponsor)
       http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=HB100&WIN_TYPE=BillResult
    2. First performs a check to see of the element
        //span[@id='ContentPlaceHolder1_lblBill']/text() <-- This is the heading of the page
       contains the correct session name or not. Since the website uses POST requests, it is possible to get
       bills from an incorrect session using the URL in 1.
    3. Tries to get the bill Title, default: "[No title given by state]"
        //span[@id="ContentPlaceHolder1_lblShotTitle"] <-- This is the title element
    4. After this it looks for the Ammendments/substitute versions using
        //table[@id="ContentPlaceHolder1_gvHistory"]/tr/td[3]
       this is the document ID and is considered only if it has a '-' in it.
    5. A new session identifier is generated using the session name
        Regular Session 2018 --> 2018RS
       The name of the file is caught from the next td element in that row
       and the document URL is generated:
       http://alisondb.legislature.state.al.us/Alison/SearchableInstruments/2018RS/PrintFiles/189882-2.pdf
    6. Tries to get the Fiscal Notes for this bill using
        //table[@class="box_fiscalnote"]/tr[@onclick]
    7. Then it looks for the version documents by going to:
        //table[@class="box_versions"]/tr/td[2]/font/text() <-- This idicates the version type
       The scraper generates the links to these files based on the version type,
       type Introduced:
       http://alisondb.legislature.state.al.us/Alison/SearchableInstruments/2018RS/PrintFiles/HB100-int.pdf
       type Engrossed:
       http://alisondb.legislature.state.al.us/Alison/SearchableInstruments/2018RS/PrintFiles/HB100-eng.pdf
       type Enrolled:
       http://alisondb.legislature.state.al.us/Alison/SearchableInstruments/2018RS/PrintFiles/HB100-enr.pdf
    8. It also extracts the cosponsors from the version documents after getting the text entities.
    9. The actions ar extracted from the table //table[@id="ContentPlaceHolder1_gvHistory"]//tr
    10. It also extracts Budget action rows using //div[@class="box_bir"]//table//table/tr
    11. Finally save the bill object

    NOTE: We cannot generate a bill source for Alabama. As mentioned above, the bill url contains the id,
    but not the session. The only way to get to the correct bill page is to do a POST request with the session
    info, and then do a GET request to the url containing the id. Thus, we cannot generate a source link.
    """
    _session = None

    @injector.inject(files_session=files.Session, http_session=http.Session)
    def __init__(self, files_session, http_session):
        files_session.set_as_instance()
        http_session.set_as_instance()
        super(ALBillScraper, self).__init__("al")

    def scrape_bill_ids(self, session):
        """
        scrape's bills from AL for given session/chambers

        :param session: session to scrape
        :type session: string
        """

        self._set_session(session)
        page = self.scraper.url_to_lxml(AL_BASE_URL, BRP.bill)
        bill_ids = {}

        chambers = ["upper", "lower"]
        for chamber in chambers:
            bill_info = self._get_sponsors_and_subjects(chamber, session)
            bill_ids.update(bill_info)
        return bill_ids

    def _get_sponsors_and_subjects(self, chamber, session):
        """
        When doing a local run, the sponsors won't be extracted if a specific bill_id is provided
        """
        chamber_name = "House" if chamber == 'lower' else "Senate"
        session_name = self._get_session_name(session)

        bill_info = {}
        for type_name in ['Bills', 'Resos']:
            # Urls for Bills/Res list and Bills/Res by House/Chamber
            spons_type_url = AL_BASE_URL + 'SESS%sBy%sSponsorSelect.aspx' % (type_name, chamber_name)
            spons_list_url = AL_BASE_URL + 'SESS%sList.aspx' % type_name
            doc = self.scraper.url_to_lxml(spons_type_url, BRP.bill_list)
            # Save the viewstate for sponsor view
            view_state = self._get_view_state(doc)
            # controls, ids, and names, used for "clicking" and url building
            sponsor_cts = doc.xpath('//input[@type="image"]/@name')
            sponsor_ids = doc.xpath('//input[@type="image"]/@alt')
            sponsor_objs = doc.xpath('//span[@class="label label-default"]')
            sponsor_names = []

            # have to do this because ElementWrapperList has no remove function
            for sponsor in sponsor_objs:
                if sponsor.text is not " ":
                    sponsor_names.append(sponsor.text)

            for spon_ct, spon_id, spon_name in zip(sponsor_cts, sponsor_ids, sponsor_names):
                # Send command to toggle sponsor type
                target = 'ctl00$MainDefaultContent$btn%s' % ("Sponsor")
                type_post_data = {'ctl00$ScriptManager1': 'ctl00$UpdatePanel1|%s' % target,
                                  '__EVENTARGUMENT': '',
                                  '__EVENTTARGET': '',
                                  '__ASYNCPOST': True,
                                  'ctl00$ContentPlaceHolder1$btnSponsor': 'Sponsor'
                                  }
                type_post_data.update(view_state)
                self.scraper.http_request(spons_type_url, method="POST",
                                          request_args={"data": type_post_data},
                                          retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

                # send command to "click" on sponsor, x/y coords are arbitrary
                spons_post_data = {'ctl00$ScriptManager1': 'ctl00$UpdatePanel1|%s' % spon_ct,
                                   '__EVENTARGUMENT': '',
                                   '__EVENTTARGET': '',
                                   '__ASYNCPOST': True,
                                   '%s.x' % spon_ct: '21',
                                   '%s.y' % spon_ct: '21',
                                   }
                spons_post_data.update(view_state)
                self.scraper.http_request(spons_type_url, method="POST",
                                          request_args={"data": spons_post_data},
                                          retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

                new_spons_url = spons_list_url + "?NAME={}&SPONSOROID={}&BODY={}SESSNAME={}".format(
                    spon_name, spon_id, BODY_CODES[chamber], session_name)
                new_spons_url = '%20'.join(new_spons_url.split(' '))
                spons_doc = self.scraper.url_to_lxml(new_spons_url, BRP.bill_sponsors,
                                                     retry_policy=RETRY500_REQUESTS_RETRY_POLICY)

                """
                This is an elaborate scheme to ensure that the order of all the lists is maintained
                because the info we're looking for has no distinguishing factors that would make it
                easier to scrape
                """
                bill_ids = []

                """
                For some reason the desired input elements elements in the HTML text retrieved using GET are enclosed
                in font elements, which is not reflected when doing an inspect element on the website. The following
                XPath works in Chrome, but has to be changed when using with LXML:

                //tr/td[1]/input[@type='button'] for bill_ids and
                //tr/td[1]/input[@type='button'] for sponsors
                """
                buttons = spons_doc.xpath("//tr/td[1]/font/input[@type='button']", BRP.debug)
                for button in buttons:
                    val = button.element.value
                    bill_ids.append(val)

                sponsors = spons_doc.xpath("//tr/td[2]/font/input[@type='button']", BRP.debug)

                subject_tabs = spons_doc.xpath("//tr/td[3]", BRP.debug)
                committee_tabs = spons_doc.xpath('//tr/td[6]', BRP.debug)

                # adds the subjects and committees to the bill info dictionary
                for bill_id, subject, committee, sponsor in zip(bill_ids, subject_tabs, committee_tabs, sponsors):
                    bill_dict = {
                        'Subject': subject.text_content(policy=BRP.debug) or '',
                        'Committee': committee.text_content(policy=BRP.debug) or '',
                        'Sponsor': sponsor.element.value,
                    }
                    bill_info.update({bill_id: bill_dict})
        return bill_info

    def scrape_bill(self, session, bill_id, **kwargs):
        '''
        So Alabama's website get's confused - it forgets what bill we're
        requresting as it is building the response. Thankfully this is very
        rare (1-3% I believe, or 3-5 bills per scrape) so we do a best-of-3
        when requesting.
        '''
        bills = []
        for i in range(0, 3):
            bill = self.get_bill(i, session, bill_id, **kwargs)
            if any(prev_bill == bill for prev_bill in bills):
                return self.save_bill(bill)
            if bills:
                logger.warning("Bills failed to match (%s)", len(bills))
            bills.append(bill)
        raise ScrapeError(
            BRP.bill, "Could not determine bill {}: {}@{}".format(
                bill_id, self.process_id, socket.gethostname()
        ))

    def get_bill(self, cnt, session, bill_id, **kwargs):
        bill_info = kwargs.get("bill_info")
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        if not self._session or self._session != session:
            self._set_session(session)

        formatted_id = "".join(bill_id.split())
        bill_url = AL_BASE_URL + 'SESSBillStatusResult.aspx?BILL={}&WIN_TYPE=BillResult'.format(formatted_id)

        fil = files.request_file(bill_url)
        bill_doc = ElementWrapper(fil.get_lxml_html(), BRP.bill, bill_url,
                                  self.scraper.process_data)

        # If we aren't on the correct bill page for this session, we try to reset the session. If it is still incorrect,
        # throw a critical.
        try:
            self.validate_bill_page(bill_doc, formatted_id, session)
        except ValueError:
            self._set_session(session)
            bill_doc = self.scraper.url_to_lxml(bill_url, BRP.bill)
            try:
                self.validate_bill_page(bill_doc, formatted_id, session)
            except ValueError as e:
                raise ScrapeError(BRP.bill, e.message, bill_url)

        title = self._get_bill_title(bill_doc)
        if not title:
            return
        bill = Bill(session, chamber, bill_id, title, bill_type)

        if bill_info and bill_info['Subject']:
            bill.add_subject(bill_info['Subject'])

        for name, doc_url, doc_type in self._get_bill_amendments_and_substitutes(session, bill_doc):
            download_id, _, doc_ids = \
                self.scraper.register_download_and_documents(doc_url, BRP.bill_versions,
                                                             self.scraper.extraction_type.text_pdf,
                                                             True, content_type='application/pdf')

            if download_id is None or len(doc_ids) != 1 or doc_ids[0] is None:
                logger.warning("Bad Document Url %s" % doc_url)
                continue
            doc_id = doc_ids[0]
            doc_service_document = Doc_service_document(name, "version", "complete", download_id, doc_id=doc_id)
            bill.add_doc_service_document(doc_service_document)

        for name, fn_url in self._get_bill_fiscal_notes(session, bill_url, bill_doc):
            doc_download_id = self.scraper.download_and_register(fn_url, BRP.bill_documents, False)
            doc_service_document = Doc_service_document(name, 'fiscal_note', "partial", doc_download_id)
            bill.add_doc_service_document(doc_service_document)

        for name, version_url in self._get_bill_versions(session, bill_id, bill_doc):
            download_id, scraper_docs, doc_ids = \
                    self.scraper.register_download_and_documents(version_url, BRP.bill_versions,
                                                                 self.scraper.extraction_type.text_pdf,
                                                                 True, self.sponsor_parser)
            if len(scraper_docs) == 1 and scraper_docs[0] is not None:
                if bill_info and 'Sponsor' in bill_info:
                    parenthetical = re.search(r"(.*?)\s\((.+?)\)", bill_info['Sponsor'])
                    sponsor = ''
                    if parenthetical:
                        sponsor = "{} {}".format(parenthetical.group(2), parenthetical.group(1)).strip()
                    else:
                        sponsor = bill_info["Sponsor"].strip()
                    bill.add_sponsor("primary", sponsor)
                    cosponsrs = scraper_docs[0].additional_data["cosponsor"]
                    for cosponsor in cosponsrs:
                        if sponsor != cosponsor:
                            bill.add_sponsor("cosponsor", cosponsor)
            if download_id is None or len(doc_ids) != 1 or doc_ids[0] is None:
                logger.warning("Bad Document Url %s" % version_url)
                continue
            doc_id = doc_ids[0]
            doc_service_document = Doc_service_document(name, "version", "complete", download_id, doc_id=doc_id)
            bill.add_doc_service_document(doc_service_document)

        actions_and_votes = self._get_bill_actions_and_votes(session, bill_id, bill_doc)
        for action_date, actor, action_text, vote in actions_and_votes:
            if vote:
                bill.add_vote(vote)
            bill.add_action(actor, action_text, action_date)
        return bill

    @staticmethod
    def sponsor_parser(entities, parser_args=None):
        full_text = entities_text_content(entities)
        text = re.search(r"By\s+(?:Senators?|Representatives?)(.*?)(?:RFD|\(Constitutional | \(N\s+&\s+P\))",
                         full_text, re.DOTALL)

        if text:
            cospons = text.group(1)
            lines = re.split(r"(?:^and|\s+and\s+|,)", cospons, flags=re.U)
            cosponsors = []
            for line in lines:
                line = line.strip()
                if line:
                    sponsor = re.sub(ur"\s*\d+\s*", u" ", line, flags=re.U).strip()
                    parenthetical = re.search(r"(.*?)\s\((.+?)\)", sponsor)
                    if parenthetical:
                        person = "{} {}".format(parenthetical.group(2), parenthetical.group(1)).strip()
                        if person:
                            cosponsors.append(person)
                    else:
                        if sponsor.strip():
                            cosponsors.append(sponsor.strip())
        return [ScraperDocument(full_text, additional_data={"cosponsor": cosponsors})]

    @staticmethod
    def _get_bill_title(bill_doc):
        title = bill_doc.xpath_single('//span[@id="ContentPlaceHolder1_lblShotTitle"]')
        if title:
            title = title.text_content(policy=BRP.debug)
        if not title:
            title = "[No title given by state]"
        return title

    def _get_bill_fiscal_notes(self, session, bill_url, bill_doc):
        fiscal_note_rows = bill_doc.xpath('//table[@class="box_fiscalnote"]/tr[@onclick]', BRP.bill_documents)
        for fn_row in fiscal_note_rows:
            title = fn_row.text_content()
            if len(title.split('for')) > 1:
                title = title.split('for')[1]
                title = "Fiscal Note for" + title

            event_re = re.compile(r"_doPostBack\('(.*?)','(.*?)'\)")
            event_arg = event_re.search(fn_row.element.attrib['onclick']).group(2)
            event_tgt = event_re.search(fn_row.element.attrib['onclick']).group(1)

            post_args = {
                '__EVENTARGUMENT': event_arg,
                '__EVENTTARGET': event_tgt,
                'ctl00$ScriptManager1': 'ctl00$UpdatePanel1|' + event_tgt,
                'ctl00$cboSession': self._get_session_name(session),
            }

            post_args.update(self._get_view_state(bill_doc))
            doc = self.scraper.url_to_lxml(bill_url, BRP.bill_documents, method="POST",
                                           request_args={"data": post_args})
            text = doc.text_content()
            url_re = re.compile(r"window\.open\('(.*?)',")

            fn_url = url_re.search(text)
            if fn_url:
                fn_url = fn_url.group(1).strip()
                yield title, fn_url

    def _get_bill_versions(self, session, bill_id, bill_doc):
        # Alabama changed session names on bill versions to acronyms
        session = self._get_session_name(session)
        if "Session" in session:
            session = session.split()
            session = session[-1] + session[0][0] + session[-2][0]
        formatted_id = "".join(bill_id.split())
        version_url_base = AL_BASE_URL + 'SearchableInstruments/{0}/PrintFiles/{1}-'.format(session, formatted_id)
        versions = bill_doc.xpath('//table[@class="box_versions"]/tr/td[2]/font/text()', BRP.bill_versions)
        for version in versions:
            name = version
            if version == "Introduced":
                version_url = version_url_base + 'int.pdf'
            elif version == "Engrossed":
                version_url = version_url_base + 'eng.pdf'
            elif version == "Enrolled":
                version_url = version_url_base + 'enr.pdf'

            yield name, version_url

    def _get_bill_amendments_and_substitutes(self, session, bill_doc):
        amend_sub_xpath = '//table[@id="ContentPlaceHolder1_gvHistory"]/tr/td[3]'
        table = bill_doc.xpath(amend_sub_xpath, BRP.bill_versions)

        session = self._get_session_name(session)
        if "Session" in session:
            session = session.split()
            session = session[-1] + session[0][0] + session[-2][0]

        # look for dashes in the name of amendments and substitutes
        dash = re.compile('-')
        doc_list = []
        for td in table:
            text = td.text_content()
            if dash.search(text) is None:
                continue
            else:
                doc_id = td.text_content()
                doc_name = td.xpath('following-sibling::td[1]')[0].text_content()
                doc_name = doc_name.replace('Offered', '')
                if not doc_name:
                    doc_name = doc_id
                doc_url = AL_BASE_URL + 'SearchableInstruments/{}/PrintFiles/{}.pdf'.format(session, doc_id)
                if doc_url in doc_list:
                    continue
                doc_list.append(doc_url)
                doc_type = 'amendment' if 'amendment' in doc_name.lower() else None
                doc_type = 'substitute' if 'substitute' in doc_name.lower() else doc_type

                yield doc_name, doc_url, doc_type

    def _get_bill_actions_and_votes(self, session, bill_id, bill_doc):
        action_rows = bill_doc.xpath('//table[@id="ContentPlaceHolder1_gvHistory"]//tr', BRP.bill_actions)[1:]
        budget_action_rowss = bill_doc.xpath('//div[@class="box_bir"]//table//table/tr', BRP.bill_actions)[1:]

        action_date = None
        for action_row in action_rows:
            first_td = action_row.xpath('td[1]/font/text()')
            if first_td and first_td[0].encode('ascii', 'ignore').strip():
                action_date = datetime.datetime.strptime(first_td[0].strip(), '%m/%d/%Y')

            action_chamber = action_row.xpath_single('td[2]/font/text()')
            action_text = action_row.xpath_single('td[4]/font/text()')

            if action_chamber == "H":
                actor = 'lower'
            elif action_chamber == "S":
                actor = 'upper'
            else:
                actor = 'executive'

            vote = None
            vote_button = action_row.xpath('td[9]//text()')
            if vote_button and vote_button[0].strip().startswith("Roll "):
                action_chamber = action_row.xpath('td[2]/font/text()')[0]
                vote_id = vote_button[0].strip().split(" ")[-1]

                vote = self._scrape_vote(session, bill_id, actor, vote_id, action_date, action_text)

            yield action_date, actor, action_text, vote

        for b_action_row in budget_action_rowss:
            action_text = b_action_row.xpath('td[1]')[0].text_content().strip()
            if bill_id not in action_text:
                logger.warning('Budget action found %s - that does not match bill_id - %s', action_text, bill_id)
                continue
            b_action_date = b_action_row.xpath('td[2]/font/text()')[0].strip()
            b_action_date = datetime.datetime.strptime(b_action_date, '%m/%d/%Y')
            b_action_type = b_action_row.xpath('td[1]/font/text()')[0].split(' ')[0]
            b_action_chamber = BILL_ID_CHAMBER[b_action_type[0]]

            b_action_text = '%s - %s' % (b_action_type, b_action_row.xpath('td[3]/font/text()')[0].strip())

            vote = None
            b_vote_id = b_action_row.xpath('td[4]/font/input/@value')

            if b_vote_id and b_vote_id[0].strip().startswith("Roll "):
                b_vote_id = b_vote_id[0].strip().split(" ")[-1]
                b_bill_id = '{0}%20for%20{1}'.format(b_action_type, bill_id)

                vote = self._scrape_vote(session, b_bill_id, b_action_chamber, b_vote_id, b_action_date, b_action_text)

            yield b_action_date, b_action_chamber, action_text, vote

    def _scrape_vote(self, session, bill_id, vote_chamber,
                     vote_id, vote_date, action_text):

        url = AL_BASE_URL + 'GetRollCallVoteResults.aspx?VOTE={0}&BODY={1}&INST={2}&SESS={3}&AMDSUB=&nbsp;'
        chamber_stub = 'H' if vote_chamber == 'lower' else 'S'
        formatted_id = "".join(bill_id.split())
        url = url.format(vote_id, chamber_stub, formatted_id, "1065")
        doc = self.scraper.url_to_lxml(url, BRP.bill_votes)
        voters = dict(Y=[], N=[], P=[], A=[])

        voters_and_votes = doc.xpath('//table/tr/td/font/text()')
        voters_and_votes = [x.strip() for x in voters_and_votes if x.strip()]
        voters_and_votes = self._tuples_from_alternating_list(voters_and_votes)

        for voter, vote in voters_and_votes:
            assert vote in voters.keys()
            if 'Vacant' in voter or 'Total' in voter:
                logger.info("Not a voter, %s %s", voter, vote)
            voters[vote].append(voter)

        yes_count = 0
        no_count = 0
        other_count = 0

        yes_count = len(voters['Y'])
        no_count = len(voters['N'])

        other_count = len(voters['P']) + len(voters['A'])
        vote_passed = yes_count > no_count

        if vote_chamber == 'executive':
            vote_chamber = 'joint'

        vote = Vote(vote_chamber, vote_date, action_text, vote_passed, yes_count, no_count, other_count)
        vote.add_source(url)

        for member in voters['Y']:
            vote.yes(member)
        for member in voters['N']:
            vote.no(member)
        for member in voters['A'] + voters['P']:
            vote.other(member)

        return vote

    @staticmethod
    def _get_view_state(lxml_doc):
        vsg = dict(
            __VIEWSTATE=lxml_doc.xpath_single('//input[@id="__VIEWSTATE"]/@value'),
            __VIEWSTATEGENERATOR=lxml_doc.xpath_single('//input[@id="__VIEWSTATEGENERATOR"]/@value')
        )
        return vsg

    def _set_session(self, session):
        '''
        Activate an ASP.NET session, and set the legislative session
        '''
        set_session_url = AL_BASE_URL + 'SelectSession.aspx'
        self._session = session
        doc = self.scraper.url_to_lxml(set_session_url, BRP.bill_list)

        # grabs a list of session names, then finds the one we're looking for
        session_rows = doc.xpath('//tr/td')
        name = self._get_session_name(session)
        for x, item in enumerate(session_rows):
            if item.text_content() == name:
                arg = x
                break

        # got these arguments by messing around with the site using a web proxy
        post_data = {'__EVENTTARGET': 'ctl00$ContentPlaceHolder1$gvSessions',
                     '__EVENTARGUMENT': '${}'.format(arg),
                     'ctl00$ScriptManager1': 'ctl00$UpdatePanel1|ctl00$ContentPlaceHolder1$gvSessions',
                     '__ASYNCPOST': True
                     }
        post_data.update(self._get_view_state(doc))
        self.scraper.http_request(set_session_url, method="POST", request_args={"data": post_data})

    @staticmethod
    def _get_session_name(session):
        year = session[:4]
        if "r" in session:
            session_type = "Regular Session"
        else:
            session_number = int(session[-1])
            session_type = "{} Special Session".format(ORDINAL_NUMBERS[session_number - 1])
        return "{} {}".format(session_type, year)

    @staticmethod
    def _tuples_from_alternating_list(a_list):
        '''
        Given any list, i.e. ['Jim','Y','Tom','N'], convert to a list of tuples,
        i.e. [('Jim','Y'),('Tom','N')]. Raises AssertionError if list count
        isn't divisible by provided step of two.
        '''
        assert len(a_list) % 2 == 0, (
            'Not divisible by step, step:%s len:%s' % (2, len(a_list)))
        return [(a_list[i], a_list[i+1]) for i in range(0, len(a_list), 2)]

    @classmethod
    def validate_bill_page(cls, bill_doc, formatted_id, session):
        '''
        Validate that the given page is infact the correct bill and session.
        '''
        session = cls._get_session_name(session)

        status = bill_doc.xpath_single(
            u"//span[@id='ContentPlaceHolder1_lblBill']/text()") or ''
        # status can start with "Bill Status ..." or "Resolution Status ..."
        expected = "Status for {} ({})".format(formatted_id, session)
        if expected not in status.strip():
            msg = u'Incorrect page returned {} ({}): "{}"'.format(
                formatted_id, session, status)
            raise ValueError(msg)

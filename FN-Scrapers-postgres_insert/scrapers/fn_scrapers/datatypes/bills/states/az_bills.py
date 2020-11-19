from __future__ import absolute_import

import re
import logging
import datetime
import urlparse
from lxml import etree

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document

from fn_scrapers.api.scraper import scraper, tags

from fn_scraperutils.doc_service.util import ScraperDocument


logger = logging.getLogger(u"AZBillScraper")
bill_list_url = u"https://www.azleg.gov/bills/"
update_bill_list_session_url = u"https://www.azleg.gov/azlegwp/setsession.php"

bill_types = {
    u'sb': u'bill',
    u'sm': u'memorial',
    u'sr': u'resolution',
    u'scr': u'concurrent_resolution',
    u'scm': u'joint_memorial',
    u'scj': u'joint_resolution',
    u'hb': u'bill',
    u'hm': u'memorial',
    u'hr': u'resolution',
    u'hcr': u'concurrent_resolution',
    u'hcm': u'joint_memorial',
    u'hjr': u'joint_resolution',
    u'mis': u'resolution'
}

ORDINALS = ('first second third fourth fifth sixth seventh eighth ninth tenth '
            'eleventh twelfth thirteenth fourteenth fifteenth sixteenth '
            'seventeenth eighteenth nineteenth twentieth').split()

api_template = u"https://apps.azleg.gov/api/{}/?{}={}"
api_domain = u"https://apps.azleg.gov"
vote_abbr_to_result_map = {u"DP": True,
                           u"adopt unan": True,
                           u"adv": True,
                           u"dp": True,
                           u"dpa": True,
                           u"dpa/se": True,
                           u"dnp": False,
                           u"PAI": True,
                           u"Z": True}


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-AZ", group="fnleg")
class AZBillScraper(BillScraper):
    def __init__(self):
        super(AZBillScraper, self).__init__("az")

    def scrape_bill_ids(self, session):
        bill_ids = {}
        bill_list_doc, session_id = self.get_session_page(session)
        tables = bill_list_doc.xpath(u"//div[@name='HBTable' or @name='SBTable']//table")
        for table in tables:
            rows = table.xpath(u"./tbody/tr", BRP.debug)
            for row in rows:
                cells = row.xpath(u"./td")
                bill_id = cells[0].text_content()
                bill_title = cells[1].text_content().split(u"(NOW")[0].strip()
                url = cells[0].xpath_single(u"./a").get_attrib(u"href")
                bill_status_id = url.split(u"BillOverview/")[1].split(u"?SessionId")[0]
                bill_ids[bill_id] = {u"title": bill_title,
                                     u"bill_status_id": bill_status_id,
                                     u"url": url,
                                     u"session_id": session_id}
        logger.info(u"A total of {} bill ids scraped for AZ for {} session".format(len(bill_ids), session))
        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):

        logger.info(u"Scraping bill id {} for {} session".format(bill_id, session))

        bill_info = kwargs.get(u"bill_info")
        bill_status_id = bill_info[u"bill_status_id"]
        title = bill_info[u"title"]
        title = title.capitalize()
        url = bill_info[u"url"]
        session_id = bill_info[u"session_id"]
        chamber = u"lower" if bill_id[0] == u"H" else u"upper"

        bill = Bill(session, chamber, bill_id, title, self.get_bill_type(bill_id))
        bill.add_source(url)
        # sponsors
        sponsors_url = api_template.format(u"BillSponsor", u"id", bill_status_id)
        sponsors_json = self.get_json(sponsors_url)
        self.map(bill, u"sponsors", sponsors_json,
                 lambda data: {
                     u"type": u"primary" if u"prime" in data[u"SponsorType"].lower() else u"cosponsor",
                     u"name": data[u"Legislator"][u"FullName"],
                     u"chamber": 'upper' if data[u"Legislator"][u"Body"].lower() == 's' else 'lower'
                 }
        )

        # keywords
        keywords_url = api_template.format(u"Keyword", u"billStatusId", bill_status_id)
        keywords_json = self.get_json(keywords_url)
        self.map(bill, u"subjects", keywords_json, lambda data: data[u"Name"])
        # actions
        status_api_url = \
            u"https://apps.azleg.gov/api/BillStatusOverview/?billNumber={}&sessionId={}". \
              format(bill_id.replace(" ", ""), session_id)
        status_json = self.get_json(status_api_url)
        self.map(bill, u"actions", status_json, self.action_transformer)

        # votes
        standing_action_url = u"https://apps.azleg.gov/api/BillStatusStandingAction/" \
                              u"?billStatusId={}&includeVotes=true&officialOnly=true". \
            format(bill_status_id)

        standing_action_json = self.get_json(standing_action_url)
        self.map(bill, u"votes", standing_action_json, self.vote_transformer)

        # additional actions
        self.map(bill, u"actions", standing_action_json, self.standing_action_transformer)

        # additional votes and governor action
        overview_url = u"https://apps.azleg.gov/api/Bill/?billNumber={}&sessionId={}". \
            format(bill_id.replace(" ", ""), session_id)
        overview_json = self.get_json(overview_url)
        governor_action = overview_json.get(u"GovernorAction")
        governor_action_date = overview_json.get(u"GovernorActionDate")
        if governor_action and governor_action_date:
            action_text = u"{} by Governor".format(governor_action)
            action_date_str = re.match(r"(\d+-\d+-\d+)T", governor_action_date).group(1)
            action_date = datetime.datetime.strptime(action_date_str, u"%Y-%m-%d")
            bill.add_action(u"executive", action_text, action_date)

        self.map(bill, u"votes", overview_json[u"FloorHeaders"], self.additional_vote_transformer,
                 bill_status_id=bill_status_id)
        # documents
        documents_url = u"https://apps.azleg.gov/api/DocType/?billStatusId={}".format(bill_status_id)
        documents_json = self.get_json(documents_url)
        versions = []
        amendments = []
        misc = []
        for element in documents_json:
            if u"Documents" not in element:
                continue
            if element[u"DocumentGroupName"] == u"Bill Versions":
                versions += element[u"Documents"]
            elif element[u"DocumentGroupName"] == u"Misc. Bill Documents":
                misc += element[u"Documents"]
            elif u"Amendment" in element[u"DocumentGroupName"]:
                amendments += element[u"Documents"]
            else:
                logger.warning(u"Document with group name {} is not properly processed".
                               format(element[u"DocumentGroupName"]))
        self.map(bill, u"documents", versions, self.version_transformer)
        self.map(bill, u"documents", amendments, self.amendment_transformer)
        self.map(bill, u"documents", misc, self.misc_transformer)
        self.save_bill(bill)

    def get_session_page(self, session):
        '''Return a tuple (bill_list_doc, session_id), attempting to change
         session ONLY if required. '''
        bill_list_doc = self.load_session_page()
        session_id = self.get_session_id(session, bill_list_doc)
        if session_id:
            return bill_list_doc, session_id
        # we didn't get the session we want on initial load, set it and reload
        self.change_session(session, bill_list_doc)
        bill_list_doc = self.load_session_page()
        session_id = self.get_session_id(session, bill_list_doc)
        if session_id:
            return bill_list_doc, session_id
        raise Exception(u"Unable to safely determine session " + session)

    def get_session_id(self, session, bill_list_doc):
        '''Check if the page is on the current desired session, returning the id
        if so and None if not. '''
        # check if the current dropdown matches:
        selected = bill_list_doc.xpath_single("//option[@selected]", BRP.debug)
        if selected and session == self.dropdown_to_session(selected.xpath_single('./text()')):
            return selected.xpath_single('./@value')
        # check if all links match
        # https://apps.azleg.gov/BillStatus/BillOverview/69922?SessionId=119

        select = bill_list_doc.xpath_single(u"//div[@id='top-bar']//select[@class='selectSession']")
        for option in select.xpath(u"./option"):
            text = option.text_content()
            value = option.get_attrib(u"value")
            if not text:
                continue
            if self.dropdown_to_session(text) == session:
                break
        else:
            raise ValueError(u"Unable to determine session " + session)

        checked = False
        for link in bill_list_doc.xpath("//div[@class='PRTable']//table//tr/td/a/@href"):
            if u'?' not in link:
                continue
            querystring = urlparse.parse_qs(link.split(u'?', 1)[1])
            session_id = querystring.get('SessionId')
            if not session_id:
                continue
            if not checked:
                checked = True
            if session_id[0] != value:
                logging.critical(u"Checking for {}, Found {}".format(
                    value, session_id[0]
                ))
                return None
        if not checked:
            return None
        return value


    def load_session_page(self):
        '''Return an ElementWrapper with removed invalid text. '''
        response = self.scraper.http_request(bill_list_url)
        source = response.text
        source = re.sub(r"<\?php.*?\?>", "", source)
        source = re.sub(r"<!--.*?--!>", "", source)
        return self.scraper.wrap_html(bill_list_url, source, BRP.bill_list)

    def change_session(self, session, bill_list_doc):
        '''Attempt to set a new session. Raises value error on failure. '''
        session_dict = {}
        select = bill_list_doc.xpath_single(u"//div[@id='top-bar']//select[@class='selectSession']")
        session_index = 0
        for option in select.xpath(u"./option")[::-1]:
            if session != self.dropdown_to_session(option.text_content()):
                continue
            value = option.get_attrib(u"value")
            self.scraper.http_request(
                update_bill_list_session_url, method=u"POST",
                request_args={u"data": {u"sessionID": value}})
            return
        raise ValueError("Unable to load session: " + session)

    def dropdown_to_session(self, text):
        """
        Decrypts session dropdown text to FN session format.

        The session name text looks like:
        "2018 - Fifty-third Legislature - Second Regular Session"
        or
        "2018 - Fifty-third Legislature - First Special Session"

        So, we want to extract the year - which are the first 4 digits,
        and the session type (which comes at the end)

        If we cannot parse the session description text, we return None.
        It is the caller's responsibility to appropriately log errors
        if a session it is looking for cannot be found.
        """
        m = re.match(
            r"^(\d{4}) - \S+ Legislature - (\w+) (Special|Regular) Session$", text, re.UNICODE | re.IGNORECASE)
        if not m:
            return None
        year, session_ordinal, session_type = m.groups()
        if session_type == "Regular":
            return '{0}{0}r'.format(year)
        elif session_type == "Special":
            try:
                special_session_num = ORDINALS.index(session_ordinal.lower()) + 1
            except ValueError:
                raise ValueError("Unable to decode session text: '{}' - unknown ordinal".format(text))
            return '{year}{year}ss{sess_num}'.format(year=year, sess_num=special_session_num)
        else:
            raise Exception("bad session_type: {}".format(session_type))

    @staticmethod
    def get_bill_type(bill_id):
        """
        borrowed directly from our old scraper
        :param bill_id: e.g. HB 123, SB 456
        :return: bill type: bill, resolution, memorial, etc.
        """
        prefix = re.match(r"([a-z]*)", bill_id.lower()).group()
        if prefix in bill_types:
            return bill_types[prefix]
        else:
            return u"bill"

    def map(self, bill, field, json_data, transformer, **kwargs):
        """
        This function maps transformed json_data to the specific field in bill object
        :param bill: bill object currently scraping
        :param field: to which field of the bill object the data should be mapped to
        :param json_data: the specific part of json data to be passed into transformer
                note that this json data will be looped over, and each item in the loop will be
                processed by transformer
        :param transformer: takes in one element in the json_data list, and transform it into an object to
                be added to the bill object. e.g. data --> a single vote object
        :return:
        """
        # the mapper function is field-specific, because some fields in the bill object are
        # stored as a list while some just stored as a simple string/object.
        mapper = self.get_mapper(field, bill)
        if not isinstance(json_data, list):
            # this is added just in case the json_data is not a list
            json_data_list = [json_data]
        else:
            json_data_list = json_data

        for json_data in json_data_list:
            if not json_data:
                continue
            transformed_data = transformer(json_data, **kwargs)
            if not isinstance(transformed_data, list):
                transformed_data = [transformed_data]
            for entry in transformed_data:
                if not entry:
                    continue
                mapper(entry)

    def get_json(self, query):
        json_file = self.scraper.url_to_json(query, BRP.json)
        return json_file


    @staticmethod
    def get_mapper(field, bill):
        """
        Return the proper mapper basing on the field chosen
        :param field: a field to be populated in the bill object (e.g. "votes", "documents")
        :param bill: bill object currently being scraped
        :return: proper mapper function
        """

        # currently all fields (other than title, chamber, session, bill_id, etc. which were used to
        # construct the bill object) are stored as lists except for "summary"
        def list_mapper(element):
            if field not in bill:
                bill[field] = []
            bill[field].append(element)

        def single_item_mapper(element):
            bill[field] = element

        return single_item_mapper if field == u"summary" else list_mapper

    def action_transformer(self, json_data):
        action_key = json_data[u"DateType"]
        actor = u"lower" if json_data[u"Body"] == u"H" else u"upper"
        other = json_data[u"Other"]
        body = json_data[u"Body"]
        comments = json_data[u"Comments"]
        vote_result = json_data[u"Action"]
        action_date_string = json_data[u"SortedDate"]
        action_date_string = re.match(r"(\d+-\d+-\d+)T", action_date_string).group(1)
        action_date = datetime.datetime.strptime(action_date_string, u"%Y-%m-%d")
        action_text = self.bill_action_text_mapper(action_key, other, body, comments, vote_result)
        if not action_text:
            return None
        action = dict(actor=actor, action=action_text, date=action_date)
        return action

    @staticmethod
    def standing_action_transformer(standing_action_json):
        actions = []
        committee = standing_action_json[u"Committee"][u"CommitteeName"]
        actor = u"lower" if standing_action_json[u"Committee"][u"LegislativeBody"] == u"H" else u"upper"
        if standing_action_json[u"AssignedDate"] is not None:
            assigned_date_string = standing_action_json[u"AssignedDate"].split(u"T")[0]
            assigned_date = datetime.datetime.strptime(assigned_date_string, u"%Y-%m-%d")
            action_text = u"Assigned to {} Committee".format(committee)
            actions.append(dict(actor=actor, action=action_text, date=assigned_date))
        if standing_action_json[u"ReportDate"] is not None:
            report_date_string = standing_action_json[u"ReportDate"].split(u"T")[0]
            report_date = datetime.datetime.strptime(report_date_string, u"%Y-%m-%d")
            action_description = standing_action_json[u"ActionDescription"]
            action_text = u"Reported {} out of {} Committee".format(action_description, committee)
            actions.append(dict(actor=actor, action=action_text, date=report_date))
        return actions

    @staticmethod
    def vote_transformer(json_data):
        if not json_data[u"Votes"]:
            return None
        yes_voters = []
        no_voters = []
        other_voters = []
        motion = json_data[u"ActionDescription"]
        chamber = u"lower" if json_data[u"Committee"][u"LegislativeBody"] == u"H" else u"upper"
        for vote in json_data[u"Votes"]:
            if vote[u"Vote"] == u"Y":
                yes_voters.append(vote[u"Legislator"][u"FullName"])
            elif vote[u"Vote"] == u"N":
                no_voters.append(vote[u"Legislator"][u"FullName"])
            else:
                other_voters.append(vote[u"Legislator"][u"FullName"])
        date_string = json_data[u"ReportDate"].split(u"T")[0]
        date = datetime.datetime.strptime(date_string, u"%Y-%m-%d")
        action = json_data[u"Action"]
        passed = vote_abbr_to_result_map[action] \
            if action in vote_abbr_to_result_map \
            else len(yes_voters) > len(no_voters)
        vote = Vote(chamber, date, motion, passed, len(yes_voters), len(no_voters), len(other_voters))
        for voters, method in [(yes_voters, vote.yes), (no_voters, vote.no), (other_voters, vote.other)]:
            for voter in voters:
                method(voter)
        return vote

    def version_transformer(self, document_info):
        name = document_info[u"DocumentName"]
        html_url = document_info[u"HtmlPath"]
        download_id, _, doc_ids = self.scraper.register_download_and_documents(
            html_url, BRP.bill_versions, self.scraper.extraction_type.html, False,
            self.version_amendment_html_parser, parser_args={u"url": html_url})
        doc_service_document = Doc_service_document(name, u"version", u"complete", download_id, doc_id=doc_ids[0])
        return doc_service_document

    def amendment_transformer(self, document_info):
        """
        takes in a document_info json object and returns a doc service document object for the amendment
        :param document_info: json object containing amendment info
        :return: doc service document object representing the amendment
        """
        name = document_info[u"DocumentName"]
        # unlike version bills which always have html format, the available format for amendments varies
        # we prefer html over pdf but will use pdf if html is not available
        # preferred_url is the url for the format with the highest preference in all available formats
        # extraction_type and parser are the proper extraction type and parser function for the preferred_url format
        preferred_url = None
        extraction_type = None
        parser = None
        preferred_formats = [(u"HtmlPath", self.scraper.extraction_type.html, self.version_amendment_html_parser),
                             (u"PdfPath", self.scraper.extraction_type.text_pdf, None)]
        for preferred_format, preferred_extraction_type, preferred_parser in preferred_formats:
            if document_info[preferred_format]:
                preferred_url = document_info[preferred_format]
                if preferred_format == u"PdfPath":  # have to prefix pdf urls with the domain
                    preferred_url = u"https://apps.azleg.gov" + preferred_url
                extraction_type = preferred_extraction_type
                parser = preferred_parser
                break
        try:
            assert preferred_url is not None
        except AssertionError:
            logger.warning(u"No html or pdf format available for document {}".format(name))
            return None

        try:
            download_id, _, doc_ids = self.scraper.register_download_and_documents(
                preferred_url, BRP.bill_versions, extraction_type, False, parser, parser_args={"url": preferred_url})
            assert download_id is not None and doc_ids and doc_ids[0]
        except AssertionError:
            logger.warning(u"Failed to process document {} from {}. This is likely to be because the website's "
                           u"API gives an image pdf url".format(name, preferred_url))
            # in other cases, the pdf might end up being an image pdf:
            # https://apps.azleg.gov/BillStatus/GetDocumentPdf/440634
            return None

        doc_service_document = Doc_service_document(name, u"amendment", u"complete", download_id, doc_id=doc_ids[0])
        return doc_service_document

    def misc_transformer(self, document_info):
        name = document_info[u"DocumentName"]
        pdf_url = api_domain + document_info[u"PdfPath"]
        download_id = self.scraper.download_and_register(pdf_url, BRP.bill_documents, True)
        doc_service_document = Doc_service_document(name, u"summary", u"partial", download_id)
        return doc_service_document

    @staticmethod
    def bill_action_text_mapper(action_key, other, body, comments, vote_result):
        # this entire function is based on the website's html view
        # they bind data returned from their API and fill the html
        # reference: https://apps.azleg.gov/BillStatus/BillOverview/67298?SessionId=115
        # it's cleaner when viewed with "debugger" in Firefox or "sources" in Chrome
        # there might be ways where we can aggregate/reorder all these ugly if statements, but I split it
        # out since it's easier to compare with their js code in case they decided to change their code.

        location_map = {u"H": u"House", u"S": u"Senate"}
        if action_key in [u"FIRST", u"SECOND", u"THIRD", u"FINAL"]:
            if vote_result:
                return u"{} {} {} Reading".format(vote_result, location_map[body], action_key.title())
            else:
                return u"{} {} Reading".format(location_map[body], action_key.title())

        if action_key == u"TRANSMIT":
            return u"Transmit to {}".format(location_map[body])

        caucus_map = {u"MAJCAUCUS": {u"H": u"House Majority Caucus",
                                     u"S": u"Senate Republican Caucus"},
                      u"MINCAUCUS": {u"H": u"House Minority Caucus",
                                     u"S": u"Senate Democrat Caucus"}}
        caucus_result_map = {u"Y": u"Yes", u"N": u"No", u"H": u"Held"}
        if action_key in caucus_map:
            if other not in caucus_result_map:
                return caucus_map[action_key][body]
            return u"{} - {}".format(caucus_map[action_key][body], caucus_result_map[other])

        if action_key == u"CONSENT":
            return u"Consent Calendar - {}".format(u"No" if other == u"0" else u"Yes")

        if action_key == u"_STANDING":
            return None

        if action_key in [u"COW", u"SCOW"]:
            return u"COW action"

        motion_map = {u"MISC": u"Misc Motion",
                      u"MOTION": u"Motion to amend"}
        if action_key in motion_map:
            return motion_map[action_key]

        if action_key == u"VETO":
            return u"{} Veto Override".format(location_map[body])

        if action_key == u"SEC OF STATE":
            return u"Transmit to Sec. of State"

        if action_key == u"DISCHARGE":
            return u"{} CC Discharge Date".format(location_map[body])

        if action_key == u"GOVERNOR":
            return u"Transmit to Governor"

        if action_key == u"MINORITYREPORT":
            return u"{} Minority Report Date".format(location_map[body])

        if action_key == u"CONFREPORT":
            return u"Conf Committee Report Date. Recommendation: {}".format(comments)

        if action_key == u"CONFADOPTED":
            return u"{} CC Adopted Date".format(location_map[body])

        if action_key == u"CONFCAUCUS":
            return u"{} CC Rpt Caucus Date".format(location_map[body])

        if action_key == u"CONFTYPECHG":
            return comments

        if action_key == u"CONFMEMCHANGE":
            return u"{} Member Change Date".format(location_map[body])

        reconsider_map = {u"RECON_3RD": u"RECONSIDER THIRD",
                          u"RECON_FNL": u"RECONSIDER FINAL"}
        if action_key in reconsider_map:
            return u"{} {}".format(location_map[body], reconsider_map[action_key])

        if action_key == u"CONCUR":
            return u"{} Concur".format(location_map[body])

        if action_key == u"CONFRECOMMEND":
            return u"Concurrence Recommended" if other == u"C" \
                else (u"Conference Committee Recommended" if other == u"R"
                      else u"Referred To Committee")

        if action_key == u"CONF":
            return u"{} Conf Committee Appointed Date".format(location_map[body])

        if action_key == u"CONFMEM":
            return u"{} Members: {}".format(location_map[body], other)

        logger.warning(u"Action key {} was not properly handled. No action added basing on it".format(action_key))
        return None

    def version_amendment_html_parser(self, element_wrapper, parser_args=None):
        source = etree.tostring(element_wrapper.element)
        # stricken text are enclosed with a span tag with some inline css "color:red"
        # remove before doing any text extraction
        source = re.sub(r"<span style=\"color:red\".*?</span>", "", source)
        url = parser_args[u"url"]
        element_wrapper = self.scraper.wrap_html(url, source, BRP.bill_documents)
        text_doc = element_wrapper.xpath_single(u"//div[@class='WordSection2']", BRP.bill_documents)
        if not text_doc:
            text_doc = element_wrapper.xpath_single(u"//div[@class='WordSection1']", BRP.bill_documents)
        text = text_doc.text_content()

        # text normalization
        lines = text.replace(u"\r\n", u"\n").split(u"\n")
        lines = [re.sub(r"\s+", " ", line).strip() for line in lines]
        text = u"\n".join(lines)

        return [ScraperDocument(text)]

    def additional_vote_transformer(self, floor_headers_json, **kwargs):
        bill_status_id = kwargs.get(u"bill_status_id")
        if int(floor_headers_json[u"TotalVotes"]) == 0:
            return None
        vote_chamber = u"lower" if floor_headers_json[u"LegislativeBody"] == u"H" else u"upper"
        action_id = floor_headers_json[u"BillStatusActionId"]
        motion = floor_headers_json[u"CommitteeName"]
        vote_date_raw = floor_headers_json[u"ActionDate"]
        vote_date_raw = vote_date_raw.split(u"T")[0]
        vote_date = datetime.datetime.strptime(vote_date_raw, u"%Y-%m-%d")
        vote_details_url = u"https://apps.azleg.gov/api/BillStatusFloorAction/?" \
                           u"billStatusId={}&billStatusActionId={}&includeVotes=true". \
            format(bill_status_id, action_id)
        vote_details_json = self.get_json(vote_details_url)[0]
        yes_voters = []
        no_voters = []
        other_voters = []
        for vote_info in vote_details_json[u"Votes"]:
            cast = vote_info[u"Vote"]
            voter_name = vote_info[u"Legislator"][u"FullName"]
            if cast == u"Y":
                yes_voters.append(voter_name)
            elif cast == u"N":
                no_voters.append(voter_name)
            else:
                other_voters.append(voter_name)

        vote_result_text = vote_details_json[u"Action"]
        passed = True if u"pass" in vote_result_text.lower() else len(yes_voters) > len(no_voters)

        vote = Vote(vote_chamber, vote_date, motion, passed, len(yes_voters), len(no_voters), len(other_voters))
        for voters, method in [(yes_voters, vote.yes), (no_voters, vote.no), (other_voters, vote.other)]:
            for voter in voters:
                method(voter)
        return vote

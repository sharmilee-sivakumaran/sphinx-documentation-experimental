'''
GA Bill Scraper

https://drive.google.com/file/d/1OReJ8e_2PuFRA8sm1Mo5-wq2g2rUhAN9/view

Rewritten to use json endpoints per DI-1900. Issue was that end of February
2018 GA Webmaster blocked all AWS instance outside of 8pm-5am. Site updates at
2am so we have a very small window for scraping.

To fascilitate this, we are skipping documents if we've successfully extracted
them in a previous scrape.

Example URLs:
 - Sessions:
    http://webservices.legis.ga.gov/GGAServices/Session/Service.svc/query/json
 - Members:
    http://webservices.legis.ga.gov/GGAServices/Members/Service.svc/query/json
 - All Bills for session:
    http://webservices.legis.ga.gov/GGAServices/Legislation/Service.svc/query/indices/forSession/25/json
 - Bill:
    http://webservices.legis.ga.gov/GGAServices/Legislation/Service.svc/query/details/forSession/25/hb/87/json
 - Vote Rollcall:
    http://webservices.legis.ga.gov/GGAServices/Votes/Service.svc/query/forVote/14945/json
'''

from __future__ import absolute_import

from collections import defaultdict
import datetime
import json
import logging
import pytz
import re

from fn_scrapers.datatypes.bills.common import (
    BillScraper, Bill, Vote, BillReportingPolicy as BRP, Doc_service_document)
from fn_scrapers.datatypes.bills.common.normalize import (
    get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id)
from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger(u'GABillScraper')

@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-GA", group="fnleg")
class GABillScraper(BillScraper):
    root = 'http://webservices.legis.ga.gov/GGAServices/'
    sess_url = 'Session/Service.svc/query/json'
    member_url = 'Members/Service.svc/query/json'
    leg_url = 'Legislation/Service.svc/query/indices/forSession/{}/json'
    bill_url = 'Legislation/Service.svc/query/details/{}/json'
    bill_lookup_url = (
        'Legislation/Service.svc/query/details/forSession/{}/{}/{}/json')
    vote_url = 'Votes/Service.svc/query/forVote/{}/json'
    bill_source_url = (
        'http://www.legis.ga.gov/legislation/en-US/display/{}/{}/{}')

    exec_re = re.compile(r'(?:by|to) Governor|Effective Date|Act \d+')

    # from GGA
    DOCUMENT_ENUM = {
        1: 'sb',
        2: 'sr',
        3: 'hr',
        4: 'hb',
    }
    SPONSOR_ENUM = {
        1: 'senate_coauthor',
        3: 'senate_sponsor',
        4: 'senate_author',
        5: 'house_author',
        7: 'house_sponsor',
    }
    STATUS_ENUM = {
        0: 'any',
        1: 'prefiled',
        2: 'firstreadinhouse',
        3: 'firstreadinsenate',
        4: 'signedbygovernor',
        5: 'houseaction',
        6: 'senateaction',
        7: 'houseorsenateaction',
    }
    BRANCH_ENUM = {
        0: 'lower',
        1: 'upper'
    }
    VOTE_ENUM = {
        0: Vote.yes,
        1: Vote.no,
        2: Vote.other, # excused
        3: Vote.other, # not voting
        4: None # unknown
    }
    COMMITTEE_ENUM = {
        2: 'house',
        3: 'senate'
    }

    session_list = [] # cached session list
    members = {} # cached member list
    session_lookup = {}

    session_pattern = re.compile(
        r'^(?P<year>\d{4}(?:-\d{4})?) (?:(?P<offset>\d+)[a-z]+ )?'
        r'(?P<sess_type>\w+) Session$')
    date_pattern = re.compile(r'/Date\((\d+)([+-]\d+)\)/')

    def __init__(self):
        super(GABillScraper, self).__init__("ga")
        self.set_cache_objects()
        # some votes are for multiple bills (?). this cache is for votes where
        # multiple bills are detected.
        self.rollcalls = {}

    def json(self, url, brp=None):
        '''Shorten self.scraper.url_to_json(url, brp) call'''
        logger.info(u"Requesting %s", url)
        return self.scraper.url_to_json(url, brp or BRP.critical)

    def scrape_bill_ids(self, session,):
        sess_id = self.get_session(session)['Id']
        bill_list = self.json(self.root + self.leg_url.format(sess_id))
        bill_ids = [bill['Description'] for bill in bill_list]

        logger.info(u"A total of %s bill ids scraped for session %s",
                    len(bill_ids), session)

        return bill_ids

    def set_cache_objects(self):
        '''Sets the one-time load cache objects (session_list, member_list). '''
        if self.__class__.session_list:
            return
        self.set_sessions(self.json(self.root + self.sess_url))
        members = self.json(self.root + self.member_url)
        self.__class__.members = {member['Id']: member for member in members}

    @classmethod
    def get_session(cls, session):
        '''Retrieves a session object from the . '''
        try:
            return cls.session_lookup[session]
        except KeyError:
            raise ValueError("Session {} was not found.".format(session))

    @classmethod
    def set_sessions(cls, session_list):
        '''Builds the session_lookup dictionary. '''
        cls.session_list = session_list
        url_pattern = re.compile('http://www.legis.ga.gov/Legislation/([^/]+)/')

        sessions = {}
        specials = []
        for session in cls.session_list:
            match = cls.session_pattern.match(session['Description'])
            session.update(match.groupdict())
            session['slug'] = url_pattern.search(session['Library']).group(1)
            if "Regular" == session['sess_type']:
                # we can short-circuit regular sessions
                key = session['year'].replace('-', '') + 'r'
                sessions[key] = session
            if "Special" == session['sess_type']:
                # so annoyingly this list is sorted year descending, special
                # session ascending.
                session['year'] = int(session['year'])
                session['offset'] = int(session['offset'] or 1)
                session['years'] = session['year'] - 1, session['year']
                if session['year'] % 2: # if odd
                    session['years'] = session['year'], session['year'] + 1
                specials.append(session)
        specials = sorted(specials, key=lambda s: (s['year'], s['offset']))
        session_count = defaultdict(int)
        for session in specials:
            years = '{}{}'.format(*session['years'])
            session_count[years] += 1
            key = '{}ss{}'.format(years, session_count[years])
            sessions[key] = session
        cls.session_lookup = sessions

    def scrape_bill(self, session, bill_id, bill_info=None):
        '''
        Scrape a bill.

        Args:
            session: FN session ID (20172018r)
            bill_id: A FN external ID ("HR 123")
        '''
        logger.info("Scraping bill %s/%s", session, bill_id)
        bill_type, bill_number = re.match(
            r'([A-Za-z]+)\s?([0-9]+[A-Za-z]*)', bill_id).groups()
        sess_id = self.get_session(session)['Id']
        bill_doc = self.json(self.root + self.bill_lookup_url.format(
            sess_id, bill_type.lower(), bill_number
        ))
        
        bill = self.build_bill(session, bill_doc)

        self.scrape_sponsors(bill, bill_doc)
        self.scrape_actions(bill, bill_doc)
        self.scrape_summary(bill, bill_doc)
        self.scrape_votes(bill, bill_doc)
        self.scrape_versions(bill, bill_doc)

        self.save_bill(bill)

        
    @classmethod
    def build_bill(cls, session, bill_doc):
        '''Returns a bill object bsed on the bill document. '''
        doctype = cls.DOCUMENT_ENUM[bill_doc['DocumentType']]
        bill_id = '{} {}'.format(doctype.upper(), bill_doc['Number'])
        title = bill_doc['Caption']
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        bill = Bill(session, chamber, bill_id, title, bill_type)
        
        bill.add_source(cls.bill_source_url.format(
            cls.get_session(session)['slug'], doctype.upper(),
            bill_doc['Number']))
        
        return bill

    @classmethod
    def scrape_sponsors(cls, bill, bill_doc):
        '''
        Adds any sponsors (Authors/Sponsor) to the bill. 
        
        GA has authors and sponsors. An author is a primary sponsor while a
        sponsor is a cross-chamber primary sponsor. The only cosponsors are
        senate coauthors.
        '''
        sponsor_pattern = re.compile(r'^(.*?)\s+\d+\w+$')
        for author in bill_doc.get('Authors', []):
            name = sponsor_pattern.match(author['MemberDescription']).group(1)
            is_primary =  cls.SPONSOR_ENUM[author['Type']] != 'senate_coauthor'
            bill.add_sponsor(u"primary" if is_primary else 'cosponsor', name)
        sponsor = bill_doc.get('Sponsor')
        if sponsor:
            name = sponsor_pattern.match(sponsor['MemberDescription']).group(1)
            bill.add_sponsor(u"primary", name)

    @classmethod
    def scrape_actions(cls, bill, bill_doc):
        '''
        Adds bill actions to bill object.

        Actions will also have committee's associated with them. How GA handles
        committees: each bill has a house and senate committee assigned to it
        (only one of each). Then the actions will list the committees findings
        such as "House Committee Favorably Reported". This is a problem with
        FN-Mapper as the committee information is not contained in the action,
        so they are explicitly added here.
        '''
        for action in bill_doc.get('StatusHistory', []):
            date = cls.parse_time(action['Date']).date()
            actor = bill['chamber']
            action_text = action['Description'].strip()
            action_words = action_text.lower().split(' ')
            if action_words[0:1] == [u"house"]:
                actor = u"lower"
            if action_words[0:1] == [u"senate"]:
                actor = u"upper"
            if cls.exec_re.search(action_text):
                actor = u"executive"
            # committee extraction
            kwargs = {}
            if action_words[1:2] == ['committee']:
                for committee in bill_doc.get('Committees', []):
                    if cls.COMMITTEE_ENUM[committee['Type']] == action_words[0]:
                        kwargs['related_entities'] = [{
                            "name": committee['Name'], 
                            "type": "committee"
                        }]
            bill.add_action(actor, action_text, date, **kwargs)

    @classmethod
    def parse_time(cls, dt_str):
        '''
        Takes a datetime string of "/Date(1493697599000-0400)/" and returns
        a datetime object.  Datetime is UTC.
        '''
        match = cls.date_pattern.match(dt_str)
        dtime = datetime.datetime.fromtimestamp(int(match.group(1))/1000.0)
        return pytz.utc.localize(dtime)

    @classmethod
    def scrape_summary(cls, bill, bill_doc):
        '''Adds summary to bill if summary exists. '''
        summary = bill_doc.get('Summary', '').strip()
        if summary:
            bill.add_summary(summary)

    def scrape_votes(self, bill, bill_doc):
        '''Iterates across bill_doc['Votes']'''
        def _vote_sort(vote):
            return self.parse_time(vote['Date'])
        votes = bill_doc.get('Votes') or []
        for vote in sorted(votes, key=_vote_sort, reverse=True):
            rollcall = self.get_rollcall(vote['VoteId'])
            bill.add_vote(self.scrape_vote(vote, rollcall))

    def get_rollcall(self, vote_id):
        '''
        Returns a rollcall object, conditionally caching it in case it is used
        on multiple bills.
        '''
        if vote_id in self.rollcalls:
            return self.rollcalls[vote_id]
        rollcall = self.json(self.root + self.vote_url.format(vote_id))
        if len(rollcall.get('Legislation', [])) > 1:
            self.rollcalls[vote_id] = rollcall
        return rollcall

    @classmethod
    def scrape_vote(cls, vote_rec, rollcall):
        '''
        Returns a vote object built from a vote_rec (from bill document) and
        rollcalll object. Uses GABillScraper.members static collection.
        '''
        vote = Vote(
            cls.BRANCH_ENUM[rollcall['Branch']],
            cls.parse_time(rollcall['Date']).date(),
            '{Description} ({Caption})'.format(**vote_rec),
            rollcall['Yeas'] > rollcall['Nays'],
            rollcall['Yeas'],
            rollcall['Nays'],
            rollcall.get('NotVoting', 0) + rollcall.get('Excused', 0)
        )

        for record in rollcall.get('Votes', []):
            # handle vacant members:
            # {"Member": {"Id": 0, "Name": "VACANT"},"MemberVoted": 3}
            if not record['Member']['Id']:
                logger.warning("Unknown member: %s", record['Member']['Name'])
                continue
            member = cls.members[record['Member']['Id']]
            name = '{First} {Last}'.format(**member['Name'])
            callback = cls.VOTE_ENUM.get(record['MemberVoted'])
            if callback:
                callback.__call__(vote, name)
                continue
            logger.warning('Unknown vote: Id: {} Member: {} ({}): {}'.format(
                rollcall['VoteId'], record['Member']['Id'], name,
                record['MemberVoted']
            ))

        return vote

    def scrape_versions(self, bill, bill_doc):
        '''
        Adds versions to the bill object. NOTE: Only new documents are scraped 
        (new being defined as not having a document ID). Already extracted
        documents will simply use the information returned by
        last_download_info(). To override this behavior, run the scraper with
        the `--s3_skip_checks` argument.
        '''
        for version in bill_doc['Versions']:
            url = version['Url']
            name = version['Description'].strip()
            doc_type = u"amendment" if u"amend" in name.lower() else u"version"
            ldi = self.scraper.doc_service_client.last_download_info(url)
            if ldi and ldi.documentIds and not self.scraper.s3_skip_checks:
                # if we've successfully extracted in the past, no need to try
                # again. TODO: Update this with more advanced http conditional
                # request logic as that will more accurately capture changing
                # documents.
                logger.info(u"[CACHED] DL/Doc IDs: %s/%s: %s", ldi.id,
                            ldi.documentIds[0], url)
                bill.add_doc_service_document(Doc_service_document(
                    name, doc_type, u"complete", ldi.id,
                    doc_id=ldi.documentIds[0]))
                continue
            dl_docs = self.scraper.register_download_and_documents(
                url, BRP.bill_documents, self.scraper.extraction_type.text_pdf,
                True, content_type=u"application/pdf")
            dl_id, _, doc_ids = dl_docs
            logger.info(u"DL/Doc IDs: %s/%s: %s", dl_id, dl_docs[0], url)
            if doc_ids and doc_ids[0]:
                bill.add_doc_service_document(Doc_service_document(
                    name, doc_type, u"complete", dl_id, doc_id=doc_ids[0]))
            else:
                logger.warning(u"Failed to process file at %s. Skip it", url)

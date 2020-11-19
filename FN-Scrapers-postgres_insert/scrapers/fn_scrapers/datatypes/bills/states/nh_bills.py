"""
:class NHBillScraper: scrapes New Hampshire Bills

*******
!!!DO NOT RUN ON SPECIAL SESSIONS!!!

NH doesn't have any identifier for special sessions on their website, and not even in the DB
so there's no way to separata special session bills from regular session ones
*******

To run the scraper locally, copy NHBillScraper section from build_fnscraper_config function of
FiscalNote-DevOps/deploy_by_role/fabfile.py into the local config.yaml

This scraper uses a MS SQL public DB provided by NH to get the bill data.
The DB has most of the infomation required by us to save a bill. Only the current session
bills can be scraped using this scraper.

They have some information missing from their database such as:
- Action dates in the Docket view
- Ammendment documents
- Hearing documents
- Bills without an associated LSR Number

So the scraper still goes to the docket page per bill to get the:
- Action dates
- Ammendment documents
- Hearing documents

An Example docket page:
http://www.gencourt.state.nh.us/bill_status/bill_docket.aspx?lsr=0037&sy=2018&txtsessionyear=2018

This is keeping this scraper from completely running over the database and getting blocked
in case it is run outside of working hours

The Database has several views that provides us the needed information

- Docket: This view contains the action timeline for every bill starting from 1989 to the
          current session year. The OrderDate field used to contain the action dates until 2017.
- Legislation: This view contains the bill information, such as the title, chmaber,
               description, bill type, etc. for the current session only
- LegislationText: This view contains all the unique version documents associated with a bill
- DocumentVersion: This view contains the Version document descriptions for the the version documents
                   It also has the precedence order of the documents which helps in identifying
                   the Current Version document
- Sponsors: This view contains the sponsor and co-sponsor information about for the bills
- Legislators: This view contains the names, chambers and all the related information for the legislators
               of NH. This is used when we need the names of the sponsors and voters for bills.
- RollCallSummary: Contains the different votes associated with a bill along with the vote counts
- RollCallHistory: Contains the voter details for every vote in the RollCallSummary view. Sometimes
                   the actual number of people who voted in a particular category doesn't match the
                   counts provided in the RollCallSummary view.
"""
from __future__ import absolute_import

import os
import re
import hashlib
from datetime import datetime, timedelta
import injector

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_scraperutils.doc_service.util import ScraperDocument
from dateutil.parser import parse
import logging
from tempfile import TemporaryFile

from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.api.resources import ScraperConfig

from fn_service.server import BlockingEventLogger, fmt

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select, and_
from sqlalchemy.engine import reflection


chamber_dict = {'H': 'lower',
                'C': 'lower',
                'S': 'upper'
                }

type_dict = {
            'CACR': 'constitutional_amendment',
            'HA': 'bill',
            'HB': 'bill',
            'HCR': 'concurrent_resolution',
            'HJR': 'joint_resolution',
            'HR': 'resolution',
            'SB': 'bill',
            'SR': 'resolution',
            # This is supposed to be a bill in special session
            'SSSB': 'bill',
            }


def html_parser(element_wrapper):
    text = element_wrapper.xpath_single("//body").text_content()
    return [ScraperDocument(text)]


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-NH", group="fnleg")
class NHBillScraper(BillScraper):
    """
    Scrape New Hampshire Bills
    """
    @injector.inject(
        logger=BlockingEventLogger,
        config=ScraperConfig
    )
    def __init__(self, logger, config):
        super(NHBillScraper, self).__init__("nh")
        self.logger = logger
        self.event_keys = {
            'locality': self.locality
        }
        """
        Fetch the public credentials for NH from the config.yaml file to create DB engine
        """
        engine = create_engine("mssql+pymssql://{}:{}@{}:1433/{}?charset=utf8".format(
                config["uid"],
                config["pwd"],
                config["server"],
                config["db"],
            ))

        meta = MetaData()

        """
        Loading all the needed views from the NH database
        """
        global leg, docket, leg_text, sponsors, legislators, rollcall_sum, rollcall_hist
        global doc_ver, bill_text, spon_names, vote_people, conn, version_codex
        leg = Table('Legislation', meta, autoload=True, autoload_with=engine)
        docket = Table('Docket', meta, autoload=True, autoload_with=engine)
        leg_text = Table('LegislationText', meta, autoload=True, autoload_with=engine)
        sponsors = Table('Sponsors', meta, autoload=True, autoload_with=engine)
        legislators = Table('Legislators', meta, autoload=True, autoload_with=engine)
        rollcall_sum = Table('RollCallSummary', meta, autoload=True, autoload_with=engine)
        rollcall_hist = Table('RollCallHistory', meta, autoload=True, autoload_with=engine)
        doc_ver = Table('DocumentVersion', meta, autoload=True, autoload_with=engine)

        bill_text = leg_text.join(doc_ver, leg_text.c.DocumentVersionID == doc_ver.c.DocumentVersionID)

        spon_names = sponsors.join(legislators, sponsors.c.employeeNo == legislators.c.Employeeno)

        vote_people = rollcall_hist.join(legislators, rollcall_hist.c.EmployeeNumber == legislators.c.Employeeno)

        conn = engine.connect()

        """
        Making a 'codex' to help map documents in the LegislationText to the version we need, since multiple
        copies of version documents need to be registered under different aliases to reflect what is present on the
        website and the DB only contains one copy of a version document.
        """
        stmt = select([
                doc_ver.c.DocumentVersion,
                doc_ver.c.V,
            ])

        rs = conn.execute(stmt)

        version_codex = {v: v_id for v_id, v in rs if v}

    def scrape_bill_ids(self, session):
        """
        NH Database has all of the bills listed in the 'Docket' view. It only stores information about sessions in
        the 'Legislation' view however. 'Legislation' doesn't contain all the bills, but we can assume it stores
        all the special session bills.
        For special session bills, we scrape the special sessions in 'Legislation'
        For regular bills, we scrape 'Docket' and then remove all the ones in special sessions
        """
        bill_ids = {}
        begin_year = int(session[0:4])
        end_year = int(session[4:8])
        session_type = self.get_session_type(session)

        if session_type == "R":
            stmt = select([docket.c.SessionYear, docket.c.CondensedBillNo]).where(and_(
                    begin_year <= docket.c.SessionYear,
                    docket.c.SessionYear <= end_year,
                ))

            rs = conn.execute(stmt)
            for year, bill_id in rs.fetchall():
                if bill_id not in bill_ids:
                    bill_ids[bill_id] = set()
                bill_ids[bill_id] |= set([str(year)])

        # TODO: Check when ss1 starts
        elif session_type != "R":
            # sessionyear is not capitalized in this table
            stmt = select([leg.c.sessionyear, leg.c.CondensedBillNo]).where(and_(
                begin_year <= leg.c.sessionyear,
                leg.c.sessionyear <= end_year,
                leg.c.SessionType == session_type
            ))

            rs = conn.execute(stmt)
            for year, bill_id in rs.fetchall():
                if bill_id not in bill_ids:
                    bill_ids[bill_id] = set()
                bill_ids[bill_id] |= set([str(year)])

        else:
            raise NotImplementedError("We only handle regular and special sessions")

        return bill_ids

    def get_session_type(self, session):
        if session.endswith("r"):
            return "R"
        elif re.search("ss\d", session):
            # TODO:
            """
            The special session bill had SessionType = R for some reason,
            not sure if NH meant to do it or not.
            """
            return "S"

    def scrape_bill(self, session, bill_id, **kwargs):
        bill_info = kwargs.get('bill_info')
        session_type = self.get_session_type(session)
        c_bill_id = re.sub(r'\s+', '', bill_id)
        self.event_keys['obj_id'] = bill_id

        # TODO: Make this more specific when we know what NH uses for ss1
        if session_type == "R":
            stmt = select([
                    leg.c.legislationID,
                    leg.c.lsr,
                    leg.c.documenttypecode,
                    leg.c.legislationnbr,
                    leg.c.sessionyear,
                    leg.c.LegislativeBody,
                    leg.c.LSRTitle,
                ]).where(and_(
                    leg.c.CondensedBillNo == c_bill_id,
                    leg.c.sessionyear.in_(list(bill_info)),
                    leg.c.SessionType == session_type
                ))
        else:
            stmt = select([
                    leg.c.legislationID,
                    leg.c.lsr,
                    leg.c.documenttypecode,
                    leg.c.legislationnbr,
                    leg.c.sessionyear,
                    leg.c.LegislativeBody,
                    leg.c.LSRTitle,
                ]).where(and_(
                    leg.c.CondensedBillNo == c_bill_id,
                    leg.c.sessionyear.in_(list(bill_info)),
                    leg.c.SessionType != "R"
                ))

        rs = conn.execute(stmt)

        leg_id = None
        lsr = None
        doc_type = None
        bill_no = None
        year = None
        chamber = None
        title = None

        row = rs.fetchone()
        if not row:
            """
            These are Withdrawen LSRs

            We don't know how to handle them right now.
            """
            self.logger.warning(__name__, "withdrawn_LSR", fmt("Unable to get {} from DB", bill_id),
                                event_keys=self.event_keys
                                )
            return

        leg_id, lsr, doc_type, bill_no, year, chamber, title = row

        if not doc_type:
            self.logger.warning(__name__, "document_type", fmt('Unable to get doc_type for {}', bill_id),
                                event_keys=self.event_keys
                                )
            doc_type = re.search(r'([A-Z]+)\d+').groups()
            bill_type = type_dict[doc_type]
        else:
            bill_type = type_dict[doc_type]

        if not title:
            self.logger.critical(__name__, "no_title", fmt('Unable to get title for {}', bill_id),
                                 event_keys=self.event_keys
                                 )
            return

        if not lsr:
            self.logger.critical(__name__, "no_LSR_number", fmt('Unable to get lsr for {}', bill_id),
                                 event_keys=self.event_keys
                                 )
            return

        chamber = chamber_dict[chamber]

        bill = Bill(session, chamber, bill_id, title, bill_type)
        bill.add_alternate_id("LSR {}-{}".format(year, lsr))

        status_url = 'http://www.gencourt.state.nh.us/bill_status/bill_status.aspx?'\
                     'lsr={0}&sy={1}&sortoption=billnumber&txtsessionyear={1}&txtlsrnumber={0}'.format(lsr, year)

        bill.add_source(status_url)

        stmt = select([
                legislators.c.FirstName,
                legislators.c.LastName,
                sponsors.c.PrimeSponsor,
                legislators.c.LegislativeBody,
            ]).select_from(spon_names).where(and_(
                sponsors.c.LegislationID == leg_id,
                sponsors.c.SponsorWithdrawn == False,
            ))

        rs = conn.execute(stmt)
        rs = rs.fetchall()

        if not rs:
            self.logger.warning(__name__, "no_sponsors", fmt('{} has no sponsors', bill_id),
                                event_keys=self.event_keys
                                )

        for fn, ln, prime, body in rs:
            bill.add_sponsor('primary' if prime else 'cosponsor', '{} {}'.format(fn, ln), chamber=chamber_dict[body])

        """
        DocumentVersion column contains the actual description of the Document version we need, but due to the way
        Pillar handles documents, we still use 'Introduced', 'Ought to Pass', 'Ought to Pass with Amendment' and
        'Current Version', to maintain backwards compatibilty.
        """
        stmt = select([
                leg_text.c.PDFImage,
                doc_ver.c.DocumentVersion,
                leg_text.c.Text,
            ]).select_from(bill_text).where(and_(
                leg_text.c.LegislationID == leg_id,
            )).order_by(
                doc_ver.c.SortOrder
            )

        rs = conn.execute(stmt)

        docs = rs.fetchall()
        total_docs = len(docs)

        """
        The doc_shelf is a dict to easily pick up the version copy we need to save it under the alias we need.
        This is needed for 'Ought to Pass' type of documents as they are ususally the previous version documents.
        """
        doc_type = "version"
        docservice_type = "complete"
        doc_shelf = {v_id: (i, doc, doc_text) for i, (doc, v_id, doc_text) in enumerate(docs)}

        """
        Action strings are present in the Docket table. We are currently missing Action Dates in the DB
        So for now scrpaing Action Dates from the docket page.

        NH used to provide actin dates in the OrderDate field before 2017. Now they don't have a specific
        field for them, starting from 2017-2018 session, they started adding values to statusorder, which
        enables us to put the actions in correct order.
        """

        docket_url = 'http://www.gencourt.state.nh.us/bill_status/bill_docket.aspx?'\
                     'lsr={0}&sy={1}&sortoption=billnumber&txtsessionyear={1}&txtlsrnumber={0}'.format(lsr, year)

        docket_page = self.scraper.url_to_lxml(docket_url, BRP.bill)

        action_dates = docket_page.xpath("//table[@id='Table1']//table")[0].xpath(".//tr/td[1]")[1:]

        stmt = select([
                docket.c.Description,
                docket.c.LegislativeBody,
            ]).where(and_(
                docket.c.SessionYear == year,
                docket.c.legislationid == leg_id,
            )).order_by(
                docket.c.statusorder
            )

        rs = conn.execute(stmt)

        rows = rs.fetchall()

        if len(rows) != len(action_dates):
            self.logger.critical(__name__, "action_dates",
                                 fmt('{} docket has different dates than website, ID: {}, SY: {}' +
                                     '\nDB: {}, website: {}',
                                     bill_id, leg_id, year, len(rows), len(action_dates)),
                                 event_keys=self.event_keys,
                                 )
            return
        else:
            self.add_documents(rows, action_dates, leg_id, year, doc_shelf, bill)

        # Current Version document is whatever version document is present last in the LegislationText View
        curr_link = 'http://www.gencourt.state.nh.us/bill_status/billText.aspx?'\
                    'sy={0}&id={1}&txtFormat=pdf&v=current'
        try:
            _, current_doc, current_doc_text = sorted(doc_shelf.values(), key=lambda x: x[0], reverse=True)[0]
        except Exception:
            # This means that the this bill_id is a prefile.
            self.logger.warning(__name__, "db_document_error",
                                "Unable to find current version",
                                event_keys=self.event_keys
                                )
            self.save_bill(bill)
            return
        download_id, doc_id = self.register_pdf_file(
                current_doc,
                curr_link.format(year, leg_id),
                text=current_doc_text,
            )
        if download_id:
            document = Doc_service_document(
                    "Current Version",
                    "version",
                    "complete",
                    download_id,
                    doc_id=doc_id
                )
            bill.add_doc_service_document(document)
        else:
            self.logger.critical(__name__, "version_document",
                                 "Unable to register version document with doc service",
                                 event_keys=self.event_keys
                                 )

        """
        BEGINNING OF HEAVY REQUESTS

        Highlighting in case we get access to these documents in the DB.
        The database currently doesn't have ammendment and hearing documents.
        So we are still fetching them from the website. This is one of the
        things that is blocking the scraper to completely run from the database.
        """
        doc_list = set()

        amend_table = docket_page.xpath("//a[contains(@href, 'txtFormat=amend')]", BRP.test)
        for amend_row in amend_table:
            amend_link = amend_row.get_attrib('href')
            if amend_link in doc_list:
                continue
            doc_list |= set([amend_link])
            amend_name = amend_row.text_content()

            amend_name = "Amendment %s" % amend_name
            download_id = None
            head_response = self.scraper.http_request(amend_link, method=u"HEAD")
            amend_mimetype = head_response.headers.get(u"content-type")
            try:
                if amend_mimetype == u'application/pdf':
                    download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(amend_link, BRP.bill_documents,
                                                                         self.scraper.extraction_type.text_pdf,
                                                                         False, should_skip_checks=True)
                elif u"text/html" in amend_mimetype:
                    download_id, _, doc_ids = \
                            self.scraper.register_download_and_documents(amend_link, BRP.bill_documents,
                                                                         self.scraper.extraction_type.html,
                                                                         False, html_parser)

                else:
                    self.logger.warning(__name__, "parse_error", fmt("Failed to parse document {}", amend_link),
                                        event_keys=self.event_keys,
                                        )

                if download_id is not None:
                    doc_service_document = Doc_service_document(amend_name, "amendment", "complete",
                                                                download_id, doc_id=doc_ids[0])
                    bill.add_doc_service_document(doc_service_document)
            except Exception as e:
                self.logger.warning(__name__, "document_error", fmt("Skipping amendment document {}", amend_name),
                                    event_keys=self.event_keys)

        hearing_table = docket_page.xpath("//a[contains(@href, 'HearingReport.aspx')]", BRP.test)
        for hearing_row in hearing_table:
            hearing_url = hearing_row.get_attrib('href')
            if hearing_url in doc_list:
                continue
            doc_list |= set([hearing_url])
            hearing_title = hearing_row.get_attrib('title')
            try:
                # NH sometimes displays an HTML page instead fo the expected PDF which has links to multiple
                # hearing reports on them.
                if 'mult=1' not in hearing_url:
                    download_id = self.scraper.download_and_register(hearing_url, BRP.bill_documents, True)
                    doc_service_document = Doc_service_document(
                        hearing_title,
                        "committee_document",
                        "partial",
                        download_id
                    )
                    bill.add_doc_service_document(doc_service_document)
                else:
                    hearing_doc_page = self.scraper.url_to_lxml(hearing_url, BRP.test)
                    reports = hearing_doc_page.xpath('//a', BRP.bill_documents)
                    for r in reports:
                        link = r.get_attrib('href')
                        if link in doc_list:
                            continue
                        doc_list |= set([link])
                        download_id = self.scraper.download_and_register(link, BRP.bill_documents, True)
                        doc_service_document = Doc_service_document(
                            u"{0} ({1})".format(hearing_title, r.text_content().strip()),
                            "committee_document",
                            "partial",
                            download_id
                        )
                        bill.add_doc_service_document(doc_service_document)
            except Exception as e:
                self.logger.warning(__name__, "document_error", fmt("Skipping hearing document {}", hearing_title),
                                    event_keys=self.event_keys)

        """
        ENDING OF HEAVY REQUESTS
        """

        stmt = select([
                rollcall_sum.c.VoteSequenceNumber,
                rollcall_sum.c.LegislativeBody,
                rollcall_sum.c.Yeas,
                rollcall_sum.c.Nays,
                rollcall_sum.c.Present,
                rollcall_sum.c.Absent,
                rollcall_sum.c.VoteDate,
                rollcall_sum.c.Question_Motion,
            ]).where(and_(
                rollcall_sum.c.SessionYear == year,
                rollcall_sum.c.CondensedBillNo == c_bill_id,
            ))

        rs = conn.execute(stmt)

        rs = rs.fetchall()

        if not rs:
            self.logger.info(__name__, fmt('{} has no votes', bill_id), event_keys=self.event_keys)
        else:
            for vs, body, Y, N, P, A, date, motion in rs:
                vote = None
                if body == u'S':
                    vote = Vote(chamber_dict[body], date, "{} #{}".format(motion, vs), Y > N, Y, N, P+A-(Y+N))
                else:
                    vote = Vote(chamber_dict[body], date, "{} #{}".format(motion, vs), Y > N, Y, N, P+A)
                vote_link = 'http://www.gencourt.state.nh.us/bill_status/Roll_calls/billstatus_rcdetails.aspx?' +\
                    'vs={1}&sy={0}&lb={2}&eb={3}{4}&sortoption=billnumber&txtsessionyear={0}'.format(
                        year,
                        vs,
                        body,
                        doc_type,
                        bill_no,
                    ) +\
                    '&txtlsrnumber={0}&ddlsponsors=&lsr={0}'.format(lsr)
                vote.add_source(vote_link)

                stmt = select([
                        legislators.c.FirstName,
                        legislators.c.LastName,
                        rollcall_hist.c.Vote,
                    ]).select_from(vote_people).where(and_(
                        rollcall_hist.c.VoteSequenceNumber == vs,
                        rollcall_hist.c.LegislativeBody == body,
                        rollcall_hist.c.SessionYear == year,
                        rollcall_hist.c.CondensedBillNo == c_bill_id,
                    ))

                v_rs = conn.execute(stmt)

                for fn, ln, v_type in v_rs.fetchall():
                    if v_type == 1:
                        vote.yes('{} {}'.format(fn, ln))
                    elif v_type == 2:
                        vote.no('{} {}'.format(fn, ln))
                    elif 2 < v_type < 7:
                        vote.other('{} {}'.format(fn, ln))

                """
                The following conditions are there just to verify that the number of people we have match
                the number specified in the RollCallSummary View. There are 6 cases where the numbers don't match
                for the session 20172018r.

                A warning is logged so that this can be verified manually if need be.
                """
                if vote['yes_count'] > 0 and ('yes_votes' not in vote or vote['yes_count'] != len(vote['yes_votes'])):
                    self.logger.warning(__name__, "vote_count", fmt(
                           "The count of YES votes doesn't match for {}" +
                           "\nVS: {}, body: {}, year: {}" +
                           "\nSum_count: {}, got_count: {}",
                           bill_id, vs, body, year, vote['yes_count'],
                           0 if 'yes_votes' not in vote else len(vote['yes_votes'])
                        ),
                        event_keys=self.event_keys,
                    )
                if vote['no_count'] > 0 and ('no_votes' not in vote or vote['no_count'] != len(vote['no_votes'])):
                    self.logger.warning(__name__, "vote_count", fmt(
                            "The count of NO votes doesn't match for {}" +
                            "\nVS: {}, body: {}, year: {}" +
                            "\nSum_count: {}, got_count: {}",
                            bill_id, vs, body, year, vote['no_count'],
                            0 if 'no_votes' not in vote else len(vote['no_votes'])
                        ),
                        event_keys=self.event_keys,
                    )

                if vote['other_count'] > 0 and ('other_votes' not in vote or
                                                vote['other_count'] != len(vote['other_votes'])):
                    self.logger.warning(__name__, "vote_count", fmt(
                            "The count of OTHER votes doesn't match for {}" +
                            "\nVS: {}, body: {}, year: {}" +
                            "\nSum_count: {}, got_count: {}",
                            bill_id, vs, body, year, vote['other_count'],
                            0 if 'other_votes' not in vote else len(vote['other_votes'])
                        ),
                        event_keys=self.event_keys,
                    )

                bill.add_vote(vote)

        self.save_bill(bill)

    def add_documents(self, rows, action_dates, leg_id, year, doc_shelf, bill):
        """
        The version documents are retrieved using the action timeline rather than simply using the DB.
        This is being done because NH uses the same document from the DB when a version has been passed
        without any amendments. This is identified in the database using a `V` parameter, which is also
        reflected in the URLs for version documents. For example:

           1. The bill was introduced in _House_, `V='HI'`
           2. The bill passed _House_ without amendments through both committies, `V='HP'`
           3. The _Senate_ made some amendments, `V='SA'`
           4. The **2nd Committee** of _Senate_ passed without amendments, `V='SA'`
           5. The bill is enrolled, `V=null`

        In the first and second stages, there is only one document in the database, i.e., `Introduced`
        but we need to register it twice, once as **Introduced** and next as **Ought to Pass**
        with different URLs.

        For the third and fourth case, the database only has one document, i.e., `As Amended by the Senate`
        and we need to register it as **Ought to Pass with Amendment** because unless the chamber changes
        the `V` parameter does not change.
        """
        link = 'http://www.gencourt.state.nh.us/bill_status/billText.aspx?'\
               'sy={0}&id={1}&txtFormat=pdf&v={2}{3}'
        prev_desc = None
        prev_chamber = None
        intro_flag = False
        for (action_str, action_chamber), action_date in zip(rows, action_dates):
            action_str = action_str.strip()
            _action_chamber = chamber_dict[action_chamber]
            action_date = parse(action_date.text_content())
            if action_str.find('Introduced') > -1 and not intro_flag:
                desc = 'I'
                intro_flag = True
                if version_codex[action_chamber+desc] not in doc_shelf:
                    self.logger.warning(__name__, "db_document_error",
                                        "Unable to find Introduced version",
                                        event_keys=self.event_keys
                                        )
                    continue
                _, doc_pdf, doc_text = doc_shelf[version_codex[action_chamber+desc]]
                download_id, doc_id = self.register_pdf_file(
                        doc_pdf,
                        link.format(year, leg_id, action_chamber, desc),
                        text=doc_text,
                    )
                if download_id:
                    document = Doc_service_document(
                            "Introduced",
                            "version",
                            "complete",
                            download_id,
                            doc_id=doc_id
                        )
                    bill.add_doc_service_document(document)
                else:
                    self.logger.critical(__name__, "version_document",
                                         "Unable to register version document with doc service",
                                         event_keys=self.event_keys,
                                         )
                prev_chamber = action_chamber
                prev_desc = 'I'
            elif action_str.startswith('Ought to Pass with Amendment'):
                if prev_chamber != action_chamber or prev_desc not in {'A', 'P'}:
                    desc = 'A'
                    if version_codex[action_chamber+desc] not in doc_shelf:
                        self.logger.warning(__name__, "db_document_error",
                                            fmt("Unable to find {}{} version", action_chamber, desc),
                                            event_keys=self.event_keys
                                            )
                        continue
                    _, doc_pdf, doc_text = doc_shelf[version_codex[action_chamber+desc]]
                    download_id, doc_id = self.register_pdf_file(
                            doc_pdf,
                            link.format(year, leg_id, action_chamber, desc),
                            text=doc_text,
                        )
                    if download_id:
                        document = Doc_service_document(
                                "Ought to Pass with Amendment",
                                "version",
                                "complete",
                                download_id,
                                doc_id=doc_id
                            )
                        bill.add_doc_service_document(document)
                    else:
                        self.logger.critical(__name__, "version_document",
                                             "Unable to register version document with doc service",
                                             event_keys=self.event_keys
                                             )
                    prev_chamber = action_chamber
                    prev_desc = 'A'
                elif prev_desc in {'A', 'P'}:
                    desc = 'A2'
                    if version_codex[action_chamber+desc] not in doc_shelf:
                        self.logger.warning(__name__, "db_document_error",
                                            fmt("Unable to find {}{} version for 2nd committee",
                                                action_chamber, desc),
                                            event_keys=self.event_keys
                                            )
                        # Found some cases where the 2nd committee Ought to Pass had V ending with 'A'
                        desc = 'A'
                    if version_codex[action_chamber+desc] not in doc_shelf:
                        self.logger.warning(__name__, "db_document_error",
                                            fmt("Unable to find {}{} version for 2nd committee",
                                                action_chamber, desc),
                                            event_keys=self.event_keys
                                            )
                        continue
                    _, doc_pdf, doc_text = doc_shelf[version_codex[action_chamber+desc]]
                    download_id, doc_id = self.register_pdf_file(
                            doc_pdf,
                            link.format(year, leg_id, action_chamber, desc),
                            text=doc_text,
                        )
                    if download_id:
                        document = Doc_service_document(
                                "Ought to Pass with Amendment",
                                "version",
                                "complete",
                                download_id,
                                doc_id=doc_id
                            )
                        bill.add_doc_service_document(document)
                    else:
                        self.logger.critical(__name__, "version_document",
                                             "Unable to register version document with doc service",
                                             event_keys=self.event_keys
                                             )
                    prev_chamber = action_chamber
                    prev_desc = 'A2'
            elif action_str.startswith('Ought to Pass:'):
                if prev_desc == 'P' and prev_chamber != action_chamber:
                    desc = 'P'
                    if "Version adopted by both bodies" not in doc_shelf:
                        self.logger.warning(__name__, "db_document_error",
                                            "Unable to find 'Version adopted by both bodies' version",
                                            event_keys=self.event_keys
                                            )
                        continue
                    _, doc_pdf, doc_text = doc_shelf["Version adopted by both bodies"]
                    download_id, doc_id = self.register_pdf_file(
                            doc_pdf,
                            link.format(year, leg_id, action_chamber, desc),
                            text=doc_text,
                        )
                    if download_id:
                        document = Doc_service_document(
                                "Ought to Pass",
                                "version",
                                "complete",
                                download_id,
                                doc_id=doc_id
                            )
                        bill.add_doc_service_document(document)
                    else:
                        self.logger.critical(__name__, "version_document",
                                             "Unable to register version document with doc service",
                                             event_keys=self.event_keys
                                             )
                    prev_chamber = action_chamber
                    prev_desc = 'P'
                elif prev_desc != 'P' or prev_chamber != action_chamber:
                    desc = 'P'
                    if version_codex[prev_chamber+prev_desc] not in doc_shelf:
                        self.logger.warning(__name__, "db_document_error",
                                            fmt("Unable to find {}{} version", prev_chamber, prev_desc),
                                            event_keys=self.event_keys
                                            )
                        continue
                    _, doc_pdf, doc_text = doc_shelf[version_codex[prev_chamber+prev_desc]]
                    download_id, doc_id = self.register_pdf_file(
                            doc_pdf,
                            link.format(year, leg_id, action_chamber, desc),
                            text=doc_text,
                        )
                    if download_id:
                        document = Doc_service_document(
                                "Ought to Pass",
                                "version",
                                "complete",
                                download_id,
                                doc_id=doc_id
                            )
                        bill.add_doc_service_document(document)
                    else:
                        self.logger.critical(__name__, "version_document",
                                             "Unable to register version document with doc service",
                                             event_keys=self.event_keys
                                             )
                    prev_chamber = action_chamber
                    prev_desc = 'P'

            bill.add_action(_action_chamber, action_str, action_date)

    def register_pdf_file(self, doc, link, text=None):
        """
        This function is used to register an offline file with the Doc service when a valid URL to the
        document is available. This cannot be used to register a file without one.

        params:

        doc: file content of PDF file
        link: URL to the PDF file online
        text: The extracted text of the PDF document if available

        returns: A tuple containing either (download_id, doc_id) or (None, None)
        """
        with TemporaryFile() as tempfile:
            file_hasher = hashlib.sha384()
            tempfile.write(doc)
            tempfile.seek(0)
            file_hasher.update(doc)
            # Checking if the file is already registered on the doc service
            prev_file = self.scraper.doc_service_client.last_download_info(link)
            if prev_file.datetime:
                if prev_file.fileHash == file_hasher.hexdigest():
                    self.logger.info(__name__, "Found previous file, skipping upload and reg process",
                                     event_keys=self.event_keys)
                    if prev_file.documentIds:
                        return prev_file.id, prev_file.documentIds[0]
                    else:
                        self.logger.warning(__name__, "prev_doc_error",
                                            fmt("{}: Previous document not complete\nRegistering again",
                                                self.event_keys['obj_id']),
                                            event_keys=self.event_keys,
                                            extra_info={
                                                'download_id': prev_file.id,
                                                'documents': prev_file.documents,
                                                'document_ids': prev_file.documentIds,
                                            })

            s3_url = self.scraper.s3_transferer.upload_to_s3(
                    link,
                    tempfile,
                    file_hasher.hexdigest(),
                    u'application/pdf'
                )

            headers = {}
            download_id = self.scraper.register_s3_url(
                    BRP.doc_service,
                    s3_url,
                    link,
                    file_hasher.hexdigest(),
                    True,
                    u'application/pdf',
                    None,
                    headers,
                )

            docs, doc_ids = self.scraper.extract_and_register_documents(
                    self.scraper.extraction_type.text_pdf,
                    BRP.doc_service,
                    link,
                    download_id,
                    None,
                    downloaded_file=tempfile,
                    extracted_text=text,
                )

            if doc_ids and doc_ids[0]:
                return download_id, doc_ids[0]

            return None, None

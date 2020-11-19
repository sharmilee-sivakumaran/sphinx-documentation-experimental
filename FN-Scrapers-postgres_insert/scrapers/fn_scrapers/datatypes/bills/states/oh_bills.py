"""
:class OHBillScraper: scrapes Ohio Bills
"""
from __future__ import absolute_import

import datetime
import re
import logging

import requests
from collections import defaultdict
import xlrd
import urllib

from ..common.bill_scraper import BillScraper
from ..common.bill import Bill
from ..common.vote import Vote
from ..common.bill_reporting_policy import BillReportingPolicy as BRP
from ..common.doc_service_document import Doc_service_document
from ..common.normalize import get_chamber_from_ahs_type_bill_id, get_bill_type_from_normal_bill_id
from fn_scraperutils.doc_service.util import ScraperDocument

from fn_scrapers.api.scraper import scraper, tags


logger = logging.getLogger('OHBillScraper')


@scraper()
@tags(type="bills", country_code="US", subdivision_code="US-OH", group="fnleg")
class OHBillScraper(BillScraper):
    """
    OHBillScraper
    Scrape Ohio Bills
    """
    bills_xls = {}
    base_url = 'https://www.legislature.ohio.gov/legislation?generalAssemblies=%s&pageSize=500'
    bill_types = {'lower': [('hb', 'bill'),
                            ('hr', 'resolution'),
                            ('hjr', 'joint_resolution'),
                            ('hcr', 'concurrent_resolution')],
                  'upper': [('sb', 'bill'),
                            ('sr', 'resolution'),
                            ('sjr', 'joint_resolution'),
                            ('scr', 'concurrent_resolution')]}

    def __init__(self):
        super(OHBillScraper, self).__init__("oh")

    def session_num(self, session_id):
        """
        calculate the session_num from session_id
        """
        session_year = int(session_id[:4])
        session_no = 115 + (session_year - 1983) / 2
        return str(session_no)


    def validate_date(self, session, date):
        '''
        Validates a date from actions (they have bad years sometimes)
        attempts to correct if its bad
        '''
        start_year = int(session[:4])
        end_year = int(session[4:8])
        session_years = [start_year, end_year+1]
        date_year = date.year
        if date_year > end_year + 1:
            # check if its fatfingered, 2022 instead of 2012
            if str(date_year)[-1] in [year for year in str(session_years)[-1]]:
                date_year = [year for year in session_years if str(year)[-1] == str(date_year)[-1]][0]
                date = date.replace(year=date_year)

            else:
                logger.warning("Invalid date - %s, not between %s and %s", date, start_year, end_year)
        return date

    def scrape_bill_ids(self, session):
        """
        Scrape list of bill ids
        """
        if session == '20152016r':
            logger.warning("Currently Scraper won't work on session 20152016r, SKip")
            return []
        bill_ids = set()
        session = self.session_num(session)
        if int(session) < 131:
            for chamber in ['lower', 'upper']:
                for bill_prefix, bill_type in self.bill_types[chamber]:
                    sh, sh_url = self.get_spreadsheet_obj(chamber, session, bill_type)
                    if not sh:
                        logger.warning("Bad sh")
                        continue
                    elif sh_url == '':
                        base_url = 'http://www.lsc.state.oh.us/status%s/' % session
                        url = base_url + '%s.xlsx' % bill_prefix
                    else:
                        url = sh_url
                    for rn in range(1, sh.nrows):
                        test_bill_id = '%s%s' % (bill_prefix.upper(), rn)
                        bill_ids.add(test_bill_id)

            return list(bill_ids)

        base_url = self.base_url % session

        doc = self.scraper.url_to_lxml(base_url, BRP.bill_list, request_args={'verify': False})

        next_page = doc.xpath("//a[@class='next']/@href", BRP.test)
        next_page = next_page[0] if next_page else None
        
        pages = 1
        def get_bills_from_page(list_page):
            for bills in list_page.xpath("//a[@class='actionLinks']"):
                bill_id = bills.text_content()
                bill_ids.add(bill_id)
        #scrape first page
        get_bills_from_page(doc)
        while next_page:
            pages += 1
            
            doc = self.scraper.url_to_lxml(next_page, BRP.bill_list, request_args={'verify': False})
            get_bills_from_page(doc)
            np = doc.xpath("//a[@class='next']/@href")
            np = np[0] if np else None
            if np == next_page:
                break
            else:
                next_page = np
        return list(bill_ids)


    def scrape_bill(self, session, bill_id, **kwargs):
        """
        Scraper single bill
        """
        session_id = session
        session = self.session_num(session)
        #OH website doesn't provide bill page for historical session
        #Historical Data are scraped from xlsx file stored in http://www.lsc.ohio.gov/status%s/ % session
        if int(session) <= 130:
            self.scrape_bill_old(session, session_id, bill_id)
            return

        #Data in crruent session is scraped from web page
        re_bill_id = re.sub(r'\s+', '-', bill_id.strip())
        bill_url = 'https://www.legislature.ohio.gov/legislation/legislation-summary?id'\
                   '=GA{session}-{bill_id}'.format(session=session,
                                                   bill_id=re_bill_id)

        doc = self.scraper.url_to_lxml(bill_url, BRP.bill, request_args={'verify': False})

        #If the header is 'Error', which means it is an error page, ignore
        error_page = doc.xpath_single("//h1[text()='Error']", BRP.test)
        if error_page:
            bill_format = re.findall(r'(.*?)(\d+)', bill_id)[0]
            bill_id_type = bill_format[0]
            bill_num = bill_format[1]
            bill_url = 'https://www.legislature.ohio.gov/legislation/legislation-summary?id'\
                       '=GA{session}-{bill_id_type}-{bill_num}'.format(session=session.strip(),
                                                                       bill_id_type=bill_id_type.strip(),
                                                                       bill_num=bill_num.strip())

            doc = self.scraper.url_to_lxml(bill_url, BRP.bill, request_args={'verify': False})

        # Ohio has both a short and long title; extract both
        title = doc.xpath_single('//h3[contains(text(), "Short Title:")]/span/span', BRP.bill_title)
        title = title.text_content() if title is not None else ""
        # get long title
        long_title = doc.xpath_single('//*[@id="longTitle"]/h3/span/span', BRP.bill_title)
        used_long_title = False
        if title == "" and long_title is not None:
            title = long_title.text_content()
            used_long_title = True
            if not title:
                logger.error("Couldn't Get bill title for {bill_id}".format(bill_id=bill_id))
                return
        
        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        
        bill = Bill(session_id, chamber, bill_id, title, bill_type)
        bill.add_source(bill_url)
        clean_re = r'(Sen.(tor)?|Rep.(esentative)?)'
        if not used_long_title:
            bill.add_alternate_title(long_title.text_content())

        primary_sponsors_text = doc.xpath('//h3[contains(text(), "Sponsor")]/span/a/text()')
        primary_sponsors = [re.sub(r'\s+', ' ', re.sub(clean_re, '', x)).strip()
                            for x in primary_sponsors_text]


        all_sponsors_text = doc.xpath('//td[text()="Co-Sponsors"]/following-sibling::td/a/text()', BRP.bill_sponsors)
        all_sponsors = [re.sub(r'\s+', ' ', re.sub(clean_re, '', x)).strip()
                        for x in all_sponsors_text]

        co_sponsors = [x for x in all_sponsors if x.strip() and x not in primary_sponsors]

        version = doc.xpath("//a[contains(text(),'Current')]", BRP.bill_versions)[0].get_attrib('href')

        for sponsor in primary_sponsors:
            bill.add_sponsor('primary', sponsor)
        for sponsor in co_sponsors:
            bill.add_sponsor('cosponsor', sponsor)


        # Add the current bill version and source
        version_di = []
        try:
            download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(version, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, download_args={'verify': False})
            if len(doc_ids) != 1:
                logger.warning("Document %s get more than one doc id, ignore" % version)
            else:
                doc_service_document = Doc_service_document("Current Version", "version", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)
                version_di.append(version)
        except Exception as err:
            logger.warning("Failed to download document %s : %s" % (version, err))


        # Actions are listed on a different page
        status_url = re.sub("summary", "status", bill_url)
        doc = self.scraper.url_to_lxml(status_url, BRP.bill, request_args={'verify': False})
        for actions in doc.xpath("//table[@class='dataGridOpen']/*", BRP.bill_actions)[2:]:
            date = actions.xpath("td")[0].text_content()
            date = datetime.datetime.strptime(date, "%m/%d/%y")
            acting_chamber = 'lower' if 'House' in actions.xpath("td")[1].text_content() else 'upper'
            action = actions.xpath("td")[2].text_content()
            committee = actions.xpath("td")[3].text_content()

            if committee.strip():
                action += ' ' + committee

            bill.add_action(acting_chamber, action, date)


        doc_url = re.sub('summary', 'documents', bill_url)
        doc_doc = self.scraper.url_to_lxml(doc_url, BRP.bill_versions, request_args={'verify': False})
        doc_table = doc_doc.xpath("//table[@class='dataGridOpen']/tr", BRP.bill_versions)
        for doc_sub_table in doc_table:
            title = doc_sub_table.xpath(".//div[@class='reportTitle']")[0].text_content().strip()
            doc_links = doc_sub_table.xpath(".//a")
            for doc_link in doc_links:
                doc_url = doc_link.get_attrib("href")
                doc_name = title + ' - ' + doc_link.text_content().strip()
                if doc_url not in version_di:
                    version_di.append(doc_url)
                    if title == 'Legislation Text':
                        doc_type = None
                        try:
                            download_id, _, doc_ids = \
                                self.scraper.register_download_and_documents(doc_url, BRP.bill_versions,
                                                                             self.scraper.extraction_type.text_pdf,
                                                                             True, download_args={'verify': False})
                        except Exception as err:
                            logger.warning("Failed to download document %s : %s" % (doc_url, err))
                            continue

                        if len(doc_ids) != 1:
                            logger.warning("Document %s get more than one doc id, ignore" % version)
                            continue
                        version_doc_service_document = Doc_service_document(doc_name, "version", "complete",
                                                                            download_id=download_id,
                                                                            doc_id=doc_ids[0])
                        bill.add_doc_service_document(version_doc_service_document)
                    elif title == 'Analysis':
                        doc_type = 'summary'
                    elif title == 'Fiscal Notes':
                        doc_type = 'fiscal_note'
                    else:
                        doc_type = 'other'
                        if 'committee' in doc_name.lower():
                            doc_type = 'committee_document'

                    if doc_type:
                        try:
                            download_id = self.scraper.download_and_register(doc_url, BRP.bill_documents, True,
                                                                             download_args={'verify': False})
                        except Exception as err:
                            logger.warning("Failed to download document %s : %s" % (doc_url, err))
                            continue
                        doc_service_document = Doc_service_document(doc_name, doc_type, "partial", download_id)
                        bill.add_doc_service_document(doc_service_document)


        vote_url = re.sub('summary', 'votes', bill_url)
        vote_doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes, request_args={'verify': False})
        vote_table = vote_doc.xpath("//table[@class='dataGridOpen']/tr")
        if len(vote_table) > 2:
            for vote_row in vote_table[2:]:
                vote_col = vote_row.xpath("./td")
                vote_date = vote_col[0].text_content()
                if not vote_date:
                    continue
                vote_date = datetime.datetime.strptime(vote_date, "%m/%d/%Y")
                vote_chamber = vote_col[1].text_content()
                result = vote_col[2].text_content()
                motion = "%s-%s" % (vote_chamber, result)
                if vote_chamber == "House":
                    vote_chamber = "lower"
                elif vote_chamber == "Senate":
                    vote_chamber = "upper"

                yes_count = vote_col[3].xpath_single("./span/div[@class='yeasCount']").text_content().strip()
                no_count = vote_col[3].xpath_single("./span/div[@class='naysCount']").text_content().strip()
                yes_count = int(re.findall(r'Yeas : (\d+)', yes_count)[0])
                no_count = int(re.findall(r'Nays : (\d+)', no_count)[0])


                vote = Vote(vote_chamber, vote_date, motion, yes_count>no_count, yes_count, no_count, 0)
                vote_details = vote_row.xpath(".//div[@class='legislationVotes']/table")
                for vote_list in vote_details:
                    vote_type = vote_list.xpath(".//th")[0].text_content().strip()
                    for name in vote_list.xpath(".//a"):
                        if vote_type == "Yeas":
                            vote.yes(name.text_content().strip())
                        elif vote_type == "Nays":
                            vote.no(name.text_content().strip())
                bill.add_vote(vote)
        self.save_bill(bill)

    def get_spreadsheet_obj(self, chamber, session, bill_type):
        """
        get spreadsheet of bills
        :param chamber: chamber of bills
        :param session: session of bills
        :param bill_type: bill_type of bill
        """
        ret = None

        ## 129 all are in one, change keys so all point to one copy
        if session == '129' or '130':
            chamber = 'all'
            bill_type = 'all-types'

        if (chamber, session, bill_type) not in self.bills_xls.keys():
            base_url = 'http://www.lsc.state.oh.us/status%s/' % session
            ## old are xls
            if session == '130':
                url = base_url + 'srl%s.xlsx' % session
            else:
                url = base_url + 'srl%s.xls' % session
            try:
                fname, resp = urllib.urlretrieve(url)
            except requests.exceptions.HTTPError:
                logger.error("HTTPError with %s" % url)
                return
            sh = xlrd.open_workbook(fname)
            if session == '129' or '130':
                save_sh = sh.sheet_by_index(0)
                self.bills_xls[(chamber, session, bill_type)] = save_sh
                ret = save_sh
            else:
                # iterate through each page and set type based on page title
                for i in range(0, sh.nsheets):
                    sheet = sh.sheet_by_index(i)
                    sname = sheet.name
                    schamber, stype = (None, None)
                    if 'House' in sname:
                        schamber = 'lower'
                    elif 'Senate' in sname:
                        schamber = 'upper'
                    if 'Bill' in sname:
                        stype = 'bill'
                    elif 'Conc' in sname and 'Re' in sname:
                        stype = 'concurrent resolution'
                    elif 'Joint' in sname and 'Re' in sname:
                        stype = 'joint resolution'

                    # store each page based on bill type
                    self.bills_xls[(schamber, session, stype)] = sheet
                    if schamber == chamber and stype == bill_type:
                        ret = sheet
        else:
            ret = self.bills_xls[(chamber, session, bill_type)]
            url = ""
        return ret, url


    def scrape_bill_old(self, session, session_id, bill_id, rownum=None):
        """
        scraper bills in old session
        :param chamber: chamber of bills
        :param session: session of bills
        """
        bill_id_rgx = re.compile(r'^[HS][BCJR]{1,2}\s?\d+$', re.IGNORECASE)
        if not bill_id_rgx.search(bill_id):
            logger.error("Invaild Bill ID %s" % bill_id)
            return
        bill_id = re.sub(r'\s', '', bill_id)
        bill_type = None
        bill_prefix = None


        chamber = get_chamber_from_ahs_type_bill_id(bill_id)
        bill_type = get_bill_type_from_normal_bill_id(bill_id)
        bill_prefix = bill_id.split().lower()

        sh, sh_url = self.get_spreadsheet_obj(chamber, session, bill_type)
        if not sh:
            logger.error("Bad sh")
            return
        elif sh_url == '':
            base_url = 'http://www.lsc.state.oh.us/status%s/' % session
            url = base_url + '%s.xlsx' % bill_prefix
        else:
            url = sh_url
        if not rownum:
            for rn in range(1, sh.nrows):
                test_bill_id = '%s%s' % (bill_prefix.upper(), rn)
                if test_bill_id != bill_id.replace(' ', ''):
                    continue
                else:
                    rownum = rn

        bill_title = unicode(sh.cell(rownum, 3).value)
        bill = Bill(session_id, chamber, bill_id, bill_title, bill_type)

        bill.add_source(url)
        bill.add_sponsor('primary', str(sh.cell(rownum, 1).value))

        if sh.cell(rownum, 2).value:
            value = str(sh.cell(rownum, 2).value)
            if str(sh.cell(0, 2).value).strip() == 'Subject':
                bill['title'] = value
            else:
                bill.add_sponsor('cosponsor', value)

        #  Fix extra quotes in bill title string
        if re.search(r'^"', bill['title']) and re.search(r'"$', bill['title']):
            bill['title'] = re.sub(r'(^"|"$)"', '', bill['title'])

        actor = ""
        # Actions start column after bill title
        for colnum in range(4, sh.ncols - 1):
            action = str(sh.cell(0, colnum).value)
            cell = sh.cell(rownum, colnum)
            date = cell.value
            #date could potentially be in third column
            date2 = sh.cell(rownum, 3).value
            if len(action) != 0:
                if action.split()[0] == 'House':
                    actor = "lower"
                elif action.split()[0] == 'Senate':
                    actor = "upper"
                elif action.split()[-1] == 'Governor':
                    actor = "executive"
                elif action.split()[0] == 'Gov.':
                    actor = "executive"
                elif action.split()[-1] == 'Gov.':
                    actor = "executive"

            if action == 'House Committee' or action == 'Senate Committee':
                actor = action+": "+cell.value
                continue

            if not actor:
                actor = chamber

            if action != '':
                action = action.strip()
                def get_chamber_for_committee(committee):
                    for ch in (('House', 'lower'), ('Senate', 'upper'), ('Joint', 'joint')):
                        if re.search(ch[0], committee):
                            committee = ch[1]
                    return committee

                if isinstance(date, float):
                    date = str(xlrd.xldate_as_tuple(date, 0))
                    date = datetime.datetime.strptime(
                        date, "(%Y, %m, %d, %H, %M, %S)")

                    if "Committee" in actor:
                        actor = get_chamber_for_committee(actor)
                    date = self.validate_date(session_id, date)
                    bill.add_action(actor, action, date)
                elif isinstance(date2, float):
                    date = str(xlrd.xldate_as_tuple(date2, 0))
                    date = datetime.datetime.strptime(date, "(%Y, %m, %d, %H, %M, %S)")
                    if "Committee" in actor:
                        actor = get_chamber_for_committee(actor)
                    self.validate_date(session_id, date)
                    bill.add_action(actor, action, date)
        bill_num = re.findall(r'\d+', bill_id)[0]
        self.scrape_votes(bill, bill_prefix, bill_num, session)
        self.scrape_versions(bill, bill_prefix, bill_num, session)
        self.save_bill(bill)


    def scrape_versions(self, bill, prefix, number, session):
        """
        scraper versions for  bills
        :param bill: bill that versions belong to
        :param prefix: prefix of bill_id which help to find url
        :param number: number of bill_id which help to find url
        :param session: session of bill
        """
        base_url = 'http://archives.legislature.state.oh.us'
        if 'r' in prefix:
            piece = '/res.cfm?ID=%s_%s_%s' % (session, prefix.upper(), number)
        else:
            piece = '/bills.cfm?ID=%s_%s_%s' % (session, prefix.upper(), number)

        def _get_html_or_pdf_version(doc, url=None):
            name = doc.xpath_single('//font[@size="2"]/a/text()')
            html_links = doc.xpath_single('//a[text()="(.html format)"]')
            as_enrolled = doc.xpath_single('//a[text()="As Enrolled"]')
            pdf_links = doc.xpath_single('//a[text()="(.pdf format)"]')
            pdf_links_2 = doc.xpath_single('//a[text()="View PDF format"]')
            version_dict = defaultdict(list)

            if html_links:
                link = html_links.get_attrib('href')
                version_dict[name] += [(base_url + link, 'text/html')]

            elif pdf_links:
                link = pdf_links.get_attrib('href')
                version_dict[name] += [(base_url + link, 'application/pdf')]

            elif pdf_links_2:
                link = pdf_links_2.get_attrib('href')
                version_dict[name] += [(base_url + link, 'application/pdf')]

            for name in version_dict.keys():
                versions = version_dict[name]

                # prefer html versions
                vers = [x for x in versions if x[1] != 'application/pdf']
                if not vers:
                    vers = versions
                if not vers:
                    logger.warning('Something went wrong with versions %s' % versions)
                link, mimetype = vers[0]
                if minetype == 'application/pdf':
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(link, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True)
                else:
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(link, BRP.bill_versions,
                                                                     self.scraper.extraction_type.html,
                                                                     True, self.html_parser)
                assert len(doc_ids) == 1
                doc_id = doc_ids[0]
                doc_service_document = Doc_service_document(name, "version", "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

        def _get_html_or_pdf_version_base(doc, url_link):
            name = doc.xpath_single('//font[@size="2"]/a/text()')
            html_links = doc.xpath_single('//a[text()="(.html format)"]')
            as_enrolled = doc.xpath_single('//a[text()="As Enrolled"]')
            as_introduced = doc.xpath_single('//a[text()="As Introduced"]')
            pdf_links = doc.xpath_single('//a[text()="(.pdf format)"]')
            pdf_links_2 = doc.xpath_single('//a[text()="View PDF format"]')
            version_dict = defaultdict(list)
            if html_links:
                link = html_links.get_attrib('href')
                version_dict[name] += [(link, 'text/html')]

            if pdf_links:
                link = pdf_links.get_attrib('href')
                version_dict[name] += [(link, 'application/pdf')]

            if as_enrolled:
                link = as_enrolled.get_attrib('href')
                version_dict[name] += [(link, 'text/html')]

            if as_introduced:
                link = as_introduced.get_attrib('href')
                if link == url_link:
                    page = self.scraper.url_to_lxml(link, BRP.bill, request_args={'verify': False})
                    link = page.xpath("//*[contains(text(),'Bill Text')]")
                    if link:
                        link = link[0].get_attrib('href')
                        version_dict[name] += [(link, 'text/html')]
                    else:
                        version_dict[name] += [(url_link, 'text/html')]
                else:
                    version_dict[name] += [(link, 'text/html')]

            if pdf_links_2:
                link = pdf_links_2.get_attrib('href')
                version_dict[name] += [(link, 'application/pdf')]


            for name in version_dict.keys():
                versions = version_dict[name]
                # prefer html versions
                vers = [x for x in versions if x[1] != 'application/pdf']

                if not vers:
                    vers = versions
                if not vers:
                    logger.warning('Something went wrong with versions %s' % versions)
                link, mimetype = vers[0]
                if mimetype == 'application/pdf':
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(link, BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True)
                else:
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(link, BRP.bill_versions,
                                                                     self.scraper.extraction_type.html,
                                                                     True, self.html_parser)
                assert len(doc_ids) == 1
                doc_service_document = Doc_service_document(name, "version",
                                                            "complete",
                                                            download_id=download_id,
                                                            doc_id=doc_ids[0])
                bill.add_doc_service_document(doc_service_document)

        try:
            doc = self.scraper.url_to_lxml(base_url + piece, BRP.bill)
        except requests.exceptions.HTTPError:
            logger.warning("Failed to retrieve verion page %s", base_url + piece)


        # pass over missing bills - (unclear why this happens)
        if doc is None or 'could not be found.' in doc.text_content():
            logger.warning('missing page: %s' % base_url + piece)
            return


        bill.add_source(base_url + piece)

        _get_html_or_pdf_version_base(doc, base_url + piece)
        for href in doc.xpath('//a[starts-with(@href, "/bills.cfm")]/@href'):
            if href != piece:
                doc = self.scraper.url_to_lxml(base_url + href, BRP.bill)
                _get_html_or_pdf_version(doc, url=base_url+href)
        for href in doc.xpath('//a[starts-with(@href, "/res.cfm")]/@href'):
            if href != piece:
                doc = self.scraper.url_to_lxml(base_url + href, BRP.bill)
                _get_html_or_pdf_version(doc, url=base_url+href)


    def scrape_votes(self, bill, bill_prefix, number, session):
        """
        scrape votes for bills
        :param bill: bill that votes belong to
        :param bill_prefix: bill_prefix of bill id
        :param number: number of bill id
        :session: session of bill
        """
        vote_url = ('http://archives.legislature.state.oh.us/votes.cfm?ID=' +
                    session + '_' + bill_prefix + '_' + str(number))
        try:
            page = self.scraper.url_to_lxml(vote_url, BRP.bill)
        except requests.exceptions.HTTPError:
            logger.warning("Failed to retrieve vote page %s", vote_url)
            return

        for jlink in page.xpath("//a[contains(@href, 'JournalText')]"):
            date = datetime.datetime.strptime(jlink.text,
                                              "%m/%d/%Y").date()

            details = jlink.xpath("../../../td")[1].text_content()
            chamber = details.split(" - ")[0]
            if chamber == 'House':
                chamber = 'lower'
            elif chamber == 'Senate':
                chamber = 'upper'
            else:
                logger.warning("Bad chamber: %s" % chamber)
                continue

            motion = details.split(" - ")[1].split("\n")[0].strip()
            vote_row = jlink.xpath("../../..")[0].getnext()
            yea_div = vote_row.xpath(
                "td/font/div[contains(@id, 'Yea')]")[0]
            yeas = []
            for td in yea_div.xpath("table/tr/td"):
                name = td.text_content()
                if name:
                    yeas.append(name)

            no_div = vote_row.xpath(
                "td/font/div[contains(@id, 'Nay')]")[0]
            nays = []
            for td in no_div.xpath("table/tr/td"):
                name = td.text_content()
                if name:
                    nays.append(name)

            if '' in details:
                passed = True
            else:
                passed = yes_count > no_count

            yes_count = len(yeas)
            no_count = len(nays)
            vote = Vote(chamber, date, motion, passed,
                        yes_count, no_count, 0)

            for yes in yeas:
                vote.yes(yes)
            for no in nays:
                vote.no(no)

            vote.add_source(vote_url)
            bill.add_vote(vote)

    @staticmethod
    def html_parser(element_wrapper):
        return [ScraperDocument(element_wrapper.xpath_single("//body").text_content())]

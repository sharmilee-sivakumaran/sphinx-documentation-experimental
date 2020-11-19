'''
    The module to grab New Hampshire State Regulations.
'''
from __future__ import absolute_import

from collections import namedtuple, defaultdict
import logging
import re
import urllib
import mechanize
from datetime import datetime
import pytz
import injector
from urlparse import urlparse

from lxml import html
from fn_scrapers.datatypes.state_regs import (
    Notice, NoticeScraper, NoticeReportingPolicy)

from fn_ratelimiter_client.blocking_util import standard_retry
from fn_ratelimiter_client.blocking_client import BlockingRateLimiterClient
from fn_ratelimiter_client.retry_policy import RetryPolicy

from fn_scrapers.api.scraper import scraper, tags, argument

from fn_scrapers.common import http, files
from fn_scrapers.common.files.extraction import ScraperDocument


logger = logging.getLogger(__name__)
base_url = "http://www.gencourt.state.nh.us/nholsrulesdbsearch/"

post_data = {
    'chkIFP':'on',
    'btnSearch':'Search',
    'ddlRulestoDisplay': 1000 # The value when selecting "All"
}

class SkipNotice(Exception):
    '''This notice should not be scraped. '''

class NHDocument(object):
    summary_pattern = re.compile(
        r'\d\.\s*(?:\(\w\))?\s*Summary[^:]+:(.*?)^\d\.', re.MULTILINE | re.DOTALL)
    def __init__(self, notice_id, page_num, pages):
        self.notice_id = notice_id
        self.page_num = page_num
        self.pages = pages
        self.document = None
        self.summary = None
        self.doc_id = None

    def set_content(self):
        self.document = files.extractors.text_pdf.parse(self.pages)[0]
        self.document.page_num = self.page_num
        self.document.scraper_id = self.notice_id
        self.document.text = re.sub(ur'([^\t])\xB7', r'\1', self.document.text)
        summary = self.summary_pattern.search(self.document.text)
        if summary:
            self.summary = re.sub('\s+', ' ', summary.group(1).strip())


class MechanizeRetryPolicy(RetryPolicy):
    def __init__(self):
        super(MechanizeRetryPolicy, self).__init__(max_attempts=None, max_retry_time=180, max_attempt_delay=30)

    def is_recoverable(self, err):
        import urllib2
        if isinstance(err, urllib2.HTTPError):
            if err.code == 500:
                return True
        return False


def xp_first(elm, xpath):
    result = elm.xpath(xpath)
    if not result:
        raise ValueError("Xpath [{}] failed on {}".format(xpath, elm))
    return result[0]

@argument("--agency", help="Agency filer", default=None)
@scraper()
@tags(type="notices", country_code="US", subdivision_code="US-NH", group="state_regs")
class NHNoticeScraper(NoticeScraper):
    '''
    New Hampshire Notice Scraper
    '''

    # matches "2012-12" or "2012-12" but not "20123-0011" (zipcodes)
    report_pattern = re.compile(r'((?<!\d)20\d{2}-\d+)')

    agent = (
        'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 '
        'Fedora/3.0.1-1.fc9 Firefox/3.0.1'
    )
    error_text = "Sorry, but an error has occurred in the application"

    nh_timezone = None
    nh_locality = None

    def __init__(self):
        super(NHNoticeScraper, self).__init__("nh")
        self.__class__.nh_timezone = self._timezone
        self.__class__.nh_locality = self._locality


    def do_scrape(self, start_date, end_date, agency):
        http.Session.get().user_agent = self.agent
        br = mechanize.Browser()
        br.addheaders = [('User-agent', self.agent)]
        url = base_url

        br.set_handle_robots(False)
        br.set_handle_refresh(False)

        document = http.request_lxml_html(url)

        agencies = document.xpath("//select[@name='ddlAgency']/option/@value")

        start_year = str(start_date.year)
        end_year = str(end_date.year)

        post_data['ddlFromYears'] = start_year
        post_data['ddlToYears'] = end_year
        post_data['__VIEWSTATE'] = xp_first(document, '//input[@name="__VIEWSTATE"]').get("value")
        view_state_xpath = '//input[@name="__VIEWSTATEGENERATOR"]'
        post_data['__VIEWSTATEGENERATOR'] = xp_first(document, view_state_xpath).get("value")
        post_data['__EVENTVALIDATION'] = xp_first(document, '//input[@name="__EVENTVALIDATION"]').get("value")

        for agency_name in agencies:
            if agency and agency not in agency_name:
                continue
            logger.debug('Scraping agency %s', agency_name)
            try:
                standard_retry(
                    lambda: self.scrape_agency(agency_name, br),
                    MechanizeRetryPolicy())
            except:
                logger.critical("Failed for agency: %s", agency_name)
                raise

    def select_form_by_id(self, br, id):
        for form in br.forms():
            if form.attrs.get("id") == id:
                br.form = form
                break
        else:
            raise ValueError("Could not form for id {} on the site".format(id))

    def scrape_agency(self, agency, br):
        post_data['ddlAgency'] = agency
        data = urllib.urlencode(post_data)
        self.open(br, base_url)
        self.open(br, base_url, data)
        self.select_form_by_id(br, "form1")
        br.set_all_readonly(False)
        doc = html.fromstring(br.response().read())

        current_url = None

        for rows in doc.xpath("//table[@id='dlAgencyRules']/tr")[1:]:
            if len(rows.xpath("td/table/tr/td")) != 8:
                continue
            self.open(br, base_url)
            self.open(br, base_url, data)
            self.select_form_by_id(br, "form1")
            br.set_all_readonly(False)
            notice_num = rows.xpath(".//td/table/tr/td")[2].text_content().strip()
            view = rows.xpath(".//td/table/tr/td")[7]
            more_info = view.xpath("a/@href")[0]
            more_info = re.findall(r"\'(.+?)[\',]", more_info)[0]
            br["__EVENTTARGET"] = more_info
            br["__EVENTARGUMENT"] = ""
            br.find_control("btnSearch").disabled = True
            source = self.submit(br).read()
            web_page = html.fromstring(source)

            notice_num = web_page.xpath(
                "//span[@id='lblNoticeNo']")[0].text_content().strip()
            logger.debug("Found notice %s", notice_num)
            if self.error_text in web_page.text_content():
                logger.warning("Search error: %s", notice_num)
                continue
            try:
                notice_gen = self.scrape_notice(web_page)
                for notice in notice_gen:
                    url = notice['external_url']
                    reg_id = notice['regulation']['scraper_regulation_id']
                    if url != current_url:
                        reports = self.register_document(url)
                        current_url = url
                    
                    if reg_id in reports and reports[reg_id].doc_id:
                        notice.set_attachment(document_id=reports[reg_id].doc_id)
                        notice['regulation']['summary'] = reports[reg_id].summary
                        logger.debug(
                            "Matched notice to document %s",
                            reports[reg_id].doc_id)
                    else:
                        logger.warning(
                            "Unable to find document %s in %s", reg_id, url)
                    logger.debug("Scraped notice %(title)s / %(scraper_notice_id)s", notice)
                    self.save_notice(notice)
            except SkipNotice:
                logger.info("Notice is for future date, skipping.")
            except Exception as exc:
                logger.warning('%s: %s', exc.__class__.__name__, exc.message,
                               exc_info=True)
                continue

    def open(self, br, url, data=None):
        '''Given a browser, opens a url via GET or POST '''
        logger.debug("%s %s", 'POST' if data else 'GET', url)
        http.delay_for_host(url)
        return br.open(url, data)

    def submit(self, br):
        '''Submits the currently selected form. '''
        url = br.form.action
        logger.debug("Submitting form to %s", url)
        http.delay_for_host(url)
        return br.submit()

    def register_document(self, url):
        '''
        Downloads a file, registers it with document service, extracts the
        contents, passes the contents to parse_document for parsing, and
        registers the resulting documents.
        '''
        logger.debug("Scraping registar %s", url)
        fil = files.download_and_register(url)
        entities = fil.extract(files.extractors.text_pdf)
        reports = self.entities_to_reports(entities)

        logger.debug("Extracted (%s) documents.", len(reports))
        keys = list(reports.keys())
        _, ids = fil.register_documents(reports[key].document for key in keys)
        for i, doc_id in enumerate(ids):
            reports[keys[i]].doc_id = doc_id

        logger.debug("Registered documents. [%s]", ', '.join(str(i) for i in ids))
        return reports

    @classmethod
    def xtxt(cls, elw, xpath):
        '''
        XPath Text Content - Runs the xpath, retrieves the first item, calls
        text_content, and strips the output.
        '''
        return elw.xpath(xpath)[0].text_content().strip()

    @classmethod
    def scrape_notice(cls, page):
        title = cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel1_lblRuleTitle']")

        notice_num = cls.xtxt(page, "//span[@id='lblNoticeNo']")
        agency = cls.xtxt(page, "//span[@id='lblAgency']")

        filed = cls.dt_parse(cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel2_lblDateNoticeFiledOLS']"))

        notice = Notice(cls.nh_locality, 'Proposed Rulemaking', filed,
                        notice_num+'Proposal')

        hearing_l = cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel2_lblOralHearingLocation']")

        hearing_d = cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel2_lblOralHearingDate']")

        if hearing_d and hearing_l:
            hearing_d = cls.dt_parse(hearing_d)
            notice.add_hearing(hearing_l, hearing_d, timezone=cls.nh_timezone)

        # Don't attempt to scrape registers with future dates - they likely aren't
        # posted to the website yet!
        source_file_date = u"".join(page.xpath(
            "//span[@id='TabContainer1_TabPanel2_lblRuleRegisterDate']/a[1]"
        )[0].itertext())
        source_file_date = cls.dt_parse(source_file_date).date()
        now = cls.nh_timezone.localize(datetime.now()).date()
        if source_file_date > now:
            raise SkipNotice("Future Notice")

        notice.add_contents(["hearing", "proposal_notice"])
        notice.add_agency(re.sub(r"\[.+\] ", "", agency).strip())

        notice.set_regulation("regular", scraper_regulation_id=notice_num,
                              title=title)

        source_file = page.xpath(
            "//span[@id='TabContainer1_TabPanel2_lblRuleRegisterDate']/a/@href"
        )[0]

        notice['external_url'] = source_file

        yield notice

        adopt = cls.xtxt(page, "//span[@id='TabContainer1_TabPanel3_lblAdoptedDate']")

        if not adopt:
            raise StopIteration()

        final_notice = Notice(notice['locality'], 'Final Rulemaking',
                              cls.dt_parse(adopt), notice_num+'Final')
        final_notice['regulation'] = notice['regulation']
        final_notice.add_contents(['final_text', 'final_notice'])
        final_notice.add_agency(re.sub(r"\[.+\] ", "", agency).strip())
        final_notice['external_url'] = source_file

        expiration_date = cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel3_lblExpirationDate']")

        if expiration_date:
            final_notice['expiration_date'] = cls.dt_parse(expiration_date)

        effec = cls.xtxt(
            page, "//span[@id='TabContainer1_TabPanel3_lblEffectiveDate']")

        if effec:
            final_notice['effective_date'] = cls.dt_parse(effec)
        yield final_notice

    @classmethod
    def dt_parse(cls, dt_str):
        '''
        Parses a datetime string into a datetime.
        '''
        for form in ['%m/%d/%Y', '%m/%d/%Y %I:%M %p']:
            try:
                return datetime.strptime(dt_str, form)
            except ValueError:
                pass
        raise ValueError("Unable to parse datetime [{}]".format(dt_str))
        

    @classmethod
    def filter_notice_title(cls, title):
        '''Filter titles into just lower-case letters. '''
        return re.sub('[^a-z]+', '', title.lower())

    @classmethod
    def entities_to_pages(cls, entities):
        '''Groups entities by page. '''
        buffer = []
        page_num = 1
        for ent in entities:
            content = ent.textEntity or ent.headerEntity or ent.tableEntity
            if not content:
                continue
            if content.pageNum != page_num:
                yield page_num, buffer
                buffer = []
                page_num = content.pageNum
            buffer.append(ent)
        yield page_num, buffer

    @classmethod
    def entities_to_reports(cls, entities):
        '''
        Returns a dictionary of the following structure:
            str(report_num) => NHDocument()

        May have inadvertant collisions (NH uses a very similar format for
        notices and interim notices, ie 2018-3 can refer to both with no
        straight-forward way to distinguish in the document) but this is not
        considered too critical as 1) document use is used for text searching
        only and 2) the actual url/download will contain the information for
        the end user.
        '''
        reports = {}
        report_id = None
        for num, page in cls.entities_to_pages(entities):
            if num < 3:
                continue
            text = files.extractors.text_pdf.parse(page)[0].text
            report_ids = set(cls.report_pattern.findall(text))
            if len(report_ids) != 1:
                continue
            report_id = report_ids.pop()
            if report_id not in reports:
                reports[report_id] = NHDocument(report_id, num, [])
            reports[report_id].pages.extend(page)
        for report in reports.values():
            report.set_content()
        return reports

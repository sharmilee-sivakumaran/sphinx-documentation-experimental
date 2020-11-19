'''
    Parses District of Columbia Regulations
'''
import datetime
import re
import logging
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.http import request_lxml_html
from fn_scrapers.common.files import register_download_and_documents
from fn_scrapers.common.files.extraction import Extractors
from fn_scrapers.datatypes.state_regs import Notice, NoticeScraper, NoticeReportingPolicy
from fn_scraperutils.events.reporting import ScrapeError
from urlparse import urljoin

logger = logging.getLogger(__name__)

doc_types = {"Public Hearing": ["regular", "hearing"],
             "Final Rulemaking": ["regular", "final_text"],
             "Proposed Rulemaking": ["regular", "proposal_text"],
             "Emergency and Proposed Rulemaking": ["emergency", "proposal_text"],
             "Notices, Opinions, and Orders": ["regular", "hearing"]
             }

base_url = "https://www.dcregs.dc.gov/Common/DCR/SearchIssues.aspx?Year=%s"

def single(xpath_result):
    if len(xpath_result) == 0:
        raise ValueError("Expected 1 result - got 0")
    elif len(xpath_result) == 1:
        return xpath_result[0]
    elif len(xpath_result) > 1:
        raise ValueError("Expected 1 result - got {}".format(len(xpath_result)))


"""
This scraper was rewritten December 17 and so it's not recommended to run this before then

We obtain our notices from a page like this:
https://www.dcregs.dc.gov/Common/DCR/Issues/IssueCategoryList.aspx?CategoryID=14&IssueID=706
in which we use the section number(non-unique-id) as our main linking.
The title names from proposal to final will be different almost all the time except for simple short titles
This section number groups notices by topic and with our timeline logic
"""
@scraper()
@tags(type="notices", country_code="US", subdivision_code="US-DC", group="state_regs")
class DCNoticeScraper(NoticeScraper):
    '''
    District of Columbia Document Scraper
    '''
    def __init__(self):
        super(DCNoticeScraper, self).__init__("dc")


    def do_scrape(self, start_date, end_date, **kwargs):
        start_year = start_date.year
        end_year = end_date.year

        for year in range(start_year, end_year + 1):
            year_reg_url = base_url % year
            reg_page = request_lxml_html(year_reg_url, abs_links=True)
            try:
                reg_list = reg_page.xpath('//a[contains(@href, "IssueDetailPage")]')
            except ScrapeError:
                logger.warning("Failed to scrape register list for %s" % year)  # pylint:disable=logging-not-lazy
                continue

            for reg_row in reg_list:
                date, vol_id = re.findall(r'(.*?) - (.*)', reg_row.text_content())[0]
                formed_date = datetime.datetime.strptime(date.strip(), "%B %d, %Y")

                if formed_date.date() < start_date or formed_date.date() > end_date:
                    continue

                reg_link = reg_row.attrib.get('href')
                doc_list_page = request_lxml_html(reg_link, abs_links=True)

                agencies_links = doc_list_page.xpath('//a[contains(@id, "dlAgencyCats")]')

                for agencies_link in agencies_links:
                    agencies_type = agencies_link.text_content().strip()
                    if agencies_type in doc_types:
                        if agencies_type == "Public Hearing":
                            continue
                        reg_type, doc_type = doc_types[agencies_type]
                        doc_list_url = agencies_link.attrib.get('href')
                        self.scrape_doc_list(doc_list_url, doc_type, formed_date, reg_type, agencies_type)

    def scrape_doc_list(self, list_url, doc_type, date, reg_type, agencies_type):
        """
        scrape doc list
        """
        doc_list_page = request_lxml_html(list_url, abs_links=True)
        doc_table = doc_list_page.xpath("//table[@id='noticeTable']//tr")
        page_num = 0
        pages = doc_list_page.xpath("//a[contains(@href, 'Page$')]")
        if pages:
            page_num = len(pages)
        self.scrape_doc_list_page(doc_table, doc_type, date, list_url, reg_type, agencies_type)
        if page_num > 0:
            # pylint: disable=C0301
            post_data = {
                'ctl00$ContentPlaceHolder$ScriptManager1': 'ctl00$ContentPlaceHolder$UpdatePanel1|ctl00$ContentPlaceHolder$gvNotice',
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder$gvNotice',
                '__VIEWSTATEENCRYPTED': ''
                }
            for i in range(2, page_num + 1):
                post_data['__EVENTARGUMENT'] = 'Page$%d' % i
                post_data['__VIEWSTATE'] = doc_list_page.xpath("//input[@name='__VIEWSTATE']")[0].element.value
                doc_list_page = request_lxml_html(list_url, abs_links=True, method="POST", json={"data": post_data})

                doc_table = doc_list_page.xpath("//table[@class='noticeform']//tr", NoticeReportingPolicy.notice_list)
                self.scrape_doc_list_page(doc_table, doc_type, date, list_url, reg_type, agencies_type)

    def scrape_doc_list_page(self, doc_table, doc_type, date, list_url, reg_type, agencies_type):
        category = agencies_type
        for doc_row in doc_table[1:]:
            if doc_row.xpath('.//table'):
                break
            doc_cell = doc_row.xpath('./td')
            notice_id = doc_cell[1].text_content().strip()
            rule_id = single(doc_cell[1].xpath('./u/a')).text_content().strip()
            try:
                non_unique_id = single(doc_cell[2].xpath('./u/a')).text_content().strip()
            except ValueError:
                non_unique_id = None
            if doc_type == 'hearing':
                index = 2
            else:
                index = 3
            reg_title = doc_cell[index].text_content().strip()
            if reg_title == 'View Text':
                reg_title = doc_cell[index - 1].text_content().strip()
            document = Notice("dc", category, date, notice_id)
            if agencies_type == "Notices, Opinions, and Orders" and "meeting" not in reg_title.lower():
                # for all notices within "Notices, Opinions, and Orders", if the word "meeting"
                # appears in the subject, give it a doc type of hearing, otherwise give it a
                # doc type of final notice
                # the default value in the global dict is "hearing", and this if statement
                # overwrites it with "final notice"
                doc_type = "final_notice"
            document.add_contents(doc_type)
            notice_page_url = 'https://www.dcregs.dc.gov/Common/NoticeDetail.aspx?NoticeId='
            notice_page = request_lxml_html(
                notice_page_url + re.sub("N00", "", notice_id), abs_links=True)

            # Get the notice text
            text_url = single(notice_page.xpath("//a[@id='MainContent_lnkNoticeFile']/@href"))
            # We need to overwrite the mimetype we get from the headers,
            # as the site gives us 'application/force-download'.

            form = single(notice_page.xpath("//form[@id='Form1']"))

            inputs = form.xpath("//input[@name = '__VIEWSTATE' or @name = '__EVENTVALIDATION']")
            data = {i.get("name"): i.get("value") for i in inputs}
            data["__EVENTTARGET"] = "ctl00$MainContent$lnkNoticeFile"

            request_args = {"method": "POST", "data": data}
            fil = register_download_and_documents(
                urljoin(notice_page_url, form.get("action")),
                Extractors.msword_doc,
                request_args=request_args,
                serve_from_s3=True,
                mimetype='application/msword'
            )
            doc_ids = fil.document_ids

            if doc_ids and doc_ids[0]:
                document.set_attachment(document_id=doc_ids[0])

            if rule_id:
                document.set_regulation(reg_type,
                                        regulation_id=rule_id,
                                        scraper_non_unique_regulation_id=non_unique_id,
                                        title=reg_title)
            else:
                document.set_regulation(reg_type, title=reg_title)

            try:
                agency_name_cell = single(notice_page.xpath("//a[contains(@id, 'AgencyName')]"))
                agency_name = agency_name_cell.text_content().strip()
                document.add_agency(agency_name)
            except AttributeError:
                logger.warning("No Agency name")
            self.save_notice(document)

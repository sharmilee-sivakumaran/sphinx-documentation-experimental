'''
    The module to grab Rhode Island State Regulations.
'''
from __future__ import absolute_import
import datetime
import json
import logging
import re
import urllib

from lxml import html
from fn_scrapers.common import http, files
from dateutil.parser import parse
from fn_scrapers.datatypes.state_regs import Notice, NoticeScraper, NoticeReportingPolicy
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.xpath_shortcuts import one

logger = logging.getLogger(__name__)
base_url = "http://sos.ri.gov/?"
rss_feed = "http://sos.ri.gov/rssonate/getFeed/?"

rss_feed_payload = {
    'feed': 'getRegsByKeyword',
    'AGENCY': 'ALL',
    'SORT': 'date',
    'limit_start': 0,
    'limit': '10',
    'BOOLSEARCH': '0',
    'INCSUPER': '0',
    'INCEXPIRED': '0',
    'KEYWORD': '%',
    'INCREPEALS': '0',
    'ORDER': 'desc'
}

doc_type = {
    "rules": "rule:adopted",
    "ProposedRules": "rule:proposal"
}

effective_date_regex = r"Effective date:([\d\/]+)"
erlid_number_regex = r"ERLID Number: (\d+)"
agency_regex = r"Agency:([\w ]+)"
filing_regex = r"Type of Filing:([\w ]+)"


@scraper()
@tags(type="notices", country_code="US", subdivision_code="US-RI", group="state_regs")
class RINoticeScraper(NoticeScraper):
    def __init__(self):
        super(RINoticeScraper, self).__init__("ri")

    def do_scrape(self, start_date, end_date):
        url = rss_feed + urllib.urlencode(rss_feed_payload)
        document = http.request_xml(url)
        if document is None:
            logger.critical("Unable to load register from %s", url)
            return
        items = document.xpath("//item")
        while len(items):
            for item in items:
                description = one("description/text()", item)
                # Grab the text of the description, remove html tags (CDATA doesn't not appear in element Tree)
                text_content = re.sub('<[^>]*>', '', description)
                html_link = html.fromstring(description)
                source = html_link.xpath("a[contains('View Details',text())]/@href")[0]
                link = html_link.xpath("a[contains('View Regulation',text())]/@href")[0]
                # Effective date comes in the format MM/dd/YYYY
                effective_date = parse(re.findall(effective_date_regex, text_content)[0])
                erlid_number = re.findall(erlid_number_regex, text_content)[0]
                agency = re.findall(agency_regex, text_content)[0]
                status = re.findall(r"Status: (.*)", text_content)[0]

                type_of_filing = re.findall(filing_regex, text_content)
                if type_of_filing:
                    type_of_filing = type_of_filing[0]
                else:
                    type_of_filing = ""

                publish_date = one("pubDate", item).text
                formed_publish_date = parse(publish_date)  # datetime.strptime(publish_date, '%Y-%d-%M %I:%M%p')
                if formed_publish_date.date() < start_date or formed_publish_date.date() > end_date:
                    continue
                author = one("author", item).text
                title = one("title", item).text

                view_details = html_link.xpath("a[contains(text(), 'View Details')]/@href")[0]
                view_detail_page = http.request_lxml_html(view_details, abs_links=True)
                view_page_text = view_detail_page.text_content()
                hearing_date = re.findall(r"Hearing Date: (.*)", view_page_text)[0]

                document = Notice(self._locality, 'Final Rulemaking', formed_publish_date, erlid_number+'Final')
                if effective_date:
                    document['effective_date'] = effective_date

                if 'Proposed' in status:
                    document.add_contents("proposal_notice")
                    document.add_contents("proposal_text")
                else:
                    document.add_contents("final_notice")
                    document.add_contents("final_text")

                if "Expired" in status:
                    document['expiration_date'] = parse(re.findall(r"\d{2}\/\d{2}\/\d{4}", status)[0])
                    document.set_regulation("emergency", title=title, scraper_regulation_id=erlid_number)
                elif 'Emergency' in type_of_filing:
                    document.set_regulation("emergency", title=title, scraper_regulation_id=erlid_number)
                else:
                    document.set_regulation("regular", title=title, scraper_regulation_id=erlid_number)

                summary = re.findall(r"Purpose and Reason\n(.*)", view_page_text)
                if summary:
                    document['regulation']['summary'] = summary[0]

                document.add_agency(agency)
                link = remove_whitespace_from_link(link)
                file_obj = files.register_download_and_documents(link, files.extractors.text_pdf, serve_from_s3=True)
                try:
                    document.set_attachment(document_id=file_obj.document_ids[0])
                except TypeError:
                    logger.warning('Unable to register document from %s', link)
                self.save_notice(document)

            rss_feed_payload['limit_start'] += 10
            url = rss_feed + urllib.urlencode(rss_feed_payload)
            document = http.request_xml(url)
            if document is None:
                logger.critical("Unable to load register from %s", url)
                return
            items = document.xpath("//item")

        rss_feed_payload['feed'] = 'getProposedByKeyword'
        rss_feed_payload['limit_start'] = 0
        url = rss_feed + urllib.urlencode(rss_feed_payload)
        document = http.request_xml(url)
        if document is None:
            logger.critical("Unable to load register from %s", url)
            return

        items = document.xpath("//item")
        while len(items):
            for item in items:
                description = one("description/text()", item)
                # Grab the text of the description, remove html tags (CDATA doesn't not appear in element Tree)
                text_content = re.sub('<[^>]*>', '', description)
                html_link = html.fromstring(description)
                link = one("link/text()", item).replace(" ", "%20")

                # Effective date comes in the format MM/dd/YYYY
                erlid_number = re.findall(erlid_number_regex, text_content)[0]
                agency = re.findall(agency_regex, text_content)[0]
                status = re.findall(r"Status: (.*)", text_content)[0]

                publish_date = one("pubDate", item).text
                formed_publish_date = parse(publish_date)  # datetime.strptime(publish_date, '%Y-%d-%M %I:%M%p')
                if formed_publish_date.date() < start_date or formed_publish_date.date() > end_date:
                    continue
                author = one("author", item).text
                title = one("title", item).text
                comment_date = re.findall(r"Comment period ends: (\d+\/\d+\/\d+)", text_content)
                hearing_date = re.findall(r"Filing Hearing Date: (\d+\/\d+\/\d+)", text_content)
                document = Notice(self._locality, 'Proposed Rulemaking', formed_publish_date, erlid_number+'Proposal')
                document.add_contents("proposal_notice")
                document.add_contents("proposal_text")

                if comment_date:
                    try:
                        document.set_comment_period(formed_publish_date, parse(comment_date[0]))
                    except ValueError as e:
                        logger.warning("Unable to parse date from {!r}".format(comment_date[0]))

                if "Expired" in status:
                    document['expiration_date'] = parse(re.findall(r"\d{2}\/\d{2}\/\d{4}", status)[0])
                    document.set_regulation("emergency", title=title, scraper_regulation_id=erlid_number)
                else:
                    document.set_regulation("regular", title=title, scraper_regulation_id=erlid_number)

                if hearing_date and hearing_date[0] not in "Hearing Not Required" and hearing_date[0] != '00/00/0000':
                    hearing_date = parse(hearing_date[0])
                    document.add_contents("hearing")
                    document.add_hearing("N/A", hearing_date, timezone=self._timezone)

                document.add_agency(agency)
                link = remove_whitespace_from_link(link)
                file_obj = files.register_download_and_documents(link, files.extractors.text_pdf, serve_from_s3=True)
                try:
                    document.set_attachment(document_id=file_obj.document_ids[0])
                except TypeError:
                    logger.warning('Unable to register document from %s', link)
                self.save_notice(document)

            rss_feed_payload['limit_start'] += 10
            url = rss_feed + urllib.urlencode(rss_feed_payload)
            document = http.request_xml(url)
            if document is None:
                logger.critical("Unable to load register from %s", url)
                return
            items = document.xpath("//item")


def remove_whitespace_from_link(link):
    """
    There have been cases where the link contains whitespace URL safe whitespace characters,
    which causes the calls to the URLs fail.

    %20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20http://sos.ri.gov/documents/archives/regdocs/holding/DLT/Notice%20and%20Reg%20Elevators.pdf
    %20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20
    """
    link = urllib.unquote(link).strip()
    return link

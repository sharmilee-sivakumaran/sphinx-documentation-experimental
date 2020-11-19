'''
The module to grab Washington State Regulations.
'''
import logging
import re
import enum
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.http import request_lxml_html
from fn_scrapers.common.xpath_shortcuts import first_or_none
from fn_scrapers.datatypes.state_regs import Notice, NoticeScraper
import string
from fn_scraperutils.events.reporting import EventType

logger = logging.getLogger(__name__)
base_url = "http://lawfilesext.leg.wa.gov/law/WSR/Agency/"

non_ascii_filter = '[^{}]'.format(string.printable)
non_ascii_filter = re.compile(non_ascii_filter)


class WAEventType(enum.Enum):
    website_update = "website_update"


@scraper()
@tags(type="notices", country_code="US", subdivision_code="US-WA", group="state_regs")
class WANoticeScraper(NoticeScraper):
    """
    Washington Notice Scraper

    3 debug variables just to count the number of:
        - Notices scraped in the specified time frame; total_notices
        - Notices with Hearing locations present in the <div> element; hearing_location_new
        - Notices with the phrase 'Hearing Location' present but not extracted by xpath; unknown_format

    Replaced the RegEx with XPath expressions due to changes in website structure, the new structure can be noticed
    from the end of 2013.

    NOTE: This code may fail to scrape pages successfully before the start date 01/01/2014
          It has been tested for successful scraping from notices with filing dates from 01/01/2016
    """
    total_notices = 0
    unknown_format = 0
    hearing_location_new = 0

    def __init__(self):
        super(WANoticeScraper, self).__init__("wa")

    def do_scrape(self, start_date, end_date):

        agencies = request_lxml_html(base_url, abs_links=True)

        for agency_link in agencies.xpath("/html/body/pre/a/@href")[1:]:
            agency_page = request_lxml_html(agency_link, abs_links=True)
            agency = first_or_none("//center[1]/font", agency_page).text.strip()

            notice_list = agency_page.xpath("//table[1]//a")

            i = 0
            while i < len(notice_list):
                filing_number = notice_list[i].text.strip()
                # The last regs of the year end up in next year's register, so we need to look at end_year+1
                if int(str(start_date.year)[2:]) <= int(filing_number[:2]) <= int(str(end_date.year)[2:]) + 1:
                    rule_url = notice_list[i].get("href")
                    notice_type = notice_list[i + 1]
                    if notice_type is not None and notice_type.text and notice_type.text.strip() != "MISC":

                        regulation = "regular"
                        notice_contents = []
                        notice_type = notice_type.text.strip()

                        # Skipping EXEC for now (until we know how to handle it)

                        if notice_type == "EMER":
                            notice_type = "Emergency Rulemaking"
                            regulation = "emergency"
                            notice_contents.append('final_notice')
                            notice_contents.append('final_text')

                        elif notice_type == "EXPE":
                            notice_type = "Expedited Rulemaking"
                            regulation = "emergency"
                            notice_contents.append('final_notice')
                            notice_contents.append('final_text')

                        elif notice_type == "PREP":
                            notice_type = "Pre-Proposed Rulemaking"
                            notice_contents.append('pre_proposal_notice')

                        elif notice_type == "PROP":
                            notice_type = "Proposed Rulemaking"
                            notice_contents.append('proposal_notice')
                            notice_contents.append('proposal_text')

                        elif notice_type == "PERM":
                            notice_type = "Final Rulemaking"
                            notice_contents.append('final_notice')
                            notice_contents.append('final_text')

                        else:
                            i += 2
                            continue

                        try:
                            self.scrape_rule(rule_url, agency, filing_number, notice_type, regulation, notice_contents,
                                             start_date, end_date)
                        except Exception as e:
                            logger.critical(e)
                i += 2

        logger.info(
            "Total notices: {}".format(self.total_notices))

        logger.info(
            "Unknown format: {}".format(self.unknown_format))

        logger.info(
            "New format: {}".format(self.hearing_location_new))

    def scrape_rule(self, url, agency, filing_number, notice_type, regulation, notice_contents, start, end):
        """
        Scrape and save a single notice
        """
        rule_page = request_lxml_html(url, abs_links=True, encoding='utf-8')
        content = rule_page.text_content()
        """
        lxml library is unable to parse the HTML structure correctly, i.e.:

            <html>
                <head></head>
                <body>
                    :
                    <table>
                        :
                        <tbody>
                            :
                            <tr>
                                <td>
                                    <div>
                                    :
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    :
                </body>
            </html>

        is being parsed incorrectly, so the xpath element finds the element in the structure that is being
        parsed correctly
        """

        """
        The following XPath may return more than one matches where the text in the <div>
        element starts with '['.
        So the first step is to find all the eligible elements, 
        then form a text string and use that to find the filing date
        """
        filed_candidates = rule_page.xpath(
            "//div[starts-with(text(),'[')]")
        if filed_candidates:
            for cand in filed_candidates:
                text_list = cand.itertext()
                text = u''.join([non_ascii_filter.sub(' ', t) for t in text_list])
                if 'Filed' in text:
                    filed = text
                    break
            else:
                logger.critical(
                    "Unable to get date for {}".format(url))
                return

            filed_list = re.findall(r"Filed(.+?)\]", filed, flags=re.S)
            filed = re.sub(r"\s{2,}", " ", filed_list[0]).strip()
            filed = non_ascii_filter.sub(' ', filed).strip()
            filed = filed.split(u"effective")[0].strip()

            if re.findall(r"Filed(.+?)\]", content, flags=re.S) and not filed:
                logger.critical(
                    "Could not find filing date for {}".format(url))

            filed = parse(filed)

        else:
            logger.critical(
                "Could not find filing date for {}".format(url))
            return

        if start <= filed.date() <= end:
            self.total_notices += 1

            if any(t in content for t in (
                    "WITHDRAWAL OF PROPOSED RULES",
                    "WITHDRAWAL OFPREPROPOSAL STATEMENT",
                    "WITHDRAWAL OF PREPROPOSAL STATEMENT",
            )):
                notice_type = "Withdrawn Rulemaking"
                notice_contents = ["withdrawn_notice"]

            hearing = None
            hearing_div_span = first_or_none(
                "//div[./span[contains(text(),'Hearing Location')]]", rule_page)
            if hearing_div_span is not None:
                self.hearing_location_new += 1
                text_elems = list(hearing_div_span.itertext())
                hearing_location_string = u''.join([elem for elem in text_elems[1:]])
                hearing = hearing_location_string

            location_found = False
            if re.findall(r"Hearing Location[.\s\S]*?:", content):
                location_found = True

            if location_found and hearing is None:
                """
                This code block determined that the page contains the location
                but the XPath expression is incorrect, possibly due to change in
                website structure, so it logs a warning.
                """
                logger.critical(
                    "Unable to get Hearing Location for {}".format(url))
                self.unknown_format += 1

            """
            Preproposal notices should have a summary field but don't need to
            so a the event where nothing is found is logged as a warning
            """

            summary = None

            # PROPOSED RULES usually have "Purpose of ....:"
            purpose = first_or_none("//div[./span[starts-with(text(), 'Purpose')]]", rule_page)
            # It appears that now they also put these in just divs
            if purpose is None:
                purpose = first_or_none("//div[starts-with(text(), 'Purpose')]", rule_page)

            subject_of = first_or_none(
                "//div[./span[starts-with(text(), 'Subject of ')]]", rule_page)
            if purpose is not None:
                purpose = re.search(r"Purpose.*?:(.+)", purpose.text_content(), re.DOTALL)
                if purpose:
                    summary = purpose.group(1).strip()

            elif subject_of is not None:
                subject_of_text = list(subject_of.itertext())
                summary = u''.join([t for t in subject_of_text[1:]])

            elif notice_type == "Pre-Proposal Rulemaking":
                logger.critical(
                    "Unable to extract summary for {}".format(url))

            title = None
            title_span = first_or_none(
                "//div[./span[starts-with(text(), 'Title of')]]", rule_page)
            if title_span is not None:
                title_text_list = list(title_span.itertext())
                title = u''.join([t for t in title_text_list[1:]])

            if re.findall(r"Title of(.+[.?!]?)[\n].*", content) and title_span is None:
                logger.warning(
                    "Unable to find title")

            p_id = None
            preproposal = first_or_none(
                "//div[./span[contains(text(),'Preproposal statement of inquiry')]]", rule_page)

            # Have not yet seen this. Probably because I've only looked at preproposals
            proposal = first_or_none(
                "//div[./span[contains(text(),'Adopted under notice filed as WSR')]]", rule_page)
            if preproposal is not None:
                preproposal_text = list(preproposal.itertext())
                preproposal_str = u''.join([non_ascii_filter.sub(' ', s) for s in preproposal_text[1:]])
                p_id = re.findall(r"\d+-\d+-\d+", preproposal_str)
                if p_id:
                    p_id = p_id[0]
                else:
                    logger.critical(
                        'Unable to extract preceding ID\n%s' % (preproposal_str))

                    p_id = None
            elif proposal is not None:
                proposal_text = list(proposal.itertext())
                proposal_str = u''.join([non_ascii_filter.sub(' ', t) for t in proposal_text[1:]])
                p_id = re.findall(r"[0-9]+-[0-9]+-[0-9]+", proposal_str)
                if p_id:
                    p_id = p_id[0]
                else:
                    logger.critical(
                        'Unable to extract preceding ID\n%s' % (proposal_str))

                    p_id = None

            elif (re.findall(
                    r"(Adopted under notice filed as (.+?)\.|Preproposal statement of inquiry (.+?)\.)",
                    content) and not (preproposal or proposal)):
                logger.critical(
                    "Unable to find preceding ID")

            notice = Notice(self._locality, notice_type, filed, filing_number)
            notice.set_regulation(regulation_type=regulation)
            notice.add_agency(agency)

            inquiry = None
            inquiry_span = first_or_none(
                "//div[./span[starts-with(text(), 'Submit Written Comments to')]]", rule_page)
            if inquiry_span is None and re.findall(r"Submit Written Comments to:(.*)", content):
                logger.critical(
                    "Unable to find contact")

            elif inquiry_span is not None:
                inquiry_text = list(inquiry_span.itertext())
                inquiry = u''.join([t for t in inquiry_text[1:]])
            if inquiry:
                notice.add_contact("comment", inquiry.strip())
            if p_id:
                notice['preceding_scraper_notice_id'] = p_id

            if summary:
                notice['regulation']['summary'] = summary

            if title:
                notice['regulation']['title'] = title

            if hearing:
                notice.add_hearing(location=hearing)

            notice.add_contents(notice_contents)
            notice.set_attachment(url)
            self.save_notice(notice)
            logger.info("Saved notice: {}".format(notice))

from __future__ import absolute_import
from collections import namedtuple
import re
from dateutil.parser import parse
from fn_scrapers.datatypes.bills.common import BillScraper, Bill, Vote, BillReportingPolicy as BRP, Doc_service_document
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_scrapers.datatypes.bills.common.normalize import get_chamber_from_ahs_type_bill_id
from fn_scrapers.api.scraper import scraper, tags
import logging

logger = logging.getLogger(__name__)


@scraper()
@tags(type="bills", group="fnleg", subdivision_code="US-NC", country_code="US")
class NCBillScraper(BillScraper):
    def __init__(self):
        super(NCBillScraper, self).__init__("nc")

    BillType = namedtuple('BillType', 'header name code')
    bill_types = [
        BillType('Joint Resolution', 'joint_resolution', 'JR'),
        BillType('Resolution', 'resolution', 'R'),
        BillType('Bill', 'bill', 'B')
    ]

    def scrape_bill_ids(self, session):
        slug = self.create_slug(session)
        bill_ids = {}
        for chamber in ['H', 'S']:
            bill_list_url = 'https://www.ncleg.net/gascripts/SimpleBillInquiry/' \
                            'displaybills.pl?Session=%s&tab=Chamber&Chamber=%s' % (slug, chamber)

            bill_list_doc = self.scraper.url_to_lxml(bill_list_url, BRP.bill_list)
            bill_list = bill_list_doc.xpath("//div[@class='body-content']//center[2]//tr")
            for bill_ele in bill_list[1:]:
                bill_id = bill_ele.xpath("./td/a")[0].text_content()
                if bill_id not in bill_ids:
                    bill_ids[bill_id] = []

        return bill_ids

    def scrape_bill(self, session, bill_id, **kwargs):
        bid = re.sub(r'\s+', '', bill_id)
        if not re.match(r"[SH]\d+", bid):
            """
            The new website is tolerant of URLs like https://www2.ncleg.net/BillLookup/2017/SB628
            which can mislead someone when running the scraper for a single bill to pass in the bill
            ID "SB 628" instead of "S 628" because the actual URL is
            https://www2.ncleg.net/BillLookup/2017/S628

            which cause the scraper to generate external IDs like 'SB B 628', so adding this check here
            just to be sure.
            """
            raise Exception("Invalid bill ID, should be of the form 'S 1231' or 'H 1231'")

        slug = self.create_slug(session)

        bill_detail_url = 'https://www2.ncleg.net/BillLookup/%s/%s' % (slug, bid)
        bill_doc = self.scraper.url_to_lxml(bill_detail_url, BRP.bill)
        title_div = bill_doc.xpath_single('//div[contains(@class, "body-content")]/div[@class="row"][2]//a')
        if not title_div:
            logger.warning("No Data for %s" % bill_id)
            return
        title = title_div.text_content()
        version_url = title_div.get_attrib('href')

        chamber = get_chamber_from_ahs_type_bill_id(bill_id)

        header = bill_doc.xpath_single(
            '//div[contains(@class, "body-content")]/div[@class="row"][1]/div[contains(@class, "text-center")]'
        ).text_content()
        bill_types = [bt for bt in self.bill_types if bt.header in header]
        if not bill_types:
            raise ValueError("Unrecognized bill type: " + header)
        bill_type = bill_types[0]
        alternate_id = bill_id
        bill_id = "{}{} {}".format(bill_id[0], bill_type.code, bill_id[2:])

        bill = Bill(session, chamber, bill_id, title, bill_type.name)
        bill.add_source(bill_detail_url)
        bill.add_alternate_id(alternate_id)

        alter_id = re.search(r'\(=\s*([HS])(\d+)\s*\)', header)
        if alter_id:
            alter_id = '{1}{0} {2}'.format(bill_type.code, *alter_id.groups())
            bill.add_companion(alter_id, "companion")

        sponsors_list = bill_doc.xpath("//div[contains(@class, 'bill-lookup-section')]" +
                                       "//div[contains(text(), 'Sponsors:')]/following-sibling::div//a")
        spon_type = "primary"
        for sponsor_ele in sponsors_list:
            bill.add_sponsor(spon_type, sponsor_ele.text_content())
            if '(Primary)' in sponsor_ele.element.tail:
                spon_type = "cosponsor"

        file_list = bill_doc.xpath("//div[contains(@class, 'bill-lookup-section')][1]//div[@class='card-body']//a",
                                   BRP.test)
        for file in file_list:
            file_name = file.text_content()
            if file_name:
                doc_url = file.get_attrib('href')
                if '/Bills/' in doc_url:
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(doc_url,
                                                                     BRP.bill_versions,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, content_type="application/pdf")

                    doc_service_document = Doc_service_document(file_name, 'version',
                                                                "complete",
                                                                download_id=download_id,
                                                                doc_id=doc_ids[0])

                    bill.add_doc_service_document(doc_service_document)
                elif '/FiscalNotes/' in doc_url:
                    match = re.search(r'/PDF/[HS](IN|FN|AR|AH)\d+(.*)\.pdf', doc_url)
                    if match:
                        version_number = match.group(2)
                        note_type = match.group(1)
                        version_number = "(%s)" % version_number
                        if note_type == 'FN':
                            fiscal_name = "Fiscal Note for %s %s" % (bill_id, version_number)
                        elif note_type == 'IN':
                            fiscal_name = "Incarceration for %s %s" % (bill_id, version_number)
                        elif note_type == 'AH':
                            fiscal_name = "Actuarial Health for %s %s" % (bill_id, version_number)
                        elif note_type == 'AR':
                            fiscal_name = "Actuarial Retirement for %s %s" % (bill_id, version_number)
                    else:
                        fiscal_name = "Fiscal Note"

                    doc_id = self.scraper.download_and_register(doc_url, BRP.bill_documents, True,
                                                                content_type="application/pdf")
                    doc_service_document = Doc_service_document(fiscal_name, "fiscal_note", "partial", doc_id)
                    bill.add_doc_service_document(doc_service_document)
                else:
                    doc_id = self.scraper.download_and_register(doc_url, BRP.bill_documents, True,
                                                                content_type="application/pdf")
                    doc_service_document = Doc_service_document(file_name, "other", "partial", doc_id)
                    bill.add_doc_service_document(doc_service_document)

        summary_url = bill_doc.xpath_single("//a[text()='View Available Bill Summaries']").get_attrib('href')
        summ_doc = self.scraper.url_to_lxml(summary_url, BRP.bill_summary)
        summ_list = summ_doc.xpath(".//a[contains(@href, '/BillSummary/')]")
        for summ_ele in summ_list:
            sum_link = summ_ele.get_attrib('href')
            sum_name = summ_ele.text_content()

            doc_id = self.scraper.download_and_register(sum_link, BRP.bill_documents, True,
                                                        content_type="application/pdf")
            doc_service_document = Doc_service_document(sum_name, "summary", "partial", doc_id)
            bill.add_doc_service_document(doc_service_document)

        """
        This may be a result of NC's website updates still being in progress
        but the action table is supposed to be the last div element on the whole page that matches
          //div[contains(@class, 'bill-lookup-section')]

        For example:
            XPath - //div[contains(@class, 'bill-lookup-section')][last()]

            https://www2.ncleg.net/BillLookup/2017/S88
            will return multiple results for the XPath above

            https://www2.ncleg.net/BillLookup/2017/S99
            will only return one result
        """
        action_table = bill_doc.xpath("//div[contains(@class, 'bill-lookup-section')][last()]")[-1]
        action_table = action_table.xpath(".//div[@class='card-body']/div[@class='row']")
        for action_row in action_table:
            action_date = action_row.xpath_single("./div[contains(., 'Date:')]/following-sibling::div[1]")
            action_date = parse(action_date.text_content())

            actor = action_row.xpath_single("./div[contains(., 'Chamber:')]/following-sibling::div[1]").text_content()
            action = action_row.xpath_single("./div[contains(., 'Action:')]/following-sibling::div[1]").text_content()
            amend_ele = action_row.xpath_single("./div[contains(., 'Documents:')]/following-sibling::div[1]//a",
                                                BRP.test)
            if amend_ele:
                amend_name = amend_ele.text_content()
                amend_url = amend_ele.get_attrib('href')
                if re.match(r"(?:A\d+?|CC?S|CR):", amend_name.strip()):
                    download_id, _, doc_ids = \
                        self.scraper.register_download_and_documents(amend_url,
                                                                     BRP.bill_documents,
                                                                     self.scraper.extraction_type.text_pdf,
                                                                     True, content_type="application/pdf")
                    if len(doc_ids) == 0 or doc_ids[0] is None:
                        continue

                    doc_service_document = Doc_service_document(amend_name, 'amendment',
                                                                "complete",
                                                                download_id=download_id,
                                                                doc_id=doc_ids[0])

                    bill.add_doc_service_document(doc_service_document)
                else:
                    doc_id = self.scraper.download_and_register(amend_url, BRP.bill_documents, True,
                                                                content_type="application/pdf")
                    doc_service_document = Doc_service_document(amend_name, "other", "partial", doc_id)
                    bill.add_doc_service_document(doc_service_document)
            if 'Senate' in actor:
                actor = 'upper'
            elif 'House' in actor:
                actor = 'lower'
            else:
                actor = 'executive'

            if 'Gov.' in action or action.startswith('Ch.'):
                actor = 'executive'

            bill.add_action(actor, action, action_date)

            vote_ele = action_row.xpath_single("./div[contains(., 'Votes:')]/following-sibling::div[1]//a", BRP.test)
            if vote_ele:
                vote_url = vote_ele.get_attrib('href')
                vote_result = vote_ele.text_content()
                passed = None
                if 'pass' in vote_result.lower():
                    passed = True
                elif 'fail' in vote_result.lower():
                    passed = False
                self.scrape_vote(bill, vote_url, action, action_date, actor, passed)

        self.save_bill(bill)

    @staticmethod
    def create_slug(session):
        slug = session[:4]
        if 'ss' in session:
            sp_num = re.findall(r'ss(\d+)', session)[0]
            slug += 'E%s' % sp_num
        return slug

    def scrape_vote(self, bill, vote_url, action, action_date, chamber, passed):
        vote_doc = self.scraper.url_to_lxml(vote_url, BRP.bill_votes)
        count_string = vote_doc.xpath_single("//*[contains(text(), 'Total Votes:')]").text_content()
        yes_count = int(re.findall(r"Ayes:\s+(\d+)", count_string)[0])
        no_count = int(re.findall(r"Noes:\s+(\d+)", count_string)[0])
        nv_count = int(re.findall(r"Not:\s+(\d+)", count_string)[0])
        abs_count = int(re.findall(r"Exc\. Absent:\s+(\d+)", count_string)[0])
        ev_count = int(re.findall(r"Exc\. Vote:\s+(\d+)", count_string)[0])
        other_count = nv_count + abs_count + ev_count

        if passed is None:
            passed = yes_count > (no_count + other_count)
        vote = Vote(chamber, action_date, action, passed, yes_count, no_count, other_count)
        vote.add_source(vote_url)

        if yes_count > 0:
            yes_result = vote_doc.xpath_single("//b[text()='Ayes:']/ancestor::tr[1]").text_content()
            yes_result = re.sub(r'Ayes\:|Representative\(s\)\:|Senator\(s\):', '', yes_result)
            voters_list = re.split(r";|\n", yes_result)
            for voter in voters_list:
                if not voter.strip():
                    continue
                vote.yes(voter.strip())
        if no_count > 0:
            no_result = vote_doc.xpath_single("//b[text()='Noes:']/ancestor::tr[1]").text_content()
            no_result = re.sub(r'Noes\:|Representative\(s\)\:|Senator\(s\):', '', no_result)
            voters_list = re.split(r";|\n", no_result)
            for voter in voters_list:
                if not voter.strip():
                    continue
                vote.no(voter.strip())

        if nv_count > 0:
            nv_result = vote_doc.xpath_single("//b[text()='Not Voting:']/ancestor::tr[1]").text_content()
            nv_result = re.sub(r'Not Voting\:|Representative\(s\)\:|Senator\(s\):', '', nv_result)
            voters_list = re.split(r";|\n", nv_result)
            for voter in voters_list:
                if not voter.strip():
                    continue
                vote.other(voter.strip())
        if abs_count > 0:
            abs_result = vote_doc.xpath_single("//b[text()='Exc. Absence:']/ancestor::tr[1]").text_content()
            abs_result = re.sub(r'Exc\. Absence\:|Representative\(s\)\:|Senator\(s\):', '', abs_result)
            voters_list = re.split(r";|\n", abs_result)
            for voter in voters_list:
                if not voter.strip():
                    continue
                vote.other(voter.strip())
        if ev_count > 0:
            ev_result = vote_doc.xpath_single("//b[text()='Exc. Vote:']/ancestor::tr[1]").text_content()
            ev_result = re.sub(r'Exc\. Vote\:|Representative\(s\)\:|Senator\(s\):', '', ev_result)
            voters_list = re.split(r";|\n", ev_result)
            for voter in voters_list:
                if not voter.strip():
                    continue
                vote.other(voter.strip())
        bill.add_vote(vote)

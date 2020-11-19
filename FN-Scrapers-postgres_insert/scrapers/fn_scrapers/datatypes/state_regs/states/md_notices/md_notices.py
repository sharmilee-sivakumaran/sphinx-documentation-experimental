'''
Grab state regulations for Maryland State Regulations
'''
from __future__ import absolute_import

import logging
import re
import fn_scrapers.common.files as files

from string import capwords
from collections import deque
from dateutil.parser import parse
from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.common.http import request_lxml_html
from fn_scrapers.common.files.extraction import Extractors, ScraperDocument
from fn_scrapers.datatypes.state_regs import Notice, NoticeScraper

logger = logging.getLogger(__name__)

base_url = 'http://www.dsd.state.md.us/MDR/mdregister.html'

"""
Maryland StateRegs deal with a document in which each line is a separate element. This makes it hard to easily parse
the information that we want. It also makes it hard to find the start/end of notices/regulations.

The basic workflow is that we find an element common to each notice:
Final: 'Notice of Final Action'
Proposed: 'Notice of Proposed Action'
Governor action: 'EXECUTIVE ORDER'

and from there we determine that there is a notice for each of those elements that contain that element.

We try our best to guess the starting and ending positions for each based on titles, subtitles, and signatures because
there are often mistakes making hard rules impossible.

From these starting/ending positions, we then make sure that none overlap, and then we process and register each of
these notices to the document service. At the end we use this info to create the notices and regulations.
"""

def single(xpath_result):
    if len(xpath_result) == 0:
        raise ValueError("Expected 1 result - got 0")
    elif len(xpath_result) == 1:
        return xpath_result[0]
    elif len(xpath_result) > 1:
        raise ValueError("Expected 1 result - got {}".format(len(xpath_result)))


def maybe_single(xpath_result):
    if len(xpath_result) == 0:
        return None
    elif len(xpath_result) == 1:
        return xpath_result[0]
    elif len(xpath_result) > 1:
        raise ValueError("Expected 0 or 1 results - got {}".format(len(xpath_result)))


@scraper()
@tags(type="notices", country_code="US", subdivision_code="US-MD", group="state_regs")
class MDNoticeScraper(NoticeScraper):
    '''
    Maryland Notice Scraper
    '''

    def __init__(self):
        super(MDNoticeScraper, self).__init__("md")

    @property
    def max_title_len(self):
        return 600

    def do_scrape(self, start_date, end_date, **kwargs):
        main_page = request_lxml_html(base_url, rep_nbsp=False, abs_links=True)

        doc_list = main_page.xpath(".//a[contains(text(), 'Volume ')]")
        for doc in doc_list:
            file_name = doc.text_content()
            file_url = doc.attrib['href']
            if '.pdf' in file_url:
                continue
            date = re.findall(r'- (.*)', file_name)[0]
            publish_date = parse(date)

            if publish_date.date() > end_date:
                break
            if publish_date.date() < start_date:
                continue
            self.scrape_file(file_url, publish_date)

    def scrape_file(self, file_url, publish_date):
        """
        Scrape Register File
        """
        fil = files.register_download_and_documents(
            file_url,
            Extractors.html,
            parser=self.parse,
            serve_from_s3=True,
            encoding="cp1252")

        for k, doc in enumerate(fil.documents):
            self.create_notice(doc, fil.document_ids[k], publish_date, fil.s3_url)

    @staticmethod
    def _get_element_index(ele, parent):
        return None if ele is None else parent.index(ele)

    # pylint: disable=R0201
    def parse(self, file_page):
        """
        Parses page for executive orders and notices
        :param file_page:
        :return:
        """
        documents = []

        gov_actions_list = self.get_governer_actions_list(
            file_page.xpath('//p[(@class="ST" or @class="TI") and contains(., "EXECUTIVE ORDER")]'))
        for notice in gov_actions_list:
            try:
                documents.append(self.scrape_gov_action(notice))
            except Exception as e:
                logger.critical(e)

        notice_gen = self.get_notice_gen(file_page.xpath('//p[@class="Notice" and .="Notice of Final Action"]'))
        for notice in notice_gen:
            try:
                documents.append(self.scrape_notice(
                    notice, self._get_agency_for_final, "Final Action on Regulations", "final_notice"))
            except Exception as e:
                logger.critical(e)

        notice_gen = self.get_notice_gen(file_page.xpath('//p[@class="Notice" and .="Notice of Proposed Action"]'))
        for notice in notice_gen:
            try:
                documents.append(self.scrape_notice(
                    notice, self._get_agency_for_proposed, "Proposed Action on Regulations", "proposal_notice"))
            except Exception as e:
                logger.critical(e)

        return documents

    def get_governer_actions_list(self, not_list):
        """
        Gets a notice list in which notices do not overlap
        :param not_list:
        :return:
        """
        if not_list:
            parent = not_list[0].getparent()
        ind_list = []
        for i, notice in enumerate(not_list):
            if notice.getparent() != parent:
                logger.critical("Governer notices do not have the same parent element.")
            try:
                next_not = not_list[i + 1]
            except IndexError:
                # The last governer action will end at the next class = "TI" instance
                try:
                    next_not = notice.xpath('./following-sibling::p[@class="TI"][1]')[0]
                except IndexError:
                    # If there is no next title, then that probably means that these elements are within a subelement
                    # of the total document, so we use the last element in this subelement as the end
                    next_not = parent.getchildren()[-1]
                    logger.warning("There is no next title for the last governer action")
            ind_list.append((parent.index(notice), parent.index(next_not)))

        gov_actions_list = []
        for start, end in ind_list:
            gov_actions_list.append(list(parent.iterchildren())[start: end])
        return gov_actions_list

    def get_notice_gen(self, notices):
        """
        Get a notice list in which notices do not overlap.
        :param notices:
        :return:
        """
        if not notices:
            return
        notice_list = []
        # We assume parent is the same for each notice, which has always been true so far
        parent = notices[0].getparent()

        for i, notice in enumerate(notices):
            if notice.getparent() != parent:
                # In the case that parents are not the same, we recursively call this function on the rest of
                # the notices and skip the overlapping index check. Luckily this is very rare
                logger.warning("Notices do not have the same parent element")
                for other_notice in self.get_notice_gen(notices[i:]):
                    yield other_notice
                break
            notice_list.append(self.get_ind_of_notice(notice, notice.getparent()))

        # We make sure that notices do not overlap by making sure every element following will have a start index
        # after its preceding element's end index
        for i, (start, end) in enumerate(notice_list):
            try:
                next_start, _ = notice_list[i + 1]
            except IndexError:
                break
            if end > next_start:
                notice_list[i] = (start, next_start)

        for start, end in notice_list:
            yield list(parent.iterchildren())[start: end]

    def get_ind_of_notice(self, notice, parent, lookback=5):
        """
        Get the start, end index of the notice in relation to the parent element. Will look back to find a better
        starting element of the notice.
        :param notice: element with text 'notice of'
        :param parent: parent element of notice
        :param lookback: number of lines to lookback for start of notice
        :return:
        """
        pre_subtitle = maybe_single(notice.xpath('./preceding-sibling::p[@class="ST"][1]'))
        pre_chapter = maybe_single(notice.xpath('./preceding-sibling::p[@class="CH"][1]'))

        not_start_ind = start_ind = self._get_element_index(notice, parent)
        pre_subtitle_ind = self._get_element_index(pre_subtitle, parent)
        pre_chapter_ind = self._get_element_index(pre_chapter, parent)
        if pre_subtitle_ind and pre_subtitle_ind + lookback >= not_start_ind:
            start_ind = pre_subtitle_ind
        if pre_chapter_ind and pre_chapter_ind + lookback >= not_start_ind:
            start_ind = pre_chapter_ind

        # We will use the earliest index of the next signature, title, notice, or document to end our current notice.
        next_sig_ind = self._get_element_index(
            maybe_single(notice.xpath('./following-sibling::p[starts-with(@class, "Sig")][1]')), parent)
        next_title_ind = self._get_element_index(
            maybe_single(notice.xpath('./following-sibling::p[@class="TI"][1]')), parent)
        next_not_ind = self._get_element_index(
            maybe_single(notice.xpath('./following-sibling::p[@class="Notice" and starts-with(., "Notice")][1]')),
            parent)
        try:
            end_ind = min(x for x in [next_sig_ind, next_title_ind, next_not_ind] if x is not None)
        except ValueError:
            end_ind = len(parent.getchildren()) - 1
        return start_ind, end_ind

    def scrape_gov_action(self, siblings):
        """
        For governor actions, we don't really need much because we know a lot already. Because of this we just grab
        the title, summary, id and then stop
        :param siblings:
        :return:
        """
        additional_data = {"regulation_type": "executive", "notice_title": "Governor Action", "rule_id": "",
                           "agency": "Governor", "title": None, "summary": "", "anchor": None,
                           'contents': ["final_text", "final_notice"]}
        text = ""
        for i, sibling in enumerate(siblings):
            _class = sibling.get("class")
            text += sibling.text_content()
            if not additional_data["anchor"]:
                try:
                    additional_data["anchor"] = sibling.xpath(".//a")[0].get("name")
                except IndexError:
                    pass

            if not additional_data["title"] and (_class == "ST" or _class == "TI") and re.search("EXECUTIVE ORDER",
                                                                                                 sibling.text_content()):
                additional_data["title"] = sibling.text_content()
                # Next sibling will always be the one line summary we use
                try:
                    additional_data["summary"] = siblings[i + 1].text_content()
                except IndexError:
                    logger.critical(
                        "Governor Action's title should not be at the end for: {}".format(additional_data["title"]))
                    break

            if re.match(r"\[(\d+-\d+-\d+)\]", sibling.text_content()):
                additional_data["notice_id"] = additional_data["rule_id"] = re.match(r"\[(\d+-\d+-\d+)\]",
                                                                                     sibling.text_content()).group(1)
                break

        if not additional_data.get("notice_id") or not additional_data.get("summary"):
            raise ValueError("Notice ID or summary cannot be None")
        return ScraperDocument(text, scraper_id=additional_data["notice_id"], additional_data=additional_data)

    def scrape_notice(self, siblings, _get_agency_func, not_title, contents):
        """
        Scrape given notice and return a scraper document
        :param siblings: siblings to scrape
        :param _get_agency_func: Function to retrieve agency which is different in final and proposed notices
        :param not_title: title of notice
        :param contents: contents
        :return: ScraperDocument
        """
        additional_data = {"regulation_type": "regular", "notice_title": not_title, "summary": "", "rule_id": "",
                           "agency": None, "backup_agency": None, "title": "", "anchor": None, "effective_date": None,
                           'contents': [contents]}
        text = ""

        pre_title = maybe_single(siblings[0].xpath('./preceding-sibling::p[@class="TI"][1]'))
        if pre_title:
            additional_data["agency"] = re.sub(r"Title \w+", "", pre_title.text_content()).strip()

        queue = deque(siblings)
        # This loop needs to only hold a switch statement because within, it internally goes through lines
        while len(queue) != 0:
            sibling = queue.popleft()
            if sibling.get("class") == "ST" or sibling.get("class") == "CH":
                pass

            elif sibling.get("class") == "Notice" and "statement of purpose" in sibling.text_content().lower():
                if additional_data["summary"] != "":
                    logger.critical("Notice has more than one statement of purpose.")
                # We assume Statement of Purpose will go on until the next 'Notice' class
                while True:
                    sibling = queue[0] if len(queue) != 0 else None
                    if sibling is not None and sibling.get("class") != "Notice":
                        queue.popleft()
                        additional_data["summary"] += sibling.text_content()
                    else:
                        break

            else:
                # Typically class = "DN" or "regtext", but document is very inconsistent
                if not additional_data["rule_id"]:
                    try:
                        additional_data["notice_id"] = re.match(r"^\[([^]]+?)\]", sibling.text_content()).group(1)
                        additional_data["rule_id"] = re.search(r"(\d+-\d+)", additional_data["notice_id"]).group(1)
                    except AttributeError:
                        pass

                self._set_effective_date(sibling, additional_data)

                if additional_data["backup_agency"] is None:
                    # If we get the backup agency from this statement, it is also the summary so we need to
                    # iterate through the next lines to form the whole summary. We assume that the summary will continue
                    # until a class that's not NR1, NR2, or MSoNormal
                    if _get_agency_func(sibling.text_content(), additional_data):
                        while True:
                            additional_data["title"] += sibling.text_content()
                            self._set_effective_date(sibling, additional_data)

                            sibling = queue[0] if len(queue) != 0 else None
                            if sibling is not None and (
                                    sibling.get("class") == "NR1" or sibling.get("class") == "NR2" or sibling.get(
                                    "class") == "MSoNormal"):
                                queue.popleft()
                            else:
                                break

        for sibling in siblings:
            # Get anchor as early in the notice as possible
            if not additional_data["anchor"]:
                try:
                    additional_data["anchor"] = sibling.xpath(".//a")[0].get("name")
                except IndexError:
                    pass

            # Extracted text
            text += sibling.text_content() + "\n\n"

        if not additional_data.get("notice_id") or not additional_data.get("rule_id") or not additional_data.get(
                "agency") or not additional_data.get("title"):
            logger.critical("Notice missing required fields")
            pass

        return ScraperDocument(text, scraper_id=additional_data["notice_id"], additional_data=additional_data)

    def _set_effective_date(self, sibling, additional_data):
        effective_date = re.match(r"Effective Date:(.+?)\.", sibling.text_content())
        if effective_date:
            try:
                date = effective_date.group(1).strip()
                additional_data["effective_date"] = parse(date)
            except ValueError:
                logger.critical("Unable to extract Effective Date: {}".format(date))

    # Mostly copied over from previous md scraper
    def _get_agency_for_final(self, sibling_text, additional_data):
        agency = re.search(r"On .+?\d{4}, the(.*?)adopted[\s\S]+?", sibling_text, flags=re.S)
        if agency:
            agency_name = agency.group(1)
            agency_name = re.sub(r'\r\n', ' ', agency_name)
            agency_name = re.sub(r'\s+', ' ', agency_name).strip()
            if agency_name.endswith(' jointly'):
                agency_name = re.sub(r' jointly', '', agency_name).strip()
                agency_names = [name.strip() for name in agency_name.split('and the')]
            else:
                agency_names = [agency_name]
            additional_data["backup_agency"] = agency_names
            return True
        return False

    def _get_agency_for_proposed(self, sibling_text, additional_data):
        agency = re.search(r"The(.*?)(?:propose|proposes).*?", sibling_text, flags=re.S)
        if agency:
            agency_name = re.sub(r'\s+', ' ', agency.group(1)).strip()
            if agency_name.endswith(',') or ', in cooperation with the' in agency_name:
                agency_name = agency_name.strip(',')
                agency_name = agency_name.replace(' in cooperation with the ', ' ')
                agency_names = []
                for _agency in agency_name.split(','):
                    _agency = _agency.strip()
                    if _agency.startswith('and'):
                        _agency = _agency[4:]
                    agency_names.append(_agency)
            else:
                agency_names = [agency_name]
            additional_data["backup_agency"] = agency_names
            return True
        return False

    def create_notice(self, doc, doc_id, publish_date, s3_url):
        """
        Creates a notice
        :param doc:
        :param doc_id:
        :param publish_date:
        :param s3_url:
        :return:
        """
        additional_data = doc.additional_data

        title = additional_data.get("title")
        # Shortens title to the max title length, because pillar can't process large titles
        if len(title) > self.max_title_len:
            title = title[:self.max_title_len - 3] + "..."
        summary = additional_data.get("summary")

        notice = Notice("md", additional_data.get("notice_title"), publish_date, additional_data["notice_id"])
        notice["notice_contents"] = additional_data["contents"]
        if additional_data.get("effective_date"):
            notice["effective_date"] = additional_data.get("effective_date")
        if additional_data.get("agency"):
            notice.add_agency(name=capwords(additional_data.get("agency")))
        elif additional_data.get("backup_agency"):
            for agency in additional_data.get("backup_agency"):
                notice.add_agency(name=capwords(agency))

        # TODO: When we one day can set our own urls, this would be the url we would want for the notice
        notice.set_attachment(document_id=doc_id, url="{}#{}".format(s3_url, additional_data["anchor"]), quote=False)
        notice.set_regulation(additional_data["regulation_type"], scraper_regulation_id=additional_data['rule_id'],
                              title=title, summary=summary)

        self.save_notice(notice)

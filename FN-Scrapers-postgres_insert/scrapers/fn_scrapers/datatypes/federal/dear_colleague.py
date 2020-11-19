"""
Dear Colleague Scraper
"""
import re
from xml.etree import ElementTree
import boto
import os
import hashlib
import dateutil.parser
import injector

from fn_service.server import BlockingEventLogger, fmt
from fn_scraperutils.events.reporting import EventComponent, ReportingPolicy, Severity
from fn_scraperutils.doc_service.util import ScraperDocument
from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from fn_scrapers.api.scraper import scraper, tags
from fn_scrapers.api.resources import ScraperUtilsScraper
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.utils import JSONEncoderPlus


DEAR_COLLEAGUE_BUCKET_NAME = u"dearcolleagues"


class DearColleagueReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """
    def __init__(self):
        self.letter_list = ReportingPolicy(u"Dear Colleague List", Severity.critical)
        self.letter = ReportingPolicy(u"Dear Colleage Letter", Severity.warning)
        self.doc_service = ReportingPolicy(u"Doc Service Call", Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)


def _set_event_component(binder):
    binder.bind(EventComponent, u"scraper_dear_colleague_letters")


@scraper(handler_modules=[_set_event_component])
@tags(type=u"dear_colleague", country_code=u"US", group=u"federal")
class DearColleagueScraper(object):
    """
    Dear Colleague Scraper.
    """

    @injector.inject(
        logger=BlockingEventLogger,
        scraper=ScraperUtilsScraper,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher)
    def __init__(self, logger, scraper, metadata_client, scrape_item_publisher):
        self._logger = logger
        self._scraper = scraper
        self._locality = u"us"
        self.policy = DearColleagueReportingPolicy()
        self._metadata_client = metadata_client
        self.scrape_item_publisher = scrape_item_publisher
        self._scraper.send_create_process_event(u"dear_colleague")
        self.local_path = self._scraper.config.scraperutils.tempdir

    def scrape(self):
        s3_connection = boto.connect_s3(self._scraper.config.aws.access_key,
                                     self._scraper.config.aws.secret_access_key)
        bucket = s3_connection.get_bucket(DEAR_COLLEAGUE_BUCKET_NAME)
        for bucket_ele in bucket.list():
            filename = str(bucket_ele.key)
            full_path = self.local_path + filename
            if os.path.exists(full_path):
                os.remove(full_path)
            bucket_ele.get_contents_to_filename(full_path)
            self.scrape_xml(full_path)
            os.remove(full_path)

    def scrape_xml(self, path):
        """
        scrape xml file
        """
        try:
            root = ElementTree.parse(path).getroot()
        except ElementTree.ParseError:
            self.scrape_broken_file(path)
            return

        for result_entry in root.findall(u'channel/item'):
            output_entity = {}
            for attr in result_entry:
                attr.tag = re.sub(ur'\{.*\}', '', attr.tag)
            title = result_entry.find(u'title').text
            output_entity[u'title'] = title
            origin_link = result_entry.find(u'link').text

            public_date = result_entry.find(u'pubDate').text
            formed_updated_date = dateutil.parser.parse(public_date).date()
            output_entity[u'publication_date'] = formed_updated_date
            # TODO Figure out what this unused variable is
            author_str = result_entry.find(u'creator').text
            output_entity[u'authors'] = []

            category_list = result_entry.findall(u'category')

            bill_external_ids = []
            category_result = set()
            authors_list = []
            for category in category_list:
                if re.match(ur'[HhSs][JjCc]?(?:on)?[BRbr]?(?:es)?\d+', category.text.strip()):
                    letter, num = re.findall(ur'([HhSs][JjCc]?(?:on)?[BRbr]?(?:es)?)(\d+)', category.text.strip())[0]
                    bill_id = letter + u' ' + num
                    bill_external_ids.append(bill_id)
                    category_name = bill_id
                elif category.attrib[u'domain'] == u'post_tag':
                    name = category.text.strip()
                    name = re.sub(ur'<br>', '', name)
                    if name == u'Sr.' or name == u'Jr.':
                        if len(authors_list) == 1:
                            authors_list[0] = authors_list[0] + u' ' + name
                    else:
                        authors_list.append(name)
                    category_name = name
                elif re.match(ur'(?:(?:T[Hh])|(?:[A-Z]))\d+', category.text.strip()):
                    continue
                else:
                    category_name_group = re.findall(ur'[A-Z][^A-Z]*', category.text.strip())
                    category_name = u' '.join(category_name_group).strip()

                category_name = re.sub(r'\s+', ' ', category_name)
                category_result.add(category_name)

            if bill_external_ids:
                output_entity[u'bill_external_ids'] = bill_external_ids
            if category_result:
                output_entity[u'categories'] = list(category_result)
            content_list = result_entry.findall(u'encoded')
            full_text = u""

            for content in content_list:
                if content.text:
                    full_text += content.text

            name = re.findall(ur'From: The Honorable (.*?)\nSent By:', full_text)

            if name:
                author_name = re.sub(ur'<br>', u'', name[0].strip())
                output_entity[u'authors'] = [author_name]
            elif len(authors_list) > 0:
                output_entity[u'authors'] = authors_list
            else:
                self._logger.warning(__name__,
                                     u"scrape_error",
                                     fmt(u"No authors, skip letter with title: '{}'",
                                         title)
                                     )
                continue

            pdf_link = re.findall(ur'pdf-embedder url="(.*?)"', full_text)

            if pdf_link:
                pdf_link = pdf_link[0]

                documents, doc_service_ids = self._scraper.handle_file(
                    pdf_link, self.policy.doc_service, self._scraper.extraction_type.unknown, True)
                if doc_service_ids and doc_service_ids[0]:
                    output_entity[u'document_id'] = doc_service_ids[0]
                else:
                    self._logger.warning(__name__,
                                         u"doc_service_error",
                                         fmt(u"PDF Handling failed for url {}",
                                             pdf_link)
                                         )
                    continue
            else:
                flag = self.register_html_content(output_entity, full_text, origin_link)
                if not flag:
                    continue
            self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                         u"dear_colleague_letters",
                                                         self._locality,
                                                         output_entity,
                                                         json_encoder=JSONEncoderPlus)

    def scrape_broken_file(self, path):
        data = open(path, u'r').read().decode(u"utf-8")
        data += u'</item>'

        item_list = re.findall(ur'<item>(.*?)</item>', data, re.S)
        for item in item_list:
            output_entity = {}
            title = re.findall(ur'<title>(.*?)</title>', item, re.S)[0]
            output_entity[u'title'] = title

            origin_link = re.findall(ur'<link>(.*?)</link>', item, re.S)[0]

            public_date = re.findall(ur'<pubDate>(.*?)</pubDate>', item, re.S)[0]
            formed_updated_date = dateutil.parser.parse(public_date).date()

            if u'all.xml' in path and formed_updated_date.year == 2016:
                break
            output_entity[u'publication_date'] = formed_updated_date

            output_entity[u'authors'] = []

            category_list = re.findall(ur'(<category.*?><!\[CDATA\[(.*?)\]\]></category>)', item, re.S)

            bill_external_ids = []
            category_result = set()
            authors_list = []

            for cate_str, category in category_list:
                domain = re.findall(ur'domain=\"(.*?)\"', cate_str)
                if re.match(ur'[HhSs][JjCc]?(?:on)?[BRbr]?(?:es)?\d+', category.strip()):
                    letter, num = re.findall(ur'([HhSs][JjCc]?(?:on)?[BRbr]?(?:es)?)(\d+)', category.strip())[0]
                    bill_id = letter + u' ' + num
                    bill_external_ids.append(bill_id)
                    category_name = bill_id
                elif len(domain) > 0 and domain[0] == u'post_tag':
                    name = category.strip()
                    if name == u'Sr.' or name == u'Jr.':
                        if len(authors_list) == 1:
                            authors_list[0] = authors_list[0] + ' ' + name
                    else:
                        authors_list.append(name)
                    category_name = name
                elif re.match(ur'(?:(?:T[Hh])|(?:[A-Z]))\d+', category.strip()):
                    continue
                else:
                    category_name_group = re.findall(ur'[A-Z][^A-Z]*', category.strip())
                    category_name = u' '.join(category_name_group).strip()

                category_name = re.sub(ur'\s+', ' ', category_name)
                category_result.add(category_name)

            if bill_external_ids:
                output_entity[u'bill_external_ids'] = bill_external_ids
            if category_result:
                output_entity[u'categories'] = list(category_result)
            content_list = re.findall(ur'<content:encoded><!\[CDATA\[(.*?)\]\]></content:encoded>', item, re.S)
            full_text = u""

            for content in content_list:
                if content.strip():
                    full_text += content.strip()

            name = re.findall(ur'From: The Honorable (.*?)\nSent By:', full_text)

            if name:
                author_name = re.sub(ur'<br>', '', name[0].strip())
                output_entity[u'authors'] = [author_name]
            elif len(authors_list) > 0:
                output_entity[u'authors'] = authors_list
            else:
                self._logger.warning(__name__,
                                     u"scrape_error",
                                     fmt(u"No authors, skip letter with title: '{}'",
                                         title)
                                     )
                continue

            pdf_link = re.findall(ur'pdf-embedder url="(.*?)"', full_text)
            if pdf_link:
                pdf_link = pdf_link[0]

                documents, doc_service_ids = self._scraper.handle_file(
                    pdf_link, self.policy.doc_service, self._scraper.extraction_type.unknown, True)
                if doc_service_ids and doc_service_ids[0]:
                    output_entity[u'document_id'] = doc_service_ids[0]
                else:
                    self._logger.warning(__name__,
                                         u"doc_service_error",
                                         fmt(u"PDF Handling failed for url {}",
                                             pdf_link)
                                         )
                    continue
            else:
                flag = self.register_html_content(output_entity, full_text, origin_link)
                if not flag:
                    continue
            self.scrape_item_publisher.publish_json_item(u"federal_hub",
                                                         u"dear_colleague_letters",
                                                         self._locality,
                                                         output_entity,
                                                         json_encoder=JSONEncoderPlus)

    def register_html_content(self, output_entity, full_text, origin_link):
        save_to = self.local_path + u"temp.html"

        # Take this HTML snippet it and convert it to HTML
        head = u"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <body>
        """
        full_text = head + full_text + u"</body></html>"
        with open(save_to, u'w+') as tempfile:
            file_hasher = hashlib.sha384()
            tempfile.write(full_text.encode(u"utf-8"))
            tempfile.seek(0)
            file_hasher.update(full_text.encode(u"utf-8"))
            s3_url = self._scraper.s3_transferer.upload_to_s3(
                origin_link, tempfile, file_hasher.hexdigest(), u'text/html')

            headers = {}
            download_id = self._scraper.register_s3_url(
                self.policy.doc_service, s3_url, origin_link,
                file_hasher.hexdigest(), True, u'text/html', None, headers)

            docs, doc_ids = self._scraper.extract_and_register_documents(
                self._scraper.extraction_type.html, self.policy.doc_service,
                origin_link, download_id, None, downloaded_file=tempfile)

            if doc_ids and doc_ids[0]:
                output_entity[u'document_id'] = doc_ids[0]
            else:
                return False
        return True

    @staticmethod
    def html_parser(entities, parser_args=None):
        if not parser_args or u"text" not in parser_args:
            return []
        text = parser_args[u"text"]
        text = re.sub(ur'<.*?>', '', text)
        return [ScraperDocument(text)]


from __future__ import absolute_import

import os
import json
import jsonschema
import datetime
import html5lib

from fn_scraperutils.events.reporting import EventComponent
from abc import ABCMeta, abstractmethod, abstractproperty
from fn_scrapers.api.scraper import argument
from fn_document_service.blocking import ttypes
from ..common.base_scraper import ScraperBase


@argument('--start', metavar='mm/dd/yy', type=str, default=None,
          help='start date(Default: 30 days before today)(Format:04/01/16)')
@argument('--end', type=str, metavar='mm/dd/yy',
          default=None, help='end date(Default: today)(Format:04/30/16)')
@argument('--library', type=str, default=None, help='library to scrape')
@argument('--id', type=str, default=None, help='Legalminer id to scrape. Must also have correct library')
class ChinaBaseScraper(ScraperBase):
    """
    ChinaDocScraper

    Abstract class with
    properties: libraries, schema_name
    method: scrape_spec

    API
    search - http://api.legalminer.com/laws/libraryRecordList?pageIndex={}&pageSize={}&library={}&updateDate={}
    detail - http://api.legalminer.com/laws/detail?library={}&gid={}

    The scrape function is first called and then calls scrape_library on the libraries we support.
    scrape_library then calls scrape_ids within the update time period and then scrapes each bill(scrape_bill)
    scrape_bill sets up the payload with all of the required fields and then calls the method scrape_spec which
    will scrape the specific fields for the library/libraries.
    """
    __metaclass__ = ABCMeta

    # List of libraries to search through
    @abstractproperty
    def libraries(self):
        pass

    # Schema file name that will be used to validate payload
    @abstractproperty
    def schema(self):
        pass

    @property
    def search_url(self):
        return "http://api.legalminer.com/laws/libraryRecordList?pageIndex={}&pageSize={}&library={}&updateDate={}"

    @property
    def detail_url(self):
        return "http://api.legalminer.com/laws/detail?library={}&gid={}"

    def __init__(self, scraper_name, logger):
        super(ChinaBaseScraper, self).__init__(EventComponent.scraper_bills, scraper_name, "china")
        self.logger = logger

    def scrape(self, start, end, library, id):
        """
        :param start: start date in form "mm/dd/yy" defaults to 3 days before
        :param end: end date in form "mm/dd/yy" defaults to now
        """
        if not start:
            start_date = datetime.date.today()-datetime.timedelta(days=3)
        else:
            try:
                start_date = datetime.datetime.strptime(start, '%x').date()
            except ValueError:
                raise Exception("Unknown start date %s" % start)
        if not end:
            end_date = datetime.date.today()
        else:
            try:
                end_date = datetime.datetime.strptime(end, '%x').date()
            except ValueError:
                raise Exception("Unknown end date %s" % end)

        if start_date > end_date:
            raise Exception("Start date (%s) is after end date (%s)." % (start_date, end_date))

        if library:
            if id:
                self.scrape_bill(id, library)
            else:
                self.scrape_library(library, start_date, end_date)

        else:
            for library in self.libraries:
                self.scrape_library(library, start_date, end_date)

    def scrape_ids(self, library, start, end):
        """
        Uses the search api to find all relevant bills in library between start and end date
        :param library: string
        :param start: date object
        :param end: date object
        :return: list of unique doc_ids
        """
        doc_ids = set()
        page_index = 0
        page_size = 1000
        finished = False
        update_date = "{}%2000:00:00,{}%2023:59:59".format(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        while not finished:
            search_response = self.url_to_json(self.search_url.format(page_index, page_size, library, update_date),
                                               self.scraper_policy.doc_list)
            for doc in search_response["Data"]["Collection"]:
                doc_ids.add(doc["gid"])

            page_index += 1
            finished = page_index >= search_response["Data"]["PageCount"]
            self.logger.info(__name__, "Scraped bill ids for page {} of {} for total results {}"
                             .format(page_index, search_response["Data"]["PageCount"], search_response["Data"]["RecordCount"]))
        self.logger.info(__name__, "Finished scrape of ids")
        return list(doc_ids)

    def scrape_library(self, library, start, end):
        """
        Scrapes bill ids in the library, then scrapes each bill and sends to pillar
        :param library: string
        :param start: date object
        :param end: date object
        """
        for doc_id in self.scrape_ids(library, start, end):
            try:
                self.scrape_bill(doc_id, library)
            except:
                self.logger.critical(__name__, "individual_bill_scrape_failed", "scrape {} in library {} failed"
                                     .format(doc_id, library), exc_info=True)
        self.logger.info(__name__, "Successful scrape of Library: {}'".format(library))

    @staticmethod
    def set_val(key, val, bill_doc):
        if val:
            bill_doc[key] = val

    @staticmethod
    def get_schema(name):
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, name)
        with open(bill_json_schema_file_path, 'r') as f:
            return json.loads(f.read())

    def get_codename(self, val, download_id):
        try:
            code, name = val.items()[0]
            if len(val) > 1:
                self.logger.critical(__name__, "Skipping data",
                                     u"More than one value for code: {} name: {} in download id: {}"
                                     .format(code, name, download_id), exc_info=True)
            return {"code": code, "name": name}
        except:
            return None

    @staticmethod
    def get_codename_list(val):
        try:
            codename_list = []
            for code, name in val.items():
                codename_list.append({"code": code, "name": name})
            return codename_list
        except AttributeError:
            return None

    def html_to_text(self, html):
        """
        Transforms given html to text or None if html is None
        :param html:
        :return: Text of given html
        """
        if html:
            document = html5lib.parse(html, treebuilder="lxml", namespaceHTMLElements=False)
            return self._element_to_text(document.getroot())

    def _element_to_text(self, element):
        if not element.text:
            element.text = ""
        if not element.tail:
            element.tail = ""

        child_text = ""
        if element.getchildren():
            for child in element.getchildren():
                child_text += self._element_to_text(child)
        return element.text + child_text + element.tail

    def scrape_bill(self, doc_id, library):
        """
        Scrapes single bill and sends to pillar
        :param doc_id: string
        :param library: string
        :return:
        """
        # Downloads the api response for the needed download_id
        download_id, downloaded_file, _ = self.get_file_and_download_id(self.detail_url.format(library, doc_id),
                                                                        self.scraper_policy.doc_single, True)
        downloaded_file.seek(0)
        detail_response = json.load(downloaded_file)

        if detail_response["Code"] != u"OK":
            raise ValueError("API call for {} in {} not 'OK'".format(doc_id, library))

        detail_response = detail_response.get("Data")

        document_id = None
        # If there is fullText, we register document to get document_id
        if detail_response.get("fullText"):
            full_text = self.html_to_text(detail_response.get("fullText"))
            # Registers response's fullText(document) for the needed document_id
            doc = ttypes.Document(full_text)
            document_id = self.doc_service_client.register_documents(download_id, [doc])[0]

        bill_doc = {}

        # Required fields
        self.set_val("external_id", doc_id, bill_doc)
        self.set_val("library", library, bill_doc)
        self.set_val("title", detail_response.get("title"), bill_doc)
        self.set_val("document_id", document_id, bill_doc)
        self.set_val("download_id", download_id, bill_doc)

        self.scrape_spec(detail_response, bill_doc)
        jsonschema.validate(bill_doc, self.schema)
        self.save_doc(bill_doc)
        self.logger.info(__name__, "Successful scrape of ID: {}".format(doc_id))

    @abstractmethod
    def scrape_spec(self, detail_response, bill_doc):
        """
        Scrapes the special properties unique to the libraries
        """
        pass

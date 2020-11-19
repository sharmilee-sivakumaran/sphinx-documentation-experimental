import injector
import os
import json
import jsonschema

from ..common.base_scraper import ScraperBase
from fn_scrapers.api.scraper import scraper, tags
from fn_service.server import BlockingEventLogger
from fn_scraperutils.events.reporting import EventComponent

@scraper()
@tags(type="metadata", country_code="CN", group="international")
class ChinaMetadataScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ChinaMetadataScraper, self).__init__(EventComponent.scraper_bills, "china_metadata", "china")
        self.logger = logger

    @property
    def libraries(self):
        return ["CHL", "LAR", "PROTOCOL"]

    @property
    def dicts(self):
        return [("categories", "Category"), ("ratify_departments", "RatifyDepartment"),
                ("issue_departments", "IssueDepartment")]

    @property
    def category_url(self):
        return "http://api.legalminer.com/laws/getCategory?library={}&property={}&type=tree"

    @property
    def schema(self):
        return self.get_schema("china_metadata.json")

    @staticmethod
    def get_schema(name):
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, name)
        with open(bill_json_schema_file_path, 'r') as f:
            return json.loads(f.read())

    def scrape(self):
        for library in self.libraries:
            for metadata_type, name in self.dicts:
                url = self.category_url.format(library, name)
                mapping = self.url_to_json(url, self.scraper_policy.doc_list)

                if mapping and mapping["status"] == "ok":
                    metadata_values = mapping["data"].values()[0]
                    metadata = {"metadata_type": metadata_type, "metadata_values": metadata_values}
                    jsonschema.validate(metadata, self.schema)

                    self.save_doc(metadata)
                    self.logger.info(__name__, "Scraped {} for library {}".format(metadata_type, library))
                elif metadata_type == "ratify_departments" and library == "PROTOCOL":
                    # There is no metadata for this combination
                    pass
                else:
                    self.logger.critical(__name__, "individual_metadata_scrape_failed",
                                         "Failed to scrape metadata {} for library {}"
                                         .format(metadata_type, library), exc_info=True)
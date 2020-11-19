from __future__ import absolute_import, print_function

from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scrapers.common import http, files
from csv import DictReader
from io import StringIO


@scraper()
@argument("--count", default=1, type=int, help=u"Number of documents to 'scrape'")
@tags(type="example", group="example")
class CrsExampleScraper(object):
    def scrape_ids(self, count):
        csv_text = http.request_text("https://www.everycrsreport.com/reports.csv")
        reports = DictReader(StringIO(csv_text))
        ids = []
        for row in reports:
            if len(ids) >= count:
                break
            ids.append((row["url"], row["latestPDF"], row["latestHTML"]))

        return ids

    def scrape_item(self, item_id):
        json_url, latest_pdf, latest_html = item_id

        # Extract the report number
        info = http.request_json("https://www.everycrsreport.com/" + json_url)
        print("REPORT NUMBER: " + info["id"])

        # Get some info out of the html
        h = http.request_html5("https://www.everycrsreport.com/" + latest_html)
        title = u"".join(h.xpath("//h1")[0].itertext())
        print("REPORT TITLE: " + title)

        # And register the PDF
        fil = files.register_download_and_documents(
            "https://www.everycrsreport.com/" + latest_pdf,
            files.extractors.text_pdf)
        print("DOCUMENT ID: {}".format(fil.document_ids[0]))

# --*-- coding: utf-8 --*--
from __future__ import absolute_import

from fn_scrapers.common import http, files
from fn_scrapers.common.xpath_shortcuts import one_or_none
from fn_scrapers.common.http import HttpException
from requests import HTTPError
from fn_scrapers.api.scraper import scraper, tags, argument
from fn_service.server import fmt
import injector
import contextlib
import logging
from urlparse import urljoin

from fn_scrapers.common.experimental.persistent_fork_join import fork_join

logger = logging.getLogger(__name__)


@scraper()
@tags(type="tool", country_code="RU", group="international")
@argument('--parts', default=1, type=int)
@argument('--mypart', default=0, type=int)
@argument('--start-from', default=0, type=int)
class RussiaSpeedupScraper(object):

    @injector.inject(
        http_session=http.Session,
        file_session=files.Session)
    def __init__(self, http_session, file_session):
        http_session.set_as_instance()
        file_session.set_as_instance()

    def scrape(self, parts, mypart, start_from):
        fork_join(
            lambda: self.get_document_links(start_from),
            self.create_worker,
            thread_count=1,
            persistence_file="russia-reg-speedup.db",
            parts=parts,
            mypart=mypart)

    def get_document_links(self, start_from):
        pdf_links = set()

        limit = 50
        offset = start_from
        while True:
            logger.info("Getting offset: %s, limit: %s", offset, limit)
            request_data = {
                "keywords":"",
                "limit":limit,
                "offset":offset,
                "filters":[
                    ["range_yyyymmdd",["20170101","20180711"]],
                    ["obj_kind","doc"]
                ],
                "view":"json",
                "highlight":0,
                "sort_mode":"timestamp"
            }
            api_response = http.request_json(
                'https://rg.ru/api/search/',
                method="POST",
                headers={
                    'content-type': 'application/json',
                    'accept': 'application/json',
                },
                json=request_data)
            if len(api_response["items"]) == 0:
                break
            for item in api_response["items"]:
                page_url = urljoin('https://rg.ru/api/search/', item["uri"])
                doc = http.request_html5(page_url)
                doc_links = doc.xpath('//a[@target="_blank" and contains(@href, ".pdf")]/@href')
                if doc_links:
                    for doc_link in doc_links:
                        doc_link = urljoin(page_url, doc_link)
                        logger.info("Found PDF Link: %s on page: %s", doc_link, page_url)
                        pdf_links.add(doc_link)
                else:
                    logger.info("No PDF Link on page: %s", page_url)

            offset += limit

        return list(pdf_links)

    def register_and_extract(self, link):
        try:
            f = files.register_download_and_documents(
                link,
                files.extractors.tesseract,
                serve_from_s3=True,
                mimetype='application/pdf',
                extract_args={
                    'language': 'rus',
                    'pageCount': 25
                })
            logger.info("Extracted document: %s", f.document_ids[0] if f.document_ids else "NOTHING")
        except (HttpException, HTTPError):
            logger.critical(fmt("Unable to complete process for {}", link), exc_info=True)
            raise

    @contextlib.contextmanager
    def create_worker(self):
        current = self

        def worker(item):
            current.register_and_extract(item)

        yield worker

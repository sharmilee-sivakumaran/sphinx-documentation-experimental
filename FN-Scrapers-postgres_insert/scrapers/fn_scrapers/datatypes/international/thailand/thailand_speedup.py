# --*-- coding: utf-8 --*--
from __future__ import absolute_import

from fn_scrapers.common import http, files
from fn_scrapers.common.http import HttpException
from requests import HTTPError
from fn_scrapers.api.scraper import scraper, tags, argument
from fn_service.server import fmt
import injector
import contextlib
import logging
from uritools import urijoin

from multiprocessing import Process
from fn_scrapers.common.experimental.persistent_fork_join import fork_join

logger = logging.getLogger(__name__)


@scraper()
@tags(type="tool", country_code="TH", group="international")
@argument('--parts', default=1, type=int)
@argument('--mypart', default=0, type=int)
class ThailandSpeedupScraper(object):

    @injector.inject(
        http_session=http.Session,
        file_session=files.Session)
    def __init__(self, http_session, file_session):
        http_session.set_as_instance()
        file_session.set_as_instance()

    def scrape(self, parts, mypart):
        fork_join(self.get_document_links, self.create_worker, thread_count=1,
                  persistence_file="test.db", parts=parts, mypart=mypart)

    def get_document_links(self):
        links = []
        url = u'http://web.senate.go.th/w3c/senate/' + \
              u'lawdraft/index.php?kw=&page={page}&orby=&orrg=ASC'

        page = http.request_html5(url.format(page=1))
        total_pages = page.xpath('//ul[@class="pagination"]/li/a/@href')
        page_no = 1
        self.get_links(page, links)
        for page_link in total_pages:
            page_no += 1
            page = http.request_lxml_html(url.format(page=page_no))
            self.get_links(page, links)
        logger.info(fmt("Total number of documents: {}", len(links)))
        return reversed(links)

    def get_links(self, page, link_list):
        rows = page.xpath('//table[@class="table"]/tbody/tr')
        for row in rows:
            for i in xrange(5, 12):
                document_link = row.find('./td[{}]/a'.format(i))
                if document_link is not None:
                    link_list.append(urijoin(page.base, document_link.get('href')))

    def register_and_extract(self, link):
        try:
            files.register_download_and_documents(link, files.extractors.tesseract,
                                                  serve_from_s3=True, mimetype='application/pdf',
                                                  extract_args={
                                                    'language': 'tha',
                                                    'pageCount': 25
                                                  })
        except HttpException, HTTPError:
            logger.warning(fmt("Unable to complete process for {}", link), exc_info=True)

    @contextlib.contextmanager
    def create_worker(self):
        current = self

        def worker(item):
            logger.info("URL: %s", item)
            current.register_and_extract(item)

        yield worker

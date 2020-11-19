# -*- coding: utf-8 -*-
import os
import re
import injector
import datetime
import json

from dateutil.relativedelta import relativedelta
from lxml import html

from fn_scraperutils.events.reporting import EventComponent, ScrapeError, EventType
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info, get_default_http_headers

@scraper()
@argument('--start_date', help='start_date in the format YYYY-mm-dd i.e.2017-02-07', default=(datetime.date.today()-datetime.timedelta(days=30)).strftime("%Y-%m-%d"))
@argument('--end_date', help='end_date in the format YYYY-mm-dd i.e.2018-02-07', default=datetime.date.today().strftime("%Y-%m-%d"))
@tags(type="notices", country_code="RU", group="international")
class RussiaRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(RussiaRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "russia_reg_notice", "russia_reg_notice")

        self.logger = logger

        self.base_url = u'https://rg.ru'
        self.search_url = u'{base_url}/doc-search/'
        self.search_result_url = u'{base_url}/doc-search/doctype={doctype}&materialTypes=doc&from={from_date}&to={to_date}'
        self.api_url = u"https://rg.ru/api/search/"
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), u"schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, u"russia_notice.json")

        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info(u'Russian Federation').alpha_2)
        self.current_date = datetime.date.today()
        self.doc_type = { #u'Все типы': u'any' ,
                          u'Федеральный закон': u'fedzakon' ,
                          u'Конституция': u'main' ,
                          u'Постановление': u'postanov' ,
                          u'Указ': u'ukaz' ,
                          u'Приказ': u'prikaz' ,
                          u'Сообщение': u'soobshenie' ,
                          u'Распоряжение': u'raspr' ,
                          u'Законопроект': u'zakonoproekt' }


    @staticmethod
    def return_date_as_date_obj(date_string):
        return datetime.datetime.strptime(date_string, "%Y-%m-%d").date()

    @staticmethod
    def check_date(start_date, end_date):
        date_re = re.compile('\d{4}\-\d{2}\-\d{2}')
        if date_re.match(start_date) and date_re.match(end_date):
            return True
        else:
            return False

    @staticmethod
    def html_to_text(string):
        return html.fromstring(string).text_content()

    # function for scrape data
    def scrape(self, start_date, end_date):

        if self.check_date(start_date, end_date):
            start_date = self.return_date_as_date_obj(start_date)
            end_date = self.return_date_as_date_obj(end_date)
        else:
            raise ValueError("Invalid date format")

        if end_date < start_date:
            raise ValueError("Invalid Start and end date parameter. Failed to start scraping.")

        self.scrape_notices(start_date, end_date)

    def scrape_notices(self, start_date, end_date):
        # https://rg.ru/doc-search/
        search_url = self.search_url.format(base_url=self.base_url)
        self.logger.info(__name__, fmt(u"Going to notice search page: {}", search_url))
        self.http_get(search_url, self.scraper_policy.doc_list, request_args={'timeout':(180,180)})
        country = u"Russia"
        publication_name = u"Russian Gazette (Российская газета)"
        for doc_type_key, doc_type_val in self.doc_type.items():
            notice_type_per_loop = doc_type_key

            self.logger.info(__name__, fmt(u"Scraping for Notice type: {}", notice_type_per_loop, ))
            limit = 50
            offset = 0
            while True:
                from_date_str = start_date.strftime("%Y%m%d")
                from_date_str_for_referer = start_date.strftime("%d.%m.%Y")
                # this date will be current date or any latest date
                to_date_str = end_date.strftime("%Y%m%d")
                to_date_str_for_referer = end_date.strftime("%d.%m.%Y")

                search_url_for_referer = "{}?doctype=any&materialTypes=doc&from={}&to={}".format(search_url,
                                                                                                 from_date_str_for_referer,
                                                                                                 to_date_str_for_referer)
                post_data_for_ajax = {"keywords": "", "limit": limit, "offset": offset,
                                      "filters": [["doctype", doc_type_val],
                                                  ["range_yyyymmdd", [from_date_str, to_date_str]],
                                                  ["obj_kind", "doc"]], "view": "json", "highlight": 0,
                                      "sort_mode": "timestamp"}

                extra_http_headers = {'X-Requested-With': 'XMLHttpRequest',
                                      'Accept': 'application/json, text/javascript, */*; q=0.01',
                                      'Content-type': 'application/json',
                                      'Origin': self.base_url,
                                      'Referer': search_url_for_referer}
                http_headers = get_default_http_headers()
                http_headers.update(extra_http_headers)

                req_args = {'json': post_data_for_ajax, 'headers': http_headers, 'timeout':(180,180)}
                resp = self.http_request(self.api_url, method='POST', request_args=req_args)
                resp.encoding = 'utf-8'
                response_data = json.loads(resp.text)
                if response_data["count"] == 0:
                    self.logger.info(__name__, fmt(u"No record found for doctype {}",doc_type_val))
                    break
                if len(response_data["items"]) == 0 and offset >= response_data["count"]:
                    self.logger.info(__name__, fmt(u"This was last iteration of fetching records using Ajax."))
                    break
                self.logger.info(__name__, fmt(u"No. of record found {} with {} items. Notice type: {}", response_data["count"], len(response_data["items"]), notice_type_per_loop))
                self.logger.info(__name__, fmt(u"Successfully fetch the next {} bills starting from {}.", limit, offset))

                for item in response_data["items"]:
                    try:
                        notice_title = self.html_to_text(item["title"])
                        notice_id = None
                        publication_date = datetime.datetime.strptime(item["datetime"], "%Y%m%d%H%M")
                        notice_year = publication_date.date().year
                        publication_date = publication_date.date().strftime("%Y-%m-%d")
                        publication_issue = None
                        source_url = urljoin(self.base_url,item["uri"])
                        self.logger.info(__name__, fmt(u"Going to visit url: {}", search_url))

                        '''
                        #
                        # https://rg.ru/2018/04/12/invalid-dok.html
                        # https://rg.ru/2018/03/12/mrot-dok.html
                        # https://rg.ru/2018/03/14/minobr-prikaz127-site-dok.html
                        # https://rg.ru/2017/09/15/strategiya-dok.html (gif)
                        #
                        '''
                        self.http_get(source_url, self.scraper_policy.doc, request_args={'timeout':(180,180)})
                        publication_issue_text = self.extract_as_one(u'//div[@class="b-material-head__row"]/div[@class="b-material-head__gazeta"]/div[@class="b-material-head__anchor"]/a//text()')
                        if publication_issue_text:
                            publication_split_list = publication_issue_text.split(u"Российская газета -")
                            if publication_split_list:
                                publication_issue = publication_split_list[-1]
                                publication_issue = re.sub(r'\(.+\)', '', publication_issue)
                                publication_issue = publication_issue.strip()

                        article_section = self.xpath_single(u"//article[contains(@class,'b-material-wrapper__body')]")
                        if not article_section:
                            self.logger.critical(__name__, u"individual_notice_scrape_failed", fmt(
                                u"Notice could not be scraped. Article section not found on page. Url:{}", source_url))

                        document_url = ''
                        document_title = ''
                        notice_id_html = article_section.xpath(u'.//p[contains(.,"Зарегистрирован")]')
                        if notice_id_html:
                            notice_id_raw_text = self.extract_as_one(u'.//text()', sel=notice_id_html[0])
                            notice_id_split_list = notice_id_raw_text.split(u"№")
                            if notice_id_split_list:
                                notice_id = notice_id_split_list[-1]
                                notice_id = notice_id.strip()
                                notice_id = u"{notice_year} - {notice_id}".format(notice_year=notice_year,notice_id=notice_id)

                        document_list = article_section.xpath(u".//div[contains(@class,'b-material-wrapper__attachments__item__desc')]/a", )
                        if document_list and len(document_list)>0:
                            document_url = self.extract_single('@href', sel=document_list[0])
                            document_url = urljoin(self.base_url, document_url)
                            document_title = self.extract_as_one('text()', sel=document_list[0])

                            temp_document_url = document_url.lower().strip()
                            if temp_document_url.endswith("gif") or temp_document_url.endswith("jpg") or temp_document_url.endswith("jpeg"):
                                self.logger.info(__name__, fmt(u"Skipping document because it is of image type: {}", document_url))
                                document_url = source_url
                        else:
                            document_url = source_url
                            document_title = notice_title

                        # Assign extracted notice details.
                        russia_notice = self.model_factory.create_bill_doc()
                        russia_notice.country = country
                        russia_notice.title = notice_title
                        if notice_id:
                            russia_notice.notice_id = notice_id
                        russia_notice.notice_type = notice_type_per_loop
                        russia_notice.publication_name = publication_name
                        russia_notice.publication_date = publication_date
                        if publication_issue:
                            russia_notice.publication_issue = publication_issue
                        russia_notice.source_url = source_url
                        russia_notice.document_title = document_title

                        resp = self.http_request(document_url, "HEAD", request_args={'timeout':(180,180)})

                        # Default PDF type
                        extraction_type = self.extraction_type.unknown

                        if 'pdf' in resp.headers['Content-Type']:
                            extraction_type = self.extraction_type.tesseract
                            content_type = resp.headers['Content-Type']
                            extract_args = {
                                'language': 'rus',
                                'pageCount': 25
                            }
                        else:
                            self.logger.info(__name__, fmt(u"Response header: {}", resp.headers['Content-Type']))
                            extraction_type = self.extraction_type.html
                            content_type = resp.headers['Content-Type']
                            extract_args = None

                        download_id, _, doc_ids = self.register_download_and_documents(
                            document_url,
                            self.scraper_policy.doc_service,
                            extraction_type,
                            True,
                            content_type=content_type,
                            extract_args=extract_args)

                        if len(doc_ids) > 0 and doc_ids[0]:
                            document_id = doc_ids[0]
                        else:
                            raise ValueError(
                                "Document ID not found while registering document with url {}".format(
                                    document_url))
                        if not download_id:
                            raise ValueError(
                                "Download ID not found while registering document with url {}".format(
                                    document_url))

                        russia_notice.document_id = document_id
                        russia_notice.download_id = download_id

                        if self.validate_doc(russia_notice):
                            self.save_doc(russia_notice.for_json())
                        else:
                            self.logger.critical(__name__, u"validation error",
                                                 self.json_dumps(message=russia_notice.for_json()))

                    except Exception as e:
                        self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e),
                                         exc_info=True)
                self.logger.info(__name__, fmt(u"Successfully fetch the next {} bills starting from {}.", limit, offset))
                offset += limit


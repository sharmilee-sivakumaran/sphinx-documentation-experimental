# -*- coding: utf-8 -*-
import re
import os
import injector
import datetime

from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger, fmt

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info

@scraper()
@argument("--start-year", help="Start Year to scrape data from Chile Bulletin", type=int, default=0, choices=range(2017, datetime.date.today().year+1))
@tags(type="notices", country_code="CL", group="international")
class ChileRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(ChileRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "chile_reg_notice", "chile_reg_notice")

        self.logger = logger
        self.base_url = u'http://www.diariooficial.interior.gob.cl'
        self.electronic_edition_url = u'http://www.diariooficial.interior.gob.cl/edicionelectronica/'
        # http://www.diariooficial.interior.gob.cl/edicionelectronica/index.php?date=28-12-2017&edition=41943
        self.search_url_for_general_rule = u'{base_url}/edicionelectronica/index.php?date={date}'
        # http://www.diariooficial.interior.gob.cl/edicionelectronica/normas_particulares.php?date=28-12-2017&edition=41943
        self.search_url_for_specific_rule = u'{base_url}/edicionelectronica/normas_particulares.php?date={date}'

        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "chile_notice.json")

        self.model_factory = ModelFactory(notice_json_schema_file_path, notice_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("chile").alpha_2)

    def scrape(self, start_year):
        try:
            self.scrape_notices(int(start_year))
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed", fmt("{} notices could not be scraped. {}",self.scraper_name, e), exc_info=True)
            raise ScrapeError(fmt("{} bills could not be scraped. {}",self.scraper_name, e))

    def scrape_notices(self, start_year):
        end_date = datetime.date.today()
        if start_year == 0:
            start_date = end_date - datetime.timedelta(days=30)
        elif start_year in range(2017, start_year + 1):
            start_date = datetime.date(start_year, 1, 1)
        else:
            raise ValueError("Invalid Start year parameter. Failed to start scraping.")

        self.logger.info(__name__,fmt(u"Going to home page."))
        self.http_get(self.base_url, self.scraper_policy.doc_list)
        general_rule_page_url = self.electronic_edition_url

        previous_date = end_date
        i = 1
        self.logger.info(__name__, fmt(u"Going forward with start date {} and end date {}", start_date, end_date))
        while True:
            self.logger.info(__name__, fmt(u"Going to Electronic Edition page. {}",general_rule_page_url))
            self.http_get(general_rule_page_url, self.scraper_policy.doc_list)
            general_rule_page_url = self.resp.url
            self._sel.root.make_links_absolute(base_url=general_rule_page_url)
            # <a href="index.php?date=13-02-2018&amp;edition=41982&amp;v=1" title="">Ver edición</a>
            notice_edition_xpath = u'//section[@class="norma_general"]/div[contains(@class, "wrapsection") and contains(@class, "selectedEdition")]/ul/li[./a[contains(@href,"index.php")  and contains(.,"edici")]]/a/@href'
            edition_urls = self.extract(notice_edition_xpath)
            if edition_urls:
                for url in edition_urls:
                    general_rule_page_url = url
                    self.logger.info(__name__, fmt(u"Going to Electronic Edition page. {}", general_rule_page_url))
                    self.http_get(general_rule_page_url, self.scraper_policy.doc_list)
                    general_rule_page_url = self.resp.url
                    self._sel.root.make_links_absolute(base_url=general_rule_page_url)
                    self.parse_notice_details(general_rule_page_url)
                    specific_rule_page_url = self.extract_single(u"//a[contains(@href,'normas_particulares.php') and contains(.,'Particulares')]/@href")
                    self.logger.info(__name__, fmt(u"Going to Electronic Edition page. {}", specific_rule_page_url))
                    self.http_get(specific_rule_page_url, self.scraper_policy.doc_list)
                    self._sel.root.make_links_absolute(base_url=specific_rule_page_url)
                    self.parse_notice_details(specific_rule_page_url)
            else:
                specific_rule_page_url = self.extract_single(u"//a[contains(@href,'normas_particulares.php') and contains(.,'Particulares')]/@href")
                self.parse_notice_details(general_rule_page_url)
                self.logger.info(__name__, fmt(u"Going to Electronic Edition page. {}", specific_rule_page_url))
                self.http_get(specific_rule_page_url, self.scraper_policy.doc_list)
                self._sel.root.make_links_absolute(base_url=specific_rule_page_url)
                self.parse_notice_details(specific_rule_page_url)

            if previous_date.weekday() == 0:
                i += 1
                previous_date = end_date - datetime.timedelta(days=i)
            else:
                previous_date = end_date - datetime.timedelta(days=i)

            if start_date > previous_date:
                break
            i += 1

            general_rule_page_url = self.search_url_for_general_rule.format(base_url=self.base_url, date=previous_date.strftime("%d-%m-%Y"))


    def parse_notice_details(self, general_rule_page_url=None):

        # <p class="nofound">No existen publicaciones en esta edición en la fecha seleccionada</p>
        not_found_message = self.xpath_single('//section[@class="norma_general"]//p[@class="nofound"]')
        if not_found_message:
            self.logger.warning(__name__ , u"individual_notice_scrape_failed", u"Notices not found.")
            return None

        country = 'Chile'
        publication_name = 'Official Journal (Diario Oficial)'

        # class="containerdate"
        upper_section_div = self.xpath_single('//div[@class="containerdate"]')
        if not upper_section_div:
            raise Exception("Website may have changed - Div can not be parsed.")

        # Issue Number (Edición Núm): 41.981.
        # Issue Number (Edición Núm): 41.936.
        # Issue Number (Edición Núm): 41.982-B. -
        issue_number = self.extract_single('.//li[1]/text()', sel=upper_section_div)
        issue_number = issue_number.strip()
        m = re.search(u'Núm\.\s*(\d+\.\d+[\-\.\w]*)', issue_number, re.I|re.U)
        if m:
            issue_number = m.group(1)
            issue_number = issue_number.strip('.')
        else:
            issue_number = None

        publication_date = self.extract_single('.//li[2]/strong/text()', sel=upper_section_div)
        publication_date = publication_date.strip()
        publication_date = parse_date_as_str(publication_date)

        norma_general_section = self.xpath_single('//section[@class="norma_general"]')
        if not norma_general_section:
            raise Exception("Website may have changed - Normas Generales can not be parsed.")

        rows = norma_general_section.xpath('.//tr[not(contains(td/@class,"title1") or contains(td/@class,"title2") or contains(td/@class,"title3"))]')
        if not rows:
            raise Exception("Website may have changed - Notice rows can not be parsed.")

        department_name = ''
        sub_department_name = ''
        notice_type = u'Specific Rule' if u'normas_particulares.php' in general_rule_page_url else u'General Rule'
        for row in rows:
            if row.re(r'class\W+title4\W'):
                department_name = self.extract_single('./td[1]/text()', sel=row)
                # When a new department row will start it means there should be new sub-department.
                # So, we can reset sub-department to blank.
                sub_department_name = ''
            elif row.re(r'class\W+title5\W'):
                sub_department_name = self.extract_single('./td[1]/text()', sel=row)
                if sub_department_name:
                    sub_department_name = sub_department_name.split(u" / ")
            elif row.re(r'class\W+content\W'):
                # Extract notice details for given notice search url
                department_list = []
                chile_notice = self.model_factory.create_bill_doc()
                chile_notice.country = country
                chile_notice.publication_name = publication_name
                chile_notice.publication_date = publication_date
                if issue_number:
                    chile_notice.publication_issue = issue_number

                notice_title = self.extract_single('./td[1]/text()', sel=row)
                document_title = self.extract_single('./td[2]/a/text()', sel=row)
                m = re.search(r'\(\s*(.+?)\s*\)',document_title)
                if m:
                    chile_notice.notice_id = m.group(1)
                document_url = self.extract_single('./td[2]/a/@href', sel=row)

                chile_notice.notice_type = notice_type
                chile_notice.title = notice_title
                chile_notice.source_url = general_rule_page_url
                chile_notice.document_title = document_title

                if department_name:
                    department_obj = self.model_factory.create('NoticeDepartmentSchema')
                    department_obj.department_name = department_name.title()
                    department_obj.department_hierarchy = 0
                    department_list.append(department_obj)
                    if sub_department_name and isinstance(sub_department_name,list):
                        for index, sub_department_name_str in enumerate(sub_department_name, start=1):
                            department_obj = self.model_factory.create('NoticeDepartmentSchema')
                            department_obj.department_name = sub_department_name_str.strip()
                            department_obj.department_hierarchy = index
                            department_list.append(department_obj)

                    chile_notice.departments = department_list

                resp = self.http_request(document_url, "HEAD")

                if 'html' in resp.headers['Content-Type']:
                    extraction_type = self.extraction_type.html
                    content_type = resp.headers['Content-Type']
                else:
                    extraction_type = self.extraction_type.unknown
                    content_type = resp.headers['Content-Type']

                download_id, _, doc_ids = self.register_download_and_documents(document_url,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)

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

                chile_notice.document_id = document_id
                chile_notice.download_id = download_id

                if self.validate_doc(chile_notice):
                    self.save_doc(chile_notice.for_json())
                else:
                    self.logger.critical(__name__, "validation error",
                                         self.json_dumps(message=chile_notice.for_json()))


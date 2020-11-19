# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import injector
import datetime as dt
import math
import dateparser
import lxml.html as LH
from random import randint
from time import sleep
from dateutil.relativedelta import relativedelta
from lxml.html import fromstring, tostring
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy
from ..common.utils import parse_date_as_str, get_official_language_list, get_country_info

REQUESTS_RETRY_POLICY = Retry500RequestsRetryPolicy(max_attempts=100,max_retry_time=2000,max_attempt_delay=1000)

@scraper()
@argument("--start_year", help="Start Year to scrape data from Mexico Notice Website", type=int, default=0, choices=range(2017,dt.date.today().year+1))
@tags(type="notices", country_code="MX", group="international")
class MexicoRegNoticeScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(MexicoRegNoticeScraper, self).__init__(EventComponent.scraper_bills, "mexico_reg_notice", "mexico_reg_notice")
        self.base_url = u'http://www.dof.gob.mx/index.php'
        self.advance_search_url = u'http://www.dof.gob.mx/busqueda_avanzada.php?pb=S&cod_orga=TODOS'
        self.list_url = u"http://www.dof.gob.mx/busqueda_detalle.php?vienede=avanzada&busqueda_cuerpo=&BUSCAR_EN=T&textobusqueda=&TIPO_TEXTO=Y&dfecha={start_day}%2F{start_month}%2F{start_year}&choosePeriodDate=D&hfecha={end_day}%2F{end_month}%2F{end_year}&orga%5B%5D=TODOS%2C0"
        self.logger = logger
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "mexico_notice.json")
        self.model_factory = ModelFactory(notice_json_schema_file_path,notice_json_schema_file_path)
        self.current_date = dt.datetime.now().date()
        self.document_link = ''

    def fetch_static_content(self, html_file):
        html_page = html_file.read()
        page_content = html_page.decode('utf-8')
        self.set_selector(page_content)
        required_block = self.xpath('//div[@id="DivDetalleNota"]').extract()
        html_text = ''
        if required_block:
            document_block = '<div id="cuerpo_principal">' + required_block[0] + "</div>"
            xroot = self._sel.root
            xroot.make_links_absolute(self.document_link)
            html_tag = xroot.xpath('//html')
            parent_tag = xroot.xpath('//body')
            #replacing head tag
            head_tag = html_tag[0].xpath('head')
            html_tag[0].replace(head_tag[0],
                                LH.fromstring('<head><title>DOF - Diario Oficial de la Federación</title></head>'))
            block = parent_tag[0].xpath('//div[@id="cuerpo_principal"]')
            #replacing div tag
            if block:
                parent_tag[0].replace(block[0] , LH.fromstring(document_block))
                table_block = block[0].xpath('//div[@id="DivDetalleNota"]//table')
                if table_block:
                    data = table_block[-1].text_content()
                    if data.strip().startswith('En el documento que '):
                        table_block_parent = table_block[-1].getparent()
                        table_block_parent.remove(table_block[-1])
            #replace style block
            style_tag = xroot.xpath('//div[@id="DivDetalleNota"]/style')
            if style_tag:
                style_parent = style_tag[0].getparent()
                style_parent.remove(style_tag[0])
            #replacing all script tag
            script_tags = parent_tag[0].xpath('script')
            for script_tag in script_tags:
                parent_tag[0].remove(script_tag)
            #replacing noscript tag
            no_script_tag = parent_tag[0].xpath('noscript')
            if no_script_tag:
                parent_tag[0].remove(no_script_tag[0])

            html_text = tostring(xroot)
            html_tags = xroot.xpath('//html')
            if len(html_tags) == 2 and html_tags[1].text_content().startswith('DOF - Diario Oficial'):
                html_text = html_text.replace("<html>",'',1)
                html_text = html_text.replace("</html>",'',1)
            html_text = html_text.replace('\r', '').replace('\n', '').replace('\t', '')
        else:
            raise Exception(fmt("No HTML content on link : {}", self.document_link))
        return html_text

    def scrape(self,start_year):
        try:
            self.logger.info(__name__, fmt(u"Fetching for search page link  - {} ", self.advance_search_url))
            self.http_get(self.advance_search_url,self.scraper_policy.doc_list,request_args={'timeout':(180, 180)},retry_policy=REQUESTS_RETRY_POLICY)
            if start_year == 0:
                start_date = self.current_date - relativedelta(days=30)
            else:
                start_date = dt.datetime(year=int(start_year), month=1, day=1).date()
            end_date = start_date
            while end_date < self.current_date:
                start_date = end_date
                #overcoming website limitations so fetching the records in three months slot
                end_date = start_date + relativedelta(months=+3)
                if end_date > self.current_date:
                    end_date = self.current_date

                start_date_day = str(start_date.day).zfill(2)
                start_date_month = str(start_date.month).zfill(2)
                start_date_year = str(start_date.year)
                day = str(end_date.day).zfill(2)
                month = str(end_date.month).zfill(2)
                year = str(end_date.year)
                sleep_time = randint(5,10)
                self.logger.info(__name__,fmt("Going into sleep mode for {} seconds ",sleep_time))
                sleep(sleep_time)
                data = {'globalPage':1,'iniciaMuestra':0,'actualPage':1}
                search_url = self.list_url.format(start_day=start_date_day,start_month=start_date_month,start_year=start_date_year,end_day=day,end_month=month,end_year=year)
                self.http_post(search_url,self.scraper_policy.doc_list,request_args={'data':data,'timeout':(180, 180)},retry_policy=REQUESTS_RETRY_POLICY)
                table = self.xpath_single('//td/form[@name="formbus"]//table[@class="bus_det_list"]')
                if table:
                    total_data = self.extract_as_one('./tr/td[position()=1]/text()',sel=table)
                    if total_data:
                        total_data = total_data.split(u'de')
                        if len(total_data)==2:
                            total_data = total_data[1].strip()
                            total_pages = (int(total_data) +9)//10
                            self.logger.info(__name__,fmt("Total no of records from {} to {}: {}",start_date.strftime('%d/%m/%Y'),end_date.strftime('%d/%m/%Y'),total_data))
                            self.logger.info(__name__, "Records on page no : 1")
                            self.scrape_notice(table)
                            for val in range(2,total_pages+1):
                                self.logger.info(__name__, fmt("Records on page no : {}", val))
                                #adding sleep for delay in consecutive hits to the website
                                sleep_time = randint(5, 10)
                                self.logger.info(__name__, fmt("Going into sleep mode for {} seconds ", sleep_time))
                                sleep(sleep_time)
                                data = {'globalPage':val,'iniciaMuestra':0,'actualPage':val}
                                self.http_post(search_url,self.scraper_policy.doc_list,request_args={'data':data,'timeout': (180, 180)},retry_policy=REQUESTS_RETRY_POLICY)
                                table = self.xpath_single('//td/form[@name="formbus"]//table[@class="bus_det_list"]')
                                if table:
                                    self.scrape_notice(table)
                                else:
                                    self.logger.info(__name__,fmt("No table found on page no  :{}  from {} to {} ",val,start_date.strftime('%d/%m/%Y'),end_date.strftime('%d/%m/%Y')))
                        else:
                            self.logger.critical(__name__, u"scraper_run_finished",
                                             fmt(u"{} : No Notices Found from {} to {}", self.scraper_name.title(),start_date.strftime('%d/%m/%Y'),end_date.strftime('%d/%m/%Y')))
                    else:
                        self.logger.critical(__name__, u"scraper_run_finished",
                                             fmt(u"{} : HTML table row not found for extracting total number of pages.",
                                                 self.scraper_name.title()))
                else:
                    self.logger.critical(__name__, u"scraper_run_finished",
                                         fmt(u"{} : HTML table not found for extracting notices",
                                             self.scraper_name.title()))
        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} notices could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{}  notices could not be scraped.", self.scraper_name.title()),
                              self.advance_search_url)


    def scrape_notice(self,table):
        rows = table.xpath('./tr/td[@class="txt_azul"]')
        for row in rows:
            publication_date = self.extract_single('./b/text()', sel=row)
            department_name = self.extract_as_one('text()', sel=row)
            title = self.extract_single('./a//text()', sel=row)
            title_link = self.extract_single('./a[@class="txt_azul"]/@href', sel=row)
            if publication_date and department_name and title and title_link:
                self.scrape_mexico_notice(publication_date,department_name,title,title_link)


    def scrape_mexico_notice(self,publication_date,department_name,title,title_link):
        try:
            mexico_notice = self.model_factory.create_bill_doc()
            mexico_notice.publication_name = "Official Diary (Diario Oficial de le Federación DOF)"
            mexico_notice.country = "Mexico"
            department_list = []
            department_hierarchy = 0
            department_obj = self.model_factory.create("DepartmentSchema")
            if department_name:
                department_obj.department_name = department_name.title()
                department_obj.department_hierarchy = department_hierarchy
                department_list.append(department_obj)
            if len(department_list)>0:
                mexico_notice.departments = department_list

            if title:
                mexico_notice.title = title

            #handle some date issues
            if title_link:
                resp = self.http_request(title_link, "HEAD", request_args={'timeout':(180, 180)})
                if title_link != resp.url:
                    date_val = resp.url.split("fecha=")
                    title_link = resp.url
                    if len(date_val) == 2:
                        publication_date = date_val[1]
                mexico_notice.source_url = title_link

            if publication_date:
                publication_date_obj = dateparser.parse(publication_date, languages=['es']).date()
                if publication_date_obj > self.current_date:
                    raise ValueError(
                        u"Publication date is more than the current date {} for notice titled : {} ".format(publication_date_obj.strftime('%Y-%m-%d'),title))
                else:
                    mexico_notice.publication_date = parse_date_as_str(publication_date,languages=['es'])

            document_link = title_link
            if document_link:
                self.document_link = document_link
                doc_details, status = self.document_download_block(document_link,file_status=True)
                if status:
                    download_id, _, doc_ids = doc_details
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        mexico_notice.document_id = document_id
                        mexico_notice.download_id = download_id
                        mexico_notice.document_title = "HTML version"
                    else:
                        raise ValueError(
                            "Document ID not found while registering document with url {}".format(
                                document_link))
                    if not download_id:
                        raise ValueError(
                            "Download ID not found while registering document with url {}".format(
                                document_link))

                else:
                    raise Exception(fmt("No document found for title : {}", title))
            else:
                raise Exception(fmt("No document found for title : {}", title))

            if self.validate_doc(mexico_notice):
                self.save_doc(mexico_notice.for_json())
            else:
                self.logger.critical(__name__, "individual_notice_scrape_failed",
                                     fmt("JsonSchema validation failed for notice having url: {}",title_link))
                self.logger.info(__name__,self.json_dumps(message = mexico_notice.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e), exc_info=True)


    def document_download_block(self,document_link=None,file_status=None):
        try:
            resp = self.http_request(document_link, "HEAD")
            if resp.status_code != 200:
                self.logger.critical(__name__, "individual_notice_document_extraction_failed",
                                     fmt('http request is failing with error: {} for url  ', document_link))
                return None, False

            if 'Content-Length' in resp.headers:
                if int(resp.headers["Content-Length"]) > MAX_FILE_DOWNLOAD_SIZE:
                    error_message = "File @ '{}' is larger than max size {} bytes.".format(
                        document_link, MAX_FILE_DOWNLOAD_SIZE)
                    self.logger.critical(__name__, "individual_notice_document_extraction_failed",
                                         fmt('While extracting document Doc-Service is failing with error: {}',
                                             error_message))
                    return None, False
            self.logger.info(__name__,fmt("Content type of link : {}",resp.headers['Content-Type']))
            if 'pdf' in resp.headers['Content-Type']:
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"
            else:
                extraction_type = self.extraction_type.html
                content_type = "text/html"

            if file_status:
                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type,
                                                                               get_static_content=self.fetch_static_content,
                                                                               should_skip_checks=True)
            else:
                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                                   self.scraper_policy.doc_service,
                                                                                   extraction_type, True,
                                                                                   content_type=content_type)
            if download_id and _ and doc_ids:
                return (download_id , _ , doc_ids),True
            else:
                return None, False
        except Exception as e:
            self.logger.critical(__name__,"Document Download failed" , fmt("Content type of link : {} ", document_link))
            return None, False

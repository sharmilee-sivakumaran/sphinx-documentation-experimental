# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import sys
import re
import injector
import dateparser
import datetime as dt
from lxml.html import fromstring, tostring
import lxml.html as LH
from dateutil.relativedelta import relativedelta
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scraperutils.doc_service.transfer_to_s3 import MAX_FILE_DOWNLOAD_SIZE
from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin

@scraper()
@argument("--start_year", help="Start Year to scrape data from Canada Notice Website", type=int, default=0, choices=range(2015,dt.date.today().year+1))
@tags(type="notices", country_code="CA", group="international")
class Canada_GazetteDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(Canada_GazetteDocScraper, self).__init__(EventComponent.scraper_bills, "canada_gazette", "canada_gazette")
        self.base_url = u'http://gazette.gc.ca'
        self.list_url = u'http://gazette.gc.ca/rp-pr/publications-eng.html'
        self.member_url = u'{base_url}{list_url}'
        self.logger = logger
        notice_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        notice_json_schema_file_path = os.path.join(notice_json_schema_dir_path, "regulation_notice.json")
        self.model_factory = ModelFactory(notice_json_schema_file_path,notice_json_schema_file_path)
        self.tag = ''
        self.document_link = ''
        self.current_date = dt.datetime.now().date()

    def find_pattern(self,html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    def fetch_static_content(self, html_file):
        flag_id_status = False
        flag_name_status = False
        index_current_tag = -1
        page_data_id = self.tag
        html_page = html_file.read()
        page_content = html_page.decode('utf-8')
        self.set_selector(page_content)
        tag_list_ids = self.extract('//div[@id="content"]//a/@id')
        tag_list_name = self.extract('//div[@id="content"]//a/@name')

        if tag_list_ids:
            if self.tag in tag_list_ids:
                index_current_tag = tag_list_ids.index(self.tag)
                flag_id_status = True
                attribute_name = "id"
                tag_list = tag_list_ids
        if tag_list_name:
            if self.tag in tag_list_name:
                index_current_tag = tag_list_name.index(self.tag)
                flag_name_status = True
                attribute_name = "name"
                tag_list = tag_list_name

        if page_data_id:
            if flag_id_status or flag_name_status:
                if index_current_tag < (len(tag_list)-1):
                    page_path = u'//div[@id="content"]/*[preceding-sibling::a[@{attribute_name}="{tag_value_before}"] and following-sibling::a[@{attribute_name}="{tag_value_after}"]]'.format(tag_value_before=self.tag,tag_value_after=tag_list[index_current_tag+1],attribute_name=attribute_name)
                else:
                    page_path = u'//div[@id="content"]/*[preceding-sibling::a[@{attribute_name}="{tag_value}"]]'.format(attribute_name=attribute_name,tag_value=self.tag)
                content_blocks = self.xpath(page_path)
                if not content_blocks:
                    if index_current_tag < (len(tag_list)-1):
                        page_path = u'//div[@id="content"]//*[(preceding-sibling::a[@{attribute_name}="{tag_value_before}"] or preceding-sibling::*/a[@{attribute_name}="{tag_value_before}"]) and (following-sibling::a[@{attribute_name}="{tag_value_after}"] or following-sibling::*/a[@{attribute_name}="{tag_value_after}"])]'.format(tag_value_before=self.tag,tag_value_after=tag_list[index_current_tag+1],attribute_name=attribute_name)
                    else:
                        page_path = u'//div[@id="content"]//*[(preceding-sibling::a[@{attribute_name}="{tag_value}"] or preceding-sibling::*/a[@{attribute_name}="{tag_value}"])]'.format(tag_value=self.tag,attribute_name=attribute_name)
                    content_blocks = self.xpath(page_path)
                if not content_blocks:
                    if index_current_tag < (len(tag_list) - 1):
                        page_path = u'//div[@id="content"]//*[(preceding-sibling::a[@{attribute_name}="{tag_value_before}"] or preceding-sibling::*/a[@{attribute_name}="{tag_value_before}"]) or (following-sibling::a[@{attribute_name}="{tag_value_after}"] or following-sibling::*/a[@{attribute_name}="{tag_value_after}"])]'.format(tag_value_before=self.tag,tag_value_after= tag_list[index_current_tag + 1],attribute_name=attribute_name)
                    else:
                        page_path = u'//div[@id="content"]//*[(preceding-sibling::a[@{attribute_name}="{tag_value}"] or preceding-sibling::*/a[@{attribute_name}="{tag_value}"])]'.format(tag_value=self.tag,attribute_name=attribute_name)
                    content_blocks = self.xpath(page_path)
            content_data = ''
            for element in content_blocks:
                content_data +=  element.extract()
            document_block = content_data
        else:
            document_block = self.xpath('.//div[@id="content"]').extract()
            if document_block:
                document_block = document_block[0]

        head_block = self.xpath('.//head/*').extract()
        html_text = ''
        if document_block:
            document_block = '<div id="content">' + document_block + "</div>"
            xroot = self._sel.root
            #adding referer in head tag
            if head_block:
                head_block_data_val = ''.join(head_block)
                head_block_data_val = '<meta http-equiv=\"Content-Security-Policy\" content=\"upgrade-insecure-requests\">' + \
                                      '<meta name=\"referrer\" content=\"origin-when-crossorigin\">' + head_block_data_val
                xroot.replace(xroot.xpath('//head')[0], LH.fromstring(head_block_data_val))

            main_block = xroot.xpath('//main[@role="main"]')
            block = main_block[0].xpath('//div[@id="content"]')
            main_block[0].replace(block[0], LH.fromstring(document_block))
            body_block = xroot.xpath('//body')

            #removing script and noscript from entire html content
            script_tags = xroot.xpath('//script')
            for script_tag in script_tags:
                script_tag.getparent().remove(script_tag)

            noscript_tags = xroot.xpath('//noscript')
            for noscript_tag in noscript_tags:
                noscript_tag.getparent().remove(noscript_tag)

            header_block = xroot.xpath('//header[@role="banner"]')
            extra_tags = xroot.xpath('//ul[@id="wb-tphp"]')
            xroot.make_links_absolute(self.document_link)
            #replacing header
            if header_block:
                body_block[0].replace(header_block[0],LH.fromstring('<header role="banner"></header>'))
            if extra_tags:
                body_block[0].replace(extra_tags[0],LH.fromstring('<ul></ul>'))
            #replacing footer
            footer_block_1 = xroot.xpath('//aside[@class="gc-nttvs container"]')
            footer_block_2 = xroot.xpath('//footer[@id="wb-info"]')
            if footer_block_1:
                body_block[0].replace(footer_block_1[0],LH.fromstring('<aside class="gc-nttvs container"></aside>'))
            if footer_block_2:
                body_block[0].replace(footer_block_2[0],LH.fromstring('<footer role="contentinfo" id="wb-info"></footer>'))
            html_text = tostring(xroot)
            html_text = html_text.replace('\r','').replace('\n','').replace('\t','').replace('\xa0',' ')
        else:
            raise Exception(fmt("No HTML content on link : {}",self.document_link))

        return html_text


    def scrape(self, start_year):
        try:
            if start_year == 0:
                verify_date = self.current_date - relativedelta(days=30)
            else:
                verify_date = dt.datetime(year=int(start_year), month=1, day=1).date()

            self.logger.info(__name__, fmt(u"Fetching for main page link  - {} ",self.list_url))
            self.http_get(self.list_url,self.scraper_policy.doc_list,request_args={'timeout':(180,200)})
            gazette_details_part1 = self.xpath('//h2[@id="a1"]/following-sibling::ul[1]/li')
            gazette_details_part2 = self.xpath('//h2[@id="a2"]/following-sibling::ul[1]/li')
            gazette_details_part1.extend(gazette_details_part2)
            gazette_details = gazette_details_part1
            if gazette_details :
                for detail in gazette_details:
                    gazette_detail = self.extract_as_one('.//text()',sel=detail)
                    if gazette_detail:
                        gazette_year = gazette_detail.split(":")[0]
                        gazette_edition = gazette_detail.split(",")[1]
                        gazette_edition = gazette_edition.replace(u'\xa0', u' ')
                    else:
                        continue
                    if self.check_year(gazette_year, start_year):
                        gazette_link = self.extract_single('./a/@href',sel=detail)
                        if gazette_link:
                            gazette_link = self.member_url.format(base_url=self.base_url, list_url=gazette_link)
                        if gazette_link and gazette_edition:
                            self.http_get(gazette_link,self.scraper_policy.doc_list)
                            commission_table_no = self.xpath('//table[contains(@class,"table-hover") and contains(@class,"table-bordered")]')
                            if len(commission_table_no) >= 1:
                                table_range = 2
                                start_range = 1
                            else:
                                table_range = 1
                                start_range = 1
                            for table_index in range(start_range,table_range):
                                commission_heading_path = '//table[contains(@class,"table-hover") and contains(@class,"table-bordered")][' + str(table_index) + ']/thead/tr/th'
                                commission_headings = self.xpath(commission_heading_path)

                                heading_list = []
                                for heading in commission_headings:
                                    heading_title = self.extract_as_one('.//text()',sel=heading)
                                    heading_list.append(heading_title)

                                if 'Non-official HTML version' in heading_list:
                                    html_page_index = heading_list.index('Non-official HTML version') + 1

                                if 'Official PDF version' in heading_list:
                                    pdf_page_index = heading_list.index('Official PDF version') + 1

                                if 'Edition number' in heading_list:
                                    edition_number_index = heading_list.index('Edition number') + 1

                                if 'Extra edition title' in heading_list:
                                    extra_edition_index = heading_list.index('Extra edition title') + 1

                                if 'Publication date' in heading_list:
                                    publication_date_index = heading_list.index('Publication date') + 1

                                commission_path = '//table[contains(@class,"table-hover") and contains(@class,"table-bordered")][' + str(table_index) + ']/tbody/tr'
                                commission_details = self.xpath(commission_path)

                                for com_detail in commission_details:
                                    publication_date = ''
                                    edition_number = ''
                                    extra_no = ''
                                    html_version_text = ''
                                    html_version_link = ''
                                    pdf_version_link = ''
                                    pdf_version_text = ''

                                    if publication_date_index:
                                        publication_date_path = './td[' + str(publication_date_index) +']/text()'
                                        publication_date = self.extract_single(publication_date_path,sel=com_detail)
                                        # Default condition of 30 days added
                                        if publication_date:
                                            publication_date_val = dateparser.parse(publication_date).date()
                                            if publication_date_val:
                                                if publication_date_val < verify_date:
                                                    break

                                    if edition_number_index:
                                        edition_number_path = './td[' + str(edition_number_index) + ']//text()'
                                        edition_number = self.extract_single(edition_number_path, sel=com_detail)
                                    if edition_number:
                                        if edition_number == 'N/A':
                                            edition_number = ''
                                        else:
                                            edition_number = "Edition No. " + edition_number

                                    if extra_edition_index:
                                        extra_edition_path = './td[' + str(extra_edition_index) + ']//text()'
                                        extra_no = self.extract_single(extra_edition_path , sel=com_detail)
                                        if extra_no=='N/A':
                                            extra_no = ''
                                        elif extra_no:
                                            extra_no = "Extra No. " +  extra_no

                                    if html_page_index:
                                        html_text_path = './td[' + str(html_page_index) + ']//text()'
                                        html_version_text = self.extract_as_one(html_text_path , sel=com_detail)
                                        if html_version_text:
                                            html_version_text = html_version_text.replace(u'\xa0', u' ')

                                        html_link_path = './td[' + str(html_page_index) + ']/a/@href'
                                        html_version_link = self.extract_single(html_link_path , sel=com_detail)
                                        if html_version_link:
                                            html_version_link = self.member_url.format(base_url=self.base_url,list_url=html_version_link)

                                    if pdf_page_index:
                                        pdf_page_path = './td[' + str(pdf_page_index) + ']//text()'
                                        pdf_version_text = self.extract_as_one(pdf_page_path,sel=com_detail)
                                        if pdf_version_text:
                                            pdf_version_text = pdf_version_text.replace(u'\xa0', u' ')

                                        pdf_link_path = './td[' + str(pdf_page_index) + ']/a/@href'
                                        pdf_version_link = self.extract_single(pdf_link_path , sel=com_detail)
                                        if pdf_version_link:
                                            pdf_version_link = self.member_url.format(base_url=self.base_url,list_url=pdf_version_link)
                                    self.scrape_commissions(gazette_year,gazette_edition,gazette_link,publication_date,edition_number,extra_no,html_version_text,html_version_link, pdf_version_text, pdf_version_link)
                        else:
                            continue
                    else:
                        continue
            else:
                self.logger.critical(__name__, u"scraper_run_finished",
                                     fmt(u"{} : No Notices Found ", self.scraper_name.title()))

        except Exception as ex:
            self.logger.critical(__name__, u"scraper_failed",
                                 fmt(u"{} notices could not be scraped. {}", self.scraper_name.title(), repr(ex.message)),
                                 exc_info=True)
            raise ScrapeError(self.scraper_policy.doc_list,
                              fmt(u"{} notices could not be scraped.", self.scraper_name.title()),
                              self.base_url)

    def scrape_commissions(self, year_val, gazette_edition, gazette_link , publication_date, edition_number, extra_no, html_version_text, html_version_link, pdf_version_text, pdf_version_link):
        try:
            self.logger.info(__name__, fmt(" HTML page link : {}", html_version_link))
            absolute_department_link = ''
            if html_version_link:
                self.http_get(html_version_link,self.scraper_policy.doc_list,request_args={'timeout':(180,200)})
                #######################################################################
                #parent url : http://gazette.gc.ca/rp-pr/p1/2018/index-eng.html
                #child url : http://gazette.gc.ca/rp-pr/p1/2018/2018-04-28/html/index-eng.html
                #######################################################################
                dept_blocks = self.xpath('//div[@id="content"]/*[preceding-sibling::h2[@class="department"] or following-sibling::h2[@class="department"]]')
                if not dept_blocks:
                    dept_blocks = self.xpath(
                        '//div[@id="content"]/*[preceding-sibling::h2 or following-sibling::h2]')

                department_block_list = []
                department_block = u''
                index_block_start = 0
                #to find out the starting part of the html from where to extract the data
                for i in range(0,len(dept_blocks)):
                    dept_content = dept_blocks[i].extract()
                    if dept_content.startswith('<h2'):
                        index_block_start = i
                        break

                if dept_blocks:
                    for dept_element in dept_blocks[index_block_start:]:
                        data = dept_element.extract()
                        class_name = dept_element.xpath('@class').extract()
                        if 'h2' in data and 'department' in class_name:
                            department_block_list.append(department_block)
                            department_block = u''
                        department_block += data
                    #for last block
                    department_block_list.append(department_block)

                if not department_block_list:
                    ########################################################
                    ### Because no department tag
                    ####http://gazette.gc.ca/rp-pr/p2/2018/2018-05-02/html/sor-dors86-eng.html
                    ###############################
                    department_block_list = self.xpath('//div[@id="content"]').extract()
                dept_blocks = department_block_list
                dept_blocks = [val.strip() for val in dept_blocks if val.strip()]
                if dept_blocks:
                    for dept_block in dept_blocks:
                        self.set_selector(dept_block)
                        department_agency = self.extract('./h3/text()')
                        department_link = self.extract_single('./h2/a/@href')
                        if department_link:
                            absolute_department_link = urljoin(html_version_link,department_link)
                        else:
                            absolute_department_link = ''

                        agency_block = dept_block.split("<h3")
                        if len(agency_block)==1:
                            agency_block = agency_block
                        else:
                            agency_block = agency_block[1:]
                        for service_name in agency_block:
                            service_name = "<h3 " + service_name
                            self.set_selector(service_name)
                            department_name = self.extract_single('./h3/text()')
                            if not department_name:
                                department_name = ""

                            if department_name:
                                department_name = department_name.replace(u"\\u00A0", u" ").replace(u'\xa0', u' ')

                            sub_dept_blocks_1 = self.find_pattern(service_name,'(<h4.*?</ul>)')
                            sub_dept_blocks_2 = self.find_pattern(service_name,'((<h4.+?</h4>)?<ul\s+class="noBullet[^"]*">.+?</ul>)')
                            if len(sub_dept_blocks_1) == len(sub_dept_blocks_2):
                                sub_dept_blocks = sub_dept_blocks_1
                            elif len(sub_dept_blocks_1) < len(sub_dept_blocks_2):
                                sub_dept_blocks = sub_dept_blocks_2
                                if sub_dept_blocks:
                                    sub_dept_blocks_list = []
                                    for block_val in sub_dept_blocks:
                                        sub_dept_blocks_list.append(block_val[0])
                                    sub_dept_blocks = sub_dept_blocks_list
                            else:
                                sub_dept_blocks = sub_dept_blocks_1


                            if not sub_dept_blocks:
                                ##########################################################
                                #http://gazette.gc.ca/rp-pr/p2/2018/2018-05-02/html/index-eng.html
                                ############################################################
                                #another case
                                #http://gazette.gc.ca/rp-pr/p2/2018/2018-05-02/html/sor-dors86-eng.html
                                ####################
                                sub_dept_blocks = self.xpath('.//ul',sel=service_name).extract()
                                if len(sub_dept_blocks) == 0:
                                    sub_dept_blocks = [dept_block]
                            if not sub_dept_blocks:
                                sub_dept_blocks = [dept_block]

                            for dep in sub_dept_blocks:
                                self.set_selector(dep)
                                title_part = self.extract_single(u'./h4[@class="roman"]/text()')
                                if not title_part:
                                    title_part = self.extract_single(u'./h4/text()')
                                #to handle the title formation where no h4 tag is present
                                if not title_part:
                                    title_part = self.extract_single(u'./li/text()')
                                if not title_part:
                                    title_part = ""
                                rem_title_part = self.xpath('./h4/following-sibling::ul[1]/li')
                                department_block_status = False
                                if not rem_title_part:
                                    rem_title_part = self.xpath('li')
                                    department_block_status = False
                                if not rem_title_part:
                                    rem_title_part = [dep]
                                    department_block_status = True
                                else:
                                    pass
                                for val in rem_title_part:
                                    self.tag = ''
                                    title_link = ''
                                    full_title_name = ''
                                    document_link = ''

                                    if not department_block_status:
                                        title_link = self.extract_single('./a/@href', sel=val)
                                        title_text = self.extract('.//text()', sel=val)

                                        if len(title_text)>1:
                                            title_text = [val.strip() for val in title_text if val.strip()]
                                            title_text = ':'.join(title_text)
                                        elif len(title_text)==1:
                                            title_text = title_text[0].strip()
                                        else:
                                            pass
                                        # to handle the condition where h4 tag is not present
                                        if not title_link:
                                            title_part = title_text
                                            continue

                                        if title_text.lower().startswith('footnote'):
                                            continue

                                        if title_part.strip():
                                            full_title_name = title_part + " - " + title_text
                                        elif title_text:
                                            full_title_name = title_text

                                    else:
                                        full_title_name = self.extract_as_one('//h3[@class="roman"]/text()')
                                    if full_title_name:
                                        full_title_name = full_title_name.replace(u'\u2014',u'-').replace("\r",'').replace('\n','').replace('\u00a0',' ').replace(u'\xa0', u' ')

                                    if not full_title_name:
                                        full_title_name = html_version_text.replace(',','_').replace('.','_').replace(' ','_').replace("__","_")

                                    if title_link:
                                        title_link = urljoin(html_version_link, title_link)
                                    else:
                                        title_link = html_version_link

                                    page_data_id = title_link.split("#")
                                    if len(page_data_id) > 1:
                                        self.tag = str(page_data_id[1])
                                        document_link = page_data_id[0]
                                        self.document_link = page_data_id[0]
                                    elif not page_data_id:
                                        self.tag = ''
                                    else:
                                        pass
                                    document_block_status = True
                                    if not document_link:
                                        if title_link:
                                            document_link = title_link
                                            self.document_link = title_link
                                    if full_title_name:
                                        document_title = full_title_name
                                    self.scrape_canada_notice(title_link, document_block_status, document_title,
                                                         absolute_department_link, gazette_edition, type,
                                                         publication_date, edition_number, extra_no, department_name,
                                                         full_title_name, html_version_link)

        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e), exc_info=True)
            self.logger.info(__name__,fmt(" {}:  {}",gazette_edition,html_version_link))

    def scrape_canada_notice(self,title_link,document_block_status,document_title,absolute_department_link, gazette_edition, type, publication_date, edition_number, extra_no, department_name, full_title_name, html_version_link ):
        try:
            canada_gazette_doc = self.model_factory.create_bill_doc()
            canada_gazette_doc.country = "Canada"
            canada_gazette_doc.publication_name = "Canada Gazette"
            doc_details, status = self.document_download_block(title_link, document_block_status)
            if status:
                download_id, _, doc_ids = doc_details

                if len(doc_ids) > 0:
                    document_id = doc_ids[0]
                    canada_gazette_doc.document_id = document_id
                    canada_gazette_doc.download_id = download_id
                    if document_title:
                        canada_gazette_doc.document_title = document_title
                else:
                    raise ValueError(
                        "Document ID not found while registering document with url {}".format(
                            title_link))
                if not download_id:
                    raise ValueError(
                        "Download ID not found while registering document with url {}".format(
                            title_link))

            else:
                self.logger.info(__name__,
                                 fmt(u"No Documents Found on url : {}", absolute_department_link))
                raise Exception(fmt(u"No Documents Found on url  : {} ", absolute_department_link))

            if "Part II" in gazette_edition:
                type = "Final Rule"
            elif "Part I" in gazette_edition:
                type = "Notice or Proposed Rule"
            else:
                pass

            if type:
                canada_gazette_doc.notice_type = type
            if publication_date:
                canada_gazette_doc.publication_date = parse_date_as_str(publication_date)
            if title_link:
                canada_gazette_doc.source_url = title_link

            if edition_number and gazette_edition:
                canada_gazette_doc.publication_issue = gazette_edition + "," + edition_number + extra_no
            elif gazette_edition:
                canada_gazette_doc.publication_issue = gazette_edition
            else:
                pass

            department_list = []
            department_obj = self.model_factory.create('CanadaGazetteDepartmentSchema')
            if department_name:
                department_obj.department_name = department_name
                department_obj.department_hierarchy = 0
                department_list.append(department_obj)

            if len(department_list) > 0:
                canada_gazette_doc.departments = department_list

            if full_title_name:
                full_title_name = full_title_name.replace(u"\\u00A0", " ")
                full_title_name = full_title_name.replace("\r", ' ').replace("\n", ' ')
                canada_gazette_doc.title = full_title_name

            if self.validate_doc(canada_gazette_doc):
                self.save_doc(canada_gazette_doc.for_json())
            else:
                self.logger.critical(__name__, "validation error", self.json_dumps(message=canada_gazette_doc.for_json()))
        except Exception as e:
            self.logger.critical(__name__, 'individual_notice_scrape_failed', fmt("Error occured: {}", e), exc_info=True)
            self.logger.info(__name__,fmt(" {}:  {}",gazette_edition,html_version_link))

    @staticmethod
    def check_year(year_val , start_year):
        if re.search(r'^\d{4}$', year_val) and int(year_val)>= start_year:
            return True
        else:
            return False

    def document_download_block(self,document_link=None,document_block_status=None):
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

            if document_block_status:
                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type,True,content_type=content_type,
                                                                               get_static_content=self.fetch_static_content,should_skip_checks=True)
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

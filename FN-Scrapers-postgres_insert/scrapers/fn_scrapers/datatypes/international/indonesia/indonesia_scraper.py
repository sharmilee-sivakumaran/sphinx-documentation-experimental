# -*- coding: utf-8 -*-
from __future__ import division
import re
import os
import injector
from dateparser import parse
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from datetime import datetime
import json

@scraper()
@tags(type="bills", country_code="ID", group="international")
class IndonesiaDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(IndonesiaDocScraper, self).__init__(EventComponent.scraper_bills,"indonesia","indonesia")
        self.base_url = u"http://www.dpr.go.id/uu/prolegnas-long-list"
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "indonesia.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    # for for getting the correct date format
    def get_formatted_date(self, date):
        date = re.sub('Nopember', 'November', date)
        date_converted = parse(date)
        date = date_converted.strftime('%Y-%m-%d')
        return date

    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            resu = re.sub('&nbsp;', ' ', match.group(group))
            return resu

    @staticmethod
    def get_source_url(url):
        url = "http://www.dpr.go.id" + url
        return url

    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    @staticmethod
    def get_formatted_action_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self):

        try:
            bill_page_html = self.http_get(self.base_url, self.scraper_policy.doc_list)

            bill_blocks = self.find_pattern(bill_page_html, '(<div class="accordion-toggle[^>]*>.*?</table>)')

            # session
            session = self.single_pattern(bill_blocks[0], '<div class="accordion-toggle[^>]*">(.*?)</div>', 1)
            session = self.single_pattern(session, '(\d+\-\d+)', 1)

            for block in bill_blocks:

                # bill_type
                bill_type = self.single_pattern(block, '<div class="accordion-toggle[^>]*">(.*?)</div>', 1)
                if "KUMULATIF" in bill_type:
                    bill_type = "Cumulative Bill"
                elif 'PROLEGNAS' in bill_type:
                    bill_type = "Bill"

                # bill_rows
                bill_rows = self.find_pattern(block, '(<tr class="list">.*?</tr>)')
                for row in bill_rows:
                    self.scrape_bill(row, bill_type, session)

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc, fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                          self.base_url)

    # function for scraping bills
    def scrape_bill(self, row_html, bill_type, session):
        try:
            idbill = self.model_factory.create_bill_doc()

            # session
            idbill.session = session

            # bill_type
            idbill.bill_type = bill_type

            # title
            title = self.single_pattern(row_html, '<a href="[^>]*">(.*?)</', 1)
            title = re.sub('^\s+|\s+$', '', title)
            idbill.title = title

            # sponsor_chamber
            sponsor_chamber = self.single_pattern(row_html, '<td align="center"><div class="cell">(.*?)</div></td>',1)
            if ',' in sponsor_chamber:
                sponsor_chamber = re.sub('\s*,\s*', ',', sponsor_chamber)
                sponsor_chamber_array = sponsor_chamber.split(',')
            else:
                sponsor_chamber_array = [sponsor_chamber]
            spon_cham = []
            for i in sponsor_chamber_array:
                new_dict = {'chamber_name': i}
                spon_cham.append(new_dict)
            idbill.sponsor_chamber = spon_cham

            # source_url
            source_url = self.single_pattern(row_html, '<a href="(.*?)">', 1)
            source_url = self.get_source_url(source_url)
            idbill.source_url = source_url

            self.http_get(source_url, self.scraper_policy.doc_list)

            # sponsors
            div = self.xpath('//div[@class="items"]')
            sponsors_initial_list = self.extract('.//ul[@class="custom-button"]/li/text()', sel=div[0])
            if sponsors_initial_list:
                sponsors_array = []
                for sp in sponsors_initial_list:
                    sp = sp.replace(u'\xa0', u'')
                    sp = re.sub('^\s+|\.?\s+$', '', sp)
                    sp_dict = {'sponsor_name' : sp}
                    sponsors_array.append(sp_dict)
                idbill.sponsors = sponsors_array

            # committees
            committees_array = self.extract('.//ul[@class="custom-button"]/li/text()', sel=div[1])
            if committees_array:
                idbill.committees = committees_array

            # relative_age
            relative_age = self.extract_single('.//ul[@class="custom-button"]/li/text()', sel=div[2])
            if relative_age:
                idbill.relative_age = relative_age

            # summary
            summary = self.extract_single('.//ul[@class="custom-button"]/li/text()', sel=div[3])
            if summary:
                summary = re.sub('\s{2,}', ' ', summary)
                summary = re.sub('^\s+|\s+$', '', summary)
                if summary != "-" and summary is not None:
                    idbill.summary = summary

            # introduction_date
            introduction_date = self.extract_single(
                '//div[@class="options text-center"]/div[@class="anggota"][1]/div[2]/div[2]/text()')
            if introduction_date:
                date = self.get_formatted_date(introduction_date)
                idbill.introduction_date = date

            # last_action
            last_action = self.extract_single('//div[@class="options text-center"]/div[@class="anggota"][1]/div[3]/div[2]/text()')
            last_action = re.sub('\s*:.*|\s*\)\s*|\s*\(\s*', '', last_action)
            if last_action:
                idbill.last_action = last_action

            # documents
            document_blocks = self.extract('//ul[@class="side-category-menu list-unstyled"]/ul/li')
            documents_array = []
            if document_blocks:
                for document_block in document_blocks:

                    # external url
                    doc_link = self.single_pattern(document_block, 'href="([^"]*)"', 1)
                    doc_link = "http://www.dpr.go.id" + doc_link

                    # document_title
                    document_title = re.sub('<.*?>', '', document_block)
                    document_title_dict = {"DPR":"Deskripsi Konsepsi (DPR)", "Pemerintah":"Deskripsi Konsepsi (Pemerintah)", "DPD":"Deskripsi Konsepsi (DPD)"}

                    # document and download id
                    extraction_type = self.extraction_type.html
                    content_type = 'text/html'

                    download_id, _, doc_ids = self.register_download_and_documents(doc_link,
                                                                                   self.scraper_policy.doc_service,
                                                                                   extraction_type, True,
                                                                                   content_type=content_type)
                    if len(doc_ids) > 0:
                        document_id = doc_ids[0]
                        doc_block_dict = {"external_url": doc_link, "document_title": document_title_dict[document_title],
                                          "document_id": document_id, "download_id": download_id}

                    documents_array.append(doc_block_dict)

            # actions
            action_link = self.extract_single(
                '//ul[@class="options list-unstyled mb30 clearfix"]/li/ul/li[3]/a/@href')
            action_link = "http://www.dpr.go.id" + action_link
            self.http_get(action_link, self.scraper_policy.doc_list)
            all_actions_block = self.xpath('//tr[@class="list"]')
            if all_actions_block:
                action_block_array = []
                for single_action_block in all_actions_block:

                        # action_sequence_number
                        action_sequence_number = re.sub('\.|\s*','', self.extract('.//div[@class="no cell"]/text()',
                                                                                  sel=single_action_block)[0])

                        # action_stage
                        action_stage = self.extract('.//div[@class="text cell"]/text()', sel=single_action_block)[0]

                        # action_date
                        action_date = self.extract('.//div[@class="text cell"]/text()', sel=single_action_block)[1]
                        action_date = self.get_formatted_action_date(action_date)

                        # action_text
                        action_text = self.extract('.//div[@class="text cell"]/text()', sel=single_action_block)[2]

                        single_action_block_dict = {'action_sequence_number': int(action_sequence_number),
                                                    'action_stage': action_stage, "action_date": action_date,
                                                    "action_text": action_text}
                        action_block_array.append(single_action_block_dict)

                idbill.actions = action_block_array

            # documents from action
            documents_array_1 = []
            if all_actions_block:

                for single_action_block in all_actions_block:
                    actions_documents_block = self.extract('.//div[@class="text cell"]/div[@class="tbl"]',
                                                           sel=single_action_block)
                    if actions_documents_block:
                        for action_document in actions_documents_block:

                            # action_document_title
                            action_document_title = self.single_pattern(action_document,
                                                                        '<div class="title cell">(.*?)<br>', 1)

                            # action_document_link
                            action_document_link = self.single_pattern(action_document, '<a href="([^"]*)"', 1)
                            action_document_link = "http://www.dpr.go.id" + action_document_link

                            resp = self.http_request(action_document_link, "HEAD")

                            if 'pdf' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.unknown
                                content_type = "application/pdf"

                            elif 'msword' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.msword_doc
                                content_type = 'application/msword'

                            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in \
                                    resp.headers[
                                        'Content-Type']:
                                extraction_type = self.extraction_type.msword_docx
                                content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

                            elif 'application/vnd.ms-powerpoint' in resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.mspowerpoint_ppt
                                content_type = 'application/vnd.ms-powerpoint'

                            elif 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in \
                                    resp.headers['Content-Type']:
                                extraction_type = self.extraction_type.mspowerpoint_pptx
                                content_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'

                            elif 'application/zip' in resp.headers['Content-Type']:
                                content_type = 'application/zip'
                                extraction_type = self.extraction_type.unknown

                            download_id, _, doc_ids = self.register_download_and_documents(action_document_link,
                                                                                           self.scraper_policy.doc_service,
                                                                                           extraction_type,
                                                                                           True,
                                                                                           content_type=content_type)
                            if len(doc_ids) > 0:
                                document_id = doc_ids[0]
                                doc_block_dict_1 = {"external_url": action_document_link,
                                                  "document_title": action_document_title,
                                                  "document_id": document_id, "download_id": download_id}
                                documents_array_1.append(doc_block_dict_1)
            final_document_array = []
            if documents_array and documents_array_1:
                final_document_array = documents_array + documents_array_1
            elif documents_array:
                final_document_array = documents_array
            elif documents_array_1:
                final_document_array = documents_array_1
            if final_document_array:
                idbill.documents = final_document_array

            if self.validate_doc(idbill):
                self.save_doc(idbill)
            else:
                self.logger.critical(__name__, "schema_failed",
                                     fmt("JsonSchema validation failed for : {}",
                                         json.dumps(idbill.to_json())))

        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e), exc_info=True)
# -*- coding: utf-8 -*-
from __future__ import division

import re
import os
import json
import injector
import sys
from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_service.server import BlockingEventLogger,fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str


@scraper()
@tags(type="bills", group="international")
class EUInitiativeDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self,logger):
        super(EUInitiativeDocScraper, self).__init__(EventComponent.scraper_bills,"europe","europe")
        self.base_url = u"http://ec.europa.eu"
        self.member_page_url = u"{base_url}{url}"
        self.page_url = u"{base_url}/info/law/better-regulation/initiatives_en?field_brp_inve_fb_status=All&field_brp_inve_leading_service=All&page={page_number}"
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "EU_Initiative.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    # function for scrape data
    def scrape(self):
        try:
            self.http_get(self.page_url.format(base_url=self.base_url,page_number=0), self.scraper_policy.doc_list)
            total_number_of_bills_text = self.extract_single('//div[@class="filters__result-count"]/span[@class="filters__items-number"]/text()')
            total_number_of_bills = 0

            m = re.search(r'\(\s*(\d+?)\s*\)', total_number_of_bills_text)
            if m:
                total_number_of_bills = m.group(1)
            total_number_of_pages = int(int(total_number_of_bills)/10)

            if total_number_of_pages:
                for page in range(total_number_of_pages+1):
                    self.http_get(self.page_url.format(base_url=self.base_url, page_number=page),
                                  self.scraper_policy.doc_list)
                    members_link = self.extract('//div[@class="listing__wrapper listing__wrapper--default"]/ul[@class="listing"]/li[@class="listing__item"]/div[@class="node node-brp-initiative node-teaser view-mode-teaser"]/@about')
                    if members_link:
                        for link in members_link:
                            member_link = self.member_page_url.format(base_url=self.base_url,url=link)
                            self.scrape_bill(member_link)

            else:
                self.logger.critical(__name__,"No Pages Found","eu initiative scraping failed : No pages Found",)
                raise ScrapeError(fmt("{} bills could not be scraped.", self.scraper_name.title()))

        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e))

    # function for scraping bills
    def scrape_bill(self, member_page_link):
        try:
            self.logger.info(__name__,fmt(u"Fetching for member page link  - {} ",member_page_link))
            self.http_get(member_page_link, self.scraper_policy.doc_list)
            eubill = self.model_factory.create_bill_doc()
            short_title = self.extract_single('//div[@class="col-lg-9 "]/div[@class="field field-name-title-field field--title-field"]/div[@class="field__items"]/h1/text()')
            if short_title:
                eubill.short_title = short_title

            full_title = self.extract_single('//div[@class="field field-name-field-core-description clearfix field--field-core-description"]/div[@class="field__items"]/p/text()')
            if full_title:
                eubill.full_title = full_title.replace(u"\u2026","...")

            reference_id = self.extract_single('//div[@class="field field-name-field-brp-inve-reference clearfix field--field-brp-inve-reference"]/div[@class="field__items"]/text()')
            if reference_id:
                eubill.reference_id = reference_id

            initiative_type = self.extract_single('//div[@class="field field-name-field-brp-inve-resource-type clearfix field--field-brp-inve-resource-type"]/div[@class="field__items"]/div[@class="expandable expandable--link"]/a/text()')
            if initiative_type:
                eubill.initiative_type = initiative_type


            department = self.extract_single('//div[@class="field field-name-field-brp-inve-resource-type clearfix field--field-brp-inve-resource-type"]/div[@class="field field-name-field-brp-inve-leading-service clearfix field--field-brp-inve-leading-service"]/div[@class="field__items"]/text()')
            if department:
                eubill.department = department

            date_block = self.extract_single('//div[@id="initiative-givefeedback"]/div[@class="field field-name-brp-initiative-feedback-startend clearfix field--brp-initiative-feedback-startend"]/div[@class="field__items"]/text()')
            if date_block:
                dates = date_block.split('-')
                feedback_period_start_date = dates[0].strip()

                if feedback_period_start_date:
                    feedback_period_start_date = parse_date_as_str(feedback_period_start_date)
                    eubill.feedback_period_start_date = feedback_period_start_date

                feedback_period_end_date = dates[1].strip()
                if feedback_period_end_date:
                    feedback_period_end_date = parse_date_as_str(feedback_period_end_date)
                    eubill.feedback_period_end_date = feedback_period_end_date

            eubill.source_url = member_page_link

            # Documents Attachment
            file_blocks = self.xpath('//div[@class="section__group"]/div[@class="field field-name-brp-initiative-attachments field--brp-initiative-attachments"]/div[@class="field__items"]/div[@class="file"]')
            documents_list = []

            for file_block in file_blocks:
                document = self.model_factory.create("EuCommisionInitiativeDocumentSchema")
                document_title = self.extract_single('.//span[@class="file__title"]/text()',sel=file_block)
                if document_title:
                    document.document_title = document_title.strip()

                document_link = self.extract_single('./a[@class="file__btn btn btn-default piwik_download"]/@href',sel=file_block)
                document_link = self.member_page_url.format(base_url=self.base_url,url=document_link)
                extraction_type = self.extraction_type.unknown
                content_type = "application/pdf"

                download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                               self.scraper_policy.doc_service,
                                                                               extraction_type, True,
                                                                               content_type=content_type)
                if len(doc_ids) > 0:
                    document_id = doc_ids[0]
                    document.document_id = document_id
                    document.download_id = download_id
                else:
                    raise ValueError(
                        "Document ID not found while registering document with url {}".format(
                            document_link))
                if not download_id:
                    raise ValueError(
                        "Download ID not found while registering document with url {}".format(
                            document_link))


                if self.validate_doc(document):
                    documents_list.append(document)
                else:
                    self.logger.info(__name__,'Skipping Attachment: {}'.format(document.for_json()))


            if len(documents_list)>0:
                eubill.documents = documents_list
            else:
                self.logger.info(__name__,fmt("No Documents Found on url : { }",member_page_link))

            if self.validate_doc(eubill):
                self.save_doc(eubill)
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill having link: {}",
                                         member_page_link))

        except Exception as e:
            self.logger.critical(__name__,'individual_bill_scrape_failed',fmt("Error occured: {}", e), exc_info=True)


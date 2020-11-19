# -*- coding: utf-8 -*-
from __future__ import division
import re
import json
import injector
import os
from fn_scrapers.api.scraper import scraper, argument, tags
from fn_scraperutils.events.reporting import EventComponent, ScrapeError
from fn_document_service.blocking.DocumentService import TApplicationException
from HTMLParser import HTMLParser
from fn_service.server import BlockingEventLogger, fmt
from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str

@scraper()
@tags(type="bills", group="international")
class EU_ConsultationDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(EU_ConsultationDocScraper, self).__init__(EventComponent.scraper_bills,"europe","europe")
        self.base_url = u"https://ec.europa.eu"
        self.page_url = "{base_url}/info/consultations_en?field_consultation_status_value=All&field_core_policy_areas_target_id_selective=All&page={page_number}"
        self.member_page_url = "{base_url}{url}"
        self.logger = logger
        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "EU_Consultation.json")
        self.model_factory = ModelFactory(bill_json_schema_file_path, bill_json_schema_file_path)

    # function for scrape data
    def scrape(self):
        try:
            self.http_get(self.page_url.format(base_url=self.base_url, page_number=0), self.scraper_policy.doc_list)
            total_number_of_bills_text = self.extract_single('//div[@class="filters__result-count"]/span[@class="filters__items-number"]/text()')
            total_number_of_bills = 0
            m = re.search(r'\(\s*(\d+?)\s*\)', total_number_of_bills_text)
            if m:
                total_number_of_bills = m.group(1)

            total_number_of_pages = int(total_number_of_bills)/10
            if total_number_of_pages:
                for page in range(int(total_number_of_pages + 1)):
                    page_link = self.page_url.format(base_url=self.base_url, page_number=page)
                    self.scrape_bill(page_link)

            else:
                self.logger.critical(__name__,"No Pages Found","eu consultative scraping failed : No pages Found")
                raise ScrapeError(fmt("{} bills could not be scraped.", self.scraper_name.title()))
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e))

    # function for scraping bills
    def scrape_bill(self, page_link):
        try:
            self.logger.info(__name__, fmt(u"Fetching for page link  - {} ", page_link))

            self.http_get(page_link, self.scraper_policy.doc_list)
            member_blocks = self.xpath(
                '//div[@class="view-content"]/div[@class="listing__wrapper listing__wrapper--default"]/ul[@class="listing listing--teaser"]/li[@class="listing__item"]')

            count = 0
            for member_block in member_blocks:
                try:
                    count = count+1
                    eubill = self.model_factory.create_bill_doc()

                    title = self.extract_single('.//div[contains(@class,"listing__column-main")]/div[contains(@class,"field-name-title-field")]/div[@class="field__items"]/h3[@class="listing__title"]/a/text()',sel=member_block)
                    eubill.title = title

                    policy_area = self.extract_single('.//div[contains(@class,"listing__column-main")]/div[contains(@class,"field-name-field-core-policy-areas")]/div[@class="field__items"]/text()',sel=member_block)
                    if policy_area:
                        if "," in policy_area:
                            policy_ls = policy_area.split(',')
                            policy_list = [pol.strip() for pol in policy_ls]
                        else:
                            policy_list = []
                            policy_list.append(policy_area)
                        eubill.policy_area = policy_list

                    dates = self.extract_single('.//div[@class="listing__column-main "]/div[contains(@class,"field-name-consultation-period")]/div[@class="field__items"]/text()',sel=member_block)
                    dates_list = dates.split('-')
                    consultation_start_date = dates_list[0]
                    consultation_start_date = parse_date_as_str(consultation_start_date)
                    if consultation_start_date:
                        eubill.consultation_start_date = consultation_start_date

                    if dates_list[1]:
                        consultation_end_date = dates_list[1]
                        consultation_end_date = parse_date_as_str(consultation_end_date)
                        eubill.consultation_end_date = consultation_end_date
                    consultation_page_url = self.extract_single('.//div[@class="listing__column-main "]/div[contains(@class,"field-name-title-field")]/div[@class="field__items"]/h3[@class="listing__title"]/a/@href',sel=member_block)
                    if consultation_page_url.startswith("https") or consultation_page_url.startswith("http"):
                        pass
                    else:
                        consultation_page_url = self.member_page_url.format(base_url=self.base_url, url=consultation_page_url)
                    eubill.consultation_page_url = consultation_page_url

                    if consultation_page_url.startswith("https://ec.europa.eu/info/consultations/"):
                        try:
                            self.http_get(consultation_page_url, self.scraper_policy.doc_list)
                            department = self.extract_single('//div[@id="about-this-consultation"]/div[contains(@class,"field--field-core-departments")]/div[@class="field__items"]/a/text()')
                            if department:
                                eubill.department = department



                            target_group = self.xpath(
                                '//div[@id="target_group"]/div[contains(@class,"field--field-consultation-target-groups")]/div[@class="field__items"]').extract()
                            if target_group:
                                target_group = target_group[0]
                                target_group = re.sub('</li>', ';', target_group)
                                target_group = re.sub('<.*?>', '', target_group)
                                target_group = re.sub(';$|^;|^(\n)*|(\n)*$', '', target_group)
                                eubill.target_group = target_group

                            objective = self.xpath(
                                '//div[@id="objective"]/div[@class="field field-name-field-consultation-objective field--field-consultation-objective"]/div[@class="field__items"]').extract()
                            if objective :
                                objective = objective[0]
                                objective = re.sub("\xa0", '', objective, flags=re.MULTILINE | re.DOTALL)
                                objective = re.sub("\;(\s*and)", ',\\1', objective, flags=re.MULTILINE | re.DOTALL)
                                objective = re.sub("<p[^>]*>[\-|\s+](.+?)</p>", '<li>\\1</li>', objective,
                                                   flags=re.MULTILINE | re.DOTALL)

                                objective = re.sub("(.*?)<h[^>]*>\s*Related links.+", '\\1', objective,
                                                   flags=re.MULTILINE | re.DOTALL)
                                objective = re.sub('<ol[^>]*>(.+?)</ol>', '<br>\n\t\\1', objective,
                                                   flags=re.MULTILINE | re.DOTALL)
                                objective = re.sub('<li[^>]*>(.+?)</li>', '\\1;', objective, flags=re.MULTILINE | re.DOTALL)
                                objective = re.sub('<h\d+>Background</h\d+>', '\n', objective)
                                objective = re.sub('<.*?>', '', objective, flags=re.MULTILINE | re.DOTALL)
                                eubill.objective = objective

                            contact_party = self.extract_single('//div[@id="contact"]/div[contains(@class,"listing--teaser__wrapper")]/ul[@class="listing listing--no-link listing--teaser"]/li[@class="listing__item"]/div[contains(@class,"field field-name-title")]/div[@class="field__items"]/h3/text()')
                            if contact_party:
                                eubill.contact_party = contact_party
                        except Exception,e:
                            self.logger.critical(__name__,
                                                 fmt('individual_bill_scrape_failed ')
                                                     , fmt("Error occured: {} , consultation_page_url : {} ", e,consultation_page_url),
                                                 exc_info=True)

                    if self.validate_doc(eubill):
                        self.save_doc(eubill)
                    else:
                        self.logger.critical(__name__, "individual_bill_scrape_failed",
                                             fmt("JsonSchema validation failed for bill having link: {} , consultation member id : {} , json dump : {}",
                                                 page_link, count,json.dumps(eubill.to_json())))
                except Exception as e:
                    self.logger.critical(__name__, fmt('individual_bill_scrape_failed '), fmt("Error occured: {}, page_link :{} , member block id :{}  ",e, page_link, count),
                                         exc_info=True)

                    continue


        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e))

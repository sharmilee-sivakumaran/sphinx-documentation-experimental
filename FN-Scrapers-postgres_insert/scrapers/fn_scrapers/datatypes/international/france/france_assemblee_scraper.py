# -*- coding: utf-8 -*-
from __future__ import division
import logging
from datetime import datetime
import re
import math
import injector

from fn_scraperutils.events.reporting import EventComponent,ScrapeError
from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import FRANCEBill, DocumentAttachment, stages
import json
import dateparser
from unidecode import unidecode
from fn_service.server import BlockingEventLogger, fmt
logger = logging.getLogger(__name__)


# class for URL formation at different levels
class FranceURL:

    base_url = u"http://www2.assemblee-nationale.fr"
    base_url1 = u"http://www.assemblee-nationale.fr"

    @staticmethod
    def proposition_de_loi_url(parliament):
        proposition_de_loi_url = u"{base_url}/documents/liste/(type)/propositions-loi/(legis)/{parliament}".format(base_url=FranceURL.base_url, parliament=parliament)
        return proposition_de_loi_url

    @staticmethod
    def projet_de_loi_url(parliament):
        projet_de_loi_url = u"{base_url}/documents/liste/(type)/projets-loi/(legis)/{parliament}".format(base_url=FranceURL.base_url, parliament=parliament)
        return projet_de_loi_url

    @staticmethod
    def get_bill_page_url(bill_page_number, parliament):
        bill_page_url = u"{base_url}/documents/liste/(ajax)/1/(offset)/" \
                        u"{bill_page_number}/(limit)/150/(type)/propositions-loi/(legis)/{parliament}/(no_margin)/false"\
            .format(base_url=FranceURL.base_url, bill_page_number=bill_page_number, parliament=parliament)
        return bill_page_url

    @staticmethod
    def get_projet_bill_page_url(bill_page_number, parliament):
        bill_page_url = u"{base_url}/documents/liste/(ajax)/1/(offset)/" \
                        u"{bill_page_number}/(limit)/150/(type)/projets-loi/(legis)/{parliament}/(no_margin)/false" \
            .format(base_url=FranceURL.base_url, bill_page_number=bill_page_number, parliament=parliament)
        return bill_page_url

    @staticmethod
    def get_source_url(link):
        source_url = u"{base_url}{link}".format(base_url=FranceURL.base_url, link=link)
        return source_url

    @staticmethod
    def get_sponsor_url(link, parliament):
        sponsor_url = u"{base_url}/{parliament}/dossiers/{link}".format(base_url=FranceURL.base_url1, parliament=parliament, link=link)
        return sponsor_url


@scraper()
@argument('--parliament', help='parliament in case of assemblee only e.g - 14,15', required=True)
@tags(type="bills", country_code="FR", group="international", chamber="assemblee")
class FRANCEASSEMBLEEDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):
        super(FRANCEASSEMBLEEDocScraper, self).__init__(EventComponent.scraper_bills, "france", "france")
        self.logger = logger

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
            comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
            match = comp.search(html)
            if not match:
                return match
            else:
                resu = re.sub('&nbsp;', ' ', match.group(group))
                resu = re.sub('&\S+;|\s{2,}|;', '', resu)
                return resu

    @staticmethod
    def get_date_from_french(date):
        dt = dateparser.parse(date)
        date = dt.strftime('%Y-%m-%d')
        return date

    @staticmethod
    def single_pattern_link(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return match
        else:
            return match.group(group)

    @staticmethod
    def roundup(x):
        return int(math.ceil(x / 10.0)) * 10

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return date

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    # function for downloading html from page
    def download_html(self, url):
            self.http_get(url, self.scraper_policy.doc_list)
            html = self.get_content_from_response()
            return html

    # function for scrape data
    def scrape(self, parliament):
        try:
            url_list = [FranceURL.projet_de_loi_url(parliament), FranceURL.proposition_de_loi_url(parliament)]
            for url in url_list:
                first_page_html = self.download_html(url)
                page_number_list = self.find_pattern(first_page_html, 'data-uri-init="\d+">\s*(\d+)\s*</a')
                if page_number_list:
                    last_page = page_number_list[-1]
                    for bill_page in range(int(last_page)):
                        bills = bill_page * 150
                        if "propositions-loi" in url:
                            bill_page_url = FranceURL.get_bill_page_url(bills, parliament)
                        elif "projets-loi" in url:
                            bill_page_url = FranceURL.get_projet_bill_page_url(bills, parliament)
                        self.scrape_bill(bill_page_url, parliament)
                else:
                    if "propositions-loi" in url:
                        bill_page_url = FranceURL.get_bill_page_url("0", parliament)
                    elif "projets-loi" in url:
                        bill_page_url = FranceURL.get_projet_bill_page_url("0", parliament)
                    self.scrape_bill(bill_page_url, parliament)
        except Exception as e:
            self.logger.critical(__name__, "scraper_failed",
                                 fmt("{} bills could not be scraped. {}", self.scraper_name.title(), e), exc_info=True)
            raise ScrapeError(self.scraper_policy.doc, fmt("{} bills could not be scraped. {}", self.scraper_name, e),
                              self.first_page_url)

    def scrape_bill(self, bill_page_url, parliament):
        bill_page_html = self.download_html(bill_page_url)
        bills_html_list = self.find_pattern(bill_page_html, '(<h3>[^<]*N.&nbsp;\d+\s*<.*?</p>)')
        for bill_block_html in bills_html_list:
            try:
                france_bill = FRANCEBill()

                # source_url
                source_url = self.single_pattern(bill_block_html, '<a href="([^>]*)">\s*Dossier', 1)
                if source_url:
                    source_url = re.sub('\d+/dossiers', parliament+'/dossiers', source_url)
                    france_bill.add_source_url(source_url)

                    # session
                    france_bill.add_session(parliament)

                    # document link
                    bill_final_html = self.download_html(source_url)

                    # title
                    title = self.single_pattern(bill_final_html, '<TITLE>(.*?)</TITLE>', 1)
                    title = re.sub('<.*?>|N.&nbsp;\d+|;|&nbsp;|N.\s+\d+|:|^\s+|Assembl.e nationale \-', '', title)
                    title = re.sub('\s{2,}', ' ', title)
                    title = re.sub('^\s+', '', title)
                    france_bill.add_title(title)

                    # bill id from first national assembly stage
                    bill_id = self.single_pattern(bill_final_html,
                                                  '</a>\s*Assembl.e nationale\s*\-\s*\d+\s*<sup>\s*.?r?e?\s*</sup>.*?n.\s*(\d+)\s*(\s*\d+<sup>e</sup>)?(\s*rectifi.)?,?\s*r?e?d.po..e? le',
                                                  1)
                    france_bill.add_bill_id(bill_id)

                    # sponsors
                    sponsor_list = self.find_pattern(bill_final_html, '<a target="_blank"\s*href="/\d+[^>]*>(.*?)</a')
                    if not sponsor_list:
                        sponsor_list = self.find_pattern(bill_final_html,
                                                         '<a\s*target="_blank"\s*href="http://www.senat.fr/senateur[^>]*>(.*?)</')

                    extra_sponsor_link = self.single_pattern(bill_final_html, '(cosignataires_\d+.html)', 1)
                    if extra_sponsor_link:
                        if extra_sponsor_link.startswith("http"):
                            pass
                        else:
                            extra_sponsor_link = FranceURL.get_sponsor_url(extra_sponsor_link, parliament)
                            extra_sponsor_html = self.download_html(extra_sponsor_link)
                            extra_sponsors = self.find_pattern(extra_sponsor_html, '<a target="_blank".*?>\s*(.*?)\s*</a')
                            for spon in extra_sponsors:
                                sponsor_list.append(spon)
                    if sponsor_list:
                        sponsor_final_list = []
                        for sponsor in sponsor_list:
                            sponsor = re.sub('MM\.|MMe|M\.|Mme|Mm\.|<.*?>', '', sponsor)
                            sponsor = re.sub('^\s+', '', sponsor)
                            sponsor = sponsor.title()
                            sponsor_final_list.append(sponsor)
                        france_bill.add_sponsors(list(set(sponsor_final_list)))

                    # stages
                    stages_block = bill_final_html.split('<font color="#000099" size="2" face="Arial">')

                    date = []
                    for stage in stages_block[1:]:
                        stage_obj = stages()

                        # stage name
                        stage_name = self.single_pattern(stage, '<a name="[^"]*">(.*?)</b>', 1)
                        stage_name = re.sub('<.*?>', '', stage_name)
                        stage_name = re.sub('^\s+', '', stage_name)
                        if stage_name:
                            stage_obj.add_name(stage_name)

                            # stage_date
                            stage_date = self.single_pattern(stage, 'r?e?d.po..e? le\s*(\d+\S*\s+\S+\s+\d+)', 1)
                            if not stage_date:
                                stage_date = self.single_pattern(stage, ' le\s*(\d+\S*\s+\S+\s+\d+)', 1)
                            if stage_date:
                                stage_date = re.sub('<.*?>','', stage_date)
                                stage_date = self.get_date_from_french(stage_date)
                                date.append(stage_date)
                                stage_obj.add_date(stage_date)
                            self.validate_doc(stage_obj)

                            if stage_name == "Conseil Constitutionnel":
                                stage_date = self.single_pattern(stage, '<a href="http://www.conseil-constitutionnel.fr.*?>.*?(\d+\S*\s+\S+\s+\d+)', 1)
                                if stage_date:
                                    stage_date = re.sub('<.*?>','', stage_date)
                                    stage_date = self.get_date_from_french(stage_date)
                                    date.append(stage_date)
                                    stage_obj.add_date(stage_date)

                            france_bill.add_stages_by_obj(stage_obj)

                    # First stage description text
                    first_stage_text = stages_block[1]

                    # long title from first stage description text
                    long_title = self.single_pattern(first_stage_text,
                                                     '<div align="left">(.*?)n.\s+\d+\s*(\s*\d+<sup>e</sup>)?(\s*rectifi.)?,?\s*r?e?d.po..e? le', 1)
                    if not long_title or long_title == ' ':
                        long_title = self.single_pattern(first_stage_text,
                                                         '<a\s*href=".*?">(.*?)n.\s+\d+\s*(\s*\d+<sup>e</sup>)?(\s*rectifi.)?,?\s*r?e?d.po..e? le', 1)
                    long_title = re.sub('<.*?>', '', long_title)
                    long_title = re.sub('^\s+|\s+$', '', long_title)
                    long_title = re.sub('\s*,\s*$', '', long_title)
                    france_bill.add_long_title(long_title)

                    # bill_type from first stage description text
                    if "Projet de loi" in first_stage_text:
                        bill_type = "Projet de loi"
                    elif "Proposition de loi" in first_stage_text:
                        bill_type = "Proposition de loi"
                    france_bill.add_bill_type(bill_type)

                    # documents
                    stages_block = bill_final_html.split('<font color="#000099" size="2" face="Arial">')
                    for stage in stages_block[1:]:

                        check_assembly = self.single_pattern(stage, '>\s*Assembl.e nationale\s*\-', 0)
                        if check_assembly:
                                document_attachment = DocumentAttachment()

                                document_link = self.single_pattern_link(stage, 'href="([^>]*)">\s*Proposition de loi\s*[^<]*</a>', 1)
                                if not document_link:
                                    document_link = self.single_pattern_link(stage, 'href="([^>]*)">\s*Projet de loi\s*[^<]*</a>', 1)
                                    if not document_link:
                                        document_link = self.single_pattern(stage, 'href="([^>]*)">\s*Texte\s*<', 1)

                                if document_link:
                                    if document_link.startswith("http"):
                                        pass
                                    else:
                                        document_link = "http://www.assemblee-nationale.fr" + document_link

                                    extraction_type = self.extraction_type.html
                                    content_type = "text/html"
                                    download_id, _, doc_ids = self.register_download_and_documents(document_link,
                                                                                                   self.scraper_policy.doc_service,
                                                                                                   extraction_type, True,
                                                                                                   content_type=content_type)
                                    if len(doc_ids) > 0:
                                        document_id = doc_ids[0]

                                        document_attachment.add_document_id(document_id)
                                        document_attachment.add_download_id(download_id)

                                        # bill id for document
                                        bill_id = self.single_pattern(stage, 'n.\s*(\d+)\s*(\s*\d+<sup>e</sup>)?(\s*rectifi.)?,?\s*r?e?d.po..e? le', 1)
                                        if not bill_id:
                                            bill_id = self.single_pattern(stage, 'sur le texte n.\s*([A-Z0-9]+)', 1)
                                            if not bill_id:
                                                bill_id = self.single_pattern(stage, 'TA\s+n.\s+\d+', 0)
                                                bill_id = re.sub('\s+n.\s+', ' ', bill_id)
                                        if bill_id:
                                            document_attachment.add_title(bill_id)
                                        self.validate_doc(document_attachment)
                                        france_bill.add_attachment_by_obj(document_attachment)

                    # chamber_of_origin
                    chamber_of_origin = unidecode(self.single_pattern(bill_final_html, 'Travaux pr.paratoires.*?<font face="Arial" size="1">(.*?)<', 1))
                    if "Senat" in chamber_of_origin:
                        chamber_of_origin = "Senat"
                    elif "Assemblee" in chamber_of_origin:
                        chamber_of_origin = "Assemblee nationale"
                    france_bill.add_chamber_of_origin(chamber_of_origin)

                    # other_chamber_source_url
                    other_chamber_source_url = self.single_pattern_link(unidecode(bill_final_html), 'href="([^>]*)">\s*<i>\s*\(Dossier en ligne', 1)
                    if other_chamber_source_url:
                        france_bill.add_other_chamber_source_url(other_chamber_source_url)

                    # scrape_source
                    france_bill.add_scrape_source("Assemblee nationale")

                    # introduction_date
                    introduction_date = date[0]
                    if introduction_date:
                        france_bill.add_introduction_date(introduction_date)

                    if self.validate_doc(france_bill):
                        self.save_doc(france_bill)
                    else:
                        self.logger.critical(__name__, "individual_bill_scrape_failed",
                                             fmt("JsonSchema validation failed for bill page: {}", source_url))
                else:
                    self.logger.info(__name__, 'Source url not found')
            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)
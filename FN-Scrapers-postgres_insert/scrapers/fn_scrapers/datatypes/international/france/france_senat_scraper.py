# -*- coding: utf-8 -*-
from __future__ import division
import logging
from datetime import datetime
import re
import math

from fn_scraperutils.events.reporting import EventComponent
from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import FRANCEBill, DocumentAttachment, stages
import json
import dateparser
from unidecode import unidecode
logger = logging.getLogger(__name__)


# class for URL formation at different levels
class FranceURL:
    base_url = u"http://www.senat.fr"

    @staticmethod
    def blll_list_page_url():
        list_page_url = u"{base_url}/dossiers-legislatifs/textes-recents.html".format(base_url=FranceURL.base_url)
        return list_page_url

    @staticmethod
    def get_source_url(link):
        source_url = u"{base_url}{link}".format(base_url=FranceURL.base_url, link=link)
        return source_url


@scraper()
@argument('--session', help='session to scrape e.g - 2016-2017', required=True)
@tags(type="bills", country_code="FR", group="international", chamber="senate")
class FRANCESENATDocScraper(ScraperBase):
    def __init__(self):
        super(FRANCESENATDocScraper, self).__init__(EventComponent.scraper_bills, "france", "france")

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
            comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
            match = comp.search(html)
            if not match:
                match = ' '
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
            match = ' '
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
        comp = re.compile(pattern, re.DOTALL)
        match = comp.findall(html, re.IGNORECASE)
        return match

    # function for downloading html from page
    def download_html(self, url):
            self.http_get(url, self.scraper_policy.doc_list)
            html = self.get_content_from_response()
            return html

    # function for scrape data
    def scrape(self, session):
        list_page_url = FranceURL.blll_list_page_url()
        bill_list_page_html = self.download_html(list_page_url)
        bill_blocks = self.find_pattern(bill_list_page_html, '(<li>\s*<a href=.*?</li>)')
        for bill in bill_blocks:
            self.scrape_bill(bill, session)

    # scrape bill function
    def scrape_bill(self, bill_block_html, session):

        france_bill = FRANCEBill()

        # bill_type
        if "Projet de loi" in bill_block_html or "Proposition de loi" in bill_block_html:
            if "Projet de loi" in bill_block_html:
                bill_type = "Projet de loi"
            elif "Proposition de loi" in bill_block_html:
                bill_type = "Proposition de loi"
            france_bill.add_bill_type(bill_type)

            # bill_id
            bill_url = self.single_pattern(bill_block_html, "<a href='(.*?)'>", 1)
            bill_id = self.single_pattern(bill_url, '\/([^\/]*)\.html', 1)
            bill_id = bill_id.upper()
            france_bill.add_bill_id(bill_id)

            # long_title
            long_title = self.single_pattern(bill_block_html, '<a href=.*?>(.*?)</a>', 1)
            france_bill.add_long_title(long_title)

            # source_url
            source_url = FranceURL.get_source_url(bill_url)
            france_bill.add_source_url(source_url)

            # bill_page_html
            bill_page_html = self.download_html(source_url)

            # session
            bill_session = self.single_pattern(bill_page_html, 'Texte\s*</a>\s*n.\s+\d+\s*\((\d+\-\d+)\)', 1)
            if bill_session == ' ':
                bill_session = self.single_pattern(bill_page_html, 'Texte\s*n.\s+\d+\s*\((\d+\-\d+)\)', 1)
            if bill_session != '' and bill_session != ' ':
                france_bill.add_session(bill_session)
            if bill_session == session:

                # title
                title = self.single_pattern(bill_page_html, '<h1 class="title-dosleg">\s*(.*?)\s*</h1>', 1)
                france_bill.add_title(title)

                # description
                description = self.single_pattern(bill_page_html, 'Objet du texte\s*</h3>(.*?<br>.*?)</div>', 1)
                if description != " " and description != "":
                    description = re.sub('<.*?>|\s{2,}', '', description)
                    description = re.sub('^\s+|\s+$', '', description)
                    france_bill.add_description(description)

                # sponsors
                sponsor_block = self.single_pattern(bill_page_html, '<div class="item item-big" id="timeline-1">.*?</ul></div>', 0)
                sponsors = self.find_pattern(sponsor_block, '<a href="/senateur.*?>(.*?)</')
                if not sponsors:
                    sponsor_list = []
                    sponsors = self.single_pattern(sponsor_block,
                                               '<ul class="list-disc-02">.*? de (.*?)\s*,',
                                               1)
                    if not sponsors:
                        sponsor_list = []
                        sponsors = self.single_pattern(sponsor_block,
                                                       '<ul class="list-disc-02">.*? de (.*?) et ',
                                                       1)
                    sponsors = re.sub('MM\.|MMe|M\.|Mme|Mm\.', '', sponsors)
                    sponsors = re.sub('\s*et .*|\s*Et .*', '', sponsors)
                    sponsors = re.sub('^\s+', '', sponsors)
                    if sponsors != "" and sponsors != " ":
                        sponsors = sponsors.title()
                        sponsor_list.append(sponsors)
                        france_bill.add_sponsors(sponsor_list)
                else:
                    new_sponsor = []
                    for sp in sponsors:
                        sp = sp.title()
                        new_sponsor.append(sp)
                    france_bill.add_sponsors(list(set(new_sponsor)))

                # stages
                stages_name = self.find_pattern(bill_page_html, '<a href="#timeline\-\d+"\s*title="(.*?)"')
                if stages_name:
                    for stage in stages_name:
                        stage_obj = stages()
                        stage = stage.split(' | ')
                        try:
                            stage_date = self.get_date_from_french(stage[0])
                            stage_obj.add_date(stage_date)
                        except AttributeError:
                            pass
                        stage_name = stage[1]
                        try:
                            stage_modified_name = stage_name.split("-")[1] + " - " + stage_name.split("-")[0]
                            stage_modified_name = re.sub('^\s+|\s+$', '', stage_modified_name)
                        except:
                            stage_modified_name = stage_name
                        stage_modified_name = re.sub('1.re lecture', 'Premiere lecture', stage_modified_name)
                        stage_modified_name = re.sub('2eme lecture', 'Deuxieme lecture', stage_modified_name)
                        stage_modified_name = re.sub('3eme lecture', 'Troisieme lecture', stage_modified_name)
                        stage_modified_name = re.sub('CMP', 'Commission mixte paritaire', stage_modified_name)
                        stage_modified_name = re.sub('nouv\. lect\.', 'Nouvelle lecture', stage_modified_name)
                        stage_modified_name = re.sub('L\. definitive', 'Lecture definitive', stage_modified_name)
                        if stage_modified_name:
                            stage_obj.add_name(stage_modified_name)
                        elif "Caducit" in stage[0]:
                            stage_modified_name = "Caducite"
                            stage_obj.add_name(stage_modified_name)
                        self.validate_doc(stage_obj)
                        france_bill.add_stages_by_obj(stage_obj)

                # chamber_of_origin
                chamber_of_origin = unidecode(self.single_pattern(bill_page_html,
                                                                  '<a href="#timeline-1" title="\d+\s+\S+\s+\d+\s*\|\s*.*?\-\s*(.*?)\s*">',
                                                                  1))
                if chamber_of_origin == "Senat":
                    chamber_of_origin = "Senat"
                elif chamber_of_origin == "Assemblee":
                    chamber_of_origin = "Assemblee nationale"
                france_bill.add_chamber_of_origin(chamber_of_origin)

                # introduction_date
                introduction_date = self.single_pattern(bill_page_html,
                                                                  '<a href="#timeline-1" title="(\d+\s+\S+\s+\d+)\s*\|\s*.*?\-\s*.*?\s*">',
                                                                  1)
                introduction_date = self.get_date_from_french(introduction_date)
                france_bill.add_introduction_date(introduction_date)

                # documents
                documents = self.find_pattern(bill_page_html, "(<a href='[^']*'>\s*Texte.*?</a>.*?</)")
                for document in documents:
                    document_link = self.single_pattern(document, "<a href='([^']*)'>\s*Texte.*?</a>", 1)
                    if document_link != ' ':
                        doc_link = document_link
                        if doc_link.startswith("http"):
                            pass
                        else:
                            doc_link = FranceURL.get_source_url(doc_link)
                        extraction_type = self.extraction_type.html
                        content_type = "text/html"
                        doc_link = re.sub('html.*', 'html', doc_link)
                        doc_link = re.sub('aspl', 'asp', doc_link)
                        doc_link = re.sub('/leg/\.\.', '', doc_link)
                        doc_link = re.sub('http://www\.senat\.fr/leg/http://www\.assemblee', 'http://www.assemblee', doc_link)
                        download_id, _, doc_ids = self.register_download_and_documents(doc_link,
                                                                                       self.scraper_policy.doc_service,
                                                                                       extraction_type, True,
                                                                                       content_type=content_type)
                        if len(doc_ids) > 0:
                            document_id = doc_ids[0]
                            document_attachment = DocumentAttachment()

                            # document title
                            title = self.single_pattern(document, "<a href='[^']*'>\s*(Texte.*?</a>.*?)</", 1)
                            if title != ' ':
                                title = re.sub('<.*?>|^\s+|\s+$', '', title)
                                document_attachment.add_title(title)

                            document_attachment.add_document_id(document_id)
                            document_attachment.add_download_id(download_id)
                            self.validate_doc(document_attachment)
                            france_bill.add_attachment_by_obj(document_attachment)

                # other_chamber_source_url
                other_chamber_source_url = self.single_pattern_link(unidecode(bill_page_html),
                                                                    '<a href="([^"]*)">\s*\(dossier legislatif',
                                                                    1)
                if other_chamber_source_url != "" and other_chamber_source_url != ' ':
                    france_bill.add_other_chamber_source_url(other_chamber_source_url)

                # scrape_source
                france_bill.add_scrape_source("Senat")

                if self.validate_doc(france_bill):
                    self.save_doc(france_bill)
                else:
                    logging.debug(json.dumps(france_bill.to_json()))
            else:

                pass

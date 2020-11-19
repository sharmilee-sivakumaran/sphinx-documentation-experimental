# -*- coding: utf-8 -*-
from __future__ import division
import logging
import injector
import re
from fn_scrapers.api.scraper import scraper, argument, tags
from ..common.base_scraper import ScraperBase
from .meps_schema_handler import EU_MEPs, Documentaddresses, DocumentCDM, Documentphone_information, Documentaddress, \
    EU_MEPs_main
from fn_scraperutils.events.reporting import EventComponent
import json
from fn_service.server import BlockingEventLogger, fmt

logger = logging.getLogger(__name__)
from datetime import datetime
from HTMLParser import HTMLParser


# class for URL formation at different levels
class EU_MEPs_URL:
    base_url = u"http://www.europarl.europa.eu"

    @staticmethod
    def get_page_url():
        page_url = "{base_url}/meps/en/full-list.html?filter=all&leg=".format(base_url=EU_MEPs_URL.base_url)
        return page_url

    @staticmethod
    def get_member_page_url(url):
        member_page_url = "{base_url}{url}".format(base_url=EU_MEPs_URL.base_url, url=url)
        return member_page_url


@scraper()
@tags(type="bills", group="international")
# EU Docscraper class
class EU_MEPsDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger)
    def __init__(self, logger):

        super(EU_MEPsDocScraper, self).__init__(EventComponent.scraper_bills, "eu_mep", "europe")
        self.logger = logger

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            resu = re.sub('&\S+;|\s{2,}|;', '', match.group(group))
            return resu

    @staticmethod
    def single_pattern_link(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            match = ' '
            return match
        else:
            return match.group(group)

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

    @staticmethod
    def trim_content(content):
        content = re.sub('<.*?>', '', content)
        content = re.sub('<.*?>|\s{2,}', ' ', content)
        content = re.sub('^\s+|\s+$|\r?\n', '', content)
        return content

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)', '', date)
        try:
            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
        return date

    # function for scrape data
    def scrape(self):
        home_page_html = self.download_html(EU_MEPs_URL.get_page_url())
        members_link = self.find_pattern(home_page_html, '<li class="mep_name">\s*<a\s*href="(.*?)"')
        self.scrape_bill(members_link)

    # function for scraping bills
    def scrape_bill(self, members_link):

        eubill_main = EU_MEPs_main()

        for link in members_link:
            member_link = EU_MEPs_URL.get_member_page_url(link)
            self.http_get(member_link, self.scraper_policy.doc_list)
            member_page_html = self.download_html(member_link)

            eubill = EU_MEPs()

            # source url
            eubill.add_source_url(member_link)

            # first_name and last name
            name = self.single_pattern(member_page_html, '<a id="mep_name_button".*?>(.*?)</a', 1)
            if name == " ":
                name = self.single_pattern(member_page_html, '<li class="mep_name">(.*?)</li', 1)
            name = re.sub('<[^>]*br[^>]*>', '----', name)
            name = self.trim_content(name)
            name_details = name.split("----")
            first_name = name_details[0]
            first_name = first_name.title()
            eubill.add_first_name(first_name)

            last_name = name_details[1]
            last_name = last_name.title()
            eubill.add_last_name(last_name)

            # country
            country = self.single_pattern(member_page_html, '<li class="nationality noflag">(.*?)<', 1)
            country = self.trim_content(country)
            eubill.add_country(country)

            # date_of_birth
            date_of_birth = self.single_pattern(member_page_html, 'Date of birth\s*:\s*(\d+\s*\S+\s*\d+)', 1)
            if date_of_birth != "" and date_of_birth != " ":
                date_of_birth = self.get_formatted_date(date_of_birth)
                eubill.add_date_of_birth(date_of_birth)

            # place_of_birth
            place_of_birth = self.single_pattern(member_page_html, 'Date of birth\s*:\s*\d+\s*\S+\s*\d+,\s*(.*?)\s*<',
                                                 1)
            place_of_birth = self.trim_content(place_of_birth)
            if place_of_birth != "" and place_of_birth != " ":
                eubill.add_place_of_birth(place_of_birth)

            # eu_party
            eu_party = self.single_pattern(member_page_html, '<li class="group .*?">\s*(.*?)\s*<', 1)
            eu_party = self.trim_content(eu_party)
            eubill.add_eu_party(eu_party)

            # eu_party_role
            eu_party_role = self.single_pattern(member_page_html, '<span class="more_info">\s*(.*?)\s*</', 1)
            eubill.add_eu_party_role(eu_party_role)

            # national_party
            national_party = self.single_pattern(member_page_html, '<span class="name_pol_group".*?>\s*(.*?)\s*<', 1)
            eubill.add_national_party(national_party)

            # photograph download_id
            photo_url = self.single_pattern_link(member_page_html, '<meta property="og:image" content="(.*?)"', 1)
            extraction_type = self.extraction_type.image
            download_id, _, doc_ids = self.register_download_and_documents(photo_url, self.scraper_policy.doc_service,
                                                                           extraction_type,
                                                                           True, extracted_text="")
            if download_id:
                eubill.add_photograph_download_id(download_id)

            # contact_email
            contact_email = self.extract_single(
                u'//div[@id="content_right"]/div[contains(@class,"in_boxflux")]/ul/li/a[@class="link_email"]/@href')

            if contact_email:
                contact_email = contact_email.split('mailto:')[1]
                if contact_email != "" and contact_email != " ":
                    contact_email = re.sub('\[dot\]', '.', contact_email)
                    contact_email = re.sub('\[at\]', '@', contact_email)
                    contact_email = contact_email[::-1]
                    eubill.add_contact_email(contact_email.strip())

            # contact_website
            contact_website = self.extract_single(
                u'//div[@id="content_right"]/div[contains(@class,"in_boxflux")]/ul/li/a[@class="link_website"]/@href')
            if contact_website:
                eubill.add_contact_website(contact_website.strip())

            # contact_facebook
            contact_facebook = self.extract_single(
                u'//div[@id="content_right"]/div[contains(@class,"in_boxflux")]/ul/li/a[@class="link_fb"]/@href')
            if contact_facebook:
                eubill.add_contact_facebook(contact_facebook.strip())

            # contact_twitter
            contact_twitter = self.extract_single(
                u'//div[@id="content_right"]/div[contains(@class,"in_boxflux")]/ul/li/a[@class="link_twitt"]/@href')
            if contact_twitter:
                eubill.add_contact_twitter(contact_twitter.strip())

            # committee_delegation_memberships
            portfolio_block = self.single_pattern(member_page_html, '(<div class="boxcontent nobackground">.*?</div>)',
                                                  1)
            por_sub_blocks = self.find_pattern(portfolio_block, '(<h4>.*?</ul>)')
            for sub_block in por_sub_blocks:
                comm_del_role = self.single_pattern(sub_block, '<h4>\s*(.*?)\s*</h4>', 1)
                names = self.find_pattern(sub_block, '</acronym>\s*(.*?)\s*</li>')
                for name in names:
                    cdm = DocumentCDM()
                    name = self.trim_content(name)
                    cdm.add_comm_del_name(name)
                    cdm.add_comm_del_role(comm_del_role)
                    eubill.add_cdm_by_obj(cdm)
            h = HTMLParser()
            # addresses
            address_main_block = self.single_pattern_link(member_page_html,
                                                          '(<h3>\s*Contacts\s*</h3>.*?<ul class="contact">.*?<script>)',
                                                          1)
            addresses_blocks = self.find_pattern(address_main_block,
                                                 '(<div class="boxcontent.*?>.*?<ul class="contact">.*?</div>)')
            for addresses_block in addresses_blocks:
                addresses = Documentaddresses()
                address_type = self.single_pattern_link(addresses_block, '<h4>(.*?)</h4>', 1)
                address_type = re.sub('&nbsp;|\s{2,}', ' ', address_type)
                address_type = re.sub('<.*?>|^\s+|\s+$', '', address_type)
                addresses.add_address_type(address_type)

                address = Documentaddress()

                address_block = self.single_pattern_link(addresses_block, '(<li class="address">.*?</li>)', 1)
                address_block = re.sub('<br\s*\/?>', '----', address_block)
                address_block = self.trim_content(address_block)
                address_block = re.sub('(----)$', '', address_block)
                address_block_mod = address_block.split('----')

                # postal cde and city
                postal_city = address_block_mod[-1]

                # postal code
                postal_code = self.single_pattern(postal_city, '(\S*\d+)', 1)
                if postal_code != "" and postal_code != " ":
                    address.add_postal_code(postal_code)

                # city
                city = self.single_pattern(postal_city, '\d+\s*(.*)', 1)
                if city == " ":
                    city = address_block_mod[-1]
                city = h.unescape(city)
                address.add_city(city)

                # building
                building = address_block_mod[0]
                building = re.sub('&nbsp;', ' ', building)
                building = self.trim_content(building)
                building = h.unescape(building)
                address.add_building(building)

                if "Postal" in address_type:

                    # street
                    street = address_block_mod[1]
                    street = self.trim_content(street)
                    street = h.unescape(street)
                    address.add_street(street)

                    # office
                    office = address_block_mod[2]
                    office = self.trim_content(office)
                    office = h.unescape(office)
                    address.add_office(office)

                else:
                    # office
                    office = address_block_mod[1] + address_block_mod[2]
                    office = self.trim_content(office)
                    office = h.unescape(office)
                    address.add_office(office)

                    # street
                    street = address_block_mod[3]
                    street = self.trim_content(street)
                    street = h.unescape(street)
                    address.add_street(street)

                addresses.add_address_by_obj(address)

                phone_details = self.find_pattern(addresses_block, '(<li title=.*?>.*?</li>)')
                for phone_detail in phone_details:

                    phone_obj = Documentphone_information()

                    # phone
                    phone = self.single_pattern(phone_detail, '<span class="phone">(.*?)</', 1)
                    phone_type = "phone"
                    if phone == '' or phone == ' ':
                        phone = self.single_pattern(phone_detail, '<span class="fax">(.*?)</', 1)
                        phone_type = "fax"
                    phone = self.trim_content(phone)
                    if phone != '' and phone != ' ':
                        phone_obj.add_phone_number(phone)
                        phone_obj.add_phone_type(phone_type)

                    addresses.add_phone_information_by_obj(phone_obj)
                eubill.add_addresses_by_obj(addresses)
            if self.validate_doc(eubill):
                eubill_main.add_mep_by_obj(eubill)
            else:
                self.logger.critical(__name__, "individual_bill_scrape_failed",
                                     fmt("JsonSchema validation failed for bill page: {}", member_link))

        self.save_doc(eubill_main.to_json())

# -*- coding: utf-8 -*-
from __future__ import division
import logging
import re
from fn_scraperutils.events.reporting import EventComponent
import json
from unidecode import unidecode

from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from .schema_handler import SWITZERLANDBill, DocumentAttachment, DocumentAuthor

from datetime import datetime

logger = logging.getLogger(__name__)

# class for URL formation at different levels
class SwitzerlandURL:

    base_url = u"http://ws-old.parlament.ch/affairs"

    @staticmethod
    def get_bill_page_url(bill_id):
        bill_page_url = "{base_url}/{bill_id}?format=json&lang=fr".format(base_url=SwitzerlandURL.base_url, bill_id=bill_id)
        return bill_page_url

    @staticmethod
    def get_page_url(page_number):
        page_url = "{base_url}/?format=json&pagenumber={page_number}".format(base_url=SwitzerlandURL.base_url, page_number=page_number)
        return page_url

    @staticmethod
    def get_source_url(id):
        source_url = "https://www.parlament.ch/en/ratsbetrieb/suche-curia-vista/geschaeft?AffairId={id}".format(id=id)
        return source_url


# Switzerland Docscraper class
@scraper()
@argument('--startyear', help='Startyear for the bills you want to scrape in the format yyyy', required=True)
@argument('--endyear', help='Endyear for the bills you want to scrape in the format yyyy', required=False)
@tags(type="bills", country_code="CH", group="international")
class SWITZERLANDDocScraper(ScraperBase):
    def __init__(self):
        super(SWITZERLANDDocScraper, self).__init__(EventComponent.scraper_bills, "switzerland", "switzerland")

    # function for converting html to text doc
    def memo_static_content(self, html_file):
        raw_text = html_file.read()
        raw_text = raw_text.replace("\\", "")
        raw_text = unicode(raw_text, "utf-8")
        pattern = '"name":\s*"' + self.name + '"\s*\},\s*"value":\s*"(.*?)"\s*\}'
        document_text = self.single_pattern(raw_text, pattern, 1)
        document_text = unidecode(document_text)
        return document_text

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

    # function for downloading html from page
    def download_html(self, url):
        self.http_get(url, self.scraper_policy.doc_list)
        html = self.get_content_from_response()
        return html

    # To get ids from affairs page
    def get_ids_from_page(self, page_number):
        page_url = SwitzerlandURL.get_page_url(page_number)
        page_response = self.download_html(page_url)
        page_response = json.loads(page_response)
        id_row = []
        for row in page_response:
            id = row.get("id")
            id_row.append(id)
        return id_row

    # function for scrape data
    def scrape(self, startyear, endyear):
        if SWITZERLANDDocScraper.check_year(startyear):
            if not endyear:
                logger.info(u"End year is not given using current year as end year by default")
                endyear = str(datetime.now().year)
            home_page_html = self.download_html(SwitzerlandURL.base_url)
            total_number_of_pages = int(self.single_pattern(home_page_html, 'Page 1 of (\d+)\s*\(', 1))
            for page in range(total_number_of_pages, 0, -1):
                id_row = self.get_ids_from_page(page)
                for id in reversed(id_row):
                    desired_id = int(str(id)[:4])
                    startyear = int(startyear)
                    endyear = int(endyear)
                    if startyear <= desired_id <= endyear:
                        bill_url = SwitzerlandURL.get_bill_page_url(id)
                        self.scrape_bill(bill_url, id)
                    elif desired_id > endyear:
                        pass
                    elif desired_id < startyear:
                        return

    # function for scraping bills
    def scrape_bill(self, bill_url, id):
        bill_page_html = self.download_html(bill_url)
        bill_page_html = json.loads(bill_page_html)

        switzerland_bill = SWITZERLANDBill()

        # bill_type
        bill_type = bill_page_html.get("affairType").get("name")
        bill_type = unidecode(bill_type)
        bill_types = ["Initiative parlementaire", "Initiative cantonale", "Motion", "Postulat", "Objet du Conseil federal", "Objet du Parlement"]
        if bill_type in bill_types:
            switzerland_bill.add_bill_type(bill_type)

            # bill_id
            bill_id = bill_page_html.get("id")
            switzerland_bill.add_bill_id(bill_id)

            # bill_short_id
            bill_short_id = bill_page_html.get("shortId")
            bill_short_id = unidecode(bill_short_id)
            switzerland_bill.add_bill_short_id(bill_short_id)

            # source_language
            source_language = bill_page_html.get("language")
            source_language = unidecode(source_language)
            switzerland_bill.add_source_language(source_language)

            # title
            title = bill_page_html.get("title")
            title = unidecode(title)
            switzerland_bill.add_title(title)

            # introduction_date
            introduction_date = bill_page_html.get("deposit").get("date")
            introduction_date = re.sub('T.*', '', introduction_date)
            introduction_date = unidecode(introduction_date)
            switzerland_bill.add_introduction_date(introduction_date)

            # introduction_council
            try:
                introduction_council = bill_page_html.get("deposit").get("council").get("name")
                introduction_council = unidecode(introduction_council)
                switzerland_bill.add_introduction_council(introduction_council)

            except Exception, e:
                logger.error(u"Error occured : {error} for affair_id : {id}".format(error=str(e), id=id))
                pass

            # session
            session = bill_page_html.get("deposit").get("session")
            session = int(unidecode(session))
            session_maper_dict = {5010: "Session d'automne 2017", 5009: "Session d'été 2017", 5008: "Session spéciale mai 2017", 5007: "Session de printemps 2017",
                                  5006: "Session d'hiver 2016",
                                  5005: "Session d'automne 2016", 5004: "Session d'été 2016", 5003: "Session spéciale avril 2016", 5002: "Session de printemps 2016",
                                  5001: "Session d'hiver 2015", 4920: "Session d'automne 2015", 4919: "Session d'été 2015", 4918: "Session spéciale mai 2015",
                                  4917: "Session de printemps 2015", 4916: "Session d'hiver 2014", 4915: "Session d'automne 2014", 4914: "Session d'été 2014",
                                  4913: "Session spéciale mai 2014", 4912: "Session de printemps 2014", 4911: "Session d'hiver 2013", 4910: "Session d'automne 2013",
                                  4909: "Session d'été 2013", 4908: "Session spéciale avril 2013", 4907: "Session de printemps 2013", 4906: "Session d'hiver 2012",
                                  4905: "Session d'automne 2012", 4904: "Session d'été 2012", 4903: "Session spéciale mai 2012", 4902: "Session de printemps 2012",
                                  4901: "Session d'hiver 2011", 4820: "Session d'automne 2011", 4819: "Session d'été 2011", 4818: "Session spéciale avril 2011",
                                  4817: "Session de printemps 2011", 4816: "Session d'hiver 2010", 4815: "Session d'automne 2010", 4814: "Session d'été 2010",
                                  4813: "Session de printemps 2010", 4812: "Session d'hiver 2009", 4811: "Session d'automne 2009", 4810: "Session spéciale août 2009",
                                  4809: "Session d'été 2009", 4808: "Session spéciale avril 2009", 4807: "Session de printemps 2009", 4806: "Session d'hiver 2008",
                                  4805: "Session d'automne 2008", 4804: "Session d'été 2008", 4803: "Session spéciale avril 2008", 4802: "Session de printemps 2008",
                                  4801: "Session d'hiver 2007", 4718: "Session d'automne 2007", 4717: "Session d'été 2007", 4716: "Session de printemps 2007",
                                  4715: "Session d'hiver 2006", 4714: "Session d'automne 2006", 4713: "Session d'été 2006", 4712: "Session spéciale mai 2006",
                                  4711: "Session de printemps 2006", 4710: "Session d'hiver 2005", 4709: "Session d'automne 2005", 4708: "Session d'été 2005",
                                  4707: "Session de printemps 2005", 4706: "Session d'hiver 2004", 4705: "Session d'automne 2004", 4704: "Session d'été 2004",
                                  4703: "Session spéciale mai 2004", 4702: "Session de printemps 2004", 4701: "Session d'hiver 2003", 4620: "Session d'automne 2003",
                                  4619: "Session d'été 2003", 4618: "Session spéciale mai 2003", 4617: "Session de printemps 2003", 4616: "Session d'hiver 2002",
                                  4615: "Session d'automne 2002", 4614: "Session d'été 2002", 4613: "Session spéciale avril 2002", 4612: "Session de printemps 2002",
                                  4611: "Session d'hiver 2001", 4610: "Session extraordinaire novembre 2001", 4609: "Session d'automne 2001", 4608: "Session d'été 2001",
                                  4607: "Session spéciale mai 2001", 4606: "Session de printemps 2001", 4605: "Session d'hiver 2000", 4604: "Session d'automne 2000",
                                  4603: "Session d'été 2000", 4602: "Session du printemps 2000", 4601: "Session d'hiver 1999", 4521: "Session d'automne 1999",
                                  4520: "Session spéciale août 1999", 4519: "Session d'été 1999", 4518: "Session spéciale avril 1999", 4517: "Session de printemps 1999",
                                  4516: "Session d'hiver 1998", 4515: "Session d'automne 1998", 4514: "Session d'été 1998", 4513: "Session spéciale avril 1998",
                                  4512: "Session de printemps 1998", 4511: "Session spéciale janvier 1998", 4510: "Session d'hiver 1997", 4509: "Session d'automne 1997",
                                  4508: "Session d'été 1997", 4507: "Session spéciale avril 1997", 4506: "Session de printemps 1997", 4505: "Session d'hiver 1996",
                                  4504: "Session d'automne 1996", 4503: "Session d'été 1996", 4502: "Session de printemps 1996", 4501: "Session d'hiver 1995",
                                  4420: "Session d'automne 1995", 4419: "Session d'été 1995", 4418: "Session de printemps 1995", 4417: "Session spéciale janvier 1995",
                                  4416: "Session d'hiver 1994", 4415: "Session d'automne 1994", 4414: "Session d'été 1994", 4413: "Session de printemps 1994",
                                  4412: "Session d'hiver 1993", 4411: "Session d'automne 1993", 4410: "Session d'été 1993", 4409: "Session spéciale avril 1993",
                                  4408: "Session de printemps 1993", 4407: "Session d'hiver 1992", 4406: "Session d'automne 1992", 4405: "Session spéciale août 1992",
                                  4404: "Session d'été 1992", 4403: "Session de printemps 1992", 4402: "Session spéciale janvier 1992", 4401: "Session d'hiver 1991",
                                  4320: "Session d'automne 1991", 4319: "Session d'été 1991", 4318: "Session du 700e, mai 1991", 4317: "Session de printemps 1991",
                                  4316: "Session spéciale janvier 1991", 4315: "Session d'hiver 1990", 4314: "Session d'automne 1990", 4313: "Session d'été 1990",
                                  4312: "Session de printemps 1990", 4311: "Session spéciale février 1990"}
            maped_session = session_maper_dict.get(session)
            switzerland_bill.add_session(maped_session)

            # current_status
            current_status = bill_page_html.get("state").get("name")
            current_status = unidecode(current_status)
            switzerland_bill.add_current_status(current_status)

            try:

                # author
                author = DocumentAuthor()

                # author_type
                author_dict = bill_page_html.get("author")
                author_type_list = list(author_dict)
                author_type = author_type_list[1]
                author_type = unidecode(author_type)
                author.add_author_type(author_type)

                # author_id
                author_id = bill_page_html.get("author").get(author_type).get("id")
                author.add_author_id(author_id)

                # author_name
                author_name = bill_page_html.get("author").get(author_type).get("name")
                author_name = unidecode(author_name)
                author.add_author_name(author_name)

                self.validate_doc(author)
                switzerland_bill.add_author_by_obj(author)

            except Exception,e:
                logger.error(u"Error occured : {error} for affair_id : {id}".format(error=str(e), id=id))
                pass

            # last_updated_date
            last_updated_date = bill_page_html.get("updated")
            last_updated_date = re.sub('T.*', '', last_updated_date)
            last_updated_date = unidecode(last_updated_date)
            switzerland_bill.add_last_updated_date(last_updated_date)

            # source_url
            source_url = SwitzerlandURL.get_source_url(id)
            switzerland_bill.add_source_url(source_url)

            # documents
            download_args = dict()
            download_args['headers'] = {
                'User-Agent': u'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-gb,en;q=0.5',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
                'Keep-Alive': '115',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0'}

            document_array = bill_page_html.get("texts")
            for doc in document_array:
                document_attachment = DocumentAttachment()

                # document_type
                document_type = doc.get("type").get("name")
                self.name = document_type
                document_attachment.add_document_type(document_type)
                content_type = "text/html"
                download_id, _, doc_ids = self.register_download_and_documents(bill_url,
                                                                               self.scraper_policy.doc_service,
                                                                               self.extraction_type.html,
                                                                               True, content_type=content_type, download_args=download_args,
                                                                               get_static_content=self.memo_static_content,
                                                                               should_skip_checks=True)
                if len(doc_ids) > 0:
                    document_id = doc_ids[0]
                    document_attachment.add_document_id(document_id)
                    document_attachment.add_download_id(download_id)
                    self.validate_doc(document_attachment)
                    switzerland_bill.add_attachment_by_obj(document_attachment)

            if self.validate_doc(switzerland_bill):
                self.save_doc(switzerland_bill)
            else:
                logging.debug(json.dumps(switzerland_bill.to_json()))

    @staticmethod
    def check_year(year):
        if re.search(r'\d{4}', year):
            return True
        else:
            logger.warn(u"Please give start year int the format YYYY")
            return False

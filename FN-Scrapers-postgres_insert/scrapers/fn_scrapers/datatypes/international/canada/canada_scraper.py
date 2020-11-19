# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

import os
import sys
import re

from fn_scraperutils.events.reporting import EventComponent
from fn_scrapers.api.scraper import scraper, argument, tags

from ..common.base_scraper import ScraperBase
from ..common.model_factory import ModelFactory
from ..common.utils import parse_date_as_str, urljoin, get_official_language_list, get_country_info


logger = logging.getLogger(__name__)


@scraper()
@argument("--parliament", help="The parliament or session to scrape (eg: '42' for 42nd parliament , '42-1' for 1st session of 42nd parliament )", type=str, choices=["42", "42-1", "41", "41-2", "41-1", "40", "40-3", "40-2", "40-1", "39", "39-2", "39-1", "38", "38-1", "37", "37-3", "37-2", "37-1", "36", "36-2", "36-1", "35", "35-2", "35-1"], required=True)
@tags(type="bills", country_code="CA", group="international")
class CanadaDocScraper(ScraperBase):
    def __init__(self):
        super(CanadaDocScraper, self).__init__(EventComponent.scraper_bills, "canada", "canada")
        self.base_url = u'http://www.parl.ca'
        self.list_url = u'{base_url}/LegisInfo/Result.aspx?ParliamentSession={parliament}&BillLongTitle=*&BillShortTitle=*' \
                        u'&Language=E&Mode=1'
        self.bill_url = u'{base_url}/LegisInfo/BillDetails.aspx?billId={bill_id}&Language=E&Mode=1'

        bill_json_schema_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
        bill_json_schema_file_path = os.path.join(bill_json_schema_dir_path, "canada.json")

        self.model_factory = ModelFactory(bill_json_schema_file_path,bill_json_schema_file_path)
        self.country_languages = get_official_language_list(get_country_info("canada").alpha_2)

    def get_list_url(self, parliament):
        xml_url = self.list_url.format(base_url=self.base_url, parliament=parliament)
        xml_url += '&download=xml'
        return xml_url

    def get_bill_url(self, bill_id):
        return self.bill_url.format(base_url=self.base_url, bill_id=bill_id)

    def get_bill_xml_url(self, bill_id):
        url = self.bill_url.format(base_url=self.base_url, bill_id=bill_id)
        xml_url = url + '&download=xml'
        return xml_url

    def scrape(self, parliament):
        if CanadaDocScraper.check_parliament(parliament):
            bill_ids = self.scrape_bill_ids(parliament)
            if bill_ids and len(bill_ids):
                for bill_id in bill_ids:
                    self.scrape_bill(parliament, bill_id)
            else:
                raise ValueError("Scraper could not find any bill.")
        else:
            logger.error(u"Invalid parliament format. Please check and try again.")

    def scrape_bill_ids(self, parliament):
        # Base url
        url = self.get_list_url(parliament=parliament)
        self.http_get(url, self.scraper_policy.doc_list)
        if 'your query yielded no results' in self.get_content_from_response():
            logger.error(u"No results found")
            return None
        # Extract the total number of bills for this parliament/session.
        bill_ids = self.xpath(u"//bill[boolean(@id)]/@id").extract()
        logger.debug(u"Total bills found in search result: %s", len(bill_ids))
        return bill_ids

    def scrape_bill(self, parliament, bill_id):
        try:
            # old url
            # http://www.parl.gc.ca/LegisInfo/BillDetails.aspx?Language=E&Mode=1&billId=4771490

            # new url
            # http://www.parl.ca/LegisInfo/BillDetails.aspx?billId=4771490&Language=E&Mode=1

            # Base url
            xml_url = self.get_bill_xml_url(bill_id=bill_id)

            # Fetching for given bill id and given parliament/session

            logger.debug(u"Fetching for given bill id - {} of given parliament or session - {}".format(bill_id, parliament))
            self.http_get(xml_url, self.scraper_policy.doc_list)

            # Extract bill details for given parliament/session
            bill_doc = self.model_factory.create_bill_doc()
            bill_doc.unique_bill_id = self.extract_single('//bill/@id')

            bill_title = self.extract_single('//billtitle/title[@language="en"]/text()')
            if bill_title:
                bill_doc.title = bill_title

            bill_short_title = self.extract_single('//shorttitle/title[@language="en"]/text()')
            if bill_short_title:
                bill_doc.short_title = bill_short_title

            source_url = self.get_bill_url(bill_id=bill_id)
            if source_url:
                bill_doc.source_url = source_url


            bill_number_prefix = self.extract_single('//billnumber/@prefix')
            bill_number = self.extract_single('//billnumber/@number')
            if bill_number and bill_number_prefix:
                bill_doc.bill_number = bill_number_prefix + '-' + bill_number
            else:
                logger.debug("Bill number missing.")

            parliament_number = self.extract_single('//parliamentsession/@parliamentnumber')
            session_number = self.extract_single('//parliamentsession/@sessionnumber')
            if parliament_number:
                bill_doc.parliament = int(parliament_number)  # '42' out of '42-1'
            else:
                logger.debug("Parliament is missing.")

            if session_number:
                bill_doc.session = session_number  # '1' out of '42-1'
            else:
                logger.debug("Session is missing.")

            originating_chamber = self.extract_single('//events/legislativeevents/event[position()=1]/@chamber')
            if originating_chamber:
                if 'SEN' in originating_chamber:
                    bill_doc.originating_chamber = 'senate'
                else:
                    bill_doc.originating_chamber = 'hoc'
            else:
                bill_doc.originating_chamber = 'hoc'

            introduction_date = self.extract_single(
                '//events/legislativeevents/event[position()=1]/@date')
            if introduction_date:
                introduction_date = parse_date_as_str(introduction_date, languages=self.country_languages)
                bill_doc.introduction_date = introduction_date

            current_status = self.extract_single(
                '//events/lastmajorstageevent/event[position()=1]/status/title[@language="en"]/text()')
            if current_status:
                bill_doc.current_status = current_status

            current_status_date = self.extract_single(
                '//events/lastmajorstageevent/event[position()=1]/@date')
            if current_status_date:
                current_status_date = parse_date_as_str(current_status_date, languages=self.country_languages)
                bill_doc.current_status_date = current_status_date

            sponsors = []
            actions = []
            actions_set = set()
            committees = {}
            events = self.xpath('//events/legislativeevents/event')

            for event in events:

                action = self.model_factory.create("ActionSchema")
                action_name = self.extract_single('status/title[@language="en"]/text()',sel=event)
                action_date = self.extract_single('@date',sel=event)
                if action_date:
                    action_date = parse_date_as_str(action_date, languages=self.country_languages)
                action_chamber = self.extract_single('@chamber',sel=event)
                if action_chamber:
                    action_chamber = 'senate' if 'SEN' in action_chamber else 'hoc'
                if action_name:
                    action.action_text = action_name

                    if action_date:
                        action.action_date = action_date

                    if action_chamber:
                        action.chamber = action_chamber

                    if self.validate_doc(action):
                        action_name = action_name.replace(" ","")
                        action_name = action_name.lower()
                        action_unique_id = str(action_date)+action_name

                        if action_unique_id not in actions_set:
                            actions_set.add(action_unique_id)
                            actions.append(action)
                        else:
                            logger.debug("Skipping Duplicate Action: {}".format(action.for_json()))
                    else:
                        logger.debug("Skipping ActionSchema failed in validation: {}".format(action.for_json()))

                committee_name = self.extract_single('committee/title[@language="en"]/text()',sel=event)
                committee_id = self.extract_single('committee/@id',sel=event)

                if committee_name and committee_id and committee_id not in committees:

                    committee_chamber = self.extract_single('@chamber',sel=event)

                    committee = self.model_factory.create("CommitteeSchema")
                    committee.name = committee_name

                    if committee_chamber:
                        committee_chamber = 'senate' if 'SEN' in committee_chamber else 'hoc'
                        committee.chamber = committee_chamber

                    if self.validate_doc(committee):
                        committees[committee_id] = committee

            if len(actions) > 0:
                bill_doc.actions = actions

            if len(committees) > 0:
                bill_doc.committees = committees.values()

            bill_type = self.extract_single('//billtype/title[@language="en"]/text()')
            if bill_type:
                # Map the bill_type to what Pillar is expecting (basically, an ASCII apostrophe).
                if bill_type == u"Private Member’s Bill":
                    bill_type = u"Private Member's Bill"
                bill_doc.type = bill_type

            sponsor = self.model_factory.create("CanadaSponsorSchema")

            sponsor_title = self.extract_single(
                '//sponsoraffiliation/title[@language="en"]/text()')
            if sponsor_title:
                sponsor.title = sponsor_title
            sponsor_name = self.extract_single('//sponsoraffiliation/person/fullname/text()')
            if sponsor_name:
                sponsor.name = sponsor_name
            sponsor_party = self.extract_single(
                '//sponsoraffiliation/politicalparty/title[@language="en"]/text()')

            if sponsor_party:
                sponsor.party = sponsor_party

            if self.validate_doc(sponsor):
                sponsors.append(sponsor)
                bill_doc.sponsors = sponsors


            attachments = []
            publications = self.xpath('//publication')
            attachment_order = 1
            publication_date = ''


            for publication in publications:
                try:
                    publication_id = self.extract_single('@id',sel=publication)
                    publication_title = self.extract_single('title[@language="en"]/text()',sel=publication)

                    publication_relative_url = self.extract_single(
                        'publicationfiles/publicationfile[@language="en"]/@relativepath',sel=publication)
                    publication_url = u""

                    if publication_relative_url:
                        publication_url = urljoin(self.base_url, publication_relative_url)
                        logger.debug(
                            u"Fetching Document - {} for given bill id - {} of given parliament or session - {}".format(publication_id,
                                                                                                         bill_id, parliament))

                        #publication_url = 'http://www.parl.gc.ca/HousePublications/Publication.aspx?Mode=1&DocId=2329881&Language=E'
                        # http://www.parl.ca/LegisInfo/BillDetails.aspx?Language=E&billId=8064045&download=xml
                        self.http_get(publication_url, self.scraper_policy.doc)
                        if 'DocumentViewer' in publication_url:
                            is_following_latest_website_flow = True
                        else:
                            is_following_latest_website_flow = False

                        if is_following_latest_website_flow:
                            m = re.search(r'ext\:stage\-date\-en\=\"(.+?)\"', self.get_content_from_response())
                            if m:
                                publication_date = m.group(1)
                                publication_date = parse_date_as_str(publication_date, languages=self.country_languages)
                            else:
                                date_text = self.extract_single('//div[contains(@class,"publication-container-content")]//div[contains(string(),"reading,")]/text()')
                                if date_text:
                                    m = re.search(r'\,\s+(\w+\s+\d+\,\s+\d+)', date_text)
                                    if m:
                                        publication_date = m.group(1)
                                        publication_date = parse_date_as_str(publication_date, languages=self.country_languages)
                                    else:
                                        publication_date = None

                            complete_document_relative_url = self.extract_single('//div[contains(@class,"other-options")]/a[contains(.,"Complete Document")]/@href')

                            if complete_document_relative_url:

                                document_download_url = urljoin(self.base_url, complete_document_relative_url)
                                extraction_type = self.extraction_type.html
                                download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                               self.scraper_policy.doc_service,
                                                                                               extraction_type, True)
                                if len(doc_ids) > 0 and doc_ids[0]:
                                    document_id = doc_ids[0]
                                else:
                                    raise ValueError("Document ID not found while registering document with url {}".format(document_download_url))
                                if not download_id:
                                    raise ValueError(
                                        "Download ID not found while registering document with url {}".format(
                                            document_download_url))

                                document_attachment = self.model_factory.create("AttachmentSchema")
                                document_attachment.document_id = document_id
                                document_attachment.download_id = download_id
                                document_attachment.order = attachment_order
                                if 'Language=F' in publication_relative_url:
                                    document_attachment.locale = 'fr_CA'
                                else:
                                    document_attachment.locale = 'en_CA'
                                if publication_title:
                                    document_attachment.process_status = publication_title
                                if publication_date:
                                    document_attachment.publication_date = publication_date
                                if self.validate_doc(document_attachment):
                                    attachment_order += 1
                                    attachments.append(document_attachment)
                                else:
                                    logger.debug('Skipping Attachment: {}'.format(document_attachment.for_json()))
                            else:
                                document_download_url = self.resp.url
                                extraction_type = self.extraction_type.html
                                download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                               self.scraper_policy.doc_service,
                                                                                               extraction_type, True)
                                if len(doc_ids) > 0 and doc_ids[0]:
                                    document_id = doc_ids[0]
                                else:
                                    raise ValueError("Document ID not found while registering document with url {}".format(document_download_url))
                                if not download_id:
                                    raise ValueError(
                                        "Download ID not found while registering document with url {}".format(
                                            document_download_url))


                                document_attachment = self.model_factory.create("AttachmentSchema")
                                document_attachment.document_id = document_id
                                document_attachment.download_id = download_id
                                document_attachment.order = attachment_order
                                if 'Language=F' in publication_relative_url:
                                    document_attachment.locale = 'fr_CA'
                                else:
                                    document_attachment.locale = 'en_CA'
                                if publication_title:
                                    document_attachment.process_status = publication_title
                                if publication_date:
                                    document_attachment.publication_date = publication_date
                                if self.validate_doc(document_attachment):
                                    attachment_order += 1
                                    attachments.append(document_attachment)
                                else:
                                    logger.debug('Skipping Attachment: {}'.format(document_attachment.for_json()))


                        elif not is_following_latest_website_flow:
                            m = re.search(r'ext\:stage\-date\-en\=\"(.+?)\"', self.get_content_from_response())
                            if m:
                                publication_date = m.group(1)
                                publication_date = parse_date_as_str(publication_date, languages=self.country_languages)
                            else:
                                date_text = self.extract_single('//div[@id="publicationContent"]/span/div/text()')
                                if date_text:
                                    m = re.search(r'\,\s+(\w+\s+\d+\,\s+\d+)', date_text)
                                    if m:
                                        publication_date = m.group(1)
                                        publication_date = parse_date_as_str(publication_date, languages=self.country_languages)
                                    else:
                                        publication_date = None

                            complete_document_relative_url = self.extract_single(
                                '//span[@id="lblBillCompleteDocumentLink"]/a/@href')
                            if complete_document_relative_url:

                                document_download_url = urljoin(self.base_url, complete_document_relative_url)
                                extraction_type = self.extraction_type.html
                                self.http_get(document_download_url, self.scraper_policy.doc)
                                one_column_view_url = self.extract_single('//a[contains(.,"One Column View")]/@href')

                                if one_column_view_url:
                                    document_download_url = urljoin(self.base_url, one_column_view_url)
                                    extraction_type = self.extraction_type.html

                            else:
                                one_column_view_url = self.extract_single('//a[contains(.,"One Column View")]/@href')

                                if one_column_view_url:
                                    document_download_url = urljoin(self.base_url, one_column_view_url)
                                    extraction_type = self.extraction_type.html

                                elif self.xpath('//a[contains(./img/@alt,"Next Page")]/@href'):

                                    while self.xpath('//a[contains(./img/@alt,"Next Page")]/@href'):
                                        next_page_doc_url = self.extract_single('//a[contains(./img/@alt,"Next Page")]/@href')
                                        document_download_url = urljoin(self.base_url, next_page_doc_url)
                                        extraction_type = self.extraction_type.html

                                        if re.search(r'File\W+$', document_download_url):
                                            break

                                        resp = self.http_get(document_download_url, self.scraper_policy.doc)
                                        if resp is None:
                                            break

                                        download_id, _, doc_ids = self.register_download_and_documents(document_download_url,
                                                                                                       self.scraper_policy.doc_service,
                                                                                                       extraction_type, True)

                                        if len(doc_ids) > 0 and doc_ids[0]:
                                            document_id = doc_ids[0]
                                        else:
                                            raise ValueError(
                                                "Document ID not found while registering document with url {}".format(
                                                    document_download_url))
                                        if not download_id:
                                            raise ValueError(
                                                "Download ID not found while registering document with url {}".format(
                                                    document_download_url))

                                        document_attachment = self.model_factory.create("AttachmentSchema")
                                        document_attachment.document_id = document_id
                                        document_attachment.download_id = download_id
                                        document_attachment.order = attachment_order
                                        if 'Language=F' in publication_relative_url:
                                            document_attachment.locale = 'fr_CA'
                                        else:
                                            document_attachment.locale = 'en_CA'
                                        if publication_title:
                                            document_attachment.process_status = publication_title
                                        if publication_date:
                                            document_attachment.publication_date = publication_date
                                        if self.validate_doc(document_attachment):
                                            attachment_order += 1
                                            attachments.append(document_attachment)
                                        else:
                                            logger.debug('Skipping Attachment: {}'.format(document_attachment.for_json()))

                                    continue
                                else:
                                    complete_document_relative_url = self.extract_single(
                                        '//a[contains(./img/@alt,"Print format")]/@href')
                                    if complete_document_relative_url:

                                        document_download_url = urljoin(self.base_url, complete_document_relative_url)

                                        extraction_type = self.extraction_type.text_pdf

                            if document_download_url:
                                download_id, _, doc_ids = self.register_download_and_documents( document_download_url, self.scraper_policy.doc_service, extraction_type, True)

                            if len(doc_ids) > 0 and doc_ids[0]:
                                document_id = doc_ids[0]
                            else:
                                raise ValueError("Document ID not found while registering document with url {}".format( document_download_url))
                            if not download_id:
                                raise ValueError(
                                    "Download ID not found while registering document with url {}".format( document_download_url))

                            document_attachment = self.model_factory.create("AttachmentSchema")
                            document_attachment.document_id = document_id
                            document_attachment.download_id = download_id
                            document_attachment.order = attachment_order
                            if 'Language=F' in publication_relative_url:
                                document_attachment.locale = 'fr_CA'
                            else:
                                document_attachment.locale = 'en_CA'

                            if publication_title:
                                document_attachment.process_status = publication_title
                            if publication_date:
                                document_attachment.publication_date = publication_date
                            if self.validate_doc(document_attachment):
                                attachment_order += 1
                                attachments.append(document_attachment)
                            else:
                                logger.debug('Skipping Attachment: {}'.format(document_attachment.for_json()))
                except Exception as e:
                    logger.error(
                        'While checking publication error occured on line {}. {}: {}'.format(sys.exc_info()[-1].tb_lineno, type(e), str(e)))

            if len(attachments) > 0:
                bill_doc.attachments = attachments

            if self.validate_doc(bill_doc):
                self.save_doc(bill_doc.for_json())
            else:
                logger.debug(self.json_dumps(message = bill_doc.for_json()))

        except Exception as e:
            logger.error('Error occured on line {}. {}: {}'.format(sys.exc_info()[-1].tb_lineno,type(e), str(e)))

    @staticmethod
    def check_parliament(parliament):
        if re.search(r'^\d{2,2}$', parliament) and parliament in ["42", "41", "40", "39", "38", "37", "36", "35"]:
            return True
        elif re.search(r'^\d{2,3}-\d$', parliament) and parliament in ["42-1", "41-2", "41-1", "40-3", "40-2", "40-1", "39-2", "39-1", "38-1", "37-3", "37-2", "37-1", "36-2", "36-1", "35-2", "35-1"]:
            return True
        else:
            return False

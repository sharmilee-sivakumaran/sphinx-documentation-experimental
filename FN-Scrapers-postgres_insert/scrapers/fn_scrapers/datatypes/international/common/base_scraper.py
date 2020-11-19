from __future__ import absolute_import

import re
import sys
import time

import injector
import parsel
import six
from lxml import html

try:
    import simplejson as json
except ImportError:
    import json
import logging

from .utils import JSONEncoderPlus, get_default_http_headers
from .reporting_policy import ScraperReportingPolicy

from fn_rabbit.event_publisher import BlockingEventPublisher

from fn_service.server import RequestProcessId

from fn_scrapers.api.resources import (
    ScrapeStartTime,
    BlockingRetryingPublisherManager,
    BlockingRetryPolicy,
)
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.common.kraken import KrakenScraper

from fn_scraperutils.scraper import Scraper
from fn_scraperutils.events.reporting import EventType, logging_util
from fn_scraperutils.config import Config
from fn_scraperutils.doc_service.doc_service_client import DocServiceClient
from fn_scraperutils.doc_service.transfer_to_s3 import S3Transferer
from fn_scraperutils.request.blocking_client import BlockingClient

from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

logger = logging.getLogger(__name__)

class ScraperBase(KrakenScraper, Scraper):
    @injector.inject(
        process_id=RequestProcessId,
        publisher_manager=BlockingRetryingPublisherManager,
        blocking_client=BlockingClient,
        scrape_start_time=ScrapeStartTime,
        blocking_retry_policy=BlockingRetryPolicy,
        s3_transferer=S3Transferer,
        scraper_utils_config=Config,
        doc_service_client=DocServiceClient,
        metadata_client=LocalityMetadataDataAccess.Client,
        scrape_item_publisher=ScrapeItemPublisher)
    def __init__(
            self,
            scraper_type,
            scraper_name,
            scraper_source,
            process_id,
            publisher_manager,
            blocking_client,
            scrape_start_time,
            blocking_retry_policy,
            s3_transferer,
            scraper_utils_config,
            doc_service_client,
            metadata_client,
            scrape_item_publisher):
        super(ScraperBase, self).__init__(
            scraper_type=scraper_type,
            process_id=process_id,
            publisher=BlockingEventPublisher(scraper_type.name, publisher_manager),
            ratelimiter_client=blocking_client,
            scrape_start_time=scrape_start_time,
            retry_policy=blocking_retry_policy,
            s3_transferer=s3_transferer,
            config=scraper_utils_config,
            doc_service_client=doc_service_client,
            metadata_client=metadata_client)
        self.scraper_name = scraper_name
        self.scraper_source = scraper_source
        self.scraper_policy = ScraperReportingPolicy(scraper_type.name)
        self.session_start_time = scrape_start_time.isoformat()
        timestamp = time.mktime(scrape_start_time.timetuple())
        self.scrape_session_id = u"{}{}".format(self.scraper_name, int(timestamp))

        self.process_id = process_id

        self.send_create_process_event(u"{}{}".format(self.scraper_name, self.scraper_source))

        self.scrape_item_publisher = scrape_item_publisher

        self.url = None
        self._htmlparser = html.HTMLParser(recover=True, encoding='utf8', remove_blank_text=True, remove_comments=True)
        self._resp = None
        self._sel = None
        self._prev_resp = None
        self._prev_sel = None

    def set_url(self, url):
        self.url = url

    def get_url(self):
        return self.url

    def http_get(self, url, policy, encoding=None, request_args=None, retry_policy=None):
        try:
            if not request_args:
                request_args = dict()
            if isinstance(request_args, dict):
                if 'headers' not in request_args:
                    request_args['headers'] = get_default_http_headers()
                elif request_args['headers']:
                    if isinstance(request_args['headers'], dict):
                        headers = get_default_http_headers()
                        headers.update(request_args['headers'])
                        request_args['headers'] = headers
                    else:
                        raise ValueError("Header values should be of dict type")
            else:
                raise ValueError("Request arguments should be of dict type")

            self.set_url(url)
            logger.info("GET URL: " + str(url))
            logger.debug("Request args: " + str(request_args))
            self.resp = self.http_request(url, "GET", request_args, retry_policy)
            if encoding:
                self.resp.encoding = encoding
            self.set_selector()

            return self.get_content_from_response()
        except Exception as e:
            logger.error('Error occured on line {}. {}: {}'.format(sys.exc_info()[-1].tb_lineno, type(e), str(e)))
            logging_util(policy, EventType.request_error, str(e), url, self.process_data)
            return None

    def http_post(self, url, policy, encoding=None, request_args=None, retry_policy=None):
        try:
            if not request_args:
                request_args = dict()
            if isinstance(request_args, dict):
                if 'headers' not in request_args:
                    request_args['headers'] = get_default_http_headers()
                elif request_args['headers']:
                    if isinstance(request_args['headers'], dict):
                        headers = get_default_http_headers()
                        headers.update(request_args['headers'])
                        request_args['headers'] = headers
                    else:
                        raise ValueError("Header values should be of dict type")
            else:
                raise ValueError("Request arguments should be of dict type")

            self.set_url(url)
            logger.info("GET URL: " + str(url))
            logger.debug("Request args: " + str(request_args))
            self.resp = self.http_request(url, "POST", request_args, retry_policy)
            if encoding:
                self.resp.encoding = encoding
            self.set_selector()

            return self.get_content_from_response()
        except Exception as e:
            logger.error('Error occured on line {}. {}: {}'.format(sys.exc_info()[-1].tb_lineno, type(e), str(e)))
            logging_util(policy, EventType.request_error, str(e), url, self.process_data)
            return None

    def get_content_from_response(self, resp=None):
        body = u""
        try:
            if not resp:
                resp = self.resp
            body = resp.text
        except ValueError:
            body = resp.content
        return body

    def get_headers_from_response(self):
        return self.resp.headers

    def set_selector(self, text=None):
        if text is not None:
            if not isinstance(text, six.text_type):
                raise TypeError("text argument should be of type %s" % six.text_type)
        else:
            text = self.get_content_from_response()
            if not isinstance(text, six.text_type):
                raise TypeError("Response content should be of type %s" % six.text_type)

        text = text.strip() or '<html/>'  # empty body raises error in lxml

        RE_XML_ENCODING = re.compile(ur'<\?xml[^>]*?>', re.U|re.I)
        RE_HTML_ENCODING = re.compile(ur'<html[^>]*?>', re.U|re.I)
        headers = self.get_headers_from_response()
        if "Content-Type" in headers:
            content_type = headers.get("Content-Type", "")
            if 'xml' in content_type:
                logger.debug("XML content in web response")
                if RE_XML_ENCODING.search(text):
                    text = RE_XML_ENCODING.sub("", text, count=1)
            elif 'html' in content_type:
                '''
                Sometimes in web response we find \"xml\" tag along with "html" tag and response content type is \"text/html\". And if \"xml\" tag is there with \"utf-8\" encoding then lxml will raise encoding error.
                '''
                if RE_XML_ENCODING.search(text) and RE_HTML_ENCODING.search(text):
                    logger.debug("XML tag found in html web response")
                    text = RE_XML_ENCODING.sub("", text, count=1)

        try:
            root = html.fromstring(text, parser=self._htmlparser, base_url=self.url)
            self._sel = parsel.Selector(root=root)
        except Exception, e:
            raise ValueError('[Parse lxml ERR]', str(e))

    def get_selector(self):
        return self._sel

    def backup_selector(self):
        self._prev_sel = self._sel

    def restore_selector(self):
        self._sel = self._prev_sel

    def xpath(self, query, namespace=None, **kwargs):
        if query and isinstance(query, six.string_types):
            return self._sel.xpath(query, namespace=None, **kwargs)
        else:
            raise ValueError("Query parameter must be provided as string.")

    def xpath_single(self, query, namespace=None, **kwargs):
        if query and isinstance(query, six.string_types):
            elem = self._sel.xpath(query, namespace=None, **kwargs)
            if elem and len(elem) > 0:
                return elem[0]
        else:
            raise ValueError("Query parameter must be provided as string.")

    def extract(self, query, sel=None, namespace=None, **kwargs):
        """
        Extract all results of xpath query
        :param query: XPath query to retrieve html elements
        :param sel: Selector/List object
        :return:
        """
        if query and isinstance(query, six.string_types):
            elem = None
            if sel and isinstance(sel, (parsel.Selector, parsel.SelectorList)):
                elem = sel.xpath(query, namespace=namespace, **kwargs)
            elif sel:
                raise ValueError("Sel parameter must be of Selector type or SelectorList type")
            else:
                elem = self._sel.xpath(query, namespace=namespace, **kwargs)

            if elem and len(elem) > 0:
                return elem.extract()
        else:
            raise ValueError("Query parameter must be provided as string.")

    def extract_single(self, query, sel=None, namespace=None, **kwargs):
        """
        Extract single result of xpath query
        :param query: XPath query to retrieve html elements
        :param sel: Selector/List object
        :return:
        """

        if query and isinstance(query, six.string_types):
            elem = None
            if sel and isinstance(sel, (parsel.Selector, parsel.SelectorList)):
                return sel.xpath(query, namespace=namespace, **kwargs).extract_first()
            elif sel:
                raise ValueError("Sel parameter must be of Selector type or SelectorList type")
            else:
                return self._sel.xpath(query, namespace=namespace, **kwargs).extract_first()

        else:
            raise ValueError("Query parameter must be provided as string.")

    def extract_as_one(self, query, sel=None, namespace=None, **kwargs):
        """
        Extract all result of xpath query and concatenate
        them as one string text
        :param query: XPath query to retrieve html elements
        :param sel: Selector/List object
        :return:
        """
        if query and isinstance(query, six.string_types):
            elem = None
            if sel and isinstance(sel, (parsel.Selector, parsel.SelectorList)):
                elem_text_arr = sel.xpath(query, namespace=namespace, **kwargs).extract()
            elif sel:
                raise ValueError("Sel parameter must be of Selector type or SelectorList type")
            else:
                elem_text_arr = self._sel.xpath(query, namespace=namespace, **kwargs).extract()

            if elem_text_arr and len(elem_text_arr) > 0:
                text = u' '.join(elem_text_arr)
                text = re.sub(r'\s+', ' ', text, re.U)
                return text.strip()
        else:
            raise ValueError("Query parameter must be provided as string.")

    def html_form_parser(self, search_type=None, form_name=None, form_id=None):
        """
        :param search_type: name or id or both
        :param form_name: form name value
        :param form_id: form id value
        :type search_type: str
        :type form_name: str
        :type form_id: str
        :return: input_fields_dict, method, action
        :rtype: Tuple of input_fields_dict, method, action
        """

        def _handle_input_text(field):
            """text, hidden, password"""
            field_value = field.xpath('@value').extract_first()
            if not field_value:
                field_value = ''
            else:
                if isinstance(field_value, (bytes, six.text_type)):
                    field_value = field_value.strip()
            return field_value

        def _handle_input_radio_checkbox(field):
            """Radio and checkbox checked"""
            field_checked = field.xpath('@checked').extract_first()
            if field_checked:
                field_value = field.xpath('@value').extract_first()

                if not field_value:
                    return 'on'
                else:
                    if isinstance(field_value, (bytes, six.text_type)):
                        field_value = field_value.strip()
                        if not field_value.isspace():
                            return field_value
                        else:
                            return "on"
                    else:
                        return "on"
            else:
                return None

        def _handle_select(field):
            selected = field.xpath("./option[@selected]/@value").extract_first()
            if not selected:
                selected = field.xpath("./option[position()=1]/@value").extract_first()
            if selected:
                return selected
            else:
                selected = ''
                return selected

        def _handle_textarea(field):
            textarea = field.xpath("text()").extract_first()
            if textarea:
                textarea = textarea.strip()
            else:
                textarea = ''
            return textarea

        form = ''
        input_fields_dict = {}
        action = None
        method = 'get'
        if search_type:
            if search_type.lower() == 'name':
                if form_name:
                    form = self.xpath_single('//form[@name=$form_name]', form_name=form_name)
                else:
                    raise ValueError("Form name not given.")
            elif search_type.lower() == 'id':
                if form_id:
                    form = self.xpath_single('//form[@id=$form_id]', form_id=form_id)
                else:
                    raise ValueError("Form id not given.")
            elif search_type.lower() == 'both':
                if form_name and form_id:
                    form = self.xpath_single('//form[@id=$form_id and @name=$form_name]', form_id=form_id,
                                             form_name=form_name)
                else:
                    raise ValueError("Form name and id are not given.")
        else:
            form = self.xpath_single('//form')

        if isinstance(form, parsel.Selector):

            action = form.xpath('@action').extract_first()
            method = form.xpath('@method').extract_first()

            # We extract inputs of following types.
            form_elements = form.xpath('.//input|.//select|.//textarea')
            for form_element in form_elements:

                form_element_tag_name = form_element.xpath('name(.)').extract_first()
                input_field_name = form_element.xpath('@name').extract_first()
                if not input_field_name:
                    continue
                input_field_type = form_element.xpath('@type').extract_first()
                disabled = form_element.xpath('@disabled').extract_first()

                if disabled:
                    continue
                input_text_type = ('text', 'hidden', 'password')
                input_choice_type = ('radio', 'checkbox')
                if form_element_tag_name == 'input' and input_field_type in input_text_type:
                    input_field_value = _handle_input_text(form_element)

                    if input_field_value:
                        input_fields_dict[input_field_name] = input_field_value
                    else:
                        input_fields_dict[input_field_name] = ''
                elif form_element_tag_name == 'input' and input_field_type in input_choice_type:
                    input_field_value = _handle_input_radio_checkbox(form_element)
                    if input_field_value and input_field_type == 'radio':
                        input_fields_dict[input_field_name] = input_field_value
                    elif input_field_value and input_field_type == 'checkbox':
                        input_fields_dict.setdefault(input_field_name, []).append(input_field_value)
                elif form_element_tag_name == 'select':
                    input_field_value = _handle_select(form_element)
                    input_fields_dict[input_field_name] = input_field_value
                elif form_element_tag_name == 'textarea':
                    input_field_value = _handle_textarea(form_element)
                    input_fields_dict[input_field_name] = input_field_value

            return input_fields_dict, method, action
        else:
            raise AttributeError("Form not found.")

    def scrape(self):
        """
        Get all the bills IDs to scrape for a given session.

        :rtype: List of strings
        """
        raise NotImplementedError(u'Scraper class must define a scrape method')

    def validate_doc(self, doc):
        try:
            doc.validate()
            return True
        except Exception as e:
            logger.debug(str(e))
            return False

    def json_dumps(self, message):
        return json.dumps(message, cls=JSONEncoderPlus, ensure_ascii=False, encoding='utf-8', for_json=True)

    def save_doc(self, doc):
        self.scrape_item_publisher.publish_json_item(
            "",
            self.scraper_name,
            self.scraper_source,
            doc,
            json_encoder=JSONEncoderPlus)

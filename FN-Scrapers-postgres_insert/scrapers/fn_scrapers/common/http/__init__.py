from __future__ import absolute_import

import logging
from tempfile import TemporaryFile
from urlparse import urlparse, urljoin

import certifi
import requests
from lxml import etree, html
from lxml.html import html5parser as html5

from fn_ratelimiter_client.blocking_util import (
    StandardRequestsRetryPolicy, Retry500RequestsRetryPolicy,
    STANDARD_REQUESTS_RETRY_POLICY, RETRY500_REQUESTS_RETRY_POLICY,
    standard_retry)
from fn_ratelimiter_common.const import CLIENT_DEFAULT_IDLE_TIMEOUT

from .session import Session

logger = logging.getLogger(__name__)

class HttpException(Exception):
    '''Generic HTTP Exception.'''
    def __init__(self, message, exception=None, url=None):
        super(HttpException, self).__init__(message)
        self.exception = exception
        self.url = url

def delay_for_host(url, quantity=1, http_session=None):
    '''
    Block until cleared by ratelimiter.

    Args:
        url: URL or name to check for. URL is transformed to name if "/" is in
            string.
        quantity: Optional request quantity for bulk delays.
        http_session: Optional http.Session object
    Returns:
        fn_ratelimiter_client.response_cache.RateLimiterResponse
    Raises:
        HostBlockedError: If the host should not be contacted at all.
    '''
    http_session = http_session or Session.get()
    host = urlparse(url).netloc if '/' in url else url
    return http_session.client.delay_for_host(host, quantity)


def request(url, http_session=None, **kwargs):
    '''
    Perform an HTTP Request, returning a requests.Response object.

    Args:
        url: URL to request.
        rl_client: Optional rate limiter client.
        req_session: Optional requests.Session instance.
        retry_policy: Optional retry policy
        kwargs: All unrecognized keyword arguments are sent to requests.request.
    Returns:
        requests.Response: Response object of the object being requested.
            http://docs.python-requests.org/en/master/api/#requests.Response
    '''
    logger.debug('Requesting url: %s', url)
    http_session = http_session or Session.get()

    method = kwargs.pop('method', 'GET')

    delay_for_host(url, http_session=http_session)

    if 'timeout' not in kwargs:
        kwargs['timeout'] = CLIENT_DEFAULT_IDLE_TIMEOUT
    if 'verify' not in kwargs:
        kwargs['verify'] = certifi.old_where()
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    if 'User-Agent' not in kwargs['headers']:
        kwargs['headers']['User-Agent'] = http_session.user_agent
    retry_policy = kwargs.get('retry_policy', http_session.retry_policy)

    def _try():
        response = http_session.req_session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    try:
        return standard_retry(_try, retry_policy)
    except requests.exceptions.HTTPError as exc:
        raise HttpException(str(exc), exception=exc, url=url)

def request_file(url, file_obj=None, http_session=None, **kwargs):
    '''
    Request a file via requests library.

    Args:
        url: URL to request.
        file_obj: Optional file pointer to write to.
        rl_client: Optional rate limiter client.
        req_session: Optional requests.Session instance.
        retry_policy: Optional retry policy
        kwargs: All unrecognized keyword arguments are sent to requests.request.
    Returns:
        two-tuple: (File object, requests.Response object)

    '''
    http_session = http_session or Session.get()

    chunk_size = kwargs.pop('chunk_size', 1024)
    decode_unicode = kwargs.pop('decode_unicode', False)
    file_obj = file_obj or TemporaryFile()

    if 'stream' not in kwargs:
        kwargs['stream'] = True
    response = request(url, http_session=http_session, **kwargs)

    byte_size = 0
    for chunk in response.iter_content(chunk_size, decode_unicode):
        if chunk:
            byte_size += len(chunk)
            if byte_size > http_session.max_size:
                try:
                    file_obj.close()
                except IOError:
                    pass
                raise HttpException('Max file size exceeded.')
        file_obj.write(chunk)
    logger.debug("Loaded {} bytes".format(byte_size))
    file_obj.seek(0)
    return file_obj, response

def request_json(url, **kwargs):
    '''
    Performs an http request and returns a json object.

    Args:
        url: URL to request.
        kwargs: Any unrecognized arguments are passed to http.request or
            requests.request.
    '''
    return request(url, **kwargs).json()

def request_text(url, encoding=None, **kwargs):
    '''
    Performs an http request and returns a json object.

    Args:
        url: URL to request.
        encoding: Optional, encoding to decode bytes to string with.
        kwargs: Any unrecognized arguments are passed to http.request or
            requests.request.
    Returns:
        string
    '''
    if 'text' in kwargs:
        return kwargs['text']
    res = request(url, **kwargs)
    if encoding:
        res.encoding = encoding
    return res.text

def _get_content(url, encoding, **kwargs):
    ''' Returns the content of the request, optionally applying the encoding '''
    if 'content' in kwargs:
        return kwargs['content']
    res = request(url, **kwargs)
    if encoding:
        return res.content.decode(encoding)
    return res.content

def request_xml(url, encoding=None, **kwargs):
    '''
    Performs an http request and returns a json object.

    Args:
        url: URL to request.
        encoding: Optional, encoding to decode bytes to string with.
        kwargs: Any unrecognized arguments are passed to http.request or
            requests.request.
    Returns:
        lxml.etree object
    Raises:
        etree.XMLSyntaxError
    '''
    content = _get_content(url, encoding, **kwargs)
    return etree.fromstring(content, base_url=url)

def request_lxml_html(url, rep_nbsp=False, abs_links=False, encoding=None,
                      **kwargs):
    '''
    Performs an http request and returns an lxml.html object.

    Args:
        url: URL to request.
        rep_nbsp: Optional, replaces nbsp entities with spaces if True.
        encoding: Optional, encoding to decode bytes to string with.
        kwargs: Any unrecognized arguments are passed to http.request or
            requests.request.
    Returns:
        lxml.etree object
    Raises:
        etree.XMLSyntaxError
    '''
    content = _get_content(url, encoding, **kwargs)
    if rep_nbsp:
        content = content.replace("&nbsp;", " ")
    htm = html.fromstring(content, base_url=url)
    if abs_links:
        htm.make_links_absolute(url)
    return htm

def request_html5(url, rep_nbsp=False, abs_links=False, encoding=None, **kwargs):
    '''
    Performs an http request and returns an lxml.html object with the html5
    parser.

    Args:
        url: URL to request.
        rep_nbsp: Optional, replaces nbsp entities with spaces if True.
        abs_links: Optional, change links to absolute urls.
        encoding: Optional, encoding to decode bytes to string with.
        kwargs: Any unrecognized arguments are passed to http.request or
            requests.request.
    Returns:
        lxml.etree object
    Raises:
        etree.XMLSyntaxError
    '''
    content = _get_content(url, encoding, **kwargs)
    if rep_nbsp:
        content = content.replace("&nbsp;", " ")
    parser = html5.HTMLParser(namespaceHTMLElements=False)
    htm = html5.document_fromstring(content, parser=parser)
    # html5 seems to mention the base_url in their docs but it only returns
    # an _Element object, not an etree object, so base_url is undefined.
    #
    # > 'base_url' will set the document's base_url attribute (and the tree's
    # > docinfo.URL)
    #
    # However base is defined. (?) 
    htm.base = url

    if abs_links:
        for link in htm.xpath('//a'):
            if link.get('href'):
                link.set('href', urljoin(url, link.get('href')))

    return htm


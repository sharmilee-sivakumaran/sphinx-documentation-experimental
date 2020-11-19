"""
High-level DocService/S3 library for scraper use.
"""

from __future__ import absolute_import

import hashlib
import logging
import re
from tempfile import TemporaryFile
from urlparse import urlparse

import boto
from dateutil.parser import parse as du_parse

from fn_ratelimiter_client.blocking_util import (
    StandardRequestsRetryPolicy, Retry500RequestsRetryPolicy,
    STANDARD_REQUESTS_RETRY_POLICY, RETRY500_REQUESTS_RETRY_POLICY)


from fn_scrapers.common import http

from .exceptions import FilesException, RemoteExtractionException, S3Exception
from .file import File
from .session import Session
from .extraction import Extractors as extractors


logger = logging.getLogger(__name__)

# matches both bucket.s3.com/key and s3.com/bucket/key
_S3_URL_PATTERN = re.compile(
    r'[a-z]+://(?:s3\.amazonaws\.com/|)?(?P<bucket>[^./]+)'
    r'(?:\.s3\.amazonaws\.com)?/(?P<key>.+)$'
)

def request_file_with_cache(url, request_args=None, skip_checks=None,
                            file_obj=None, session=None):
    '''
    Returns a files.File object, either downloaded from the internet or
    retrieved from S3. Implements automatic HTTP cacheing.

    Args:
        request_args: Optional dictionary of arguments to pass to
            requests.request.
        skip_checks: Skips conditional headers. Defaults False.
        file_obj: Optional file object, temporary file created if none given.
        session: Optional HttpSession instance
    Returns:
        files.File object.
    Raises:
        requests.HTTPError: On HTTP related problems (404, 502, etc).
        HttpException: On file size exceeded.
    '''
    logger.debug('Requesting url to file: %s', url)
    session = session or Session.get()
    if skip_checks is None:
        skip_checks = session.skip_checks
    request_args = request_args or {}
    file_obj = file_obj or _get_temp_file()

    ldi = session.docserv_client.last_download_info(url)

    request_args['headers'] = request_args.get('headers', {})
    headers = request_args['headers']
    if ldi.headers and not skip_checks:
        etag = headers.get('If-None-Match', ldi.headers.get('ETag'))
        if etag:
            headers['If-None-Match'] = etag
        last_mod = headers.get('If-Modified-Since', ldi.headers.get('Last-Modified'))
        if last_mod:
            headers['If-Modified-Since'] = last_mod

    should_download = True
    if not skip_checks and session.start_time and ldi.datetime:
        should_download = du_parse(ldi.datetime) < session.start_time

    if should_download:
        fil = request_file(url, file_obj=file_obj, session=session,
                                **request_args)
        fil.ldi = ldi
        if fil.source.status_code != 304:
            return fil

    if not ldi.s3Url:
        ValueError('Did not receive last-download-info: ' + url)
    download_from_s3(ldi.s3Url, file_obj, session=session)

    return File(url, file_obj, is_cached=True, ldi=ldi, download_id=ldi.id, 
                s3_url=ldi.s3Url, headers=ldi.headers)


def request_file(url, file_obj=None, session=None, **kwargs):
    '''
    Returns an files.File object, writing the file to the file_obj property.

    Args:
        file_obj: Optional file object, temporary file created if none given.
        session: Optional HttpSession instance
        kwargs: All unrecognized keyword arguments are sent to requests.request.
    Returns:
        files.File object.
    Raises:
        requests.HTTPError: On HTTP related problems (404, 502, etc).
        HttpException: On file size exceeded.
    '''
    session = session or Session.get()
    file_obj = file_obj or _get_temp_file()
    file_obj, res = http.request_file(
        url, file_obj=file_obj, http_session=session.http_session, **kwargs)

    logger.debug("Received HTTP %s Response", res.status_code)
    res.raise_for_status()
    return File(url, file_obj, source=res)


def download_from_s3(endpoint, file_obj=None, session=None):
    '''Download an s3 URL to a given file.

    Args:
        endpoint: key name in s3. NOTE - Must be complete: "file-by-sha384/..."
        temp_file: file-like object to write to.
        session: Optional HttpSession instance.
    Returns:
        File
    '''
    session = session or Session.get()
    file_obj = file_obj or _get_temp_file()
    if 's3.amazonaws.com' in endpoint:
        match = _S3_URL_PATTERN.match(endpoint)
        assert match
        assert match.group('bucket') == session.aws.bucket.name
        endpoint = match.group('key')
    key = boto.s3.key.Key(session.aws.bucket, endpoint)
    logger.info("Downloading from s3: %s", session.aws.generate_s3_url(endpoint))
    if not key.exists():
        raise S3Exception('Key {} does not exist'.format(endpoint), 404)
    key.get_contents_to_file(file_obj)
    return file_obj

def register_download(file_hash, s3_url, original_url, external_filename,
                      mime_type, encoding, response_headers, serve_from_s3=None,
                      session=None):
    '''
    Legacy function. Create a Document Service download record.
    '''
    session = session or Session.get()
    if serve_from_s3 is None:
        serve_from_s3 = session.serve_from_s3
        if serve_from_s3 is None:
            serve_from_s3 = mime_type != 'text/html'

    download_id = session.docserv_client.register_download(
        file_hash, s3_url, serve_from_s3, original_url,
        external_filename, mime_type, encoding, response_headers)
    logger.info("Registered at doc service. Download ID: %s", download_id)
    return download_id


def _get_temp_file(*args, **kwargs):
    '''
    Temporary file generator function.
    '''
    return TemporaryFile(*args, **kwargs)


def download_and_register(url, request_args=None, s3_args=None, skip_checks=None,
                          mimetype=None, encoding=None, filename=None, name=None,
                          serve_from_s3=None, session=None):
    '''
    Download a file, upload it to s3, and register it with document service.

    Args:
        url: URL to request.
        request_args: Optional dictionary of arguments to pass to
            requests.request.
        s3_args: Optional S3 headers (content-type/content-disposition)
        skip_checks: Skips conditional headers. Defaults False.
        mimetype: Optional. Override download determined mimetype.
        encoding: Optional. Override download determined encoding.
        filename: Optional. Override download determined filename.
        name: Optional. Override download determined name.
        serve_from_s3: Optional. If True S3 is present to the user, if False
            the source URL is used. Defaults to False for html content and True
            for everything else.
        session: Optional HttpSession instance.
    Returns:
        files.File object
    '''
    fil = request_file_with_cache(
        url, request_args, skip_checks=skip_checks, session=session)
    if mimetype:
        fil.mimetype = mimetype
    if encoding:
        fil.encoding = encoding
    if filename:
        fil.filename = filename
    if name:
        fil.name = name
    fil.upload_and_register(
        s3_args=s3_args, serve_from_s3=serve_from_s3, session=session)
    return fil


def register_download_and_documents(
        url, extractor, serve_from_s3=None, skip_checks=False, mimetype=None,
        encoding=None, filename=None, name=None, parser=None, request_args=None,
        s3_args=None, extract_args=None, session=None):
    '''
    The ultimate shortcut function: download a file, upload it to s3, register
    it with document service, extract content, and register documents.

    Args:
        url: URL to request.
        extractor: Extractor object (see files.extractors)
        serve_from_s3: Optional. If True S3 is present to the user, if False
            the source URL is used. Defaults to False for html content and True
            for everything else.
        skip_checks: Skips conditional headers. Defaults False.
        mimetype: Optional. Override download determined mimetype.
        encoding: Optional. Override download determined encoding.
        filename: Optional. Override download determined filename.
        name: Optional. Override download determined name
        parser: (Deprecated, Optional) A callback for a custom parser.
        request_args: Optional dictionary of arguments to pass to
            requests.request.
        s3_args: Optional S3 headers (content-type/content-disposition)
        extract_args: Specific optional arguments for the extractor.
        session: Optional files.Session instance.
    Returns:
        Three-tuple: (files.File object, list of ScraperDocument objects,
            list of document ids) where a document id can be an integer or None.
    '''

    fil = download_and_register(
        url, request_args=request_args, s3_args=s3_args, skip_checks=skip_checks,
        mimetype=mimetype, encoding=encoding, filename=filename, name=name,
        serve_from_s3=serve_from_s3, session=session)
    fil.extract_and_register_documents(
        extractor, extract_args=extract_args, parser=parser, session=session)
    return fil


def close():
    '''
    Closes the current session.
    '''
    session = Session.get()
    session.close()

'''
common.files.file

Contains the File class - designed to handle the download/upload/register
document lifecycle.
'''

import cgi
import hashlib
import logging
import mimetypes
import os
import re
from urlparse import urlparse

import boto
from rfc6266 import build_header as rfc6266_header
from requests.structures import CaseInsensitiveDict
import six
from thrift.Thrift import TApplicationException

try:
    import magic # github ahupp/python-magic
except ImportError:
    logging.warning("python-magic is not installed.")
    magic = False


from fn_document_service.blocking import ttypes
from fn_scrapers.common import http

from .session import Session
from .extraction import Extractors as extractors, ScraperDocument
from .exceptions import RemoteExtractionException

logger = logging.getLogger(__name__)

EXTENSION_NORMS = {
    '.jpe': '.jpg',
    '.jpeg': 'jpg'
}

MIMETYPE_TO_EXT = [
    (re.compile(r'^.*/yaml$'), '.yaml'),
    (re.compile(r'^.*/json$'), '.json'),
]

class File(object):
    '''
    Tracks the state of the doc service request through the pipeline.
    '''
    def __init__(self, url, file_obj, **kwargs):
        '''
        Constructor.

        Args:
            url: external url.
            file_obj: file pointer.
            is_cached: Optional, defaut False. Set to true if last download
                info shows we have a current copy.
            headers: Optional, HTTP headers provided explicitly. See also: source.
            source: Optional, The source object (requests.Response typically).
                If source is provided without headers, source.headers is checked
                and saved to headers.
            hashvalue: Optional, Hash value of file. Use hash() to access.
            filename: Optional, Filename to use in S3 headers.
            mimetype: Optional, Mimetype to use in S3 headers.
            encoding: Optional, Encoding to use in S3 headers.
            name: Optional, Used to determine filename along with the mimetype.
        '''
        self.url = url
        self.file_obj = file_obj

        self.is_cached = kwargs.pop('is_cached', False)
        self.headers = kwargs.pop('headers', {})
        self.ldi = kwargs.pop('ldi', None) # last download info
        self.source = kwargs.pop('source', None)

        self._bytes = None

        self.hashvalue = kwargs.pop('hashvalue', None)
        self.filename = kwargs.pop('filename', None)
        self.mimetype = kwargs.pop('mimetype', None)
        self.encoding = kwargs.pop('encoding', None)
        self.name = kwargs.pop('name', None) # name = filename - extension

        # document service tracking
        self.s3_url = kwargs.pop('s3_url', None)
        self.serve_from_s3 = kwargs.pop('serve_from_s3', None)
        self.download_id = kwargs.pop('download_id', None)
        self.documents = kwargs.pop('documents', None)
        self.document_ids = kwargs.pop('document_ids', None)
        self.s3_endpoint = kwargs.pop('s3_endpoint', None)

        headers_decoding = kwargs.pop('headers_decoding', None)
        if hasattr(self.source, 'headers') and not self.headers:
            self.headers = self.source.headers

        if headers_decoding:
            self.headers.update(
                    (k.decode(headers_decoding), v.decode(headers_decoding))
                    for k, v in self.headers.items())

        if kwargs:
            raise ValueError("Unrecognized keyword arguments: [{}]".format(
                ', '.join(kwargs.keys())
            ))

        self.update()

    @classmethod
    def from_local(cls, file_obj, filename=None, mimetype=None,
                   encoding=None, serve_from_s3=None, name=None):
        '''
        Create and return a files.File object using the s3 url as the external
        url. This is intended for cases where the scraper either generates the
        resource locally or receives the content through a method that does not
        support GET-based url retrieval (ftp, POST, etc).

        Args:
            file_obj: file pointer.
            filename: Optional, Filename to use in S3 headers.
            mimetype: Optional, Mimetype to use in S3 headers.
            encoding: Optional, Encoding to use in S3 headers.
            name: Optional, Used to determine filename along with the mimetype.
        '''
        fil = cls(
            None, file_obj, filename=filename, mimetype=mimetype,
            encoding=encoding, serve_from_s3=serve_from_s3, name=name)
        fil.url = fil.get_s3_url()
        return fil

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        '''
        Closes the file object.
        '''
        self.file_obj.close()

    def hash(self, chunk_size=1024):
        '''
        Perform a SHA384 hash against the file.

        Args:
            chunk_size: Optional chunk size to hash.
        Returns:
            hexdigest string.
        '''
        if self.hashvalue:
            return self.hashvalue
        if not self.file_obj:
            raise ValueError("No file available for hashing.")
        self.file_obj.seek(0)
        hasher = hashlib.sha384()
        while True:
            data = self.file_obj.read(chunk_size)
            if not data:
                break
            hasher.update(data)
        self.hashvalue = hasher.hexdigest()
        logger.info("File hashed: %s", self.hashvalue)
        return self.hashvalue

    def get_s3_url(self, session=None, endpoint=None):
        '''
        Generates a complete s3 url.
        '''
        session = session or Session.get()
        return session.aws.generate_s3_url(endpoint or self.get_s3_endpoint())

    def get_s3_endpoint(self):
        '''
        Generates a url endpoint (s3 key name, or url part without domain and
        bucket name) and returns it.
        '''
        return "file-by-sha384/" + self.hash()

    def update(self):
        '''
        Attempt to update filename, mimetype, etc data based on available
        information. Safe to run repeatedly.
        '''
        content_type_hdr = 'Content-Type'
        content_dis_hdr = 'Content-Disposition'
        # pull mimetype/filename from headers, if available
        if content_type_hdr in self.headers:
            params = cgi.parse_header(self.headers[content_type_hdr])
            if not self.mimetype:
                self.mimetype = params[0]
            if not self.encoding:
                self.encoding = params[1].get('charset')
            logger.debug('Determined mimetype from header: %s', self.mimetype)
        if not self.filename and content_dis_hdr in self.headers:
            _, params = cgi.parse_header(self.headers[content_dis_hdr])
            self.filename = params.get('filename')
            logger.debug('Determined filename from header: %s', self.filename)

        # set what we can where we can
        if not self.filename and self.url:
            self.name = os.path.basename(urlparse(self.url).path)
            if not self.mimetype and self.name and '.' in self.name:
                # if we think we have a filename from the url and nothing else,
                # use that.
                self.filename = self.name
                logger.debug('Determined filename from url: %s', self.filename)
        if not self.mimetype and self.filename:
            self.mimetype = mimetypes.guess_type(self.filename)[0]
            logger.debug('Determined mimetype from filename: %s', self.mimetype)
        if not self.mimetype and magic:
            # TODO: determine libmagic failure modes (indeterminate files, etc)
            self.file_obj.seek(0)
            self.mimetype = magic.from_buffer(self.file_obj.read(1024), mime=True)
            logger.debug('Determined mimetype from magic: %s', self.mimetype)
        if not self.filename and self.mimetype and self.name:
            # TODO: double check zip false-positives (may be docx/xslx/pptx?)
            ext = self.guess_extension(self.mimetype)
            ext = EXTENSION_NORMS.get(ext, ext)
            is_jpeg = ext == '.jpg' and self.name.endswith(('.jpg','.jpeg'))
            if not self.name.endswith(ext) and not is_jpeg:
                self.filename = self.name + ext
            else:
                self.filename = self.name
            logger.debug('Determined filename from mimetype: %s', self.filename)

        if self.serve_from_s3 is None:
            # don't server from s3 if:
            #   1) we've been told not to,
            #   2) we don't know the mimetype, or
            #   3) the mimetype is text/html
            self.serve_from_s3 = self.mimetype and self.mimetype != 'text/html'

    @classmethod
    def guess_extension(cls, mimetype):
        '''
        Return an extension, resorting to mimetypes on failure.
        '''
        if mimetype.endswith('/json'):
            return '.json'
        if mimetype.endswith('/yaml'):
            return '.yaml'
        ext = mimetypes.guess_extension(mimetype)
        if ext == '.ksh':
            ext = '.txt'
        if not ext:
            logger.critical("Unable to determine extension for %s", mimetype)
        return ext

    def add_document(self, document):
        '''
        Appends a document to the internal document collection.
        '''
        if self.documents is None:
            self.documents = []
        self.documents.append(document)

    def upload_and_register(self, s3_args=None, serve_from_s3=None,
                            session=None):
        '''
        Given an files.File object, upload it to s3 and register it with document
        service.

        Args:
            s3_args: Optional S3 headers (content-type/content-disposition)
            serve_from_s3: Optional. If True S3 is present to the user, if False
                the source URL is used. Defaults to False for html content and True
                for everything else.
            session: Optional HttpSession instance.
        Returns:
            Download ID (int)
        '''
        session = session or Session.get()
        if self.is_cached:
            logger.debug("Response is cached, skipping upload")
            return self.ldi.id
        self.upload_to_s3(headers=s3_args, session=session)
        return self.create_download(serve_from_s3=serve_from_s3, session=session)


    def upload_to_s3(self, endpoint=None, headers=None, filename=None,
                    name=None, mimetype=None, session=None):
        '''
        Upload a file-like object to s3.

        Args:
            endpoint: Optional s3 filename, "file-by-sha384/{hash}" if not set.
            headers: Optional http headers to include.
            filename: Optional filename for a downloaded file.
            name: Base name of filename. Will use part of hash if not provided.
            mimetype: Optional mimetype of source. If used, tries to determine
                mime-type of file by filename. Not used if 'Content-Disposiiton'
                header is set or filename is both set and successfully used.
        Returns:
            S3 URL
        '''
        content_type_hdr = 'Content-Type'
        content_dis_hdr = 'Content-Disposition'
        session = session or Session.get()
        if not endpoint:
            # TODO: Move "file-by-sha384/" to HttpSession
            endpoint = self.get_s3_endpoint()
        logger.debug("Uploading to %s", endpoint)
        if mimetype == 'application/octet-stream':
            mimetype = None

        if not mimetype:
            mimetype = self.mimetype
        if not filename:
            if name and mimetype:
                filename = name + self.guess_extension(mimetype)
            elif self.filename:
                filename = self.filename
            elif mimetype:
                name = endpoint.split('/')[-1][:8]
                filename = name + self.guess_extension(mimetype)

        logger.debug("Filename: %s, Mimetype: %s", filename, mimetype)

        if not headers:
            headers = {}

        # set headers and upload
        if content_dis_hdr not in headers and filename:
            headers[content_dis_hdr] = rfc6266_header(filename, disposition='inline')
        if content_type_hdr not in headers and mimetype:
            headers[content_type_hdr] = mimetype
        s3_url = self.get_s3_url(session, endpoint)
        logger.info("Uploading file to s3 \"%s\" (%s) %s", filename, mimetype, s3_url)
        logger.debug("Headers: %s", headers)

        key = boto.s3.key.Key(session.aws.bucket, endpoint)
        self.file_obj.seek(0)
        key.set_contents_from_file(self.file_obj, headers=headers)
        key.set_acl('public-read')
        key.generate_url(expires_in=0, query_auth=False)
        logger.debug("Upload complete.")

        self.s3_url = s3_url
        if not self.filename:
            self.filename = filename
        return s3_url


    def create_download(self, serve_from_s3=None, session=None):
        '''
        A shortcut method to register a download efficiently.

        Args:
            session: Optional HttpSession instance.
            kwargs: optional keyword arguments for register_downlaod
        Returns:
            download id
        '''
        session = session or Session.get()
        # serve_from_s3 set by 1) argument, 2) session, 3) file
        if serve_from_s3 is None:
            serve_from_s3 = session.serve_from_s3
            if serve_from_s3 is None:
                serve_from_s3 = self.serve_from_s3

        assert self.url, "External URL not provided"
        assert self.s3_url, "S3 URL not provided"

        download_id = session.docserv_client.register_download(
            self.hash(),
            self.s3_url,
            serve_from_s3,
            self.url,
            self.filename,
            self.mimetype,
            self.encoding,
            self.headers
        )
        self.download_id = download_id
        logger.info("Registered at doc service. Download ID: %s", download_id)
        return download_id

    def extract_and_register_documents(self, extractor, extract_args=None,
                                       parser=None, session=None):
        '''
        Runs an extractor, parses the output, and returns a list of ScraperDocument
        objects.

        Args:
            extractor: Extractor object (see files.extractors)
            extract_args: Specific optional arguments for the extractor.
            parser: (Deprecated, Optional) A callback for a custom parser.
            session: Optional HttpSession instance.
        Returns:
            Two-tuple: (list of ScraperDocument objects, list of document ids) where
                a document id can be an integer or None.
        '''
        documents = self.extract_and_parse(
            extractor, extract_args=extract_args, parser=parser,
            session=session)
        return self.register_documents(documents, session=session)

    def extract_and_parse(self, extractor, parser=None,
                          extract_args=None, session=None):
        '''
        Runs an extractor, parses the output, and returns a list of ScraperDocument
        objects.

        Args:
            extractor: Extractor object (see files.extractors)
            parser: (Deprecated, Optional) A callback for a custom parser.
            extract_args: Specific optional arguments for the extractor.
            session: Optional files.Session instance.
        Returns:
            List of ScraperDocument objects
        '''
        extractor = extractors.get(extractor)
        parser = parser or extractor.parse
        extracted_content = self.extract(
            extractor, extract_args=extract_args, session=session)
        self.documents = parser(extracted_content)
        return self.documents

    def extract(self, extractor, extract_args=None, session=None):
        '''
        Runs an extractor and returns the raw content (entities list, etree, etc)

        Args:
            extractor: Extractor object (see files.extractors)
            extract_args: Specific optional arguments for the extractor.
            session: Optional HttpSession instance.
        Returns:
            Extractor specific, list of entities for remote extractors.
        '''
        session = session or Session.get()
        extractor = extractors.get(extractor)

        extract_args = extract_args or {}
        if self.encoding and 'encoding' not in extract_args:
            extract_args['encoding'] = self.encoding

        if extractor.is_remote and session.dev_mode:
            logger.warning(" %s DEV MODE %s ", *([' == '*10]*2))
            return [ttypes.HeaderEntity(text=(
                u"Scraper was run in development mode, and so we skipped "
                u"remote extraction."))]

        return extractor.extract(extractor.name, self, **extract_args)

    def register_documents(self, scraper_docs, session=None):
        '''
        Runs an extractor, parses the output, and returns a list of ScraperDocument
        objects.

        Args:
            scraper_docs: List of scraper documents or strings.
            session: Optional HttpSession instance.
        Returns:
            Two-tuple: (list of ScraperDocument objects, list of document ids) where
                a document id can be an integer or None.
        '''
        session = session or Session.get()
        docserv_docs = []
        skipped = []
        doc_ids = []
        assert self.download_id
        for i, doc in enumerate(scraper_docs):
            if isinstance(doc, six.string_types):
                doc = ScraperDocument(doc)
                scraper_docs[i] = doc
            if not doc.text:
                skipped.append(i)
                continue
            hashed_id = None
            if doc.scraper_id:
                hashed_id = hashlib.md5(doc.scraper_id.encode('utf-8')).hexdigest()
            docserv_docs.append(ttypes.Document(doc.text, hashed_id, doc.page_num))
        if docserv_docs:
            try:
                doc_ids = session.docserv_client.register_documents(
                    self.download_id, docserv_docs)
            except TApplicationException as exc:
                raise RemoteExtractionException(
                    str(exc), exception=exc, fil=self)

        # previous version (ScraperUtils.scraper.extract_and_register_documents)
        # would reorder documents, putting skipped documents at the end of the
        # list.
        for i in skipped:
            doc_ids.insert(i, None)
        self.document_ids = doc_ids
        return scraper_docs, doc_ids

    @property
    def content(self):
        '''
        Return the bytes from the file object.
        '''
        if self._bytes is not None:
            return self._bytes
        self.file_obj.seek(0)
        self._bytes = self.file_obj.read()
        self.file_obj.seek(0)
        return self._bytes

    def get_xml(self):
        '''
        Parse the file as an lxml xml object (shortcut to html.request_xml).

        NOTE: This method is only considered partially complete in that it may
        introduce encoding issues.
        '''
        return http.request_xml(url=self.url, content=self.content)

    def get_lxml_html(self, **kwargs):
        '''
        Parse the file as an lxml html object (shortcut to html.request_lxml_html).

        NOTE: This method is only considered partially complete in that it may
        introduce encoding issues.
        '''
        return http.request_lxml_html(url=self.url, content=self.content, **kwargs)

    def get_html5(self, **kwargs):
        '''
        Parse the file as an html5 object (shortcut to html.request_html5).

        NOTE: This method is only considered partially complete in that it may
        introduce encoding issues.
        '''
        return http.request_html5(url=self.url, content=self.content, **kwargs)

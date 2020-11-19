'''Document Service Client object. '''

import requests
from thrift.protocol import TBinaryProtocol

from fn_document_service.blocking import DocumentService, ttypes
from fn_ratelimiter_client.blocking_util import Retry500RequestsRetryPolicy
from fn_ratelimiter_client.blocking_util import standard_retry
from fn_service.util.blocking_client import RequestsHttpTransport

class DocServiceClient(object):
    '''Document service client - shamelessly copied from scraperutils'''
    def __init__(self, host, timeout, doc_session=None):
        # Set up thrift clients
        self.host = host
        if doc_session is None:
            doc_session = requests.Session()
        self.session = doc_session
        doc_transport = RequestsHttpTransport(doc_session, host, timeout=timeout)
        doc_protocol = TBinaryProtocol.TBinaryProtocol(doc_transport)
        self.doc_service_client = DocumentService.Client(doc_protocol)
        self.retry_policy = Retry500RequestsRetryPolicy(max_retry_time=1800)

    def last_download_info(self, url):
        '''Retrieve last-download info. '''
        def _try_last_download():
            return self.doc_service_client.getLastDownload(url)
        return standard_retry(_try_last_download, self.retry_policy)

    def register_download(self, file_hash, s3_url, serve_from_s3, original_url,
                          external_filename, mime_type, encoding, return_headers):
        '''Register a file download. '''
        def _try_register_download():
            return self.doc_service_client.registerDownload(
                file_hash, s3_url, serve_from_s3, original_url,
                external_filename, mime_type, encoding, return_headers)
        return standard_retry(_try_register_download, self.retry_policy)

    def extract_content(self, download_id, extraction_type, **kwargs):
        '''Request a document extraction from the doc service. 

        See ExtractionParams definition for valid kwargs:
        https://github.com/FiscalNote/FN-DocumentService/blob/master/thrift/fn_document_service.thrift

        Args:
            download_id: ID of the DocServ Download
            extraction_type: Extraction type to be performed.
            kwargs: Additional optional arguments (columnSpec and update_flag)
        '''
        if 'column_spec' in kwargs: # backwards compatible
            kwargs['columnSpec'] = kwargs.pop('column_spec')
        extract_params = {}
        for key in kwargs.keys():
            if key in ttypes.ExtractionParams.__slots__:
                extract_params[key] = kwargs.pop(key)
        if kwargs:
            raise ValueError("Unrecognized parameters: [{}]".format(', '.join(
                kwargs.keys()
            )))

        def _try_extract_content():
            extraction_params = ttypes.ExtractionParams(**extract_params)
            extracted_content = self.doc_service_client.extractContent(
                download_id, extraction_type, extraction_params)
            return extracted_content.entities
        return standard_retry(_try_extract_content, self.retry_policy)

    def register_documents(self, download_id, doc_service_documents):
        '''Register a document. '''
        def _try_register_documents():
            return self.doc_service_client.registerDocuments(
                download_id, doc_service_documents)
        return standard_retry(_try_register_documents, self.retry_policy)

    def close(self):
        '''Close the current connection.'''
        self.session.close()

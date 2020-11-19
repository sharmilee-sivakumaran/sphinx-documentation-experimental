'''
Common exceptions.
'''

class FilesException(Exception):
    '''Generic HTTP Exception.'''
    def __init__(self, message, exception=None, url=None, fil=None):
        super(FilesException, self).__init__(message)
        self.exception = exception
        self.url = url
        self.fil = fil

        if not url and fil:
            self.url = fil.url

class S3Exception(FilesException):
    '''An S3 specific exception.'''
    def __init__(self, message, status_code):
        super(S3Exception, self).__init__(message)
        self.status_code = status_code

class ExtractionException(FilesException):
    '''An exception related to extraction. '''
    pass

class RemoteExtractionException(ExtractionException):
    '''An exception related to remote extraction.'''
    pass

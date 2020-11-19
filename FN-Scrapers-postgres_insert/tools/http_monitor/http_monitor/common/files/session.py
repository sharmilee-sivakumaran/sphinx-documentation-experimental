'''
common.files.session

The files.Session object is designed to capture a complete state of the scraper's
networking components. The package will manage the creation and location of the
current session object, the scraper merely needs to call:

    session = files.Session.get()

An advanced use case is to have customized sessions. For example, to set a new
User Agent while keeping the current, the following will work:

    new_session = session.copy(
        user_agent='Googlebot/2.1 (+http://www.googlebot.com/bot.html)'
    )
    response = files.request_file(url, session=new_session)

The general heirarchy in settings is:

    1. Function arguments specified by the scraper.
    2. Session settings.
    3. Framework defaults.

'''

from base64 import b64decode
from copy import copy
import json
import threading

from boto.s3.connection import S3Connection
import requests

import fn_ratelimiter_common.config as ratelimiter_config
from fn_ratelimiter_client.blocking_client import BlockingRateLimiterClientFactory
from fn_ratelimiter_client.blocking_util import STANDARD_REQUESTS_RETRY_POLICY

from .docserv_client import DocServiceClient

class SessionAWS(object):
    '''Storage object for AWS settings. '''
    def __init__(self, access_key, secret_access_key=None, region=None,
                 base64_secret_access_key=None, bucket=None, **kwargs):
        self.access_key = access_key
        if base64_secret_access_key and not secret_access_key:
            secret_access_key = b64decode(base64_secret_access_key)
        self.secret_access_key = secret_access_key

        # TODO: Review initialization. Just-In-Time Connect?
        self.conn = S3Connection(self.access_key, self.secret_access_key)

        self.region = region
        self.bucket = None

        if not bucket and kwargs.get('s3_bucket'):
            bucket = kwargs.pop('s3_bucket')

        if bucket:
            self.set_bucket(bucket)


        self.s3_url_format = kwargs.pop('s3_url_format', None)

        s3_endpoint = kwargs.pop('s3_endpoint', None)
        if not self.s3_url_format and s3_endpoint:
            self.s3_url_format = 'https://{}/{{bucket}}/{{key}}'.format(s3_endpoint)
        elif not self.s3_url_format:
            self.s3_url_format = 'https://s3.amazonaws.com/{bucket}/{key}'

        if kwargs:
            raise ValueError('Unrecognized args: ' + ', '.join(kwargs.keys()))

    def connect(self, disconnect=True):
        '''
        Establishes an S3 Connection object.

        Args:
            disconnect: If False, prevents disconnecting the current connection.
                Used when forking.
        '''
        if disconnect and self.conn:
            self.conn.close()
        self.conn = S3Connection(self.access_key, self.secret_access_key)

    def close(self):
        '''
        Closes the connection to AWS.
        '''
        self.conn.close()

    def set_bucket(self, bucket):
        '''
        Update the bucket to a new one.

        Args:
            bucket: string name of bucket.
        '''
        self.bucket = self.conn.get_bucket(bucket)

    def generate_s3_url(self, key_name):
        '''
        Return a valid s3 url given a key_name of style 'file-by-sha384/...'
        '''
        return self.s3_url_format.format(bucket=self.bucket.name, key=key_name)


class Session(object):
    '''
    Collection object of HTTP settings and handlers, including rate limiter
    objects, thrift settings, requests sessions, etc.
    '''
    instance = None
    thread_local = threading.local()
    def __init__(self, aws, docservice_host, **kwargs):
        '''
        Constructor.

        Args:
            aws: AWS specific settings dictionary. Schema:
                    {
                        access_key str, secret_access_key str, s3_endpoint str,
                        bucket str, base64_secret_access_key str (optional)
                    }
            docservice_host: host to connect to for document service.
            s3_bucket: Bucket to upload/download from.
            dev_mode: Optional - do not extract content.
            start_time: Optional - start time of scraper.
            serve_from_s3: Optional - whether to use the s3 url or the external
                url. Defaults to None (auto: False if text/html, True otherwise).
            skip_checks: Optional - set to true to ignore cache.
        '''
        self.is_closed = False

        s3_bucket = kwargs.pop('s3_bucket', None)
        if s3_bucket:
            aws['bucket'] = s3_bucket
        aws['s3_url_format'] = kwargs.pop('s3_url_format', None)

        self.http_session = None
        self.aws = SessionAWS(**aws)
        self.docserv_client = DocServiceClient(docservice_host, 1800)

        # TODO: Data-access service initialization

        self.retry_policy = kwargs.pop(
            'retry_policy', STANDARD_REQUESTS_RETRY_POLICY)
        self.dev_mode = kwargs.pop('dev_mode', False)
        self.serve_from_s3 = kwargs.pop('serve_from_s3', None)
        self.start_time = kwargs.pop('start_time', None)
        self.skip_checks = kwargs.pop('skip_checks', False)
        if kwargs:
            raise ValueError('Unrecognized args: ' + ', '.join(kwargs.keys()))

    def close(self):
        '''
        Closes the connections.
        '''
        if self.is_closed:
            return
        self.is_closed = True

        self.aws.close()
        self.docserv_client.close()

    @classmethod
    def new(cls, *args, **kwargs):
        '''
        Creates a singleton instance available from the static instance
        variable.
        '''
        cls(*args, **kwargs).set_as_instance()
        return cls.get()

    @classmethod
    def get(cls, create=True):
        '''
        Get the most context-specific instance automatically. Checks for a
        thread-specific instance, then a singleton instance. Raises ValueError
        if no instance found.

        Args:
            create: Whether to create a session if none exist. If False and
                no session found, get() returns None.
        Returns:
            files.Session object, or None if create=False and none exist.
        '''
        if 'files_session' in cls.thread_local.__dict__:
            if not cls.thread_local.files_session.is_closed:
                return cls.thread_local.files_session
        if cls.instance and not cls.instance.is_closed:
            return cls.instance
        if not create:
            return None
        session = cls.load()
        cls.instance = session
        return session

    def set_as_instance(self):
        '''
        Sets the current object as the global instance.
        '''
        self.__class__.instance = self

    def copy(self, **kwargs):
        '''
        Shallow copies the current config object. Any arguments passed via
        kwargs will be set to the new object.
        '''
        new_instance = copy(self)
        for key in kwargs:
            setattr(new_instance, key, kwargs[key])

    def create_local(self, **kwargs):
        '''
        Create a thread-local instance of the current session, decoupling the
        requests.session property.
        '''
        if 'session' not in kwargs:
            # TODO: new session, or copy?
            kwargs['session'] = requests.Session()
        self.thread_local.files_session = self.copy(**kwargs)

    @classmethod
    def load(cls, su_config=None):
        '''
        Create an files.Session instance using common config files. For any
        argument not given, load_configs will attempt to read from the files in
        the current working directory.

        Args:
            su_config: A scrapeutils configuration as defined in
                `scraperutils-config.json`. Schema is:
                    {
                        aws: {
                            access_key str, secret_access_key str, s3_endpoint str,
                            bucket str, base64_secret_access_key str (optional)
                        },
                        file_upload_bucket: {
                            s3_bucket
                        }
                    }
        '''
        if not su_config:
            with open('scraperutils-config.json') as cfg:
                su_config = json.load(cfg)
        return Session(
            aws=su_config['aws'],
            docservice_host=su_config['thrift']['doc_service_host'],
            s3_bucket=su_config['file_upload_bucket']['s3_bucket'],
        )

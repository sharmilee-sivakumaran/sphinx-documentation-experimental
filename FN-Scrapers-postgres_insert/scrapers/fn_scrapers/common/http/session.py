from __future__ import absolute_import

from copy import copy
import json
import threading

import requests

import fn_ratelimiter_common.config as ratelimiter_config
from fn_ratelimiter_client.blocking_client import (
    BlockingRateLimiterClientFactory, BlockingRateLimiterClient)
import fn_ratelimiter_client.blocking_util as rl_util


class Session(object):
    instance = None
    thread_local = threading.local()
    _user_agent = 'FiscalNote/1.0'
    _max_size = 200*1024*1024

    def __init__(self, rl_config=None, req_session=None, **kwargs):
        '''
        Constructor.

        Args:
            ratelimiter_config: Optional rate limiter client description. Can
                be a BlockingRateLimiterClient instance, a 
                BlockingRateLimiterClientFactory instance, a dictionary of
                ratelimiter config or ratelimiter config object. If not
                provided will read from ./ratelimiter-config.json. Schema:
                    {
                        db: {
                            username str, host str, base64_password str,
                            port int, dbname str
                        },
                        client: { disable_rate_limiting bool }
                    }
            req_session: Opional requests.Session object. One will be created
                if not provided.
            user_agent: Optional User Agent string.
            retry_policy: Optional retry policy (default is standard).
        '''
        self.is_closed = False
        self.factory = None
        self.client = None
        self.req_session = req_session or requests.Session()

        if rl_config is None:
            config = ratelimiter_config.read_config()
        elif isinstance(rl_config, dict):
            config = ratelimiter_config.parse_config(rl_config)
        elif isinstance(rl_config, ratelimiter_config.Config):
            config = rl_config
        elif isinstance(rl_config, BlockingRateLimiterClientFactory):
            self.factory = rl_config
        elif isinstance(rl_config, BlockingRateLimiterClient):
            self.client = rl_config
        else:
            raise ValueError(
                "Unrecognized ratelimiter_config argument: {}".format(rl_config))

        if not self.client:
            if not self.factory:
                self.factory = BlockingRateLimiterClientFactory(config)
            self.client = self.factory.create_blocking_rate_limiter_client()

        self.user_agent = kwargs.pop('user_agent', self.__class__._user_agent)
        self.max_size = kwargs.pop('max_size', self.__class__._max_size)
        self.retry_policy = kwargs.pop(
            "retry_policy", rl_util.STANDARD_REQUESTS_RETRY_POLICY)

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
        if 'http_session' in cls.thread_local.__dict__:
            if not cls.thread_local.http_session.is_closed:
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
        Session.instance = self

    @classmethod
    def load(cls, rl_config=None):
        '''
        Create an files.Session instance using common config files. For any
        argument not given, load_configs will attempt to read from the files in
        the current working directory.

        Args:
            rl_config: A rate limiter configuration setup as defined in
                `ratelimiter-config.json`. Schema is:
                    {
                        db: {
                            username str, host str, base64_password str,
                            port int, dbname str
                        },
                        client: { disable_rate_limiting bool }
                    }
        '''
        if not rl_config:
            with open('ratelimiter-config.json') as cfg:
                rl_config = json.load(cfg)
        return Session(rl_config=rl_config)

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
        self.thread_local.http_session = self.copy(**kwargs)

    def close(self):
        '''Close the current rate limiter client. '''
        self.client.close()
        self.req_session.close()

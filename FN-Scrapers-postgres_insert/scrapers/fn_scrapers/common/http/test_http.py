from __future__ import absolute_import

import requests

from . import request_html5
from .session import Session, rl_util

class MockHttpSession(Session):
    def __init__(self, callback):
        self.is_closed = False
        self.client = MockRateLimiterClient()
        self.req_session = MockRequestsSession(callback=callback)
        self.set_as_instance()
        self.user_agent = self.__class__._user_agent
        self.max_size = self.__class__._max_size
        self.retry_policy = rl_util.STANDARD_REQUESTS_RETRY_POLICY


class MockRateLimiterClient(object):
    def delay_for_host(self, *args, **kwargs):
        pass

class MockRequestsSession(object):
    def __init__(self, callback):
        self.callback = callback

    def request(self, *args, **kwargs):
        return self.callback(args, kwargs)

class MockResponse:
    def __init__(self, **kwargs):
        self.content = kwargs.get('content')
        self.text = kwargs.get('text')
        self.json_data = kwargs.get('json_data')
        
    def raise_for_status(self):
        pass
    
    def iter_content(self):
        yield self.content or self.text

def test_html5_links():
    '''
    Tests the html5 absolute link conversion, better to understand the
    limitations of urlparse.urljoin method.
    '''
    def _test(url='http://google.com/foo', href=None, absolute=True):
        MockHttpSession(lambda a, k: MockResponse(
            content="<a href='{}'>bar</a>".format(href or 'foo.html')))
        doc = request_html5(url, abs_links=absolute)
        return doc.xpath('//a/@href')[0]
    assert _test(absolute=False) == 'foo.html'
    assert _test() == 'http://google.com/foo.html'
    assert _test(url='http://google.com/bar/') == 'http://google.com/bar/foo.html'
    assert _test(href='http://foo.bar/') == 'http://foo.bar/'
    assert _test(href='#loc') == 'http://google.com/foo#loc'

    # things that don't work:
    # assert _test(href='://foo.bar/') == 'http://foo.bar/'

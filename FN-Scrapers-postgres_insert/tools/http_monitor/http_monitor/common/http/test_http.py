from __future__ import absolute_import

from . import request_html5

def test_html5_links():
    '''
    Tests the html5 absolute link conversion, better to understand the
    limitations of urlparse.urljoin method.
    '''
    def _test(url='http://google.com/foo', href=None, absolute=True):
        text = "<a href='{}'>bar</a>".format(href or 'foo.html')
        doc = request_html5(url, abs_links=absolute, text=text)
        return doc.xpath('//a/@href')[0]
    assert _test(absolute=False) == 'foo.html'
    assert _test() == 'http://google.com/foo.html'
    assert _test(url='http://google.com/bar/') == 'http://google.com/bar/foo.html'
    assert _test(href='http://foo.bar/') == 'http://foo.bar/'
    assert _test(href='#loc') == 'http://google.com/foo#loc'

    # things that don't work:
    # assert _test(href='://foo.bar/') == 'http://foo.bar/'

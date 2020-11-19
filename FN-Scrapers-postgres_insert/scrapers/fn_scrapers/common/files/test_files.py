# -*- coding: utf-8 -*-

from fn_scrapers.common.files.file import File
from cStringIO import StringIO

def test_rfc6266():
    from rfc6266 import build_header

    assert "inline; filename=abc" == build_header(
        'abc', disposition='inline') # safe chars
    assert "inline; filename=ab.c" == build_header(
        'ab.c', disposition='inline') # safe chars2
    assert "inline; filename=\"ab,c\"" == build_header(
        'ab,c', disposition='inline') # unsafe char
    assert "inline; filename*=utf-8''ab%C3%87" == build_header(
        u'abÇ', disposition='inline') # unicode

def test_content():
    fil = File('http://foo.com', StringIO('This is a test'))
    assert fil.content == 'This is a test'

def test_get_xml():
    fil = File('http://foo.com', StringIO('<foo><bar>baz</bar></foo>'))
    xml = fil.get_xml()
    import lxml
    print lxml.etree.tostring(xml)
    assert xml.xpath('//bar/text()') == ['baz']

def test_get_lxml_html():
    fil = File('http://foo.com', StringIO('''
        <html><body><p>foo</p><p>bar</p>
    '''))
    xml = fil.get_lxml_html()
    import lxml
    print lxml.etree.tostring(xml)
    assert xml.xpath('//p/text()') == ['foo', 'bar']


def test_get_html5():
    fil = File('http://foo.com', StringIO('''
        <html><body><p>foo</p><p>bar</p>
    '''))
    xml = fil.get_html5()
    import lxml
    print lxml.etree.tostring(xml)
    assert xml.xpath('//p/text()') == ['foo', 'bar']

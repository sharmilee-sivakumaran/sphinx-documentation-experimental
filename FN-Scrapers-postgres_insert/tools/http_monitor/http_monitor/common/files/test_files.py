# -*- coding: utf-8 -*-

from fn_scrapers.common.files.file import File

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

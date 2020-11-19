
import copy
from lxml import html

class XpathException(Exception):
    '''
    Base XPath Exception class.
    '''
    msg_format = 'An error occurred with xpath: {}'
    def __init__(self, xpath, element):
        super(XpathException, self).__init__(self.msg_format.format(xpath))
        self.log_info = {}
        if hasattr(element, 'base_url') and element.base_url:
            self.log_info['extra_info'] = {
                'xpath_url': element.base_url
            }
        elif hasattr(element, 'base') and element.base:
            self.log_info['extra_info'] = {
                'xpath_url': element.base
            }

class XpathReturnedNone(XpathException):
    '''
    Used when an xpath expression returned no results and we expected at
    least one.
    '''
    msg_format = "No results were returned for the xpath provided: {}"

class XpathReturnedMultiple(XpathException):
    '''
    Used when an xpath expression returned more than one result but the query
    expected only one result.
    '''
    msg_format = "Multiple results were returned for the xpath provided: {}"

class XpathMultipleParents(XpathException):
    '''
    Used when an xpath expression returned more than one result but the query
    expected only one result.
    '''
    msg_format = ("Multiple parents for each child were returned for the xpath "
                  "provided: {}")

class XpathContext(object):
    '''
    Provides a context for xpath evaluation. Used when you need to customize
    the xpath calls such as calling 
    '''
    def __init__(self, namespaces=None):
        namespaces = namespaces or {}
        self._options = {
            'namespaces': namespaces
        }

    def xpath(self, xpath, element, **kwargs):
        '''
        Wrapper for element.xpath(...)
        '''
        options = self._options.copy()
        options.update(kwargs)
        return element.xpath(xpath, **options)

    def first(self, xpath, element, **kwargs):
        '''
        Returns the first result of an xpath query or raises an
        XpathReturnedNone exception.
        '''
        result = self.xpath(xpath, element, **kwargs)
        if not result:
            raise XpathReturnedNone(xpath, element)
        return result[0]

    def one(self, xpath, element, **kwargs):
        '''
        Returns the only result of an xpath query, or raises an
        XpathReturnedNone or XpathReturnedMultiple exception.
        '''
        result = self.xpath(xpath, element, **kwargs)
        if not result:
            raise XpathReturnedNone(xpath, element)
        if len(result) > 1:
            raise XpathReturnedMultiple(xpath, element)
        return result[0]

    def first_or_none(self, xpath, element, **kwargs):
        '''
        Returns the first result of an xpath query, or None.
        '''
        try:
            return self.first(xpath, element, **kwargs)
        except XpathReturnedNone:
            return None

    def one_or_none(self, xpath, element, **kwargs):
        '''
        Returns the only result of an xpath query or None. Raises
        XpathReturnedMultiple if multiple results are returned.
        '''
        try:
            return self.one(xpath, element, **kwargs)
        except XpathReturnedNone:
            return None

    def text(self, xpath, element, sep=u"", **kwargs):
        '''
        Returns the text content of the element.
        '''
        result = self.one(xpath, element, **kwargs)
        return sep.join(result.itertext())

    def split(self, xpath, element, root_callback=None, **kwargs):
        '''
        Splits a document into multiple fragments.
        '''
        if not root_callback:
            root_callback = lambda: (
                html.fromstring('<html><body></body></html>'),
                '//body'
            )
        elements = []
        root = None
        if isinstance(xpath, basestring):
            for el in self.xpath(xpath, element, **kwargs):
                if root is None:
                    root = el.getparent()
                elif root != el.getparent():
                    raise XpathMultipleParents(xpath, el)
                elements.append(el)
        current_doc = None
        if root is None:
            return
        for el in root:
            if el in elements:
                if current_doc is not None:
                    yield current_doc
                current_doc, root_xpath = root_callback()
                body = current_doc.xpath(root_xpath)[0]
            if current_doc is not None:
                body.append(copy.deepcopy(el))
        if current_doc is not None:
            yield current_doc


def xpath(xpath, element, **kwargs):
    '''
    Wrapper for element.xpath(...)
    '''
    return XpathContext().xpath(xpath, element, **kwargs)


def first(xpath, element, **kwargs):
    '''
    Returns the first result of an xpath query, or None.
    '''
    return XpathContext().first(xpath, element, **kwargs)


def one(xpath, element, **kwargs):
    '''
    Returns the first result of an xpath query, or None.
    '''
    return XpathContext().one(xpath, element, **kwargs)


def first_or_none(xpath, element, **kwargs):
    '''
    Returns the first result of an xpath query, or None.
    '''
    '''
    Returns the first result of an xpath query, or None.
    '''
    return XpathContext().first_or_none(xpath, element, **kwargs)


def one_or_none(xpath, element, **kwargs):
    '''
    Returns the only result of an xpath query or None. Raises
    XpathReturnedMultiple if multiple results are returned.
    '''
    return XpathContext().one_or_none(xpath, element, **kwargs)


def text(xpath, element, sep=u"", **kwargs):
    '''
    Returns the text content of the element.
    '''
    return XpathContext().text(xpath, element, sep=sep, **kwargs)


def split(xpath, element, root_callback=None, **kwargs):
    '''
    Splits a document into multiple fragments.
    '''
    return XpathContext().split(
        xpath, element, root_callback=root_callback, **kwargs
    )

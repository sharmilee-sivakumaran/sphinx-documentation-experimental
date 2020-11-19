'''
Extraction specific interfaces - not intended for scraper use directly.
'''

from lxml import etree, html
from thrift.Thrift import TApplicationException

from fn_document_service.blocking import ttypes

from .session import Session
from .exceptions import ExtractionException, RemoteExtractionException

class Extractor(object):
    '''
    Describes an extractor.
    '''
    def __init__(self, name, is_remote=True, extract=None, parse=None):
        self.name = name
        self.is_remote = is_remote
        self.extract = extract if extract else remote_extract
        self.parse = parse if parse else remote_parse

def remote_extract(name, fil, **kwargs):
    """
    files.File object to remote extraction entities list via docservice. See
    ExtractionParams definition for valid kwargs:

    https://github.com/FiscalNote/FN-DocumentService/blob/master/thrift/fn_document_service.thrift
    """

    session = Session.get()
    try:
        return session.docserv_client.extract_content(
            fil.download_id, name, **kwargs)
    except (TApplicationException, ttypes.BadFile) as exc:
        raise RemoteExtractionException(str(exc), exception=exc)

def remote_parse(entities):
    '''
    Remote extraction entities list to ScraperDocument list.
    '''
    content = []
    for entity in entities:
        if entity.textEntity:
            entity_content = []
            for text_container in entity.textEntity.textContainers:
                entity_content.append(text_container.text)
            content.append(u''.join(entity_content))
        elif entity.headerEntity:
            content.append(entity.headerEntity.text)
        elif entity.tableEntity:
            for row in entity.tableEntity.rows:
                entity_content = []
                for cell in row.cells:
                    for text_container in cell.textContainers:
                        entity_content.append(text_container.text)
                content.append(u''.join(entity_content))
    content.append(u'') # previous version had a trailing newline
    return [ScraperDocument(u'\n'.join(content))]

def html_extract(name, fil, replace_nbsp=True, encoding='utf-8', **kwargs):
    """
    files.File object to lxml html object.
    """
    fil.file_obj.seek(0)
    content = fil.file_obj.read().decode(encoding)
    if replace_nbsp:
        content = content.replace("&nbsp;", " ")
    try:
        html_content = html.fromstring(content)
    except etree.XMLSyntaxError as exc:
        raise ExtractionException(str(exc), fil=fil)
    html_content.make_links_absolute(fil.url)
    return html_content

def html_parse(lxml_page):
    '''
    Text content to ScraperDocument list.
    '''
    return [ScraperDocument(lxml_page.text_content().strip())]

def text_extract(name, fil, encoding='utf-8', **kwargs):
    """
    files.File object to plain-text content.
    """
    fil.file_obj.seek(0)
    return fil.file_obj.read().decode(encoding)

def text_parse(content):
    '''
    Text content to ScraperDocument list.
    '''
    return [ScraperDocument(content)]

def xml_extract(name, fil, encoding='utf-8', **kwargs):
    """
    files.File object to lxml Element.
    """
    fil.file_obj.seek(0)
    content = fil.file_obj.read().decode(encoding)
    try:
        element = etree.fromstring(content, base_url=fil.url)
    except etree.XMLSyntaxError as exc:
        raise ExtractionException(str(exc), fil=fil)
    return element

def xml_parse(element):
    '''
    lxml Element to ScraperDocument list.
    '''
    return [ScraperDocument(''.join(element.itertext()))]

class Extractors(object):
    '''
    A collection of extractor objects.
    '''
    html = Extractor("html", 0, html_extract, html_parse)
    xml = Extractor("xml", 0, xml_extract, xml_parse)
    text = Extractor("text", 0, text_extract, text_parse)
    image_pdf = Extractor("image_pdf")
    text_pdf = Extractor("text_pdf")
    extractor_pdftotext = Extractor("extractor_pdftotext")
    extractor_pdftoxml = Extractor("extractor_pdftoxml")
    tesseract = Extractor("extractor_tesseract")
    msword_doc = Extractor("msword_doc")
    msword_docx = Extractor("msword_docx")
    image = Extractor("image")
    msexcel_xls = Extractor("msexcel_xls")
    msexcel_xlsx = Extractor("msexcel_xlsx")
    mspowerpoint_ppt = Extractor("mspowerpoint_ppt")
    mspowerpoint_pptx = Extractor("mspowerpoint_pptx")
    rtf = Extractor("rtf")
    unknown = Extractor("unknown")
    unknown_new = Extractor("unknown_new")

    @classmethod
    def get(cls, extractor):
        '''
        Returns a Extractor instance given either an extractor name or instance.

        Args:
            extractor: name/instance of extractor ("html" or Extractors.html)
        Returns:
            Extractor instance
        Raises:
            ValueError on invalid extractor
        '''
        if isinstance(extractor, basestring):
            extractor = getattr(cls, extractor, extractor)
        if not isinstance(extractor, Extractor):
            raise ValueError("Unknown extraction type: {}".format(extractor))
        return extractor


class ScraperDocument(object):
    '''Represents a scraper document, or scraper extracted text. '''
    def __init__(self, text, scraper_id=None, page_num=None, additional_data=None):
        """
        The object that parse functions return, which gets sent back to the
        scraper after the document service step.

        Args:
            text: The content of the document, as raw text.
            scraper_id: A scraper-based unique id for this document (Can just
                be the scraper_notice_id)
            page_num: The page number that the document came from in the
                original file
            additional_data: A dict for passing additional fields that the parse
                functions gets which are needed when saving the notices
        """
        self.text = text
        self.scraper_id = scraper_id
        self.page_num = page_num
        self.additional_data = additional_data

    def __unicode__(self):
        doc_str = u""
        if self.scraper_id:
            doc_str += u"Scraper ID: {} \n".format(self.scraper_id)
        if self.page_num:
            doc_str += u"Page Number: {}\n".format(self.page_num)
        doc_str += u"Text:\n {}\n".format(self.text)
        return doc_str

    def __str__(self):
        return unicode(self).encode('utf-8')

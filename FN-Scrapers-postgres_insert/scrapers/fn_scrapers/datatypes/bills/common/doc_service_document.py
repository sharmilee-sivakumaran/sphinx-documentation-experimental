from __future__ import absolute_import

import logging


logger = logging.getLogger('fn_legislation')


class Doc_service_document(dict):
    """
    Object representing a document processed by document service
    """

    def __init__(self, name, type, doc_service_type, download_id, doc_id=None):
        """
        Create a new Doc_service_document object.
        This class is created in order to handle the multiple optional fields in the pillar document schema

        :param name: name of the document
        :param type: one of ["version", "amendment", "summary", "fiscal_note", "committee_document", "other"]
                     more might be added as the schema changes
        :param doc_service_type: either "complete" or "partial"
        :param download_id: an id that indexes the download of this document
        :param doc_id: an id that indexes the extracted text of this document

        a "complete" doc service type indicates that the document has been downloaded, AND that text extraction has
        been performed on the text -- both the download and the extracted text have been registered in doc service.
        "version" and "amendment" should fall into this category. Both download id and doc id are needed for a
        complete document.

        a "partial" doc service type means the document has been downloaded but text extraction was not performed.
        Only the download has been registered in doc service. This applies to trivial documents on which we don't
        gain much value from the extracted text. Only a download id is needed for a partial document.

        """
        super(Doc_service_document, self).__init__()
        self["name"] = name
        self["type"] = type
        document_service = {"type": doc_service_type}
        if doc_service_type == "complete":
            try:
                assert doc_id is not None
            except AssertionError:
                logger.critical("A document id is needed to construct a \"complete\" doc service document!")
                raise
            document_service["document_id"] = doc_id
            document_service["download_id"] = download_id
        elif doc_service_type == "partial":
            try:
                assert doc_id is None
            except AssertionError:
                logger.critical("A \"partial\" doc service document does not need a document id!")
                raise
            document_service["download_id"] = download_id
        else:
            raise ValueError("Parameter document_service_type has to be either \"complete\" or \"partial\", "
                             "\"{}\" was provided".format(doc_service_type))
        self["document_service"] = document_service

    def add_alternate_representation(self, download_id):
        if "alternate_representations" not in self:
            self["alternate_representations"] = []
        self["alternate_representations"].append(download_id)

    def add_introduction_date(self, introduction_date):
        self["introduction_date"] = introduction_date

    def add_acceptance_date(self, acceptance_date):
        self["acceptance_date"] = acceptance_date

    def add_child(self, child_doc_index):
        """
        Add a child to this document. e.g. "Amendment 123" amends version "Introduced"
        "Amendment 123" would be a child of "Introduced" version.

        Each element in the list is the *index* of the child document within the "document" field (a list) in
        the bill document

        The elements should be ordered in that if "Amendment 1" and "Amendment 2" both happened on version
        "Introduced", index of "Amendment 1" should be added to this list before "Amendment 2".

        :param child_doc_index: index of the child document in the bill document array
        """
        if child_doc_index is None:
            logger.warning(u"A None value is passed as the child index. This indicates that the download process "
                           u"for the child document failed, so the add_doc_service_document function skipped adding "
                           u"the child document to the bill object and returned a None value as its \"index\" in the "
                           u"documents list. We will not add this None value index as a child index")
            return
        if "children" not in self:
            self["children"] = []
        self["children"].append(child_doc_index)

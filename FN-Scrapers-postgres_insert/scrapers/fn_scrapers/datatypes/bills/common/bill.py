from __future__ import absolute_import

import logging
import urllib2
import re
logger = logging.getLogger('fn_legislation')


class Bill(dict):
    """
    Object representing a piece of legislation.
    """

    def __init__(self, session, chamber, bill_id, title, bill_type, **kwargs):
        """
        Create a new :obj:`Bill`.

        :param session: the session in which the bill was introduced.
        :param chamber: the chamber in which the bill was introduced:
          either 'upper' or 'lower'
        :param bill_id: an identifier assigned to this bill by the legislature
          (should be unique within the context of this chamber/session)
          e.g.: 'HB 1', 'S. 102', 'H.R. 18'
        :param title: a title or short description of this bill provided by
          the official source

        Additional keyword arguments which are optional fields in the bill schema will also be saved in the database.
        Any additional properties will cause the bill to be rejected.
        """
        super(Bill, self).__init__(**kwargs)
        self._seen_documents = {}
        self._seen_children = {}

        self['id'] = bill_id
        self['session'] = session
        self['chamber'] = chamber
        self['title'] = title
        self['type'] = bill_type
        self['sources'] = []

    def add_source(self, url, source_type="default"):
        """
        Add a source with this bill.

        :param url: the url of the source. Must be specific to the bill and have more than just bill text
        :param source_type: will be either 'house', 'senate', 'assembly', or 'default'.
        If a state has only a single source, it's type will be 'default'
        """
        url = urllib2.quote(url, "+://?=&%")
        self['sources'].append(dict(url=url, source_type=source_type))

    def add_sponsor(self, sponsor_type, name, **kwargs):
        """
        Associate a sponsor with this bill.

        :param sponsor_type: the type of sponsorship, e.g. 'primary', 'cosponsor'
        :param name: the name of the sponsor as provided by the official source
        """
        if 'sponsors' not in self:
            self['sponsors'] = []
        self['sponsors'].append(dict(type=sponsor_type, name=name, **kwargs))

    def add_subject(self, subject):
        """
        Add a subject to a bill
        :param subject: String of bill subject
        """
        if 'subjects' not in self:
            self['subjects'] = []
        self['subjects'].append(subject)

    def add_external_resource(self, name, url, type):
        """
        Add a piece of external resource to this bill

        :param name: name of the resource, should be as descriptive, i.e. include date/title etc
        :param url: external link
        :param type: one of ["audio", "video", "other"] (as of 08-17-16 basing on the new schema)
                     more types could be added if the schema changes
        """
        url = urllib2.quote(url, "://?=&%")
        d = dict(name=name, url=url, type=type)
        if 'external_resources' not in self:
            self['external_resources'] = []
        self['external_resources'].append(d)

    def contains_action_with_unicode_space(self):
        if 'actions' in self:
            for action in self['actions']:
                if re.search(r"[^\S ]", action["action"], re.U):
                    return True
        return False

    def add_action(self, actor, action, date, **kwargs):
        """
        Add an action that was performed on this bill.
        """
        if 'actions' not in self:
            self['actions'] = []

        self['actions'].append(dict(actor=actor, action=action, date=date, **kwargs))

    def add_vote(self, vote):
        """
        Associate a :class:`~fnleg.scrape.votes.Vote` object with this
        bill.
        """
        if 'votes' not in self:
            self['votes'] = []
        self['votes'].append(vote)

    def add_alternate_title(self, title):
        """
        Associate an alternate title with this bill.
        """
        if 'alternate_titles' not in self:
            self['alternate_titles'] = []
        self['alternate_titles'].append(title)

    def add_alternate_id(self, id):
        """
        Associate an alternate id with this bill.
        """
        if 'alternate_ids' not in self:
            self['alternate_ids'] = []
        self['alternate_ids'].append(id)

    def add_companion(self, bill_id, bill_type="other"):
        if 'related_bills' not in self:
            self['related_bills'] = []
        self['related_bills'].append({"external_id":bill_id, "type":bill_type})

    def get_filename(self):
        filename = "%s_%s_%s.json" % (self['session'], self['chamber'],
                                      self['id'])
        return filename.encode('ascii', 'replace')

    def add_doc_service_document(self, doc_service_document, skip_invalid_document=True):
        """
        Add a document-service-processed document to this bill object
        The index of this document within the document array is returned in order to assist the add_child
        function within the Doc_service_document class.

        :param skip_invalid_document: boolean value representing if we skip adding an invalid document
                (e.g. document with download_id = None)
        :param doc_service_document: a Doc_service_document object
        :return: index of the added Doc_service_document object within the bill document array
        """
        download_id = doc_service_document["document_service"]["download_id"]
        if skip_invalid_document and download_id is None:
            logger.warning(u"Document \"{}\" has a None download_id value. This indicates "
                           u"that doc service has failed to download it. Skip adding this "
                           u"document to bill object to avoid Pillar rejection".format(doc_service_document["name"]))
            return None

        if "documents" not in self:
            self["documents"] = []
        doc_service_type = doc_service_document["document_service"]["type"]
        if doc_service_type == "complete":
            complete_key = (doc_service_document["document_service"]["download_id"],
                            doc_service_document["document_service"]["document_id"])
            if complete_key in self._seen_documents:
                logger.warning("Document \"{}\" has the same download_id and document_id as \"{}\". Keeping the "
                               "previous document and ignoring \"{}\"".format(doc_service_document["name"],
                                                                              self._seen_documents[complete_key]["name"],
                                                                              doc_service_document["name"]))
                return self["documents"].index(self._seen_documents[complete_key])

            else:
                self._seen_documents[complete_key] = doc_service_document
        else:
            partial_key = (doc_service_document["document_service"]["download_id"])
            if partial_key in self._seen_documents:
                logger.warning("Document \"{}\" has the same download_id as \"{}\". Keeping the "
                               "previous document and ignoring \"{}\"".format(doc_service_document["name"],
                                                                              self._seen_documents[partial_key]["name"],
                                                                              doc_service_document["name"]))
                return self["documents"].index(self._seen_documents[partial_key])
            else:
                self._seen_documents[partial_key] = doc_service_document
        if "children" in doc_service_document:
            child_indexes = doc_service_document["children"]
            valid_child_indexes= []
            for child_index in child_indexes:
                if child_index in self._seen_children:
                    old_parent_index = self._seen_children[child_index]
                    old_parent_name = self["documents"][old_parent_index]["name"]
                    new_parent_name = doc_service_document["name"]
                    child_name = self["documents"][child_index]["name"]
                    logger.warning("Trying to set \"{child_name}\" as the child of document \"{new_parent_name}\", "
                                   "but \"{child_name}\" is already set as a child of \"{old_parent_name}\". "
                                   "Will abort this attempt and keep the old child-parent relationship.".
                                   format(child_name=child_name, old_parent_name=old_parent_name,
                                          new_parent_name=new_parent_name))
                else:
                    self._seen_children[child_index] = (len(self["documents"]) + 1) - 1
                    valid_child_indexes.append(child_index)
            if valid_child_indexes:
                doc_service_document["children"] = valid_child_indexes
            else:
                # if we end up with an empty "children" field, we need to remove it
                # otherwise pillar gets angry
                logger.warning("After checking existing child-parent relationship. Document \"{}\" end up with "
                               "an empty \"children\" field. Will have to delete it to pass Pillar validation.".
                               format(doc_service_document["name"]))
                doc_service_document.pop("children")

        self["documents"].append(doc_service_document)
        index = len(self["documents"]) - 1
        return index

    def add_summary(self, summary):
        """
        :param summary: summary of the bill

        """
        if "summary" in self:
            logger.warning("Multiple summaries have been assigned. Overwriting with the value from most recent call.")
        self["summary"] = summary

    def __unicode__(self):
        return "%s %s: %s" % (self['chamber'], self['session'], self['id'])

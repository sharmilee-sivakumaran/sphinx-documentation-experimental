'''
Module containing severity of data reports
'''
from __future__ import absolute_import

from fn_scraperutils.events.reporting import ReportingPolicy, Severity


class NoticeReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in NoticeScrapers
    """
    register_list = ReportingPolicy("Register List", Severity.critical)
    register = ReportingPolicy("Register", Severity.critical)
    notice_list = ReportingPolicy("Notice List", Severity.critical)
    doc_service = ReportingPolicy("Access Document Service", Severity.critical)

    notice = ReportingPolicy("Notice", Severity.warning)
    title = ReportingPolicy("Title", Severity.warning)
    notice_type = ReportingPolicy("Notice Type", Severity.warning)
    publication_date = ReportingPolicy("Publication Date", Severity.warning)
    scraper_notice_id = ReportingPolicy("Notice ID", Severity.warning)
    regulation = ReportingPolicy("Regulation Data", Severity.warning)

    agency = ReportingPolicy("Agency", Severity.info)
    effective_date = ReportingPolicy("Effective Date", Severity.info)
    expiration_date = ReportingPolicy("Expiration Date", Severity.info)
    code_id = ReportingPolicy("Code ID", Severity.info)
    attachment = ReportingPolicy("Attachment", Severity.info)
    header = ReportingPolicy("Header", Severity.info)

    test = ReportingPolicy("Testing", Severity.debug)

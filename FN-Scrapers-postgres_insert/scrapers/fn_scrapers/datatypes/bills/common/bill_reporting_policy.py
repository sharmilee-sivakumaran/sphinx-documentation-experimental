from __future__ import absolute_import

from fn_scraperutils.events.reporting import ReportingPolicy, Severity


class BillReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """
    bill_list = ReportingPolicy("Bill List", Severity.critical)

    bill = ReportingPolicy("Bill", Severity.warning)
    json = ReportingPolicy("Json", Severity.warning)
    bill_title = ReportingPolicy("Bill Title", Severity.warning)
    bill_documents = ReportingPolicy("Bill Related Documents", Severity.warning)
    report_documents = ReportingPolicy("Report Related Documents", Severity.warning)
    doc_service = ReportingPolicy("Doc Service Call", Severity.warning)
    legislators = ReportingPolicy("Legislators", Severity.warning)

    bill_summary = ReportingPolicy("Bill Summary", Severity.info)
    bill_sponsors = ReportingPolicy("Bill Sponsors", Severity.info)
    bill_actions = ReportingPolicy("Bill Actions", Severity.info)
    bill_versions = ReportingPolicy("Bill Versions", Severity.info)
    bill_votes = ReportingPolicy("Bill Votes", Severity.info)
    bill_companions = ReportingPolicy("Bill Companions", Severity.info)
    bill_subjects = ReportingPolicy("Bill Subjects", Severity.info)
    bill_partial_documents = ReportingPolicy("Bill Partial Documents", Severity.info)

    wrong_session = ReportingPolicy("Got bill from wrong session", Severity.critical)

    test = ReportingPolicy("Testing", Severity.debug)

    ok = ReportingPolicy("ok", Severity.ok)
    debug = ReportingPolicy("debug", Severity.debug)
    info = ReportingPolicy("info", Severity.info)
    warning = ReportingPolicy("warning", Severity.warning)
    critical = ReportingPolicy("critical", Severity.critical)

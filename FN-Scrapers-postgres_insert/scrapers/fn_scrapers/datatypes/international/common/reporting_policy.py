from __future__ import absolute_import

from fn_scraperutils.events.reporting import ReportingPolicy, Severity


class ScraperReportingPolicy(object):
    """
    List of the different reporting policies based on the expected output of the various functions in BillScrapers
    """

    def __init__(self, scraper_name):
        self.doc_list = ReportingPolicy(
            u"{scraper_name} Document List".format(scraper_name=scraper_name), Severity.critical)
        self.doc_single = ReportingPolicy(
            u"{scraper_name} Document".format(scraper_name=scraper_name), Severity.critical)
        self.doc = ReportingPolicy(u"{scraper_name} Document".format(scraper_name=scraper_name),
                                   Severity.warning)
        self.doc_service = ReportingPolicy(
            u"{scraper_name} Document Service".format(scraper_name=scraper_name), Severity.warning)
        self.test = ReportingPolicy(u"Testing", Severity.debug)

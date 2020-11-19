# -*- coding: utf-8 -*-
from __future__ import division
import logging
import re

from ..common.base_scraper import ScraperBase
from .legislative_schema_handler import Documentkey_players, Documentlegislative_observatory, \
    Documentadoption_by_commission, DocumentStages, Documentprocedure

from fn_service.server import BlockingEventLogger, fmt
from fn_scrapers.api.scraper import scraper, argument, tags, argument_function
from fn_scrapers.api.resources import ScraperArguments
from fn_scraperutils.events.reporting import EventComponent
import json
import injector

logger = logging.getLogger(__name__)
from datetime import datetime, date, timedelta
from HTMLParser import HTMLParser


# class for URL formation at different levels
class EuropelegURL:

    base_url = u"https://eur-lex.europa.eu"

    @staticmethod
    def list_page_url(type, year, type_capital, qid):

        list_page_url = u'{base_url}/search.html?qid={qid}&DB_TYPE_OF_ACT={type}&DTS_DOM=EU_LAW&' \
                        u'typeOfActStatus={type_capital}&type=advanced&lang=en&' \
                        u'SUBDOM_INIT=PRE_ACTS&DTS_SUBDOM=PRE_ACTS&DD_YEAR={year}'.format(base_url=EuropelegURL.base_url,
                                                                                          type=type, type_capital=type_capital, year=year, qid=qid)
        return list_page_url

    @staticmethod
    def list_url_for_legislation(year):
        return u'{base_url}/search.html?qid=1497565332080&DTS_DOM=EU_LAW&type=advanced&lang=en&SUBDOM_INIT' \
               u'=LEGISLATION&DTS_SUBDOM=LEGISLATION&DD_YEAR={year}'.format(base_url=EuropelegURL.base_url, year=year)

    @staticmethod
    def get_next_page(list_page_url, page_number):
        next_page_url = u'{list_page_url}&page={page_number}'.format(list_page_url=list_page_url, page_number=page_number)
        return next_page_url

    @staticmethod
    def add_base_url(url):
        final_url = u'{base_url}{url}'.format(base_url=EuropelegURL.base_url, url=url)
        return final_url


def _args(parser):
    subparsers = parser.add_subparsers(help="Scraper modes")

    by_year_parser = subparsers.add_parser("by-year", help="Scrape items in a year range")
    by_year_parser.add_argument(
        '--subtype',
        help='subtype like - "COM", "JOIN", "SEC", "LEG"',
        dest="subtypes",
        action="append",
        required=True)
    by_year_parser.add_argument(
        '--year',
        help='Year in the format YYYY - 2018',
        dest="years",
        action="append",
        required=True)
    by_year_parser.set_defaults(scraper_mode="by-year")

    recently_parser = subparsers.add_parser("recently", help="Scrape items updated recently")
    recently_parser.add_argument("--days", help="Scrape items updated in the last number of days", required=True)
    recently_parser.set_defaults(scraper_mode="recently")

@scraper()
@argument_function(_args)
@tags(type="bills", group="international")
# EU Docscraper class
class EU_legislativeDocScraper(ScraperBase):
    @injector.inject(logger=BlockingEventLogger, args=ScraperArguments)
    def __init__(self, logger, args):

        super(EU_legislativeDocScraper, self).__init__(EventComponent.scraper_bills, "eu_legislative_procedure", "europe")
        self.logger = logger
        self.args = args

    # function for finding a single item from html
    @staticmethod
    def single_pattern(html, pattern, group):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.search(html)
        if not match:
            return None
        else:
            return match.group(group)

    # function for finding multiple items from html
    @staticmethod
    def find_pattern(html, pattern):
        comp = re.compile(pattern, re.DOTALL | re.IGNORECASE)
        match = comp.findall(html)
        return match

    # function for downloading html from page
    def download_html(self, url):
        self.http_get(url, self.scraper_policy.doc_list)
        html = self.get_content_from_response()
        return html

    @staticmethod
    def trim_content(content):
        content = re.sub('<.*?>', '', content)
        content = re.sub('<.*?>|\s{2,}', ' ', content)
        content = re.sub('^\s+|\s+$|\r?\n', '', content)
        return content

    # function to get total member_links present on page
    def get_member_blocks(self, url):
        html = self.download_html(url)
        member_blocks = self.find_pattern(html, '(<li class="listing__item">.*?</li>)')
        return member_blocks

    # for for getting the correct date format
    @staticmethod
    def get_formatted_date(date):
        date = re.sub('\s{2,}|\(|\)|^\s+|\s+$', '', date)
        try:
            date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
        return date

    # function to scrape data
    def scrape(self):
        if self.args.scraper_mode == "by-year":
            self.scrape_by_year(self.args.subtypes, self.args.years)
        elif self.args.scraper_mode == "recently":
            self.scrape_recently(self.args.days)
        else:
            raise ValueError(self.args.scraper_mode)

    def scrape_recently(self, days):
        try:
            d = date.today()
            d1 = d-timedelta(int(days))
            enddate = d.strftime('%d%m%Y')
            startdate = d1.strftime('%d%m%Y')
            self.logger.info(__name__, fmt(u"Considering start date as - {} and end date as - {}", startdate,enddate))
            event_date = startdate+'%7C'+enddate
            acting_bodies = \
                {
                "Activities of bodies created by international agreements":"https://eur-lex.europa.eu/search.html?qid=1516177539196&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_INTER&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DDEC_byCOUN_ASS%7C%7CDEC_byCOUN_COOP%7C%7CDEC_byACP_AMB%7C%7CDEC_byACP_MIN%7C%7CDEC_byCMT_COOP%7C%7CDEC_byCMT_JOIN%7C%7CDEC_byCMT_MIX%7C%7CDEC_byCOUN_JOIN&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of Conciliation Committee":"https://eur-lex.europa.eu/search.html?qid=1516177604116&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_CONCIL&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DDEC_byCMT_CONC%7C%7CCONV_ofCMT_CONC&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of Council":"https://eur-lex.europa.eu/search.html?qid=1516177687568&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_CONS&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DADP_FRM_byCONSIL%7C%7CADP_COMPOS_byCONSIL%7C%7CADP_R1_byCONSIL%7C%7CAGR_COMPOS_byCONSIL%7C%7CAPR_R2_byCONSIL%7C%7CAPR_R1_byCONSIL%7C%7CCH_LB_byCONSIL%7C%7CCONF_COMPOS_byCONSIL%7C%7CCONS_ofECA_byCONSIL%7C%7CCONS_ofCURIA_byCONSIL%7C%7CCONS_ofESC_byCONSIL%7C%7CCONS_ofESC_byCONSIL%7C%7CCONS_ofEMI_byCONSIL%7C%7CCONS_ofCMT_MON_byCONSIL%7C%7CCONS_ofEP_byCONSIL%7C%7CCONS_ofECB_byCONSIL%7C%7CAGR_byCONSIL%7C%7CASS_byCONSIL%7C%7CCONC_byCONSIL%7C%7CDEC_R3_byCONSIL%7C%7CDEC_BUDACT_byCONSIL%7C%7COPI_byCONSIL%7C%7COPI_BUDACT_byCONSIL%7C%7CAPR_PARR1_byCONSIL%7C%7CAPR_PARR2_byCONSIL%7C%7CRES_byCONSIL%7C%7CDEC_PROCONCERT_byCONSIL%7C%7CDIS_BUDR1_byCONSIL%7C%7CDIS_BUDR2_byCONSIL%7C%7CDIS_byCONSIL%7C%7CADP_NO_byCONSIL%7C%7CADP_PAR_byCONSIL%7C%7CRJ_byCONSIL&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of Court of Justice":"https://eur-lex.europa.eu/search.html?qid=1516177889949&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_JC&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DCC_CD_PUB_OJ%7C%7CCC_NC_PUB_OJ%7C%7CJUDG_byCURIA%7C%7COPI_byCURIA&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of European Committee of the Regions":"https://eur-lex.europa.eu/search.html?qid=1516178084240&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_RC&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DOPI_byCOR%7C%7COPI_OWINIbyCOR%7C%7CRES_byCOR%7C%7CREF_SELF_byCOR&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of European Economic and Social Committee":"https://eur-lex.europa.eu/search.html?qid=1516178236885&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_CSE&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DADP_PLSL_byEESC%7C%7CADP_SL_byEESC%7C%7COPI_EXPL_byESC%7C%7COPI_byESC%7C%7COPI_OWINI_byESC%7C%7CRES_byESC%7C%7COPI_byECSC_CMTCSL%7C%7COPI_EXPL_byEESC%7C%7COPI_byEESC%7C%7COPI_OWINI_byEESC%7C%7CRES_byEESC&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of European Parliament":"https://eur-lex.europa.eu/search.html?qid=1516178308735&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_EP&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DADP_byEP%7C%7CCC_AT_byCOM%7C%7CDIS_BUDR1_byEP%7C%7CDIS_BUDR2_byEP%7C%7CASS_byEP%7C%7COPI_R1_byEP_CMT%7C%7COPI_R2_byEP_CMT%7C%7COPI_R3_byEP_CMT%7C%7COPI_ASS_byEP_CMT%7C%7COPI_BUDACT_byEP_CMT%7C%7COPI_RSI_byEP_CMT%7C%7COPI_RES_byEP_CMT%7C%7CREP_R1_byEP_CMT%7C%7CREP_R2_byEP_CMT%7C%7CREP_R3_byEP_CMT%7C%7CREP_ASS_byEP_CMT%7C%7CREP_BUDACT_byEP_CMT%7C%7CREP_RSI_byEP_CMT%7C%7CREP_RES_byEP_CMT%7C%7CDEC_R3_byEP%7C%7CDEC_BUDACT_byEP%7C%7COPI_R1_byEP%7C%7COPI_R2_byEP%7C%7COPI_BUDACT_byEP%7C%7COPI_RSI_byEP%7C%7CRES_byEP%7C%7CINF_RJ_R2_byEP%7C%7COPI_NO_R2_byEP%7C%7CAPR_NO_byEP%7C%7CDEC_BUDDIS_byEP%7C%7CRECP_COMPOS_byEP%7C%7CRECP_byEP%7C%7CRJ_byEP%7C%7CREQ_INI_PROCONCERT_byEP%7C%7CSIGN_BUD_byEP&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of European Parliament and Council":"https://eur-lex.europa.eu/search.html?qid=1516178353364&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_EPC&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DSIGN_PAR_byEP_CONSIL%7C%7CADP_ACTDENO_byCONSIL%7C%7CSIGN_byEP_CONSIL&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of Member States":"https://eur-lex.europa.eu/search.html?qid=1516187052450&DTS_DOM=LEGAL_PROCEDURE&type=advanced&actingBody0=WORK_MS&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DINI_byEUMS&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of other institutions and bodies":"https://eur-lex.europa.eu/search.html?qid=1516187215541&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_OTHER&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DOPI_byECA%7C%7COPI_byECB%7C%7COPI_byEIB%7C%7COPI_byEMI%7C%7COPI_byCMT_MON%7C%7CPUB_OJ&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Activities of the Commission":"https://eur-lex.europa.eu/search.html?qid=1516187272679&DTS_DOM=LEGAL_PROCEDURE&actingBody0=WORK_COM&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DADD_byCOM%7C%7CADP_byCOM%7C%7CADP_PROPAME_byCOM%7C%7CADP_DCLCOMPOS_byCOM%7C%7CADP_CCPOSR1CONSIL_byCOM%7C%7CADP_OPR2AMEPE_byCOM%7C%7CADP_PROREEX_byCOM%7C%7CADP_ACT_byCOM%7C%7CAPR_byCOM%7C%7CCH_LB_byCOM%7C%7CCH_TLIS_byCOM%7C%7CCC_AT_byCOM%7C%7CPOS_AME_R1_byCOM%7C%7CPOS_AME_R2_byCOM%7C%7CPOS_AME_RSI_byCOM%7C%7CCONS_ofESC_byCOM%7C%7CCONS_ofEESC_byCOM%7C%7CCONS_ofEP_byCOM%7C%7CCORR_byCOM%7C%7CDEC_FUAC_byCOM%7C%7CEMPOW_byCOM%7C%7CRPL_IMP_byCOM%7C%7CRPL_PAR_byCOM%7C%7CWDW_PAR_byCOM%7C%7CRPL_byCOM%7C%7CSUPP_byCOM%7C%7CTRS_toCONSIL_byCOM%7C%7CWDW_byCOM%7C%7CWDW_LIS_byCOM&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Transmission by Commission to Council":"https://eur-lex.europa.eu/search.html?qid=1516187337708&DTS_DOM=LEGAL_PROCEDURE&actingBody0=TRANS_CONS&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DTRS_toCONSIL_PROPAME_byCOM%7C%7CTRS_toCONSIL_PROREEX_byCOM%7C%7CTRS_toCONSIL_byCOM%7C%7CTRS_toCONSIL_CC_R1_byCOM%7C%7CTRS_toCONSIL_OP_R2_byCOM%7C%7CTRS_toCONSIL_COMPOS_byCOM&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Transmission by Commission to European Data Protection Supervisor":"https://eur-lex.europa.eu/search.html?qid=1516187408630&DTS_DOM=LEGAL_PROCEDURE&actingBody0=TRANS_DPEC&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DOPI_byEDPS&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Transmission by Commission to European Parliament":"https://eur-lex.europa.eu/search.html?qid=1516187449531&DTS_DOM=LEGAL_PROCEDURE&actingBody0=TRANS_EP&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DTRS_EP_PROPAME_byCOM%7C%7CTRS_EP_OPIAME_R2_byCOM%7C%7CTRS_EP_DCLCOMPOS_byCOM%7C%7CTRS_EP_PROREEX_byCOM%7C%7CTRS_EP_byCOM%7C%7CTRS_EP_CC_POSR1_byCOM&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                "Transmission by Commission to other bodies":"https://eur-lex.europa.eu/search.html?qid=1516187527329&DTS_DOM=LEGAL_PROCEDURE&actingBody0=TRANS_OTHER&type=advanced&lang=en&whLP_EVENT_TYPE0=LP_EVENT_DATE:{event_date},LP_EVENT_TYPE_CODED%3DTRS_OMB_byCOM%7C%7CTRS_toCONSIL__byCOM&SUBDOM_INIT=LEGAL_PROCEDURE&DTS_SUBDOM=LEGAL_PROCEDURE&page={page_number}",
                 }

            for act_name in acting_bodies:
                basic_url = acting_bodies[act_name]

                first_page_url = basic_url.format(event_date=event_date, page_number="1")

                # calling function for getting the html based on year provided
                html_from_page = self.download_html(first_page_url)

                bills_total = self.single_pattern(html_from_page, '(<p class="resultNumber">.*?</p>)', 1)
                if bills_total:
                    bills_total = re.sub('&nbsp;', '', bills_total)
                    bills_total = int(self.single_pattern(bills_total, '</span>\s*(\d+)</p>', 1))
                    self.logger.info(__name__, fmt(u"Total number of bill found - {}", bills_total))
                    total_number_of_pages = int(bills_total / 10 + 2)
                    self.scrape_bill(html_from_page)
                    if total_number_of_pages > 2:
                        for page in range(2, total_number_of_pages):
                            next_page_url = basic_url.format(event_date=event_date, page_number=page)
                            next_page_html = self.download_html(next_page_url)
                            self.scrape_bill(next_page_html)
                else:
                    self.logger.info(__name__, fmt(u"No Bill Found For - {}", act_name))

        except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

    def scrape_by_year(self, subtypes, years):
        for subtype in subtypes:
            for year in years:
                self.scrape_subtype(subtype, year)

    def scrape_subtype(self, subtype, year):
        try:
            type_capital = ' '
            if subtype == "COM":
                subtype = "com"
                type_capital = "COM"
                qid = "1490117937643"
            elif subtype == "JOIN":
                subtype = "join"
                type_capital = "JOIN"
                qid = "1490117981989"
            elif subtype == "SEC":
                subtype = "secSwd"
                type_capital = "SEC_SWD"
                qid = "1490118003051"

            if subtype != "LEG":
                list_page_url = EuropelegURL.list_page_url(subtype, year, type_capital, qid)
            elif subtype == "LEG":
                list_page_url = EuropelegURL.list_url_for_legislation(year)

            # calling function for getting the html based on year provided
            html_from_page = self.download_html(list_page_url)

            bills_total = self.single_pattern(html_from_page, '(<p class="resultNumber">.*?</p>)', 1)
            bills_total = re.sub('&nbsp;', '', bills_total)
            bills_total = int(self.single_pattern(bills_total, '</span>\s*(\d+)</p>', 1))
            logger.debug(u"Total bills found in search result: %s", bills_total)
            total_number_of_pages = int(bills_total / 10 + 2)
            for page in range(1, total_number_of_pages):
                next_page_url = EuropelegURL.get_next_page(list_page_url, page)
                next_page_html = self.download_html(next_page_url)
                self.scrape_bill(next_page_html)
        except Exception as e:
            self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                 exc_info=True)

    # function to scrape bill information
    def scrape_bill(self, html):
        bill_blocks = self.find_pattern(html, '(<tr>\s*<td\s*rowspan=.*?</ul></td></tr>)')
        if not bill_blocks:
            bill_blocks = self.find_pattern(html, '(</td>\s*<td\s*colspan=.*?</ul></td></tr>)')
        htmlparser = HTMLParser()
        for bill_block in bill_blocks:
            try:
                # celex
                celex = self.single_pattern(bill_block, 'CELEX\s*number\s*:\s*(.*?)<', 1)
                if not celex:
                    celex = self.single_pattern(bill_block, 'Initiating\s*document\s*:\s*(.*?)</a', 1)
                if celex:

                    # source_url
                    source_url = self.single_pattern(bill_block, 'class="title" name="(https?://.*?)"', 1)

                    # procedure
                    procedure_url = re.sub('AUTO', 'HIS', source_url)
                    procedure_page_html = self.download_html(procedure_url)
                    if 'class="usermsgWarning"' in procedure_page_html:
                        logging.debug(u'No Linked procedure found')
                    else:
                        europe_leg_bill = Documentprocedure()

                        # procedure_code
                        procedure_code = self.single_pattern(procedure_page_html, 'strong>\s*Procedure\s*(.*?)\s*</', 1)

                        europe_leg_bill.add_procedure_code(procedure_code)

                        # title
                        title = self.single_pattern(procedure_page_html, '"description":"([^"]*)"', 1)
                        if title is None or "no title" in title:
                            self.logger.info(__name__, fmt(u"No title found for procedure url - {}", procedure_url))
                            pass
                        else:
                            title = re.sub('\s+', ' ', title)
                            title = re.sub('<br>', ': ', title)
                            europe_leg_bill.add_title(title)

                        # procedure_type
                        procedure_type = self.single_pattern(procedure_page_html,
                                                             'Legal basis\s*:.*?<th>\s*Procedure\s*:\s*</th>\s*(.*?)</tr>',
                                                             1)
                        if procedure_type:
                            procedure_type_list = []
                            procedure_type = re.sub('</span>\s*<br>\s*<span\s*lang="en">', ',', procedure_type)
                            procedure_type = re.sub('<.*?>', '', procedure_type)
                            if ',' in procedure_type:
                                procedure_type = re.sub('\s*,\s*', ',', procedure_type)
                                procedure_type = re.sub('^\s*|\s*$', '', procedure_type)
                                procedure_type_list = procedure_type.split(',')
                            else:
                                procedure_type = procedure_type.strip()
                                procedure_type_list.append(procedure_type)
                            europe_leg_bill.add_procedure_type(procedure_type_list)

                        # lp_file_type
                        lp_file_list = self.find_pattern(procedure_page_html, 'Type of file\s*:\s*.*?<td>(.*?)</td>')
                        if lp_file_list:
                            lp_file_type = lp_file_list[-1]
                            lp_file_type = re.sub('<br><span.*?>', ',', lp_file_type)
                            lp_file_type = re.sub('<.*?>', '', lp_file_type)
                            europe_leg_bill.add_lp_file_type(lp_file_type)

                        # adoption_by_commission
                        adoption_by_commission = self.single_pattern(procedure_page_html, '(>\s*[^<]*<span lang="en">\s*Adoption by Commission\s*</span>.*?</table>)', 1)
                        if adoption_by_commission:
                            adpt_com = Documentadoption_by_commission()

                            # adoption_date
                            adoption_date = self.single_pattern(adoption_by_commission, '(\d+\/\d+\/\d+)\s*:\s*<span\s*lang="en">\s*Adoption', 1)
                            if adoption_date:
                                adoption_date = self.get_formatted_date(adoption_date)
                                adpt_com.add_adoption_date(adoption_date)

                            # leading_person
                            leading_person = self.single_pattern(adoption_by_commission, '>\s*Leading person\s*:\s*</th>(.*?)</tr>', 1)
                            if leading_person:
                                leading_person = re.sub('<.*?>|\r?\n', '', leading_person)
                                leading_person = leading_person.title()
                                leading_person = leading_person.strip()
                                adpt_com.add_leading_person(leading_person)

                            # leading_service
                            leading_service_list = []
                            leading_service = self.single_pattern(adoption_by_commission, '>\s*Leading service\s*:\s*</th>(.*?)</tr>', 1)
                            if leading_service:
                                leading_service = re.sub('<.*?>', '', leading_service)
                                leading_service = re.sub('\s{2,}', ' ', leading_service)
                                if ',' in leading_service:
                                    leading_service = re.sub('\s*,\s*', ',', leading_service)
                                    leading_service = re.sub('^\s*|\s*$', '', leading_service)
                                    leading_service_list = leading_service.split(',')
                                else:
                                    leading_service = leading_service.strip()
                                    leading_service_list.append(leading_service)

                                adpt_com.add_leading_service(leading_service_list)

                            # addressee_for_formal_act
                            addressee_for_formal_act = self.single_pattern(adoption_by_commission,
                                                                           '>\s*Addressee for formal act\s*:\s*</th>(.*?)</tr>',
                                                                           1)
                            if addressee_for_formal_act:
                                addressee_for_formal_act_list = []
                                addressee_for_formal_act = re.sub('<.*?>|\r?\n', '', addressee_for_formal_act)
                                if ';' in addressee_for_formal_act:
                                    addressee_for_formal_act = re.sub('\s*;\s*', ';', addressee_for_formal_act)
                                    addressee_for_formal_act = re.sub('^\s*|\s*$', '', addressee_for_formal_act)
                                    addressee_for_formal_act_list = addressee_for_formal_act.split(';')
                                else:
                                    addressee_for_formal_act = addressee_for_formal_act.strip()
                                    addressee_for_formal_act_list.append(addressee_for_formal_act)
                                adpt_com.add_addressee_for_formal_act(addressee_for_formal_act_list)

                            # addressee_for_information
                            addressee_for_information = self.single_pattern(adoption_by_commission,
                                                                           '>\s*Addressee for information\s*:\s*</th>(.*?)</tr>',
                                                                           1)
                            if addressee_for_information:
                                addressee_for_information_list = []
                                addressee_for_information = re.sub('<.*?>|\r?\n', '', addressee_for_information)
                                if ';' in addressee_for_information:
                                    addressee_for_information = re.sub('\s*;\s*', ';', addressee_for_information)
                                    addressee_for_information = re.sub('^\s*|\s*$', '', addressee_for_information)
                                    addressee_for_information_list = addressee_for_information.split(';')
                                else:
                                    addressee_for_information = addressee_for_information.strip()
                                    addressee_for_information_list.append(addressee_for_information)
                                adpt_com.add_addressee_for_information(addressee_for_information_list)

                            # addressee_for_mandatory_consultation
                            addressee_for_mandatory_consultation = self.single_pattern(adoption_by_commission,
                                                                            '>\s*Addressee for mandatory consultation\s*:\s*</th>(.*?)</tr>',
                                                                            1)
                            if addressee_for_mandatory_consultation:
                                addressee_for_mandatory_consultation_list = []
                                addressee_for_mandatory_consultation = re.sub('<.*?>|\r?\n', '', addressee_for_mandatory_consultation)
                                addressee_for_mandatory_consultation = re.sub('\s{2,}', ' ',
                                                                              addressee_for_mandatory_consultation)
                                if ';' in addressee_for_mandatory_consultation:
                                    addressee_for_mandatory_consultation = re.sub('\s*;\s*', ';', addressee_for_mandatory_consultation)
                                    addressee_for_mandatory_consultation = re.sub('^\s*|\s*$', '', addressee_for_mandatory_consultation)
                                    addressee_for_mandatory_consultation_list = addressee_for_mandatory_consultation.split(';')
                                elif ',' in addressee_for_mandatory_consultation:
                                    addressee_for_mandatory_consultation = re.sub('\s*,\s*', ',',
                                                                                  addressee_for_mandatory_consultation)
                                    addressee_for_mandatory_consultation = re.sub('^\s*|\s*$', '',
                                                                                  addressee_for_mandatory_consultation)
                                    addressee_for_mandatory_consultation_list = addressee_for_mandatory_consultation.split(',')
                                else:
                                    addressee_for_mandatory_consultation = addressee_for_mandatory_consultation.strip()
                                    addressee_for_mandatory_consultation_list.append(addressee_for_mandatory_consultation)
                                adpt_com.add_addressee_for_mandatory_consultation(addressee_for_mandatory_consultation_list)

                            # addressee_for_optional_consultation
                            addressee_for_optional_consultation = self.single_pattern(adoption_by_commission,
                                                                                       '>\s*Addressee for optional consultation\s*:\s*</th>(.*?)</tr>',
                                                                                       1)
                            if addressee_for_optional_consultation:
                                addressee_for_optional_consultation_list = []
                                addressee_for_optional_consultation = re.sub('<.*?>|\r?\n', '',
                                                                             addressee_for_optional_consultation)
                                addressee_for_optional_consultation = re.sub('\s{2,}', ' ',
                                                                             addressee_for_optional_consultation)
                                if ';' in addressee_for_optional_consultation:
                                    addressee_for_optional_consultation = re.sub('\s*;\s*', ';',
                                                                                 addressee_for_optional_consultation)
                                    addressee_for_optional_consultation = re.sub('^\s*|\s*$', '',
                                                                                 addressee_for_optional_consultation)
                                    addressee_for_optional_consultation_list =addressee_for_optional_consultation.split(
                                        ';')
                                elif ',' in addressee_for_optional_consultation:
                                    addressee_for_optional_consultation = re.sub('\s*,\s*', ',',
                                                                                 addressee_for_optional_consultation)
                                    addressee_for_optional_consultation = re.sub('^\s*|\s*$', '',
                                                                                 addressee_for_optional_consultation)

                                    addressee_for_optional_consultation_list = addressee_for_optional_consultation.split(
                                        ',')
                                else:
                                    addressee_for_optional_consultation = addressee_for_optional_consultation.strip()
                                    addressee_for_optional_consultation_list.append(addressee_for_optional_consultation)
                                adpt_com.add_addressee_for_optional_consultation(addressee_for_optional_consultation_list)

                            # celexes
                            celex_array = []
                            related_celex = self.find_pattern(procedure_page_html,
                                                              'CELEX number of the main document\s*:\s*</th>\s*<td><span\s*lang="en"><a.*?>(.*?)</tr>')
                            if related_celex:
                                for cel in related_celex:
                                    cel = re.sub('<.*?>', '', cel)
                                    cel = cel.split(';')
                                    for each in cel:
                                        each = re.sub('\r?\n|\s+', '', each)
                                        if each:
                                            celex_array.append(each)
                                celex_array = set(celex_array)
                                celex_array = list(celex_array)
                                adpt_com.add_celexes(celex_array)

                            self.validate_doc(adpt_com)
                            europe_leg_bill.add_adoption_by_commission_by_obj(adpt_com)

                        # stages
                        all_stages = self.find_pattern(procedure_page_html,
                                                       '(<div class="procedureHeader[^>]*>.*?</div>\s*</div>)')
                        if all_stages:
                            for stage in all_stages:
                                initial_stage = stage
                                document_stage = DocumentStages()
                                stage = re.sub('<.*?>|\r?\n', '', stage)
                                stage = re.sub('\s{2,}', ' ', stage)
                                stage = stage.split(':')
                                date = stage[0]
                                title = stage[1]
                                title = re.sub('\s+$|^\s+', '', title)
                                date = re.sub('Top|TOP|\s*|top', '', date)
                                date = self.get_formatted_date(date)
                                document_stage.add_date(date)
                                document_stage.add_title(title)
                                chamber = self.single_pattern(initial_stage, '<img id=".*?src="(.*?)"', 1)
                                if "red" in chamber:
                                    chamber = "Economic and Social Committee"
                                elif "green" in chamber:
                                    chamber = "European Commission"
                                elif "uv" in chamber:
                                    chamber = "European Council"
                                elif "blue" in chamber:
                                    chamber = "European Parliament"
                                elif "amber" in chamber:
                                    chamber = "European Committee of the Regions"
                                elif "white-off" in chamber:
                                    chamber = "European Commission"
                                else:
                                    chamber = None
                                if chamber:
                                    document_stage.add_chamber(chamber)

                                self.validate_doc(document_stage)
                                europe_leg_bill.add_stages_by_obj(document_stage)

                        # source_url
                        if procedure_code:
                            modified_procedure_code = re.sub('\/[A-Z]+.*', '', procedure_code)
                            modified_procedure_code = re.sub('\/', '_', modified_procedure_code)
                            modified_procedure_code = re.sub('_0+', '_', modified_procedure_code)
                            source_url = "https://eur-lex.europa.eu/procedure/EN/"+modified_procedure_code
                            europe_leg_bill.add_source_url(source_url)

                        # adopted_act_celex
                        adopted_act_celex = self.single_pattern(procedure_page_html, 'Adopted acts\s*:(.*?)</div', 1)
                        if adopted_act_celex:
                            adopted_act_celex = re.sub('<.*?>|\s{2,}|\n', '', adopted_act_celex)
                            europe_leg_bill.add_adopted_act_celex(adopted_act_celex)

                        # legislative_observatory
                        leg_obs_url = self.single_pattern(procedure_page_html, '<a href="([^>]*)">\s*<b>\s*European Parliament\s*\-\s*Legislative observatory', 1)
                        if leg_obs_url:
                            leg_obs_url = re.sub('&amp;', '&', leg_obs_url)
                            leg_obs_bill = Documentlegislative_observatory()
                            leg_obs_html = self.download_html(leg_obs_url)
                            key_player = self.single_pattern(leg_obs_html,
                                                              '(title="European Parliament" target="_blank">European Parliament</a>.*?<td class="players_institution inst_separator">)',
                                                              1)
                            if key_player:
                                key_player_blocks = key_player.split('<td class="players_committee')
                                for key_player in key_player_blocks:
                                    if '<span class="players_committee_text">' in key_player:
                                        key_player = re.sub('\r?\n', '', key_player)
                                        key_player = re.sub('players_head shadow.*', '', key_player)

                                        # committee_name
                                        committee_name = self.single_pattern(key_player, '<span class="players_committee_text">\s*(.*?)\s*</span>', 1)
                                        committee_name = re.sub('<.*?>','',committee_name)
                                        committee_name = re.sub('^\s+|\s+$', '', committee_name)
                                        committee_name = re.sub('\\t+', ' ', committee_name)

                                        # committee_type
                                        if "Committee responsible" in com_name:
                                            committee_type = "Primary"
                                        elif "Committee for opinion" in com_name:
                                            committee_type = "Opinion"

                                        if '<p class="players_content">' in key_player:

                                            key_player_names = key_player.split('class="players_content"')
                                            for player_name in key_player_names:
                                                if 'class="tiptip"' in player_name or 'committee decided' in player_name or 'class="players_appointed' in player_name:

                                                    key_player_bill = Documentkey_players()
                                                    key_player_bill.add_committee_name(committee_name)

                                                    key_player_bill.add_committee_type(committee_type)

                                                    # rapporteur
                                                    rapporteur = self.single_pattern(player_name, 'class="photo".*?>\s*(.*?)\s*</', 1)
                                                    if rapporteur:

                                                            rapporteur = " ".join(rapporteur.split(" ")[::-1])
                                                            rapporteur = htmlparser.unescape(rapporteur)
                                                            rapporteur = rapporteur.title()
                                                            key_player_bill.add_rapporteur(rapporteur)

                                                    self.validate_doc(key_player_bill)
                                                    leg_obs_bill.add_key_players_by_obj(key_player_bill)
                                        else:
                                            key_player_bill = Documentkey_players()
                                            key_player_bill.add_committee_name(committee_name)
                                            key_player_bill.add_committee_type(committee_type)
                                            rapporteur = self.single_pattern(key_player, 'class="photo".*?>\s*(.*?)\s*</', 1)
                                            if rapporteur:
                                                rapporteur = " ".join(rapporteur.split(" ")[::-1])
                                                rapporteur = htmlparser.unescape(rapporteur)
                                                rapporteur = rapporteur.title()
                                                key_player_bill.add_rapporteur(rapporteur)
                                            self.validate_doc(key_player_bill)
                                            leg_obs_bill.add_key_players_by_obj(key_player_bill)
                                    else:
                                        com_name = self.single_pattern(key_player, '<p class="players_head">\s*(.*?)\s*</p>', 1)

                                # council_configuration
                                council_configuration = self.single_pattern(leg_obs_html, '<p class="players_head">Council configuration</p>.*?<p class="players_content">(.*?)</p>', 1)
                                if council_configuration:
                                    council_configuration = re.sub('\r?\n|<.*?>', '', council_configuration)
                                    council_configuration = re.sub('\s{2,}', ' ', council_configuration)
                                    leg_obs_bill.add_council_configuration(council_configuration)

                                # meeting_date
                                meeting_date = self.single_pattern(leg_obs_html, 'MEET_DATE\s*=\s*(\d{2}\/\d{2}\/\d{4})', 1)
                                if meeting_date:
                                    meeting_date = self.get_formatted_date(meeting_date)
                                    leg_obs_bill.add_meeting_date(meeting_date)

                                # source_url
                                leg_obs_bill.add_source_url(leg_obs_url)

                                europe_leg_bill.add_legislative_observatory_by_obj(leg_obs_bill)

                        if self.validate_doc(europe_leg_bill):
                            self.save_doc(europe_leg_bill)
                        else:
                            self.logger.critical(__name__, "individual_bill_scrape_failed",
                                                 fmt("JsonSchema validation failed for bill page: {}", source_url))
                else:
                    self.logger.info(__name__, fmt(u"CELEX not found"))

            except Exception as e:
                self.logger.critical(__name__, 'individual_bill_scrape_failed', fmt("Error occured: {}", e),
                                     exc_info=True)

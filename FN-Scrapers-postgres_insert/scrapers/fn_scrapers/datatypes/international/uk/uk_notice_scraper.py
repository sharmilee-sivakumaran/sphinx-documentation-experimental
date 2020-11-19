'''
UK Notice Kraken Scraper

Designed to be Kraken First (document centered).

Implimentation note: static methods assume no network transactions are required
and are therefore useful for testing.
'''

from datetime import datetime, timedelta
import json
import logging
from os import path
import re
from time import sleep

from dateutil.parser import parse as du_parse
import yaml

from fn_scrapers.api.scraper import scraper, argument

from fn_scrapers.common import http, files, xpath_shortcuts, generic_scraper
from fn_scrapers.common.experimental.json_consumer import JSONObject


logger = logging.getLogger(__name__)

@scraper()
@argument('-f', '--feeds', nargs='+', default=['uksi', 'ukdsi'],
          help='UK Feed (uksi, ukdsi, etc)')
@argument('--ingest_func', choices=[1, 2, 3], type=int, default=1,
          help='ingest function type as defined in the kraken schema')
@argument('--start', help='Date to start, default is 30 days ago (yyyy-mm-dd)')
@argument('--end', help='Date to end, default today (yyyy-mm-dd)')
class UKNoticeScraper(generic_scraper.GenericScraper):
    '''
    UK Notice Scraper.
    '''
    base_url = 'https://www.legislation.gov.uk/{}/data.feed'
    kraken_schema_id = 'international_reg_notices'
    kraken_feed_id = 'international_reg_notices_united_kingdom'

    xpc = xpath_shortcuts.XpathContext(namespaces={
        "ns": "http://www.w3.org/2005/Atom",
        "atom": "http://www.w3.org/2005/Atom",
        "leg": "http://www.legislation.gov.uk/namespaces/legislation",
        "ukm": "http://www.legislation.gov.uk/namespaces/metadata",
        "theme": "http://www.legislation.gov.uk/namespaces/theme",
        "openSearch": "http://a9.com/-/spec/opensearch/1.1/",
        "parameters": "http://a9.com/-/spec/opensearch/extensions/parameters/1.0/",
        "akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/WD16",
        "dc": "http://purl.org/dc/elements/1.1/",
    })

    def __init__(self, **kwargs):
        super(UKNoticeScraper, self).__init__(**kwargs)
        self.start = datetime.utcnow().date() - timedelta(days=30)
        if self.args.start:
            self.start = datetime.strptime(self.args.start, '%Y-%m-%d').date()

        self.end = datetime.utcnow().date()
        if self.args.end:
            self.end = datetime.strptime(self.args.end, '%Y-%m-%d').date()

        self.kraken_fields = {
            'schemaId': 'international_reg_notices',
            'feedId': 'international_reg_notices_united_kingdom',
            'ingestFunction': self.args.ingest_func
        }
        cwd = path.dirname(path.abspath(__file__))
        with open(path.join(cwd, 'schemas', 'uk_notice_schema.yaml')) as fp:
            schema = yaml.safe_load(fp.read())
        self.schema = JSONObject(schema)


    def scrape(self):
        ''' Main scraper loop - atom feeds to xml/akn documents'''
        self.scraper_loop(
            self.get_entry(self.args.feeds),
            self.process_akn,
            log_ok=False
        )


    def process_akn(self, akn):
        '''
        Secondary scraper loop - akn documents to docserver docs and kraken
        records.
        '''
        self.scraper_loop(
            self.parse_akn(akn),
            self.send_document
        )


    def send_document(self, record):
        ''' Reformat record as a kraken doc and send it. '''
        record.update(self.kraken_fields)
        kraken_doc = self.schema.consume(record)
        kraken_doc['documentMetadata'] = json.dumps(
            kraken_doc['documentMetadata'], sort_keys=True)
        self.publish_json("kraken_documents", kraken_doc)


    def get_entry(self, feeds):
        ''' Yields a set of akn entries from specified UK Notice Atom Feeds. '''
        for feed in feeds:
            url = self.base_url.format(feed)
            logger.info(u"Scraping feed %s", feed)
            while True:
                logger.info("Requesting Feed %s", url)
                url, akns = self.parse_feed(http.request_xml(url))
                for akn_url, updated in akns:
                    if updated > self.end:
                        continue
                    if self.start > updated:
                        url = None
                        continue
                    logger.info("Requesting AKN %s", akn_url)
                    yield http.request_xml(akn_url), akn_url
                if not url:
                    break


    @classmethod
    def parse_feed(cls, xml):
        ''' Parses the xml atom feed to next_url and list of akns. '''
        akns = []
        for entry in cls.xpc.xpath('//ns:entry', xml):
            ident = cls.xpc.one('./ns:id/text()', entry)

            akn = cls.xpc.one_or_none("./ns:link[@title='AKN']/@href", entry)
            if not akn:
                logger.warning(u'No AKN Specified for %s', ident)
                continue

            updated = du_parse(cls.xpc.one('./ns:updated/text()', entry)).date()

            akns.append((akn, updated))
        next_url = cls.xpc.one_or_none("//ns:link[@rel='next']/@href", xml)
        return next_url, akns


    @classmethod
    def parse_akn(cls, xml):
        ''' Generator which parses an akn file into one or more records '''
        record = {
            "country": "United Kingdom",
            "publication_name": "UK Legislation.gov.uk",
            "title": cls.xpc.one('//akn:proprietary/dc:title/text()', xml),
            "source_url": cls.xpc.one('//dc:identifier/text()', xml),
            "notice_type": cls.format_type(cls.xpc.one('//akn:act/@name', xml)),
            "publication_date": cls.xpc.one('//dc:modified/text()', xml),
            "notice_id": cls.get_notice_id(xml),
            "document_title": "Primary Document",
            'document_url': None,
            "download_id": None,
            "document_id": None,
        }

        record.update(cls.get_dl_and_doc(
            cls.xpc.one("//atom:link[@title='PDF']/@href", xml)
        ))
        yield record, record['document_url']

        urls = set([record['document_url']])
        for el in cls.xpc.xpath("//*[contains(@URI, '.pdf')]", xml):
            url = cls.xpc.one('./@URI', el)
            dic = {
                'publication_date': cls.xpc.one_or_none('./@Date', el),
                'document_title': cls.xpc.one_or_none('./@Title', el)
            }
            if url in urls or any(not v for v in dic.values()) or not url.endswith('.pdf'):
                continue
            urls.add(url)
            record = record.copy()
            record.update(cls.get_dl_and_doc(url))
            record.update(dic)
            yield record, record['document_url']


    @classmethod
    def format_type(cls, notice_type):
        '''
        Formats the notice type as required: 
            UnitedKingdomDraftStatutoryInstrument -> UK Draft Statutory Instrument
        '''
        return re.sub(r'([a-z])([A-Z])', r'\1 \2', notice_type).replace(
            "United Kingdom", "UK")


    @classmethod
    def get_notice_id(cls, xml):
        '''
        Attempts to format the notice id (may need to be expanded if additional
        fields are added).
        '''
        notice_id = cls.xpc.one_or_none('//akn:docNumber/text()', xml)
        if notice_id and re.match(r'^\d{4} No\. \d+$', notice_id):
            return notice_id
        
        return "ISBN " + '-'.join(re.match(
            r'^(\d{3})-?(\d)-?(\d\d)-?(\d{6})-?(\d)$', 
            cls.xpc.one('//ukm:SecondaryMetadata/ukm:ISBN/@Value', xml)).groups())


    @classmethod
    def get_dl_and_doc(cls, url):
        '''
        Registers and extracts url.
        
        UK Notices has one quirk where a pdf may be generated at request time.
        https://s3.amazonaws.com/fn-document-service-dev/file-by-sha384/9990b0d1c52c530f837672c909c7b115c2f3a8729899effc7252c46d7a110481642e40a3ebf79ffd2b0c51c2de360fd4
        '''
        if not files.Session.instance:
            # running in a non-instantiated scraper (testing)
            return {'document_url': url, 'download_id': 0, 'document_id': 0}
        for _ in range(10):
            fil = files.request_file_with_cache(url)
            if fil.mimetype == 'application/pdf':
                break
            logger.info("Did not receive PDF - sleeping.")
            sleep(10)
        else:
            raise ValueError("Could not retrieve PDF: Received {} - {}".format(
                fil.mimetype, url
            ))
        fil.upload_and_register()
        fil.extract_and_register_documents(
            extractor=files.extractors.extractor_pdftotext)
        return {
            'document_url': url,
            'download_id': fil.download_id,
            'document_id': fil.document_ids[0]
        }

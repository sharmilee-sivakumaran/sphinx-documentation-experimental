import json
import logging
from os import path
import yaml


from fn_service.util.tls import get_tls

from fn_scrapers.api.resources import injector, ScraperArguments
from fn_scrapers.api.scraper import argument
from fn_scrapers.api.utils import JSONEncoderPlus
from fn_scrapers.common.experimental.json_consumer import JSONObject

logger = logging.getLogger(__name__)

CWD = path.dirname(path.abspath(__file__))

class Feed(object):
    def __init__(self, schema_id, feed_id, ingest_func=1, translate_schema=None):
        self.schema_id = schema_id
        self.feed_id = feed_id
        self.ingest_func = ingest_func
        self._schema = None
        
        self.schema_dir = path.join(CWD, 'schemas', schema_id)
        if not path.exists(self.schema_dir):
            raise ValueError("Unable to find schema: " + schema_id)

        if not translate_schema:
            translate_schema = 'translate_schema.yaml'
        self.feed_schema = path.join(self.schema_dir, 'feeds', translate_schema)
        if not path.exists(self.feed_schema):
            raise ValueError("Unable to find translate schema: " + translate_schema)

    @property    
    def schema(self):
        if not self._schema:
            logger.info("Loading kraken schema: %s", self.feed_schema)
            with open(self.feed_schema, 'rb') as fp:
                self._schema = JSONObject(yaml.safe_load(fp))
        return self._schema


# The FEEDS collection below is defined as follows:
#  - Key: Scraper Class Name
#  - Value: Feed object
#    - schema_id: The Schema defined by kraken spec.
#      eg: https://github.com/FiscalNote/FN-Kraken/tree/0.1.4/feeds
#    - feed_id: The feed identifer without the schema_id. 
#    - translate_schema: provide a filename (relative to {schema_id}/feeds/),
#      defaults to 'translate_schema.yaml'

FEEDS = {
    'Argentina_GazetteDocScraper': Feed('international_reg_notices', 'argentina'),
    'BrazilRegNoticeScraper': Feed('international_reg_notices', 'brazil'),
    'Canada_GazetteDocScraper': Feed('international_reg_notices', 'canada'),
    'ChileRegNoticeScraper': Feed('international_reg_notices', 'chile'),
    'ColombiaGazatteScraper': Feed('international_reg_notices', 'colombia'),
    'FRANCEregulationnoticescraper': Feed('international_reg_notices', 'france'),
    'GermanyRegNoticeScraper': Feed('international_reg_notices', 'germany'),
    'IndiaRegNoticeScraper': Feed('international_reg_notices', 'india'),
    'IndonesiaRegNoticeScraper': Feed('international_reg_notices', 'indonesia'),
    'MexicoRegNoticeScraper': Feed('international_reg_notices', 'mexico'),
    'Peru_GazetteDocScraper': Feed('international_reg_notices', 'peru'),
    'RussiaRegNoticeScraper': Feed('international_reg_notices', 'russia'),
    'SWITZERLANDregulationnoticescraper': Feed('international_reg_notices', 'switzerland'),
    'THAILANDregulationnoticescraper': Feed('international_reg_notices', 'thailand'),
}

@argument('--kraken', action='store_true', help='Redirect output to kraken.')
@argument('--kraken_ingest_function', type=int, choices=[1, 2, 3], default=1,
          help='Kraken ingest function (1 basic, 2 overwrite, 3 update)')
class KrakenScraper(object):
    '''
    Overrides a scraper's default messaging method.
    '''
    def __init__(self, *args, **kwargs):
        super(KrakenScraper, self).__init__(*args, **kwargs)

        name = self.__class__.__name__
        self._kraken_feed = FEEDS.get(name)

        if hasattr(self, 'save_doc'):
            setattr(self, '_kraken_old_save_doc', getattr(self, 'save_doc'))
            setattr(self, 'save_doc', self._kraken_save_doc)

    def _kraken_save_doc(self, doc, *args, **kwargs):
        cli_args = get_tls(injector.Injector).get(ScraperArguments)
        if not cli_args.kraken:
            return getattr(self, '_kraken_old_save_doc')(doc, *args, **kwargs)
        if not self._kraken_feed:
            raise ValueError("Scraper not configured for kraken use")

        self._kraken_feed.ingest_func = cli_args.kraken_ingest_function
        logger.info("Sending kraken message...")
        kraken_doc = krakenize(doc, self._kraken_feed)

        getattr(self, 'scrape_item_publisher').publish_json_item(
            "", "kraken_documents", getattr(self, 'scraper_source'), kraken_doc,
            json_encoder=JSONEncoderPlus)


def krakenize(document, kraken_feed):
    '''
    Converts a scraper document into a kraken document.
    '''
    if hasattr(document, 'as_dict'):
        document = document.as_dict()
    document['schemaId'] = kraken_feed.schema_id
    document['feedId'] = '{}_{}'.format(document['schemaId'], kraken_feed.feed_id)
    document['ingestFunction'] = document.get('ingestFunction', kraken_feed.ingest_func)
    
    final = kraken_feed.schema.consume(document)
    final['documentMetadata'] = json.dumps(
        final.get('documentMetadata', {}), sort_keys=True,
        default=JSONEncoderPlus)
    return final

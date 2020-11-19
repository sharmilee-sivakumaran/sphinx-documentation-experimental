
from collections import Mapping
import logging
import traceback

from fn_logging.event_logger import OK as OK_LVL

from fn_scrapers.api.scraper import argument
from fn_scrapers.api.scrape_item_publisher import ScrapeItemPublisher
from fn_scrapers.api.resources import (
    injector, ScraperArguments, FilesSession, HttpSession)
from fn_scrapers.api.utils import JSONEncoderPlus
from fn_scrapers.common.dict import dict_deep_merge
from fn_service.server import RequestProcessId, LoggerState


class ItemSkipped(Exception):
    '''Did not scrape an item, but not for a negative reason. Examples include:
        - Already scraped this session
        - Server returned item of type we will not scrape.
    '''
    pass


class ItemIgnored(ItemSkipped):
    '''Did not scrape an item but also not log it. '''
    pass


class ScraperError(Exception):
    '''An exception that will break out of the scraper_loop function. '''
    pass


@argument('--s3_skip_checks', action='store_true',
          help='Always skip doc service checks when uploading')
@argument('--extraction_flag', help='Set a global extraction_params update_flag')
class GenericScraper(object):
    '''
    Generic scrper - designed for the most common scraper uses.

    Instantiates common.http and common.files Session objects.
    '''

    @injector.inject(
        args=ScraperArguments, publisher=ScrapeItemPublisher,
        files_session=FilesSession, http_session=HttpSession,
        logger_state=LoggerState, process_id=RequestProcessId)
    def __init__(self, **kwargs):
        kwargs['http_session'].set_as_instance()
        kwargs['files_session'].set_as_instance()
        kwargs['files_session'].skip_checks = kwargs['args'].s3_skip_checks
        self.args = kwargs['args']
        self._publisher = kwargs['publisher']
        self.process_id = kwargs['process_id']
        self.logger_state = kwargs['logger_state']
        try:
            super(GenericScraper, self).__init__(**kwargs)
        except TypeError:
            super(GenericScraper, self).__init__()


    def scrape(self):
        raise NotImplementedError()


    def publish_json(self, routing_key, message, source=None, exchange=None,
                     encoder=None):
        encoder = encoder or JSONEncoderPlus
        source = source or self.__class__.__name__
        exchange = exchange or ""

        if not encoder:
            encoder = JSONEncoderPlus

        self._publisher.publish_json_item(
            exchange, routing_key, source, message, json_encoder=encoder)


    def scraper_loop(self, iterable, consumer, name=None, logger=None, log_ok=True):
        '''
        Functional scraper loop implementation with logging and exception
        handling.
        
        :param iterable: A callable iterator that returns a two-tuple of format
            (item, event_keys). `item` will be passed to the consumer while 
            `event_keys` will be used for logging.

            event_keys is excpected to be a string if a scalar is used, or a
            dictionary if a set of values are to be used (session/external_id)
        :param consumer: A callable that accepts the `item` from the generator. Can
            optionally return a dictionary that will be used for logging purposes.
        :param name: Optional scraper common name used for logging (defaults to
            class name)
        :param logger: Optional logger to use (will construct one if none)
        :param log_ok: Whether to log ok messages on success
        
        '''

        def _log(severity, event_type, name='', **kwargs):
            '''Internal logging logic. '''
            keyword_args = {
                k: kwargs[k] for k in ['event_keys', 'extra_info']
                if k in kwargs}
            # msg can be explicit, looked up, or just event_type
            msg_opts = {
                'scrape_skipped': u'Skipped scrape',
                'failed_scrape': u'Failed scrape',
                'scrape_success': u'Successful scrape',
            }
            msg = kwargs.get('msg', msg_opts.get(event_type, event_type))
            if name:
                msg = u'{} "{}"'.format(msg, name)
            if kwargs.get('result'):
                msg = u'{} - {}'.format(msg, kwargs['result'])
            if kwargs.get('exception'):
                msg = u'{} \n{}'.format(msg, traceback.format_exc())
                if 'extra_info' not in keyword_args:
                    keyword_args['extra_info'] = {}
                keyword_args['extra_info'].update({
                    'exception_name': kwargs['exception'].__class__.__name__,
                    'exception_msg': unicode(kwargs['exception'])
                })
                if hasattr(kwargs['exception'], 'log_info'):
                    dict_deep_merge(keyword_args, kwargs['exception'].log_info)
            logger.log(severity, msg, event_type=event_type, **keyword_args)

        logger = logger or logging.getLogger(self.__class__.__name__)

        for item, event_keys in iterable:
            if not isinstance(event_keys, Mapping):
                event_keys = {'obj_id': event_keys}
            with self.logger_state.set_event_keys(event_keys):
                try:
                    result = consumer(item)
                except (KeyboardInterrupt, SystemExit, ScraperError):
                    raise
                except ItemIgnored:
                    pass
                except ItemSkipped as exception:
                    _log(OK_LVL, "scrape_skipped", name, exception=exception)
                except Exception as exception:
                    _log(logging.CRITICAL, 'failed_scrape', name, exception=exception)
                else:
                    level = OK_LVL if log_ok else logging.INFO
                    kwargs = {}
                    if result and isinstance(result, dict):
                        kwargs = result
                    elif result:
                        kwargs['result'] = str(result)
                    _log(level, 'scrape_success', name, **kwargs)

from __future__ import absolute_import

from datetime import datetime
import injector
import requests
import json
import pytz
from future.utils import iteritems

from thrift.protocol import TBinaryProtocol

from fn_rabbit.event_publisher import BlockingEventPublisher

from fn_scrapers.internal.config import get_config

from fn_service.server import per_app, per_request, RequestProcessId, Config, Reactor, ComponentName
from fn_service.components.logging import RequestEventLogExtra
from fn_service.util.postgres import create_pg_engine
from fn_service.util.blocking_client import RequestsHttpTransport

from fn_scraperutils.scraper import Scraper as ScraperUtilsScraper
from fn_scraperutils.config import Config as ScraperUtilsConfig
from fn_scraperutils.events.reporting import EventComponent as ScraperUtilsEventComponent
from fn_scraperutils.doc_service.doc_service_client import DocServiceClient as ScraperUtilsDocServiceClient
from fn_scraperutils.doc_service.transfer_to_s3 import S3Transferer as ScraperUtilsS3Transferer
from fn_scraperutils.request.blocking_client import BlockingClient as ScraperUtilsBlockingClient

from fn_ratelimiter_client.blocking_client import BlockingRateLimiterClientFactory, BlockingRateLimiterClient
from fn_ratelimiter_client.blocking_util import RETRY500_REQUESTS_RETRY_POLICY
from fn_ratelimiter_client.blocking import BlockingFetcherFactory

from fn_ratelimiter_common.config import parse_config as parse_ratelimiter_config

from fn_dataaccess_client.blocking.locality_metadata import LocalityMetadataDataAccess

from fn_scrapers.common import files, http
from fn_scrapers.common.files import Session as FilesSession
from fn_scrapers.common.http import Session as HttpSession

BlockingRetryingPublisherManager = injector.Key("BlockingRetryingPublisherManager")

BlockingRetryPolicy = injector.Key("BlockingRetryPolicy")

ScrapeStartTime = injector.Key("ScraperStartTime")

ScraperArguments = injector.Key("ScraperArguments")

ScraperName = injector.Key("ScraperName")

PillarDb = injector.Key("PillarDb")

ScraperDb = injector.Key("ScraperDb")

ScraperDbSessionMaker = injector.Key("ScraperDbSessionMaker")

ScraperConfig = injector.Key("ScraperConfig")

Tags = injector.Key("Tags")

# We use this to store a request.Session that we use for Thrift
# clients.
_ThriftSession = injector.Key("_ThriftSession")


class AppModule(injector.Module):
    def __init__(self, args):
        self.args = args

    def configure(self, binder):
        binder.bind(BlockingRetryPolicy, to=RETRY500_REQUESTS_RETRY_POLICY)
        binder.bind(ScraperArguments, to=self.args)
        binder.bind(_ThriftSession, to=requests.Session)

    # BlockingFetcherFactory is thread-safe and we don't want to destroy and re-create the connection
    # pools that it holds on every request. So, we make it @per_app scoped.
    @injector.provides(BlockingFetcherFactory)
    @per_app
    @injector.inject(reactor=Reactor)
    def _provide_blocking_fetcher_factory(self, reactor):
        return BlockingFetcherFactory(
            reactor,
            config=parse_ratelimiter_config(json.loads(get_config("ratelimiter-config.json"))))

    @injector.provides(BlockingRateLimiterClientFactory)
    @per_app
    def _provide_blocking_rate_limiter_client_factory(self):
        return BlockingRateLimiterClientFactory(
            config=parse_ratelimiter_config(json.loads(get_config("ratelimiter-config.json"))))

    # BlockingRateLimiterClient is thread-safe and we don't want to destroy and re-create the connection
    # pools that it holds on every request. So, we make it @per_app scoped.
    @injector.provides(BlockingRateLimiterClient)
    @per_app
    @injector.inject(factory=BlockingRateLimiterClientFactory)
    def _provide_blocking_rate_limiter_client(self, factory):
        return factory.create_blocking_rate_limiter_client()

    @injector.provides(PillarDb)
    @per_app
    @injector.inject(config=Config)
    def _provide_pillar_db(self, config):
        return create_pg_engine(config.app["global"]["pillar_db"])

    @injector.provides(ScraperDb)
    @per_app
    @injector.inject(config=Config)
    def _provide_scraper_db(self, config):
        return create_pg_engine(config.app["global"]["scraper_db"])

    @injector.provides(ScraperDbSessionMaker)
    @per_app
    @injector.inject(db=ScraperDb)
    def _provide_scraper_db_session_maker(self, db):
        from sqlalchemy.orm import sessionmaker
        return sessionmaker(bind=db)

    @injector.provides(ScraperConfig)
    @injector.inject(config=Config, scraper_name=ScraperName)
    def _provide_scraper_config(self, config, scraper_name):
        if 'scrapers' not in config.app:
            raise ValueError(
                "Missing app.scrapers configuration entry. See scrapers/"
                "README.md for more information.")
        return config.app["scrapers"].get(scraper_name, {})

    @injector.provides(HttpSession)
    @per_request
    @injector.inject(rate_limiter_client=BlockingRateLimiterClient)
    def _provide_http_session(self, rate_limiter_client):
        return http.Session(rl_config=rate_limiter_client)

    @injector.provides(FilesSession)
    @per_request
    @injector.inject(config=Config)
    def _provide_files_session(self, config):
        aws_config = config.app['scraperutils']['aws'].copy()
        aws_config.update(config.app['scraperutils']['file_upload_bucket'])
        return files.Session(aws_config, config.app["global"]["doc_service_url"])

    @injector.provides(LocalityMetadataDataAccess.Client)
    @per_request
    @injector.inject(config=Config, thrift_session=_ThriftSession)
    def _provide_metadata_client(self, config, thrift_session):
        data_transport = RequestsHttpTransport(
            thrift_session,
            config.app["global"]["metadata_url"],
            config.app["global"]["metadata_timeout"])
        data_protocol = TBinaryProtocol.TBinaryProtocol(data_transport)
        return LocalityMetadataDataAccess.Client(data_protocol)


class ScraperUtilsSupportModule(injector.Module):
    @injector.provides(ScraperUtilsScraper)
    @per_request
    @injector.inject(
        scraper_type=ScraperUtilsEventComponent,
        component_name=ComponentName,
        process_id=RequestProcessId,
        blocking_publisher_manager=BlockingRetryingPublisherManager,
        blocking_ratelimiter_client=ScraperUtilsBlockingClient,
        scrape_start_time=ScrapeStartTime,
        blocking_retry_policy=BlockingRetryPolicy,
        s3_transferer=ScraperUtilsS3Transferer,
        scraper_utils_config=ScraperUtilsConfig,
        doc_service_client=ScraperUtilsDocServiceClient,
        metadata_client=LocalityMetadataDataAccess.Client)
    def _provide_scraper_obj(
            self,
            scraper_type,
            component_name,
            process_id,
            blocking_publisher_manager,
            blocking_ratelimiter_client,
            scrape_start_time,
            blocking_retry_policy,
            s3_transferer,
            scraper_utils_config,
            doc_service_client,
            metadata_client):
        return ScraperUtilsScraper(
            scraper_type=scraper_type,
            process_id=process_id,
            publisher=BlockingEventPublisher(component_name, blocking_publisher_manager),
            ratelimiter_client=blocking_ratelimiter_client,
            scrape_start_time=scrape_start_time,
            retry_policy=blocking_retry_policy,
            s3_transferer=s3_transferer,
            config=scraper_utils_config,
            doc_service_client=doc_service_client,
            metadata_client=metadata_client)
    
    @injector.provides(ScraperUtilsConfig)
    @per_app
    @injector.inject(config=Config)
    def _provide_scraper_utils_config(self, config):
        from fn_scraperutils.config import parse_config
        return parse_config(config.app["scraperutils"])

    # ScraperUtilsS3Transferer appears to be thread-safe - but, its also so cheap
    # to construct, there isn't much point it making it app-scoped.
    @injector.provides(ScraperUtilsS3Transferer)
    @per_request
    @injector.inject(scraper_utils_config=ScraperUtilsConfig)
    def _provide_s3_transferer(self, scraper_utils_config):
        return ScraperUtilsS3Transferer(scraper_utils_config)

    @injector.provides(ScraperUtilsDocServiceClient)
    @per_request
    @injector.inject(config=Config, thrift_session=_ThriftSession)
    def _provide_doc_service_client(self, config, thrift_session):
        return ScraperUtilsDocServiceClient(
            config.app["global"]["doc_service_url"],
            config.app["global"]["doc_service_timeout"],
            thrift_session,
        )

    # ScraperUtilsBlockingClient appears to be thread-safe - but, its also so cheap
    # to construct, there isn't much point it making it app-scoped.
    @injector.provides(ScraperUtilsBlockingClient)
    @per_request
    @injector.inject(bff=BlockingFetcherFactory)
    def _provide_blocking_client(self, bff):
        return ScraperUtilsBlockingClient(bff)


class ScraperModule(injector.Module):
    def __init__(self, scraper_name):
        self.scraper_name = scraper_name

    def configure(self, binder):
        binder.bind(ScraperName, to=self.scraper_name)
        binder.bind(ComponentName, to="fnscrapers_" + self.scraper_name.lower())


def _make_tags_json_serializable(tags):
    # The tags are a dictionary mapping tag names sets of
    # tag values. sets can't be serialized in JSON. So, convert
    # the sets into lists.
    return {tag_key: list(tag_value) for tag_key, tag_value in iteritems(tags)}


class ScraperRequestModule(injector.Module):
    def __init__(self, scrape_start_time, tags):
        self.scrape_start_time = scrape_start_time
        self.tags = tags

    def configure(self, binder):
        binder.bind(ScrapeStartTime, to=self.scrape_start_time)
        binder.bind(Tags, to=self.tags)
        binder.bind(RequestEventLogExtra, to={"tags": _make_tags_json_serializable(self.tags)})

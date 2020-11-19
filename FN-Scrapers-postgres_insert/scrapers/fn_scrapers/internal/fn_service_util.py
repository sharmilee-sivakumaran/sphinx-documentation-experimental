from __future__ import absolute_import

import io
import injector
import json
import yaml

from fn_rabbit.load import load_router

from fn_service.rmq import RmqRouter
from fn_service.server import GlobalSetupConfig

from .config import get_config


class FnServiceConfigModule(injector.Module):
    @injector.provides(RmqRouter)
    def _provide_rmq_router(self):
        return load_router(config=json.loads(get_config("fn_rabbit.json")))


def get_global_setup_config():
    return GlobalSetupConfig() \
        .with_logging(yaml.safe_load(io.BytesIO(get_config("logging.yaml"))))

from __future__ import absolute_import

import argparse
import logging
import os
import yaml

import psycopg2
import psycopg2.extras

from sql_comp.formatters import get_formatters
from sql_comp.plugins import get_plugins
from sql_comp.sql_comp import SqlComp
CONN_STRING = ("dbname='{database}' host='{host}' user='{username}' "
               "password='{password}'")

def load_config():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with open('settings.yaml') as fp:
        config = yaml.safe_load(fp)
    with open('settings.local.yaml') as fp:
        local_config = yaml.safe_load(fp)

    def _merge(dic1, dic2):
        for key in dic2:
            if key not in dic1:
                dic1[key] = dic2[key]
            elif all(isinstance(d[key], dict) for d in (dic1, dic2)):
                _merge(dic1[key], dic2[key])
            else:
                dic1[key] = dic2[key]

    _merge(config, local_config)
    return config


def parse_args(config):
    parser = argparse.ArgumentParser(
        prog='sql_comp',
        description='Runs a sequence of scripts against two sql environments, '
                    'comparing the outputs.')
    subparsers = parser.add_subparsers(help='plugins', dest='plugin_name')
    plugins = {}
    for plugin_module in get_plugins():
        for key in plugin_module.__dict__:
            if not hasattr(plugin_module.__dict__[key], '__bases__'):
                continue
            if SqlComp not in plugin_module.__dict__[key].__bases__:
                continue
            plugin = plugin_module.__dict__[key]
            break
        else:
            print("Bad module: " + plugin_module.__file__)
            continue
        plugins[plugin_module.__name__] = plugin
        doc = (plugin_module.__doc__ or '').strip().split('\n')[0]
        subparser = subparsers.add_parser(
            plugin_module.__name__, help=doc)
        plugin.parser(config, subparser)

    args = parser.parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.getLevelName(args.logging.upper())
    )

    args.plugin = plugins[args.plugin_name]
    args.format = get_formatters()[args.format]


    return args


def main(args=None):
    config = load_config()
    if not args:
        args = parse_args(config)

    instance = args.plugin(args, config)
    instance.run()
    # try:
    #     for i, env in enumerate([args.left, args.right]):
    #         if not env:
    #             env = args.plugin.get_env(config, i)
    #         connections[i] = psycopg2.connect(CONN_STRING.format(
    #             **(config['envs'][env])
    #         ))
    #         cursors[i] = connections[i].cursor(
    #             cursor_factory=psycopg2.extras.DictCursor)
    #         logging.info("Connected to %s.", env)
    #     set_cursors(*cursors)
    #     args.plugin.run(args, config)
    # finally:
    #     for i in range(2):
    #         if cursors[i]:
    #             cursors[i].close()
    #         if connections[i]:
    #             connections[i].close()


if __name__ == '__main__':
    main()
from __future__ import print_function, division

import argparse
from collections import Counter, defaultdict
import json
import logging
import os
from pprint import pprint
import re
import time
import yaml

from slackclient import SlackClient

logging.getLogger().setLevel(logging.DEBUG)

def main(channel, days=1, max_count=10):
    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, 'settings.local.yaml')) as fp:
        settings = yaml.safe_load(fp)
    if channel not in settings['channels']:
        logging.critical("Channel %s not configured.", channel)
        return
    slack_client = SlackClient(settings['fn_bot'])
    if not slack_client.rtm_connect(with_team_state=False):
        logging.critical("Could not connect to slack servers")
        return
    channels = slack_client.api_call('channels.list')
    for chnl in channels['channels']:
        if chnl['name'] == channel:
            break
    else:
        logging.critical("Could not find channel %s on slack", channel)
        return
    latest = time.time()
    oldest = latest - 24*3600 * days
    counts = None
    patterns = settings['channels'][channel]['patterns']
    for message in get_message(slack_client, chnl['id'], oldest, latest):
        counts = count_sources(message, patterns, counts)
    print(generate_report(counts, max_count))

def get_message(client, channel_id, oldest, latest):
    has_more = True
    while has_more:
        messages = client.api_call(
            'channels.history', timeout=30, channel=channel_id, latest=latest,
            oldest=oldest, count=1000)
        logging.debug("%s - %s : %s", oldest, latest, len(messages['messages']))

        for message in messages['messages']:
            yield message
        has_more = messages['has_more']
        latest = messages['messages'][-1]['ts']

def count_sources(message, patterns, counts=None):
    if counts is None:
        counts = defaultdict(lambda: defaultdict(int))
    text = message['text']
    for pattern_def in patterns:
        name = pattern_def['name']
        pattern = re.compile(pattern_def['pattern'])
        form = pattern_def.get('format', '{0}')
        for match in pattern.findall(text):
            if not isinstance(match, tuple):
                match = (match, )
            counts[name][form.format(*match)] += 1
    counts['_meta']['total']  += 1
    return counts

def generate_report(counts, max_count):
    output = []
    total = counts['_meta']['total']
    for key in sorted(counts.keys()):
        items = counts[key]
        output.append(key)
        output.append('- '*40)
        record_count = 0
        for val, c in sorted(items.items(), key=lambda x: (x[1], x[0]), reverse=True):
            record_count += 1
            if record_count > max_count:
                record_count = 0
                break
            output.append('{:>5} ({:5.2f}%): {}'.format(c, 100*c/total, val))
        output.append('')
    return '\n'.join(output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', '-d', type=int, default=1)
    parser.add_argument('--count', '-c', type=int, default=10)
    parser.add_argument('--channel', default='di_events')
    args = parser.parse_args()
    main(args.channel, args.days, args.count)

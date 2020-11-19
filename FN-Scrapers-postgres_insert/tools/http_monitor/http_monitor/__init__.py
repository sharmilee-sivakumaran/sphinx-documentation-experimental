

import boto
import json
import logging
import os
import re
import traceback
import yaml

import requests

from common import files, http

CONFIG = None
LOGGER = None

def main():
    '''Main entry point. '''
    global CONFIG, LOGGER
    logging.basicConfig(level=logging.DEBUG)
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    cwd = os.path.dirname(os.path.abspath(__file__))

    os.chdir(cwd)
    try:
        with open('settings.yaml') as fp:
            CONFIG = yaml.safe_load(fp)
    except IOError:
        LOGGER.critical("Could not load settings.yaml")
        return

    with open(os.path.expanduser('~/.boto')) as fp:
        boto_contents = fp.read()
    aws_key = re.search(r'aws_access_key_id\s*=\s*(.*)', boto_contents).group(1)
    aws_secret = re.search(r'aws_secret_access_key\s*=\s*(.*)', boto_contents).group(1)

    files.Session({
        "access_key": aws_key,
        "secret_access_key": aws_secret,
        "s3_endpoint": CONFIG['network']['s3_endpoint'],
        "bucket": CONFIG['network']['s3_bucket']
    }, CONFIG['network']['docservice_host']).set_as_instance()

    for website in CONFIG['websites']:
        LOGGER.info("Checking [{}]".format(website['url']))
        try:
            if not website.get('type'):
                basic_process(website)
        except:
            pass # TODO: 
        
        # add other "types" of websites here

def basic_process(website):
    url = website['url']

    fil = files.request_file_with_cache(url)

    if not website.get('type'):
        if fil.is_cached:
            LOGGER.info("No change")
            return
        if fil.ldi and fil.ldi.fileHash and fil.ldi.fileHash != fil.hash():
            fil.upload_and_register()
            LOGGER.info("Update detected - sending message")
            send_slack_update(
                u'"{name}" updated!\n\t{url}\n\t<{s3old}|previous> \u2192 <{s3new}|current>'.format(
                    s3old=fil.ldi.s3Url, s3new=fil.s3_url, **website))
        elif not fil.ldi or not fil.ldi.fileHash:
            fil.upload_and_register()
            LOGGER.info("New download")
            send_slack_update('Started Monitoring {name}! {url}'.format(**website))
        return

def send_slack_update(text):
    message = {'text': text, "unfurl_media": False, "unfurl_links": False}
    return requests.post(CONFIG['slack']['webhook'], data=json.dumps(message))


if __name__ == '__main__':
    main()
from __future__ import absolute_import
import os
import subprocess32 as subprocess
import re
from tempfile import NamedTemporaryFile
from ..http import request_file


def convert_pdf(filename, type='xml', decode=True):
    commandline = {'text': ['pdftotext', '-layout', filename, '-'],
                'text-nolayout': ['pdftotext', filename, '-'],
                'xml': ['pdftohtml', '-xml', '-stdout', filename],
                'html': ['pdftohtml', '-stdout', filename]}
    try:
        FNULL = open(os.devnull, 'w')
        pipe = subprocess.Popen(commandline[type], stdout=subprocess.PIPE,
                                stderr=FNULL, close_fds=True).stdout
    except OSError as e:
        raise EnvironmentError("error running %s, missing executable? [%s]" %
                               (' '.join(commandline[type]), e))
    data = pipe.read()
    pipe.close()
    if decode and (type == 'text' or type == 'text-nolayout'):
        # Convert hyphens to retain syntax.
        data = re.sub('\xad', '-', data)
        # By default pdftotext uses "Latin1" encoding - decode it to eliminate unicode errors.
        data = data.decode("latin1")
        data = data.encode("ascii", "ignore")

    return data


def get_pdf_text(url, type='text', **kwargs):
    file_obj = NamedTemporaryFile()
    file_obj, resp = request_file(url, file_obj=file_obj, **kwargs)
    return convert_pdf(file_obj.name, type)

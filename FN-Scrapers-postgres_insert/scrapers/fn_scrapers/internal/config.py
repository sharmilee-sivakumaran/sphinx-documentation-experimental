"""
This module contain the config loading infrastructure. Basically,
whenever FN-Scrapers needs to load a config file, it should
load it from here. However, before it can do that, each available
config file must be setup during startup.
"""

from __future__ import absolute_import

import os
import threading
import errno

from .unix_util import CloseFds, set_cloexec


LOCK = threading.Lock()
CONFIGS = {}


def set_config_from_fd(config_name, fd):
    """
    Setup the configuration file named config_name to be loaded by reading the
    specified fd. Note, after calling this function, it is assumed that this
    module takes ownership of the fd and will be responsible for closing it.

    The file descriptor that is passed in must be marked with FD_CLOEXEC.
    """
    with LOCK:
        if config_name in CONFIGS:
            raise Exception(u"We already have a config for '{}'".format(config_name))
        CONFIGS[config_name] = fd


def set_config_from_func(config_name, func):
    """
    Setup the configuration file named config_name to be loaded by calling
    the specified function with a single argument - the config_name.
    """
    with LOCK:
        if config_name in CONFIGS:
            raise Exception(u"We already have a config for '{}'".format(config_name))
        CONFIGS[config_name] = func


def has_config(config_name):
    """
    Return if the specified configuration is available.
    """
    with LOCK:
        return config_name in CONFIGS


def get_config_names():
    """
    Return all the available config names as a set.
    """
    with LOCK:
        # Py3 compatibility - keys() returns an iterator in Py3
        return set(CONFIGS.keys())


def get_config(config_name):
    """
    Get the named config as a bytes value. This method may block.
    """
    with LOCK:
        if config_name not in CONFIGS:
            raise Exception(u"No such config '{}'".format(config_name))

        config_val = CONFIGS[config_name]
        if isinstance(config_val, bytes):
            # If we have bytes, it means we've already
            # loaded it, so, return it.
            return config_val
        elif isinstance(config_val, int):
            # If we have an int, it means we have a
            # file-descriptor we can read to get the
            # config contents.

            # Blank out the config - we must get the
            # config from reading the file descriptor.
            # If that fails, we can't recover.
            CONFIGS[config_name] = None

            # When f is closed, that also closes the file
            # descriptor
            # NOTE: We have to close the file descriptor here -
            # we can't seek back to the beginning since it
            # might reference a pipe. So, we have to store
            # the config value in memory since we might need
            # it again.
            with os.fdopen(config_val) as f:
                config_data = f.read()

            CONFIGS[config_name] = config_data
            return config_data
        elif hasattr(config_val, "__call__"):
            config_data = config_val(config_name)
            CONFIGS[config_name] = config_data
            return config_data
        else:
            raise Exception(u"We have a bad value for config '{}'. Did an error occur loading it? "
                            u"This is, unfortunately, unrecoverable.".format(config_name))


def get_config_fd(config_name, cloexec=True):
    """
    Get a file-descriptor that can be read from to read in the
    specified configuration file. The caller is returned that
    file-descriptor directly and takes responsibility for
    closing it.

    NOTE: The implementation relies on running a worker
    Thread that will exit once the read file-descriptor exits.
    This means that that read file-descriptor really must get closed
    to free up this thread. It also means that the file descriptor
    can't be used after os.exec*() methods - since that will cause
    the worker thread to stop!
    """
    config_val = get_config(config_name)

    def _pass(writer):
        try:
            writer.write(config_val)
        except OSError as e:
            if e.errno != errno.EPIPE:
                raise

    with CloseFds() as close_fds:
        r, w = os.pipe()
        close_fds.add_all([r, w])

        # NOTE: There is a minor race here - r and w are both initially
        # created with FD_CLOEXEC *not* set (at least on Python 2.7 - one
        # of the Python 3 versions changes this). So, if another thread were
        # to fork, it could cause one of these file-descriptors to get
        # accidentally inherited. There isn't much that can be done about
        # this on Python 2 (without using ctypes). More significantly,
        # FN-Scrapers never actually does this, so, its not a problem
        # in practice, for current usage.
        set_cloexec(r, cloexec=cloexec)
        set_cloexec(w, cloexec=True)

        # If fdopen() fails, it doesn't close the file descriptor. So,
        # we don't want to remove the file descriptor from close_fds
        # until that has already succeeded.
        writer_ = os.fdopen(w, "w")
        close_fds.remove(w)
        threading.Thread(target=_pass, args=(writer_,)).start()

        close_fds.remove(r)
        return r

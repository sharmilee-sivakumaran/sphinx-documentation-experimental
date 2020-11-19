from __future__ import absolute_import

import errno
import fcntl
import os
import sys


class CloseFds(object):
    """
    CloseFds provides a context manager that can be used
    to make sure a set of file descriptors are all closed.
    File descriptors can be added and removed from the object
    as needed. When CloseFds is closed, all file descriptors
    currently registered are also closed.
    """
    def __init__(self, initial_fds=None):
        self._fds = set()
        if initial_fds is not None:
            self._fds.update(initial_fds)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None  # re-raise the Exception

    def add(self, fd):
        if fd in self._fds:
            raise Exception(u"FD {} is already in _CloseFds set".format(fd))
        self._fds.add(fd)

    def add_all(self, fds):
        for fd in fds:
            self.add(fd)

    def remove(self, fd):
        if fd not in self._fds:
            raise Exception(u"FD {} is not in _CloseFds set".format(fd))
        self._fds.remove(fd)

    def remove_all(self, fds):
        for fd in fds:
            self.remove(fd)

    def close(self):
        exc_info = None
        while self._fds:
            try:
                fd = self._fds.pop()
                os.close(fd)
            except OSError as err:
                if err != errno.EINTR:
                    if exc_info is None:
                        exc_info = sys.exc_info()
        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]


def set_cloexec(fd, cloexec=True):
    """
    Set or unset the FD_CLOEXEC flag on the given file descriptor.
    :param fd: The file descriptor
    :param cloexec: Whether to set or unset the flag
    """
    if cloexec:
        fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.fcntl(fd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)
    else:
        fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.fcntl(fd, fcntl.F_GETFD) & ~fcntl.FD_CLOEXEC)


def eintr_retry_call(func, *args):
    """
    Execute the given function with the given arguments. If it fails
    with EINTR - which may happen if it is interrupted by a signal -
    retry it until it completes (or fails with some other error code).
    This is primarily interesting for wrapping around functions in
    the os module.
    """
    while True:
        try:
            return func(*args)
        except (OSError, IOError) as ex:
            if ex.errno == errno.EINTR:
                continue
            raise

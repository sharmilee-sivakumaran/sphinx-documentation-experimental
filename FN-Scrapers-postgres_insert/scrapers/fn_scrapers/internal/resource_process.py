from __future__ import absolute_import

import errno
import os
import signal
import time

import subprocess32 as subprocess

from .unix_util import eintr_retry_call


class ResourceProcess(object):
    """
    Wrap a subprocess.Process object to give it a new feature - we wait
    for the child process to exit using os.wait4() which means that we can
    get resource information from the child process when it exits. Otherwise,
    provide an interface much like the one provided by subprocess.Process with
    3 main differences:

    1. Once the child process has been wait()ed on, the resource_usage attribute
       provides details of the resources that the child process used.
    2. Only the subset of the subprocess.Process interface that we need is implemented.
       More interfaces can be implemented as they are needed.
    3. This class is only meant to be used by a single thread.
    """
    def __init__(self, process):
        self._process = process
        self._child_exited = False
        self.resource_usage = None
        self.returncode = None

    def wait(self, timeout=None):
        if self._child_exited:
            return self.returncode

        if timeout >= 0:
            wait_until = time.time() + timeout
        else:
            wait_until = None

        while True:
            try:
                pid, exit_state, resource_usage = eintr_retry_call(os.wait4, self._process.pid, os.WNOHANG)
            except OSError as ex:
                if ex.errno != errno.ECHILD:
                    raise
                else:
                    # This can happen if the program has done something wonky to disable
                    # waiting for child processes, but, then tries to do so anyway.
                    # In that case, subprocess will return an exit status of 0.
                    # We don't do that, however, since we also need to make
                    # the resource usage available. So, instead, we raise an Exception
                    # if that occurs.
                    self._child_exited = True
                    raise Exception("Failed to wait for child. Did you disable waiting for child processes?")

            # If the waited for process exits, we process its exit information and
            # then return
            if pid == self._process.pid:
                self._child_exited = True
                self.resource_usage = resource_usage
                if os.WIFSIGNALED(exit_state):
                    self.returncode = -os.WTERMSIG(exit_state)
                elif os.WIFEXITED(exit_state):
                    self.returncode = os.WEXITSTATUS(exit_state)
                else:
                    raise Exception("Unknown child exit status!")
                return self.returncode

            # If the waited for process is still running, either raise a TimeoutExpired
            # exception, or, wait longer, depending on the timeout. We currently just wait
            # 1 second at a time - we could be smarter about this in the future.
            else:
                if wait_until is not None and (timeout == 0 or time.time() > wait_until):
                    raise subprocess.TimeoutExpired(self._process.args, timeout)
                else:
                    time.sleep(1)

    def poll(self):
        try:
            self.wait(timeout=0)
        except subprocess.TimeoutExpired:
            pass
        return self.returncode

    def kill(self):
        if self._child_exited:
            return
        os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        return self._process.pid

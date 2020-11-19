from __future__ import absolute_import

import abc
import contextlib
from future.utils import with_metaclass
import json
import queue
import logging
import random
import sqlite3
import time
import threading

logger = logging.getLogger(__name__)

def TIME_TO_STOP(stop_event):
    return stop_event and stop_event.is_set()

class _PersistentWorkQueue(object):
    """
    A _PersistentWorkQueue stores a list of items that need
    to get executed as well as the status of those items (as
    _WorkItem instances). This class also provides methods
    to save and load the list of work. All methods are thread-safe
    so, they may be called from any thread to update the state
    of the work to be done.
    """

    def __init__(self, con, work_set):
        self.con = con
        self.work_set = work_set
        self.dirty_list = []
        self.lock = threading.Lock()

    @staticmethod
    @contextlib.contextmanager
    def create_work_queue(work_finder, persistence_file, parts, mypart):
        con = sqlite3.connect(persistence_file)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS work
                (rowid INTEGER PRIMARY KEY, item_json TEXT, is_done INTEGER, failed INTEGER)
            """)
            if con.execute("SELECT COUNT(*) FROM work").fetchone()[0] == 0:
                with con:
                    for x in work_finder():
                        con.execute(
                            "INSERT INTO work (item_json, is_done, failed) VALUES (?, ?, ?)",
                            (json.dumps(x), False, True))

            # Load the ID of each work item from the DB
            work_set = [
                row[0]
                for row in con.execute("SELECT rowid FROM work WHERE is_done = 0 AND rowid % ? = ?", (parts, mypart))
            ]
            random.shuffle(work_set)

            q = _PersistentWorkQueue(con, work_set)
            try:
                yield q
            finally:
                q.save_work()

        finally:
            con.close()

    def save_work(self):
        with self.lock, self.con:
            self.con.executemany(
                "UPDATE work SET is_done = ?, failed = ? WHERE rowid = ?",
                [
                    (is_done, failed, rowid)
                    for rowid, is_done, failed in self.dirty_list
                ])
            del self.dirty_list[:]

    def get_remaining_work(self):
        with self.lock:
            return list(self.work_set)

    def get_work_detail(self, rowid):
        with self.lock:
            result = self.con.execute("SELECT item_json FROM work WHERE rowid = ?", (rowid,))
            return next(result)[0]

    def complete_work(self, rowid):
        with self.lock:
            self.work_set.remove(rowid)
            self.dirty_list.append((rowid, True, False))

    def fail_work(self, rowid):
        with self.lock:
            self.work_set.remove(rowid)
            self.dirty_list.append((rowid, True, True))


def _start_threads(worker_creator, thread_count, thread_input_queue, work_queue, stop_event):
    def _run_loop():
        with worker_creator() as worker:
            while not TIME_TO_STOP(stop_event):
                data = thread_input_queue.get()
                try:
                    if data is None:
                        return
                    rowid, item_raw = data

                    try:
                        item = json.loads(item_raw)
                        logger.info("Starting work: %s", item_raw)
                        worker(item)
                        work_queue.complete_work(rowid)
                        logger.info("Completed work: %s", item_raw)
                    except:
                        work_queue.fail_work(rowid)
                        print "worker failed"
                        logger.exception("Work failed: %s", json.dumps(item))
                finally:
                    thread_input_queue.task_done()

        # Hack so that we check stop_event in the queueing thread
        # If the stop event is set when main thread is still doing 'blocking put',
        # This will cause that 'blocking put' to finish and on the next iteration we will reach the break point
        # If the stop event is set when the 'thread_input_queue' is doing 'join', this will clear the queue
        # so that the 'join' will complete
        logger.info("Time to stop")
        while True:
            try:
                thread_input_queue.get_nowait()
            except queue.Empty:
                break

    threads = [threading.Thread(target=_run_loop) for _ in range(thread_count)]
    for t in threads:
        t.start()
    return threads


def fork_join(work_finder, worker_creator, thread_count, persistence_file, parts, mypart, stop_event=None):
    with _PersistentWorkQueue.create_work_queue(work_finder, persistence_file, parts, mypart) as work_queue:
        thread_input_queue = queue.Queue(maxsize=thread_count * 1)

        # Startup all the threads - they will run until all
        # the work is done
        threads = _start_threads(worker_creator, thread_count, thread_input_queue, work_queue, stop_event)

        last_save = time.time()

        try:
            # Put all the work on the queue for threads to pick up
            # as they are able
            for rowid in work_queue.get_remaining_work():
                if TIME_TO_STOP(stop_event):
                    break
                thread_input_queue.put((rowid, work_queue.get_work_detail(rowid)))
                now = time.time()
                if now - last_save > 60:
                    work_queue.save_work()
                    last_save = now

            else:
                thread_input_queue.join()
        finally:
            # If there is any remaining work on the queue, drain it.
            # That work will be left marked as still not done.
            while True:
                try:
                    thread_input_queue.get_nowait()
                except queue.Empty:
                    break

            # Put a None item on the queue for each thread as a signal that
            # it should exit.
            for _ in range(thread_count):
                thread_input_queue.put(None)

            # Actually wait for all threads to complete.
            for t in threads:
                t.join()

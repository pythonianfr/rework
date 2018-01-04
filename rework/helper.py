import os
from threading import Thread
import socket
import time
import logging

from six.moves.queue import Queue

import psutil

from rework.schema import log


def memory_usage():
    process = psutil.Process(os.getpid())
    return int(process.memory_info().rss / float(2 ** 20))


def wait_true(func, timeout=6):
    outcome = []

    def loop():
        start = time.time()
        while True:
            if (time.time() - start) > timeout:
                return
            output = func()
            if output:
                outcome.append(output)
                return
            time.sleep(.1)

    th = Thread(target=loop)
    th.daemon = True
    th.start()
    th.join()
    assert outcome
    return outcome[0]


def guard(engine, sql, expr, timeout=6):

    def check():
        with engine.connect() as cn:
            return expr(cn.execute(sql))

    return wait_true(check, timeout)


def host():
    try:
        return socket.gethostbyname(socket.gethostname())
    except:  # things can get nasty :/
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 1))
        return s.getsockname()[0]


def kill(pid):
    psutil.Process(pid).kill()


def has_ancestor_pid(pid):
    parent = psutil.Process(os.getpid()).parent()
    while parent:
        if pid == parent.pid:
            return True
        parent = parent.parent()
    return False


# Logging


class PGLogHandler(logging.Handler):
    maxqueue = 100

    def __init__(self, task, sync=True):
        super(PGLogHandler, self).__init__()
        self.task = task
        self.sync = sync
        self.lastflush = time.time()
        self.queue = []
        self.formatter = logging.Formatter(
            '%(name)s:%(levelname)s: %(asctime)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def emit(self, record):
        self.queue.append(record)

        if ((time.time() - self.lastflush) > 1 or
            len(self.queue) > self.maxqueue):
            self.flush()

    def flush(self):
        if not self.queue:
            return

        values = [{'task': self.task.tid,
                   'tstamp': record.created,
                   'line': self.formatter.format(record)}
                  for record in self.queue]
        self.queue = []
        self.lastflush = time.time()

        def writeback_log(values, engine):
            sql = log.insert().values(values)
            with engine.connect() as cn:
                cn.execute(sql)

        th = Thread(target=writeback_log,
                    args=(values, self.task.engine))
        th.daemon = True
        # fire and forget
        th.start()
        if self.sync:
            th.join()

    def close(self):
        pass


class PGLogWriter(object):
    __slots__ = ('stream', 'handler', 'level')

    def __init__(self, stream, handler):
        self.stream = stream
        self.handler = handler
        if 'out' in self.stream:
            self.level = logging.INFO
        else:
            self.level = logging.WARNING

    def write(self, message):
        if not message.strip():
            return
        self.handler.emit(
            logging.LogRecord(
                self.stream, self.level, '', -1, message, (), ()
            )
        )

import os
from threading import Thread
import socket
from queue import Queue
import time
import logging

import psutil

from rework.schema import log


def host():
    try:
        return socket.gethostbyname(socket.gethostname())
    except:  # things can get nasty :/
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 1))
        return s.getsockname()[0]


def communicate(process, chunk_size=4196):
    """generator that yields events when contents are available in
    the outputs of the ``process``.

    Each time data is available from the process outputs, it copies
    the data into ``stdout`` or ``stderr`` and yield the correponding
    object.
    """
    queue = Queue()
    def pipereader(name, process):
        pipe = getattr(process, name)
        try:
            for content in iter(lambda: pipe.readline(chunk_size), b''):
                queue.put((name, content))
        finally:
            queue.put((name, None))
    th_stdout = Thread(target=pipereader, args=('stdout', process))
    th_stderr = Thread(target=pipereader, args=('stderr', process))
    th_stdout.start()
    th_stderr.start()
    nb_threads = 2
    while nb_threads:
        name, content = queue.get()
        if content is None: # not more to read
            nb_threads -= 1
            continue
        yield name, content
    th_stdout.join()
    th_stderr.join()


def kill(pid):
    psutil.Process(pid).kill()


def has_ancestor_pid(pid):
    parent = psutil.Process(os.getpid()).parent()
    while parent:
        if pid == parent.pid:
            return True
        parent = parent.parent()
    return False


def watch(proc, lines=25):
    for stream, line in read_proc_streams(proc, lines):
        print('* {} [{}] *'.format(proc.pid, stream), line)


def read_proc_streams(proc, lines=0):
    for idx, stream_line in enumerate(communicate(proc)):
        yield stream_line
        if lines and idx > lines:
            break


# Logging


class PGLogHandler(logging.Handler):
    maxqueue = 100

    def __init__(self, task):
        super(PGLogHandler, self).__init__()
        self.task = task
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

    def close(self):
        pass

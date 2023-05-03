import queue
import threading
from concurrent.futures import _base

from apscheduler.executors import pool
from apscheduler.schedulers.blocking import (
    BaseScheduler,
    BlockingScheduler
)

# this module contains two parts
# a) a vendored stripped down, less buggy version of the
#    stdlib concurrent.futures.ThreadPoolExecutor
# b) a specialisation of apscheduler executor and scheduler
#    using the former (to avoid a deadlock at shutdown time)


# this is a)

class Stop:
    pass


class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
        else:
            self.future.set_result(result)


class _ThreadPoolExecutor:

    def __init__(self, max_workers):
        self._max_workers = max_workers
        self._work_queue = queue.SimpleQueue()
        self._threads = set()
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def _worker(self):
        while True:
            work_item = self._work_queue.get(block=True)
            if work_item is Stop:
                # allow the other workers to get it
                self._work_queue.put(Stop)
                return
            work_item.run()

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = _base.Future()

            self._work_queue.put(_WorkItem(f, fn, args, kwargs))
            num_threads = len(self._threads)
            if num_threads < self._max_workers:
                t = threading.Thread(target=self._worker)
                t.start()
                self._threads.add(t)
            return f

    def shutdown(self, *a):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(Stop)
            for t in self._threads:
                t.join(timeout=1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        return False


# this is b)
# there we provide the appscheduler pool executor
# wrapping the above ThreadPoolExecutor

class threadpoolexecutor(pool.BasePoolExecutor):

    def __init__(self, max_workers=10, pool_kwargs=None):
        pool_kwargs = pool_kwargs or {}
        pool = _ThreadPoolExecutor(int(max_workers), **pool_kwargs)
        super().__init__(pool)


# this is an alternative implementation of the BackgroundScheduler
# using our simplified ThreadPoolExecutor

class schedulerservice(BlockingScheduler):

    def _create_default_executor(self):
        # here
        return threadpoolexecutor()

    def start(self, *args, **kwargs):
        if self._event is None or self._event.is_set():
            self._event = threading.Event()

        BaseScheduler.start(self, *args, **kwargs)
        self._thread = threading.Thread(
            target=self._main_loop,
            name='ReworkScheduler'
        )
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self, *args, **kwargs):
        super().shutdown(*args, **kwargs)
        # timeout=1
        # that's because we might have queued / unfinished things running
        # below us (by design, see monitor._schedule)
        self._thread.join(timeout=1)
        del self._thread

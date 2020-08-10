import os

from rework import api
from rework.worker import Worker
from rework.monitor import Monitor


def test_run_worker(engine):
    mon = Monitor(engine, maxruns=1)
    wid = mon.new_worker()

    t = api.schedule(engine, 'print_sleep_and_go_away', 0)
    worker = Worker(engine.url, wid, os.getppid(), maxruns=1)
    worker.run()

    assert t.state == 'done'

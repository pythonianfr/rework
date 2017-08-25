import time
import os
import sys
from contextlib import contextmanager

from sqlalchemy import create_engine

from rework.helper import has_ancestor_pid
from rework.schema import worker
from rework.task import grab_task


def running_sql(wid, running):
    value = {
        'running': running
    }
    if running:
        value['pid'] = os.getpid()
    return worker.update().where(
        worker.c.id == wid).values(
            **value
        )


def die_if_ancestor_died(ppid, wid):
    if not has_ancestor_pid(ppid):
        # Our ancestor does not exist any more
        # this is an ambiguous signal that we also must
        # go to bed.
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


@contextmanager
def running_status(engine, wid):
    with engine.connect() as cn:
        cn.execute(running_sql(wid, True))
    try:
        yield
    finally:
        with engine.connect() as cn:
            cn.execute(running_sql(wid, False))


def run_worker(dburi, worker_id, ppid, polling_period):
    engine = create_engine(dburi)

    with running_status(engine, worker_id):
        while True:
            die_if_ancestor_died(ppid, worker_id)
            task = grab_task(engine, int(worker_id))

            while task:
                task.run()
                task = grab_task(engine, worker_id)

            time.sleep(polling_period)

            # let's diligently emit what we're saying to watchers
            sys.stdout.flush()
            sys.stderr.flush()

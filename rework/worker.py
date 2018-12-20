import time
import os
from contextlib import contextmanager
import traceback

from sqlalchemy import create_engine, select

from rework.helper import (
    has_ancestor_pid,
    kill,
    memory_usage,
    utcnow
)
from rework.schema import worker
from rework.task import Task


def running_sql(wid, running, debugport):
    value = {
        'running': running,
        'debugport': debugport
    }
    if running:
        value['pid'] = os.getpid()
        value['started'] = utcnow()
    return worker.update().where(
        worker.c.id == wid).values(
            **value
    )


def death_sql(wid, cause):
    return worker.update().where(worker.c.id == wid).values(
        deathinfo=cause,
        running=False,
        finished=utcnow()
    )


def die_if_ancestor_died(engine, ppid, wid):
    if not has_ancestor_pid(ppid):
        # Our ancestor does not exist any more
        # this is an ambiguous signal that we also must
        # go to bed.
        with engine.begin() as cn:
            cn.execute(death_sql(wid, 'ancestor died'))
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


# Worker shutdown


def ask_shutdown(engine, wid):
    sql = worker.update().where(
        worker.c.id == wid
    ).values(
        shutdown=True
    )
    with engine.begin() as cn:
        cn.execute(sql)


def shutdown_asked(engine, wid):
    sql = select([worker.c.shutdown]).where(worker.c.id == wid)
    return engine.execute(sql).scalar()


def die_if_shutdown(engine, wid):
    if shutdown_asked(engine, wid):
        with engine.begin() as cn:
            cn.execute(death_sql(wid, 'explicit shutdown'))
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


@contextmanager
def running_status(engine, wid, debug_port):
    with engine.begin() as cn:
        cn.execute(running_sql(wid, True, debug_port or None))
    try:
        yield
    finally:
        with engine.begin() as cn:
            cn.execute(running_sql(wid, False, None))


def run_worker(dburi, worker_id, ppid, maxruns=0, maxmem=0,
               domain='default', debug_port=0):
    worker_id = int(worker_id)
    if debug_port:
        import pystuck
        pystuck.run_server(port=debug_port)

    engine = create_engine(dburi)

    try:
        with running_status(engine, worker_id, debug_port):
            _main_loop(engine, worker_id, ppid, maxruns, maxmem, domain)
    except Exception:
        with engine.begin() as cn:
            sql = worker.update().where(worker.c.id == worker_id).values(
                traceback=traceback.format_exc()
            )
            cn.execute(sql)
        raise
    except SystemExit as exit:
        raise


def heartbeat(engine, worker_id, ppid, maxmem):
    die_if_ancestor_died(engine, ppid, worker_id)

    mem = memory_usage(os.getpid())
    if (maxmem and mem > maxmem):
        ask_shutdown(engine, worker_id)

    die_if_shutdown(engine, worker_id)


def _main_loop(engine, worker_id, ppid, maxruns, maxmem, domain):
    runs = 0
    while True:
        heartbeat(engine, worker_id, ppid, maxmem)
        task = Task.fromqueue(engine, worker_id, domain)
        while task:
            task.run()

            # run count
            runs += 1
            if maxruns and runs >= maxruns:
                return

            heartbeat(engine, worker_id, ppid, maxmem)
            task = Task.fromqueue(engine, worker_id, domain)

        time.sleep(1)

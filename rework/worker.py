import time
import os
from contextlib import contextmanager
import traceback

from sqlalchemy import create_engine, select

from sqlhelp import select, update

from rework.helper import (
    has_ancestor_pid,
    kill,
    memory_usage,
    utcnow
)
from rework.task import Task


def running_sql(wid, running, debugport):
    value = {
        'running': running,
        'debugport': debugport
    }
    if running:
        value['pid'] = os.getpid()
        value['started'] = utcnow()
    return update('rework.worker').where(id=wid).values(**value)


def death_sql(wid, cause):
    return update('rework.worker').where(id=wid).values(
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
            death_sql(wid, 'ancestor died').do(cn)
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


# Worker shutdown


def ask_shutdown(engine, wid):
    with engine.begin() as cn:
        update('rework.worker').where(id=wid).values(
            shutdown=True
        ).do(cn)


def shutdown_asked(engine, wid):
    return select('shutdown').table('rework.worker').where(
        id=wid
    ).do(engine).scalar()


def die_if_shutdown(engine, wid):
    if shutdown_asked(engine, wid):
        with engine.begin() as cn:
            death_sql(wid, 'explicit shutdown').do(cn)
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


@contextmanager
def running_status(engine, wid, debug_port):
    with engine.begin() as cn:
        running_sql(wid, True, debug_port or None).do(cn)
    try:
        yield
    finally:
        with engine.begin() as cn:
            running_sql(wid, False, None).do(cn)


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
            update('rework.worker').where(id=worker_id).values(
                traceback=traceback.format_exc()
            ).do(cn)
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

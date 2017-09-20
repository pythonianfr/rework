import time
import os
import sys
from threading import Thread
from contextlib import contextmanager
import traceback

from sqlalchemy import create_engine, select

from rework.helper import has_ancestor_pid, kill, memory_usage
from rework.schema import worker
from rework.task import Task


def track_memory_consumption(engine, wid):
    mem = memory_usage()
    sql = worker.update().where(worker.c.id == wid).values(mem=mem)
    with engine.connect() as cn:
        cn.execute(sql)
    return mem


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


def death_sql(wid, cause):
    return worker.update().where(worker.c.id == wid).values(
        deathinfo=cause,
        running=False
    )


def die_if_ancestor_died(engine, ppid, wid):
    if not has_ancestor_pid(ppid):
        # Our ancestor does not exist any more
        # this is an ambiguous signal that we also must
        # go to bed.
        with engine.connect() as cn:
            cn.execute(death_sql(wid, 'ancestor died'))
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


# Worker shutdown


def ask_shutdown(engine, wid):
    sql = worker.update().where(
        worker.c.id == wid
    ).values(
        shutdown=True
    )
    with engine.connect() as cn:
        cn.execute(sql)


def shutdown_asked(engine, wid):
    sql = select([worker.c.shutdown]).where(worker.c.id == wid)
    return engine.execute(sql).scalar()


def die_if_shutdown(engine, wid):
    if shutdown_asked(engine, wid):
        with engine.connect() as cn:
            cn.execute(death_sql(wid, 'explicit shutdown'))
        raise SystemExit('Worker {} exiting.'.format(os.getpid()))


# Task abortion

@contextmanager
def abortion_monitor(engine, wid, task):

    def track_mem_and_die_if_task_aborted():
        while True:
            time.sleep(1)
            if task.status == 'done':
                return
            if not task.aborted:
                track_memory_consumption(engine, wid)
                continue

            task.finish()
            with engine.connect() as cn:
                diesql = death_sql(wid, 'Task {} aborted'.format(task.tid))
                cn.execute(diesql)
                kill(os.getpid())

    monitor = Thread(name='monitor_abort',
                     target=track_mem_and_die_if_task_aborted)
    monitor.daemon = True
    monitor.start()
    yield
    monitor.join()


@contextmanager
def running_status(engine, wid):
    with engine.connect() as cn:
        cn.execute(running_sql(wid, True))
    try:
        yield
    finally:
        with engine.connect() as cn:
            cn.execute(running_sql(wid, False))


def run_worker(dburi, worker_id, ppid, maxruns=0, maxmem=0):
    engine = create_engine(dburi)

    try:
        _main_loop(engine, worker_id, ppid, maxruns, maxmem)
    except Exception:
        with engine.connect() as cn:
            sql = worker.update().where(worker.c.id == worker_id).values(
                traceback=traceback.format_exc()
            )
            cn.execute(sql)
        raise
    except SystemExit as exit:
        raise


def _main_loop(engine, worker_id, ppid, maxruns, maxmem):
    with running_status(engine, worker_id):
        runs = 0
        while True:
            die_if_ancestor_died(engine, ppid, worker_id)
            if maxruns and runs >= maxruns:
                ask_shutdown(engine, worker_id)
            mem = track_memory_consumption(engine, worker_id)
            if maxmem and mem > maxmem:
                ask_shutdown(engine, worker_id)
            die_if_shutdown(engine, worker_id)

            task = Task.fromqueue(engine, int(worker_id))
            while task:
                with abortion_monitor(engine, worker_id, task):
                    task.run()
                    runs += 1
                task = Task.fromqueue(engine, worker_id)

            time.sleep(1)

            # let's diligently emit what we're saying to watchers
            sys.stdout.flush()
            sys.stderr.flush()

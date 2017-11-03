import os
import time
import subprocess as sub

import psutil

from sqlalchemy import create_engine

from rework.helper import host, guard
from rework.schema import worker, task


def spawn_worker(engine, maxruns, maxmem):
    wid = new_worker(engine)
    cmd = ['rework', 'new-worker', str(engine.url), str(wid), str(os.getpid()),
           '--maxruns', str(maxruns),
           '--maxmem', str(maxmem)]
    # NOTE for windows users:
    # the subprocess pid herein might not be that of the actual worker
    # process because of the way python scripts are handled:
    # +- thescript.exe <params>
    #   +- python.exe thescript.py <params>
    return wid, sub.Popen(cmd,
                          bufsize=1,
                          stdout=sub.PIPE, stderr=sub.PIPE)


def new_worker(engine):
    with engine.connect() as cn:
        return cn.execute(
            worker.insert().values(
                host=host()
            )
        ).inserted_primary_key[0]


def num_workers(engine):
    sql = ("select count(id) from rework.worker "
           "where host = %(host)s and running = true")
    with engine.connect() as cn:
        return cn.execute(sql, {'host': host()}).scalar()


def ensure_workers(engine, maxworkers, maxruns, maxmem):
    numworkers = num_workers(engine)

    procs = []
    for _ in range(maxworkers - numworkers):
        procs.append(spawn_worker(engine, maxruns, maxmem))

    # wait til they are up and running
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda c: c.scalar() == maxworkers,
          timeout=10 + maxworkers * 2)

    return procs


def reap_dead_workers(engine):
    sql = ("select id, pid from rework.worker "
           "where host = %(host)s and running = true")
    deadlist = []
    for wid, pid in engine.execute(sql, {'host': host()}).fetchall():
        try:
            cmd = ' '.join(psutil.Process(pid).cmdline())
            if 'new-worker' not in cmd and str(engine.url) not in cmd:
                print('pid {} was probably recycled'.format(pid))
                deadlist.append(wid)
        except psutil.NoSuchProcess:
            deadlist.append(wid)

    if deadlist:
        wsql = worker.update().where(worker.c.id.in_(deadlist)).values(
            running=False,
            deathinfo='Unaccounted death (hard crash)'
        )
        # also mark the tasks as failed
        tsql = task.update().where(
            worker.c.id.in_(deadlist)
        ).where(
            task.c.worker == worker.c.id
        ).where(
            task.c.status == 'running'
        ).values(
            status='done'
        )
        with engine.connect() as cn:
            cn.execute(wsql)
            cn.execute(tsql)

    return deadlist


def cleanup_unstarted(engine):
    sql = ('delete from rework.worker '
           'where not running '
           'and traceback is null '
           'and not shutdown '
           'and deathinfo is null')
    with engine.connect() as cn:
        cn.execute(sql)


def run_monitor(dburi, maxworkers=2, maxruns=0, maxmem=0):
    engine = create_engine(dburi)
    workers = []
    while True:
        dead = reap_dead_workers(engine)
        cleanup_unstarted(engine)
        if dead:
            print('reaped {} dead workers'.format(len(dead)))
            workers = [(wid, proc)
                       for wid, proc in workers
                       if wid not in dead]
        new = ensure_workers(engine, maxworkers, maxruns, maxmem)
        if new:
            print('spawned {} active workers'.format(len(new)))
        workers += new
        time.sleep(1)

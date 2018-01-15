import os
import time
import subprocess as sub
from datetime import datetime

import psutil

from sqlalchemy import create_engine, select

from rework.helper import host, guard
from rework.schema import worker, task


try:
    DEVNULL = sub.DEVNULL
except AttributeError:
    DEVNULL = open(os.devnull, 'wb')


def spawn_worker(engine, maxruns, maxmem, domain='default', debug_port=0):
    wid = new_worker(engine)
    cmd = ['rework', 'new-worker', str(engine.url), str(wid), str(os.getpid()),
           '--maxruns', str(maxruns),
           '--maxmem', str(maxmem),
           '--domain', domain,
           '--debug-port', str(debug_port)]
    # NOTE for windows users:
    # the subprocess pid herein might not be that of the actual worker
    # process because of the way python scripts are handled:
    # +- thescript.exe <params>
    #   +- python.exe thescript.py <params>
    return wid, sub.Popen(cmd,
                          stdout=DEVNULL, stderr=DEVNULL)


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


def ensure_workers(engine, maxworkers, maxruns, maxmem,
                   domain='default', base_debug_port=0):
    numworkers = num_workers(engine)

    procs = []
    debug_port = 0
    for offset in range(maxworkers - numworkers):
        if base_debug_port:
            debug_port = base_debug_port + offset
        procs.append(spawn_worker(engine, maxruns, maxmem,
                                  domain=domain, debug_port=debug_port))

    # wait til they are up and running
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda c: c.scalar() == maxworkers,
          timeout=10 + maxworkers * 2)

    return procs


def preemptive_kill(engine):
    hostid = host()
    sql = select([worker.c.id, worker.c.pid]).where(
        worker.c.host == hostid
    ).where(
        worker.c.kill == True
    ).where(
        worker.c.running == True
    )
    killed = []
    with engine.connect() as cn:
        for wid, pid in cn.execute(sql).fetchall():
            try:
                psutil.Process(pid).kill()
            except:
                print('could not kill {}'.format(pid))
                continue
            sql = worker.update().values(
                running=False,
                deathinfo='preemptive kill at {}'.format(datetime.utcnow())
            ).where(
                worker.c.id == wid
            )
            cn.execute(sql)
            sql = task.update().values(
                status='done'
            ).where(
                worker.c.id == task.c.worker
            )
            cn.execute(sql)
            killed.append((wid, pid))
    return killed


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


def run_monitor(dburi, maxworkers=2, maxruns=0, maxmem=0,
                domain='default', debug=False):
    engine = create_engine(dburi)
    workers = []
    base_debug_port = 6666 if debug else 0
    while True:
        preemptive_kill(engine)
        dead = reap_dead_workers(engine)
        cleanup_unstarted(engine)
        if dead:
            print('reaped {} dead workers'.format(len(dead)))
            workers = [(wid, proc)
                       for wid, proc in workers
                       if wid not in dead]
        new = ensure_workers(engine, maxworkers, maxruns, maxmem,
                             domain=domain,
                             base_debug_port=base_debug_port)
        if new:
            print('spawned {} active workers'.format(len(new)))
        workers += new
        time.sleep(1)

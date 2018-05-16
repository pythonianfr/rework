import os
import time
import subprocess as sub
from datetime import datetime

import psutil

from sqlalchemy import select

from rework.helper import host, guard, kill_process_tree
from rework.schema import worker, task


try:
    DEVNULL = sub.DEVNULL
except AttributeError:
    DEVNULL = open(os.devnull, 'wb')


def mark_dead_workers(cn, wids, message):
    # mark workers as dead
    cn.execute(
        worker.update().values(
            running=False,
            deathinfo=message
        ).where(worker.c.id.in_(wids))
    )
    # mark tasks as done
    cn.execute(
        task.update().values(status='done'
        ).where(worker.c.id == task.c.worker
        ).where(worker.c.id.in_(wids))
    )


class Monitor(object):
    __slots__ = ('engine', 'domain', 'maxworkers',
                 'maxruns', 'maxmem', 'debugport',
                 'workers')

    def __init__(self, engine, domain='default',
                 maxworkers=2, maxruns=0, maxmem=0, debug=False):
        self.engine = engine
        self.domain = domain
        self.maxworkers = maxworkers
        self.maxruns = maxruns
        self.maxmem = maxmem
        self.debugport = 6666 if debug else 0
        self.workers = {}

    @property
    def wids(self):
        return sorted(self.workers.keys())

    def spawn_worker(self, debug_port=0):
        wid = self.new_worker()
        cmd = ['rework', 'new-worker', str(self.engine.url), str(wid), str(os.getpid()),
               '--maxruns', str(self.maxruns),
               '--maxmem', str(self.maxmem),
               '--domain', self.domain,
               '--debug-port', str(debug_port)]
        self.workers[wid] = sub.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
        return wid

    def new_worker(self):
        with self.engine.connect() as cn:
            return cn.execute(
                worker.insert().values(
                    host=host(),
                    domain=self.domain
                )
            ).inserted_primary_key[0]

    @property
    def num_workers(self):
        return len(self.workers)

    def grab_debug_port(self, base_debug_port, offset):
        if not base_debug_port:
            return 0
        allnumbers = set(range(base_debug_port, base_debug_port + self.maxworkers))
        usednumbers = set(num for num, in self.engine.execute(
            'select debugport from rework.worker where running = true').fetchall())
        for num in range(base_debug_port, base_debug_port + offset):
            usednumbers.add(num)
        numbers = allnumbers - usednumbers
        assert numbers
        return min(numbers)

    def ensure_workers(self):
        for wid, proc in self.workers.copy().items():
            if proc.poll() is not None:
                proc.wait()  # tell linux to reap the zombie
                self.workers.pop(wid)
        numworkers = self.num_workers

        procs = []
        debug_ports = []
        for offset in range(self.maxworkers - numworkers):
            if self.debugport:
                debug_ports.append(self.grab_debug_port(self.debugport, offset))
            else:
                debug_ports.append(0)

        for debug_port in debug_ports:
            procs.append(self.spawn_worker(debug_port=debug_port))

        # wait til they are up and running
        if procs:
            guard(self.engine,
                  "select count(id) from rework.worker where running = true "
                  "and id in ({})".format(','.join(repr(wid) for wid in procs)),
                  lambda c: c.scalar() == len(procs),
                  timeout=20 + self.maxworkers * 2)

        return procs

    def killall(self):
        mark = []
        for wid, proc in self.workers.items():
            if proc.poll() is None:  # else it's already dead
                # NOTE for windows users:
                # the subprocess pid herein might not be that of the actual worker
                # process because of the way python scripts are handled:
                # +- thescript.exe <params>
                #   +- python.exe thescript.py <params>
                kill_process_tree(proc.pid)
                proc.wait()
            mark.append(wid)
        with self.engine.connect() as cn:
            mark_dead_workers(cn, mark, 'Forcefully killed by the monitor.')
        self.workers = {}

    def preemptive_kill(self):
        sql = select([worker.c.id]).where(
            worker.c.kill == True
        ).where(
            worker.c.running == True
        ).where(
            worker.c.id.in_(wid for wid in self.workers)
        )
        killed = []
        with self.engine.connect() as cn:
            for row in cn.execute(sql).fetchall():
                wid = row.id
                proc = self.workers.pop(wid)
                if not kill_process_tree(proc.pid):
                    print('could not kill {}'.format(proc.pid))
                    continue

                mark_dead_workers(cn, [wid],
                                  'preemptive kill at {}'.format(datetime.utcnow())
                )
                killed.append(wid)
        return killed

    def reap_dead_workers(self):
        sql = ("select id, pid from rework.worker "
               "where host = %(host)s and running = true "
               "and domain = %(domain)s")
        deadlist = []
        for wid, pid in self.engine.execute(sql, {
                'host': host(),
                'domain': self.domain
        }).fetchall():
            try:
                cmd = ' '.join(psutil.Process(pid).cmdline())
                if 'new-worker' not in cmd and str(self.engine.url) not in cmd:
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
            with self.engine.connect() as cn:
                cn.execute(wsql)
                cn.execute(tsql)

        return deadlist

    def cleanup_unstarted(self):
        sql = ('delete from rework.worker '
               'where not running '
               'and traceback is null '
               'and not shutdown '
               'and deathinfo is null '
               'and domain = %(domain)s')
        with self.engine.connect() as cn:
            cn.execute(sql, domain=self.domain)

    def run(self):
        workers = []
        while True:
            self.preemptive_kill()
            dead = self.reap_dead_workers()
            self.cleanup_unstarted()
            if dead:
                print('reaped {} dead workers'.format(len(dead)))
                workers = [(wid, proc)
                           for wid, proc in workers
                           if wid not in dead]
            new = self.ensure_workers()
            if new:
                print('spawned {} active workers'.format(len(new)))
            workers += new
            time.sleep(1)

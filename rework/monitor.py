import os
import time
import subprocess as sub
from datetime import datetime
import tzlocal

import psutil

from sqlalchemy import select, not_

from rework.helper import host, guard, kill_process_tree
from rework.schema import worker, task, monitor


TZ = tzlocal.get_localzone()

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


def clip(val, low, high):
    if val < low:
        return low
    if val > high:
        return high
    return val


class monstats(object):
    __slots__ = ('new', 'deleted', 'shrink')

    def __init__(self):
        self.new = []
        self.deleted = []
        self.shrink = []

    def __str__(self):
        return 'new: {} deleted: {} shrink: {}'.format(
            ','.join(str(n) for n in self.new),
            ','.join(str(d) for d in self.deleted),
            ','.join(str(s) for s in self.shrink)
        )

    __repr__ = __str__


class Monitor(object):
    __slots__ = ('engine', 'domain',
                 'minworkers', 'maxworkers',
                 'maxruns', 'maxmem', 'debugport',
                 'workers', 'host', 'monid')

    def __init__(self, engine, domain='default',
                 minworkers=None, maxworkers=2,
                 maxruns=0, maxmem=0, debug=False):
        self.engine = engine
        self.domain = domain
        self.maxworkers = maxworkers
        self.minworkers = minworkers if minworkers is not None else maxworkers
        assert 0 <= self.minworkers <= self.maxworkers
        self.maxruns = maxruns
        self.maxmem = maxmem
        self.debugport = 6666 if debug else 0
        self.workers = {}
        self.host = host()

    @property
    def wids(self):
        return sorted(self.workers.keys())

    def spawn_worker(self, debug_port=0):
        wid = self.new_worker()
        cmd = ['rework',
               'new-worker', str(self.engine.url), str(wid), str(os.getpid()),
               '--maxruns', str(self.maxruns),
               '--maxmem', str(self.maxmem),
               '--domain', self.domain,
               '--debug-port', str(debug_port)]
        self.workers[wid] = sub.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
        return wid

    def new_worker(self):
        with self.engine.begin() as cn:
            return cn.execute(
                worker.insert().values(
                    host=host(),
                    domain=self.domain
                )
            ).inserted_primary_key[0]

    @property
    def num_workers(self):
        return len(self.workers)

    def grab_debug_port(self, offset):
        if not self.debugport:
            return 0
        allnumbers = set(range(self.debugport, self.debugport + self.maxworkers))
        usednumbers = set(num for num, in self.engine.execute(
            'select debugport from rework.worker where running = true').fetchall())
        for num in range(self.debugport, self.debugport + offset):
            usednumbers.add(num)
        numbers = allnumbers - usednumbers
        assert numbers
        return min(numbers)

    def queued_tasks(self, cn):
        return cn.execute(
            'select count(*) '
            ' from rework.task as task, rework.operation as op '
            'where '
            ' task.status = \'queued\' and '
            ' task.operation = op.id and '
            ' op.domain = %(domain)s and '
            ' op.host = %(host)s',
            domain=self.domain,
            host=self.host
        ).scalar()

    def busy_workers(self, cn):
        if not self.workers:
            return []
        return [
            row.id for row in
            cn.execute(
                select([worker.c.id]
                ).where(worker.c.id.in_(self.wids)
                ).where(task.c.worker == worker.c.id
                ).where(task.c.status != 'done')
            ).fetchall()
        ]

    def idle_worker(self, cn, busylist=()):
        sql = select([worker.c.id]).where(worker.c.id.in_(self.wids))
        if busylist:
            sql = sql.where(not_(worker.c.id.in_(busylist)))
        sql = sql.where(worker.c.shutdown == False)
        sql = sql.limit(1)
        return cn.execute(sql).scalar()

    def shrink_workers(self):
        with self.engine.begin() as cn:
            needed = self.queued_tasks(cn)
            busy = self.busy_workers(cn)
            idle = self.num_workers - len(busy)
            # ask idle workers to shutdown
            # let' not even try to do anything if
            # the task queue is unempty
            if not needed and idle > self.minworkers:
                candidate = self.idle_worker(cn, busy)
                # we now have a case to at least retire one
                sql = worker.update().where(
                    worker.c.id == candidate
                ).values(shutdown=True)
                cn.execute(sql)
                return candidate

    def _cleanup_workers(self):
        stats = monstats()
        for wid, proc in self.workers.copy().items():
            if proc.poll() is not None:
                proc.wait()  # tell linux to reap the zombie
                self.workers.pop(wid)
                stats.deleted.append(wid)
        return stats

    def ensure_workers(self):
        # rid self.workers of dead things
        stats = self._cleanup_workers()

        # reduce by one the worker pool if possible
        shuttingdown = self.shrink_workers()
        if shuttingdown is not None:
            stats.shrink.append(shuttingdown)

        # compute the needed workers
        with self.engine.begin() as cn:
            numworkers = self.num_workers
            busycount = len(self.busy_workers(cn))
            waiting = self.queued_tasks(cn)

        idle = numworkers - busycount
        assert idle >= 0
        needed_workers = clip(waiting - idle,
                              self.minworkers - numworkers,
                              self.maxworkers - numworkers)

        # bail out if there's nothing to do
        if not needed_workers:
            return stats

        procs = []
        debug_ports = []
        for offset in range(needed_workers):
            debug_ports.append(self.grab_debug_port(offset))

        for debug_port in debug_ports:
            procs.append(self.spawn_worker(debug_port=debug_port))

        # wait til they are up and running
        if procs:
            guard(self.engine,
                  "select count(id) from rework.worker where running = true "
                  "and id in ({})".format(','.join(repr(wid) for wid in procs)),
                  lambda c: c.scalar() == len(procs),
                  timeout=20 + self.maxworkers * 2)

        stats.new.extend(procs)
        return stats

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
        with self.engine.begin() as cn:
            mark_dead_workers(cn, mark, 'Forcefully killed by the monitor.')
        self.workers = {}

    def preemptive_kill(self):
        sql = select(
            [worker.c.id]).where(worker.c.kill == True
            ).where(worker.c.running == True
            ).where(worker.c.id.in_(self.wids))
        killed = []
        with self.engine.begin() as cn:
            for row in cn.execute(sql).fetchall():
                wid = row.id
                proc = self.workers.pop(wid)
                if not kill_process_tree(proc.pid):
                    print('could not kill {}'.format(proc.pid))
                    continue

                mark_dead_workers(
                    cn, [wid],
                    'preemptive kill at {}'.format(TZ.localize(datetime.utcnow()))
                )
                killed.append(wid)
        return killed

    def reap_dead_workers(self):
        sql = ('select id, pid from rework.worker '
               'where host = %(host)s and running = true '
               'and domain = %(domain)s')
        deadlist = []
        for wid, pid in self.engine.execute(
                sql, {'host': host(), 'domain': self.domain}).fetchall():
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
            tsql = task.update().where(worker.c.id.in_(deadlist)
            ).where(task.c.worker == worker.c.id
            ).where(task.c.status == 'running'
            ).values(status='done')
            with self.engine.begin() as cn:
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
        with self.engine.begin() as cn:
            cn.execute(sql, domain=self.domain)

    def register(self):
        # register in db
        with self.engine.begin() as cn:
            cn.execute('delete from rework.monitor where domain = %(domain)s',
                       domain=self.domain)
            self.monid = cn.execute(
                monitor.insert().values(
                    domain=self.domain,
                    options={
                        'maxworkers': self.maxworkers,
                        'minworkers': self.minworkers,
                        'maxruns': self.maxruns,
                        'maxmem': self.maxmem,
                        'debugport': self.debugport
                    })
            ).inserted_primary_key[0]

    def dead_man_switch(self):
        with self.engine.begin() as cn:
            cn.execute(
                monitor.update().where(
                    monitor.c.id == self.monid
                ).values(lastseen=TZ.localize(datetime.utcnow()))
            )

    def unregister(self):
        assert self.monid
        with self.engine.begin() as cn:
            cn.execute(
                monitor.delete().where(monitor.c.id == self.monid)
            )

    def run(self):
        try:
            self.register()
            self._run()
        finally:
            self.unregister()

    def _run(self):
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
            stats = self.ensure_workers()
            if stats.new:
                print('spawned {} active workers'.format(len(stats.new)))
            if stats.shrink:
                print('worker {} asked to shutdown'.format(stats.shrink[0]))
            workers += stats.new
            self.dead_man_switch()
            time.sleep(1)

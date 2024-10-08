import os
import time
import subprocess as sub
import signal
import json
from datetime import (
    datetime,
    timedelta
)
import traceback as tb
from pathlib import Path
import sys

import isodate
import tzlocal
import pytz
import psutil
from sqlhelp import select, insert, update

from rework.helper import (
    cleanup_tasks,
    cleanup_workers,
    cpu_usage,
    host,
    kill_process_tree,
    memory_usage,
    iter_stamps_from_cronrules,
    partition,
    schedule_plan,
    setuplogger,
    utcnow,
    wait_true
)
from rework import api
from rework.task import Task


TZ = tzlocal.get_localzone()

try:
    DEVNULL = sub.DEVNULL
except AttributeError:
    DEVNULL = open(os.devnull, 'wb')


def mark_dead_workers(cn, wids, message, traceback=None):
    if not wids:
        return
    # mark workers as dead
    update('rework.worker').where('id in %(ids)s', ids=tuple(wids)).values(
        running=False,
        finished=utcnow(),
        deathinfo=message
    ).do(cn)
    # mark tasks as done
    update(
        'rework.task as task'
    ).table('rework.worker as worker'
    ).where(
        "task.status != 'done'",
        'worker.id = task.worker',
        'worker.id in %(ids)s',
        ids=tuple(wids),
    ).values(
        finished=utcnow(),
        status='done',
        abort=True,
        traceback=traceback
    ).do(cn)


def clip(val, low, high):
    if val < low:
        return low
    if val > high:
        return high
    return val


class monstats:
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


def run_sched(logger, lastnow, runnable, _now=None):
    now = _now or datetime.now(TZ)
    runnow, runlater = partition(
        lambda stamp_func: stamp_func[0] <= now,
        runnable
    )
    if not runnow:
        logger.debug(
            f'sched: nothing to run for {now.isoformat()} from {lastnow.isoformat()}'
        )
        return runlater, lastnow

    # consume the runnow list
    # and return the last associated stamp
    logger.info(f'sched: will run {len(runnow)} tasks now')
    for stamp, func in runnow:
        try:
            func()
        except:
            logger.exception('sched: oops, scheduling just crashed')
    logger.info(f'sched: will keep {len(runlater)} tasks for later')

    # runlater contains everything not consummed yet
    return runlater, stamp


class scheduler:
    __slots__ = ('engine', 'logger', 'domain', 'sched', 'defs', 'rulemap', 'runnable', 'laststamp')
    _step = {'minutes': 30}

    def __init__(self, engine, logger, domain):
        self.engine = engine
        self.logger = logger
        self.domain = domain
        # two items below should be synchronized
        self.defs = []  # current base definitions
        self.rulemap = []  # map from rule to wrapped api.schedule call
        # batch of (tstamp, wrapped) for at most the next hour
        self.runnable = []
        self.laststamp = datetime.now(TZ)

    def __repr__(self):
        return f'<scheduler for {self.domain} ->\n{self.defs}>'

    def schedule(self, rule, opname, domain, rawinputdata, hostid, metadata):
        self.rulemap.append(
            (
                rule,
                lambda: api.schedule(
                    self.engine,
                    opname=opname,
                    domain=domain,
                    rawinputdata=rawinputdata,
                    hostid=hostid,
                    metadata=metadata
                )
            )
        )

    def run_scheduled(self):
        if not self.defs:
            return

        if not self.runnable:
            lastnow = self.laststamp
            now = datetime.now(TZ)
            # time to build the next runnable batch
            self.runnable = list(
                sorted(
                    iter_stamps_from_cronrules(
                        self.rulemap,
                        lastnow + timedelta(milliseconds=1),
                        now + timedelta(**self._step)
                    ),
                    key=lambda stamp_func: stamp_func[0]
                )
            )
            if self.runnable:
                self.logger.info(f'sched: prepared {len(self.runnable)} items')
                for stamp, op in schedule_plan(self.engine, self.domain, timedelta(**self._step)):
                    self.logger.info(f'sched: {stamp} -> {op}')
                self.logger.info(f'sched: next item will run at {self.runnable[0][0]}')

        runnable, laststamp = run_sched(
            self.logger,
            self.laststamp,
            self.runnable
        )
        consummed = len(self.runnable) - len(runnable)
        if consummed:
            self.logger.info(f'sched: consummed {consummed} items')
            self.logger.info(
                f'sched: advanced stamp from {self.laststamp} to {laststamp}'
            )

        self.runnable = runnable
        self.laststamp = laststamp

    def step(self):
        defs = self.definitions
        if defs != self.defs:
            # reload everything
            self.logger.info(f'sched: reloading definitions for {self.domain}')
            self.rulemap = []
            self.runnable = []
            self.logger.info(f'sched: starting with {len(defs)} definitions')
            for idx, (operation, rule, rawinputdata, hostid, meta) in enumerate(defs):
                self.logger.info(f'{idx} {operation} {rule} {hostid} {meta}')
                self.schedule(
                    rule,
                    opname=operation,
                    domain=self.domain,
                    rawinputdata=rawinputdata,
                    hostid=hostid,
                    metadata=meta
                )
            self.defs = defs

        self.run_scheduled()

    @property
    def definitions(self):
        q = select(
            'op.name', 's.rule', 's.inputdata', 's.host', 's.metadata'
        ).table('rework.sched as s', 'rework.operation as op'
        ).where('s.operation = op.id'
        ).where('s.domain = %(domain)s', domain=self.domain
        ).order('op.name')

        return q.do(self.engine).fetchall()


class Monitor:
    __slots__ = ('engine', 'logger', 'domain',
                 'minworkers', 'maxworkers',
                 'maxruns', 'maxmem', 'debugport',
                 'workers', 'host', 'monid',
                 'start_timeout', 'debugfile',
                 'pending_start', 'scheduler',
                 'vacuum', 'lastvacuum')

    def __init__(self, engine, logger=None, domain='default',
                 minworkers=None, maxworkers=2,
                 maxruns=0, maxmem=0, debug=False,
                 debug_port=0, vacuum='P1D',
                 start_timeout=30, debugfile=None):
        self.engine = engine
        self.logger = logger or setuplogger()
        self.domain = domain
        self.maxworkers = maxworkers
        self.minworkers = minworkers if minworkers is not None else maxworkers
        assert 0 <= self.minworkers <= self.maxworkers
        self.maxruns = maxruns
        self.maxmem = maxmem
        self.debugport = 6666 or debug_port if debug else 0
        self.lastvacuum = datetime.now()
        self.vacuum = isodate.parse_duration(vacuum)
        self.workers = {}
        self.host = host()
        self.start_timeout = start_timeout
        self.debugfile = None
        self.monid = None
        if debugfile:
            self.debugfile = Path(debugfile).open('wb')
        self.pending_start = {}
        self.scheduler = scheduler(engine, logger, domain)
        signal.signal(signal.SIGTERM, self.sigterm)

    def sigterm(self, signum, stack):
        self.killall(f'Got a TERMINATE/{signum} signal while at {stack}')
        sys.exit(0)

    @property
    def wids(self):
        return sorted(self.workers.keys())

    def dump_to_debugfile(self, strdata):
        if not self.debugfile:
            return
        assert isinstance(strdata, str)
        self.debugfile.write(strdata.encode('utf-8') + b'\n')

    def spawn_worker(self, debug_port=0):
        wid = self.new_worker()
        cmd = [
            'rework',
            'new-worker', str(self.engine.url), str(wid), str(os.getpid()),
            '--maxruns', str(self.maxruns),
            '--maxmem', str(self.maxmem),
            '--domain', self.domain,
            '--debug-port', str(debug_port)
        ]
        self.workers[wid] = sub.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
        return wid

    def new_worker(self):
        with self.engine.begin() as cn:
            q = insert(
                'rework.worker'
            ).values(
                host=host(),
                domain=self.domain
            )
            return q.do(cn).scalar()

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
        q = select('worker.id').table(
                'rework.worker as worker'
            ).join(
                'rework.task as task on (worker.id = task.worker)'
            ).where(
                'worker.id in %(ids)s', ids=tuple(self.wids)
            ).where(
                "task.status != 'done'"
            )
        return [
            row.id for row in
            q.do(cn).fetchall()
        ]

    def idle_worker(self, cn, busylist=()):
        q = select(
            'id'
        ).table('rework.worker'
        ).where('id in %(ids)s', ids=tuple(self.wids))

        if busylist:
            q.where('not id in %(nid)s', nid=tuple(busylist))
        q.where(shutdown=False)
        q.limit(1)
        return q.do(cn).scalar()

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
                update('rework.worker').where(id=candidate).values(
                    shutdown=True
                ).do(cn)
                return candidate

    def _cleanup_workers(self):
        stats = monstats()
        for wid, proc in self.workers.copy().items():
            if proc.poll() is not None:
                proc.wait()  # tell linux to reap the zombie
                self.workers.pop(wid)
                stats.deleted.append(wid)
        return stats

    def track_resources(self):
        if not self.workers:
            return
        for wid, proc in self.workers.items():
            q = update('rework.worker').where(id=wid).values(
                mem=memory_usage(proc.pid),
                cpu=cpu_usage(proc.pid)
            )
            with self.engine.begin() as cn:
                q.do(cn)

    def track_timeouts(self):
        if not self.workers:
            return
        sql = ('select task.id, task.started, timeout '
               'from rework.operation as op, '
               '     rework.task as task '
               'where '
               ' task.operation = op.id and '
               ' timeout is not null and '
               ' task.worker in ({})'
        ).format(
            ','.join(str(wid) for wid in self.wids)
        )
        with self.engine.begin() as cn:
            for tid, start_time, timeout in cn.execute(sql).fetchall():
                start_time = start_time.astimezone(pytz.utc)
                delta = isodate.parse_duration(timeout)
                now = utcnow()
                if (now - start_time) > delta:
                    Task.byid(self.engine, tid).abort('timeout')

    def track_starting(self):
        if not self.pending_start:
            return {}
        sql = (
            'select worker.id '
            'from rework.worker '
            'where worker.running = true and '
            'worker.id in %(wids)s'
        )
        # remove started workers
        with self.engine.begin() as cn:
            for wid, in cn.execute(sql, wids=tuple(self.pending_start)):
                self.pending_start.pop(wid, None)

        # kill after timeout
        now = datetime.now()
        for wid, start in self.pending_start.copy().items():
            delta = (now - start).total_seconds()
            if delta > self.start_timeout:
                with self.engine.begin() as cn:
                    update('rework.worker').where(id=wid).values(
                        kill=True,
                        deathinfo='timeout while starting'
                    ).do(cn)
                self.pending_start.pop(wid, None)

        # signal the outcome
        if not self.pending_start:
            self.logger.info('mon: no more pending starts')
        else:
            pending = {
                wid: str(dt)
                for wid, dt in self.pending_start.items()
            }
            self.logger.info(f'mon: workers yet to start : {pending}')
        return self.pending_start

    def ensure_workers(self):
        # rid self.workers of dead things
        stats = self._cleanup_workers()

        # update mem/cpu stats
        self.track_resources()

        # reduce by one the worker pool if possible
        shuttingdown = self.shrink_workers()
        if shuttingdown is not None:
            stats.shrink.append(shuttingdown)

        # compute the needed workers
        with self.engine.begin() as cn:
            numworkers = self.num_workers
            busycount = len(self.busy_workers(cn))
            waiting = self.queued_tasks(cn)
            pending_start = len(self.pending_start)

        idle = numworkers - busycount
        assert idle >= 0
        needed_workers = clip(
            waiting - idle - pending_start,
            self.minworkers - numworkers,
            self.maxworkers - numworkers
        )

        # bail out if there's nothing to do
        if not needed_workers:
            return stats

        debug_ports = []
        for offset in range(needed_workers):
            debug_ports.append(self.grab_debug_port(offset))

        procs = {}
        for debug_port in debug_ports:
            procs[
                self.spawn_worker(debug_port=debug_port)
            ] = datetime.now()

        self.pending_start.update(procs)

        stats.new.extend(list(procs.keys()))
        return stats

    def killall(self, msg='Forcefully killed by the monitor.', traceback=None):
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
            mark_dead_workers(cn, mark, msg, traceback)
        self.workers = {}

    def preemptive_kill(self):
        if not self.wids:
            return
        q = select('id' ).table('rework.worker').where(
            'kill = true'
        ).where(
            'id in %(ids)s', ids=tuple(self.wids)
        )
        killed = []
        with self.engine.begin() as cn:
            for row in q.do(cn).fetchall():
                wid = row.id
                proc = self.workers.pop(wid)
                if not kill_process_tree(proc.pid):
                    self.logger.info(f'mon: could not kill {proc.pid}')
                    continue

                mark_dead_workers(
                    cn, [wid],
                    f'preemptive kill at {utcnow().astimezone(TZ)}'
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
                    self.logger.info(f'pid {pid} was probably recycled')
                    deadlist.append(wid)
            except psutil.NoSuchProcess:
                deadlist.append(wid)

        if deadlist:
            with self.engine.begin() as cn:
                mark_dead_workers(
                    cn, deadlist,
                    'Unaccounted death (hard crash)'
                )

        return deadlist

    def cleanup_unstarted(self):
        sql = ('with deleted as '
               '(delete from rework.worker as w '
               ' where not w.running '
               '   and pid is null '
               '   and domain = %(domain)s '
               ' returning 1) '
               'select count(*) from deleted')
        with self.engine.begin() as cn:
            deleted = cn.execute(sql, domain=self.domain).scalar()
            return deleted

    def cleanup_stale_monitors(self):
        with self.engine.begin() as cn:
            cn.execute('delete from rework.monitor '
                       'where domain = %(domain)s',
                       domain=self.domain)

    def register(self):
        # register in db
        with self.engine.begin() as cn:
            q = insert(
                'rework.monitor'
            ).values(
                domain=self.domain,
                options=json.dumps({
                    'maxworkers': self.maxworkers,
                    'minworkers': self.minworkers,
                    'maxruns': self.maxruns,
                    'maxmem': self.maxmem,
                    'debugport': self.debugport
                })
            )
            self.monid = q.do(cn).scalar()

    def dead_man_switch(self):
        with self.engine.begin() as cn:
            update('rework.monitor').where(id=self.monid).values(
                lastseen=utcnow().astimezone(TZ)
            ).do(cn)

    def autovacuum(self):
        now = datetime.now()
        if (now - self.lastvacuum) < timedelta(hours=1):
            return
        finished = now - self.vacuum
        tasks = cleanup_tasks(self.engine, finished, self.domain)
        workers = cleanup_workers(self.engine, finished, self.domain)
        self.logger.info(f'mon: vacuumed {tasks} tasks and {workers} workers')
        self.lastvacuum = now

    def unregister(self):
        assert self.monid
        with self.engine.begin() as cn:
            cn.execute(
                'delete from rework.monitor where id = %(id)s',
                id=self.monid
            )

    def run(self):
        self.dump_to_debugfile(
            'monitor start at ' + datetime.now().isoformat()
        )
        try:
            self.cleanup_stale_monitors()
            self.register()
            self.dump_to_debugfile(f'monitor id {self.monid}')
            self._run()
        except Exception:
            traceback = tb.format_exc()
            self.dump_to_debugfile(
                traceback
            )
            self.killall(msg='monitor exit', traceback=traceback)
        finally:
            self.dump_to_debugfile(
                f'monitor {self.monid} exit at {datetime.now().isoformat()}'
            )
            self.unregister()

    def wait_all_started(self, stop=False):
        """helper for the tests -- in real life this is toxic to the monitor
        as one single failure/slowness to start up will kill it and
        all its workers

        """
        try:
            wait_true(
                lambda: len(self.track_starting()) == 0,
                timeout=self.start_timeout,
                sleeptime=.2
            )
        except AssertionError:
            if stop:
                raise
            # give it another chance
            self.wait_all_started(stop=True)

    def step(self):
        self.track_timeouts()
        self.track_starting()
        self.preemptive_kill()
        dead = self.reap_dead_workers()
        if dead:
            self.logger.info(f'mon: reaped {len(dead)} dead workers')
        stats = self.ensure_workers()
        self.scheduler.step()
        self.autovacuum()
        self.dead_man_switch()
        return stats

    def _run(self):
        deleted = self.cleanup_unstarted()
        if deleted:
            self.logger.info(f'mon: cleaned {deleted} unstarted workers')
        while True:
            stats = self.step()
            if stats.new:
                self.logger.info(f'mon: spawned {len(stats.new)} active workers')
            if stats.shrink:
                self.logger.info(f'mon: worker {stats.shrink[0]} asked to shutdown')
            time.sleep(1)

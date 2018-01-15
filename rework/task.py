import sys
import imp
import time
from pickle import dumps, loads
import traceback as tb
from contextlib import contextmanager
import logging

from sqlalchemy import select

from rework.schema import task, log
from rework.helper import PGLogHandler, PGLogWriter


__task_registry__ = {}


class TimeOut(Exception):
    pass


class Task(object):
    __slots__ = ('engine', 'tid', 'operation')

    def __init__(self, engine, tid, operation):
        self.engine = engine
        self.tid = tid
        self.operation = operation

    @classmethod
    def fromqueue(cls, engine, wid, domain='default'):
        with engine.connect() as cn:
            sql = ("select task.id, task.operation "
                   "from rework.task as task, rework.operation as op "
                   "where task.status = 'queued' "
                   "  and op.domain = %(domain)s"
                   "  and task.operation = op.id "
                   "order by task.id "
                   "for update of task skip locked "
                   "limit 1")
            tid_operation = cn.execute(sql, domain=domain).fetchone()
            if tid_operation is None:
                return

            tid, opid = tid_operation
            sql = task.update().where(task.c.id == tid).values(
                status='running',
                worker=wid)
            cn.execute(sql)

            return cls(engine, tid, opid)

    @classmethod
    def byid(cls, engine, tid):
        with engine.connect() as cn:
            sql = "select operation from rework.task where id = %(tid)s"
            operation = cn.execute(sql, tid=tid).scalar()
            if operation is None:
                return

            return cls(engine, tid, operation)

    def save_output(self, data, raw=False):
        if not raw:
            data = dumps(data, protocol=2)
        sql = task.update().where(task.c.id == self.tid).values(
            output=data
        )
        with self.engine.connect() as cn:
            cn.execute(sql)

    @contextmanager
    def capturelogs(self, sync=True, level=logging.NOTSET, std=False):
        pghdlr = PGLogHandler(self, sync)
        root = logging.getLogger()
        assert not len(root.handlers)
        root.setLevel(level)
        root.addHandler(pghdlr)
        if std:
            out, err = sys.stdout, sys.stderr
            sys.stdout = PGLogWriter('stdout', pghdlr)
            sys.stderr = PGLogWriter('stderr', pghdlr)
        try:
            yield
        finally:
            root.handlers.remove(pghdlr)
            pghdlr.flush()
            if std:
                sys.stdout = out
                sys.stderr = err

    def logs(self, fromid=None):
        sql = select([log.c.id, log.c.line]).order_by(log.c.id
        ).where(log.c.task == self.tid)
        if fromid:
            sql = sql.where(log.c.id > fromid)

        with self.engine.connect() as cn:
            return cn.execute(sql).fetchall()

    def _propvalue(self, prop):
        sql = select([task.c[prop]]).where(task.c.id == self.tid)
        return self.engine.execute(sql).scalar()

    @property
    def metadata(self):
        meta = self._propvalue('metadata')
        if meta is None:
            return {}
        return meta

    @property
    def status(self):
        return self._propvalue('status')

    @property
    def aborted(self):
        return self._propvalue('abort')

    @property
    def worker(self):
        return self._propvalue('worker')

    @property
    def input(self):
        val = self._propvalue('input')
        if val is not None:
            return loads(val)

    @property
    def raw_input(self):
        return self._propvalue('input')

    @property
    def output(self):
        out = self._propvalue('output')
        if out is not None:
            return loads(out)

    @property
    def raw_output(self):
        return self._propvalue('output')

    @property
    def traceback(self):
        return self._propvalue('traceback')

    @property
    def state(self):
        " provide a comprehensive synthetic task state "
        status = self.status
        aborted = self.aborted
        if status != 'done':
            if aborted:
                return 'aborting'
            return status
        if aborted:
            return 'aborted'
        if self.traceback:
            return 'failed'
        return 'done'

    def run(self):
        try:
            name, path = self.engine.execute("""
                select name, path
                from rework.operation
                where rework.operation.id = %(operation)s
            """, {'operation': self.operation}
            ).fetchone()
            mod = imp.load_source('module', path)
            func = getattr(mod, name)
            func(self)
        except:
            sql = task.update().where(task.c.id == self.tid).values(
                traceback=tb.format_exc()
            )
            with self.engine.connect() as cn:
                cn.execute(sql)
        finally:
            self.finish()

    def join(self, target='done', timeout=0):
        " synchronous wait on the task"
        assert target in ('running', 'done')
        assert timeout >= 0
        t0 = time.time()
        while True:
            if self.status == target:
                break
            if timeout and (time.time() - t0) > timeout:
                raise TimeOut(self)
            time.sleep(1)

    def finish(self):
        with self.engine.connect() as cn:
            cn.execute(task.update().where(task.c.id == self.tid).values(
                status='done')
            )

    def abort(self):
        with self.engine.connect() as cn:
            # will still be marked as running
            # the worker kill must do the actual job
            cn.execute(
                task.update().where(task.c.id == self.tid).values(
                    abort=True
                )
            )

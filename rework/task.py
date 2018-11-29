import sys
import imp
import time
from pickle import dumps, loads
import traceback as tb
from contextlib import contextmanager
import logging

from sqlalchemy import select

from rework.schema import task, worker, log
from rework.helper import PGLogHandler, PGLogWriter, utcnow


__task_registry__ = {}


class TimeOut(Exception):
    pass


class Task(object):
    """A task object represents an execution of an operation within a worker.

    The only official way to get a task is as a return value of an
    `api.schedule(...)` call.

    Tasks provide a series of convenience methods that helps writing
    rework-enabled applications.

    """
    __slots__ = ('engine', 'tid', 'operation')

    def __init__(self, engine, tid, operation):
        self.engine = engine
        self.tid = tid
        self.operation = operation

    def __repr__(self):
        return 'Task({}, {})'.format(self.operation, self.state)

    @classmethod
    def fromqueue(cls, engine, wid, domain='default'):
        with engine.begin() as cn:
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
        with engine.begin() as cn:
            sql = "select operation from rework.task where id = %(tid)s"
            operation = cn.execute(sql, tid=tid).scalar()
            if operation is None:
                return

            return cls(engine, tid, operation)

    def save_output(self, data, raw=False):
        """saves the output of a task run for later use by other
        components. This is to be used from *within* an operation.

        The `data` parameter can be any picklable python object.

        The `raw` parameter, combined with a bytestring as input data,
        allows to bypass the pickling protocol.
        """
        if not raw:
            data = dumps(data, protocol=2)
        sql = task.update().where(task.c.id == self.tid).values(
            output=data
        )
        with self.engine.begin() as cn:
            cn.execute(sql)

    @contextmanager
    def capturelogs(self, sync=False, level=logging.NOTSET, std=False):
        """within this context, all logs at the given level will be captured
        and be retrievable through the `Task.log(...)` api call

        It is possible to specify the `level` (by default, everything
        is captured).

        The `std` parameter ensures all stdout/stderr outputs will be
        captured, at INFO level.

        """
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
            if std:
                sys.stdout.flush(force=True)
                sys.stderr.flush(force=True)
            if pghdlr in root.handlers:
                root.handlers.remove(pghdlr)
            pghdlr.flush()
            if std:
                sys.stdout = out
                sys.stderr = err

    def logs(self, fromid=None):
        """get the logs of the task as a list of tuples (log-id, log-line)

        Is possible to specify the `fromid`, which must be a valid log
        id.

        """
        sql = select([log.c.id, log.c.line]).order_by(log.c.id
        ).where(log.c.task == self.tid)
        if fromid:
            sql = sql.where(log.c.id > fromid)

        with self.engine.begin() as cn:
            return cn.execute(sql).fetchall()

    def _propvalue(self, prop):
        sql = select([task.c[prop]]).where(task.c.id == self.tid)
        return self.engine.execute(sql).scalar()

    @property
    def metadata(self):
        """get the metadata that was supplied to `api.schedule(...)` if any

        """
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
        """provide the task input python object that was supplied to
        `api.schedule(...)` if any

        """
        val = self._propvalue('input')
        if val is not None:
            return loads(val)

    @property
    def raw_input(self):
        """provide the task input that was supplied to `api.schedule(...)` if
        any (either pickled python object or raw byte string)

        """
        return self._propvalue('input')

    @property
    def output(self):
        """provide the task output that was supplied to `task.save_output(...)`
        if any

        """
        out = self._propvalue('output')
        if out is not None:
            return loads(out)

    @property
    def raw_output(self):
        """provide the task output that was supplied to `task.save_output(...)`
        if any (either pickled python object or raw byte string)

        """
        return self._propvalue('output')

    @property
    def traceback(self):
        """get the traceback of a failed task (if any)

        """
        return self._propvalue('traceback')

    @property
    def state(self):
        """provide a comprehensive synthetic task state

        States can be any of `queued`, `running`, `done`, `failed`,
        `aborting`, `aborted`

        """
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
        with self.engine.begin() as cn:
            cn.execute(
                task.update().where(task.c.id == self.tid).values(
                    started=utcnow()
                )
            )
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
            with self.engine.begin() as cn:
                cn.execute(sql)
        finally:
            self.finish()

    def join(self, target='done', timeout=0):
        """synchronous wait on the task

        It is possible to specify the `running` target if one only
        wants to wait for the start of a task.

        A `timeout` parameter can be given. If an actual timeout
        occurs, then a `TimeOut` exception will be raised.

        """
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
        with self.engine.begin() as cn:
            cn.execute(task.update().where(task.c.id == self.tid).values(
                finished=utcnow(),
                status='done')
            )

    def abort(self):
        """ask the abortion of the task

        The effective abortion will be done by the responsible monitor
        and is inherently asynchronous. To wait synchronously for an
        abortion one can do as follows:

        .. code-block:: python

            t = api.schedule(engine, 'mytask', input=42)
            t.join('running')
            t.abort()
            t.join()

        """
        with self.engine.begin() as cn:
            # will still be marked as running
            # the worker kill must do the actual job
            cn.execute(
                task.update().where(task.c.id == self.tid).values(
                    abort=True
                )
            )
            cn.execute(
                worker.update().where(worker.c.id == task.c.worker
                ).where(task.c.id == self.tid
                ).values(kill=True)
            )


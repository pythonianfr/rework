import imp
from pickle import dumps, loads

from sqlalchemy import select

from rework.schema import task


__task_registry__ = {}


class Task(object):
    __slots__ = ('engine', 'tid', 'operation')

    def __init__(self, engine, tid, operation):
        self.engine = engine
        self.tid = tid
        self.operation = operation

    def save_output(self, data):
        sql = task.update().where(task.c.id == self.tid).values(
            output=dumps(data)
        )
        with self.engine.connect() as cn:
            cn.execute(sql)

    def _propvalue(self, prop):
        sql = select([task.c[prop]]).where(task.c.id == self.tid)
        return self.engine.execute(sql).scalar()

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
    def output(self):
        out = self._propvalue('output')
        if out is not None:
            return loads(out)

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
        finally:
            self.finish()

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


def grab_task(engine, wid):
    with engine.connect() as cn:
        sql = ("select id, operation from rework.task "
               "where status = 'queued' "
               "order by id "
               "for update skip locked "
               "limit 1")
        tid_operation = cn.execute(sql).fetchone()
        if tid_operation is None:
            return

        tid, operation = tid_operation
        sql = task.update().where(task.c.id == tid).values(
            status='running',
            worker=wid)
        cn.execute(sql)

        return Task(engine, tid, operation)

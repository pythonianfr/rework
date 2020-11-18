import time
import os
from contextlib import contextmanager
import traceback

from sqlalchemy import create_engine

from sqlhelp import select, update

from rework.helper import (
    has_ancestor_pid,
    memory_usage,
    utcnow
)
from rework.task import Task


class Worker:

    def __init__(self, dburi, worker_id, ppid=None,
                 maxruns=0, maxmem=0,
                 domain='default', debug_port=0):
        self.wid = int(worker_id)
        self.ppid = ppid
        self.maxruns = maxruns
        self.maxmem = maxmem
        self.domain = domain
        self.debugport = debug_port
        self.engine = create_engine(dburi)

    def running_sql(self, running):
        value = {
            'running': running,
            'debugport': self.debugport or None
        }
        if running:
            value['pid'] = os.getpid()
            value['started'] = utcnow()
        return update('rework.worker').where(id=self.wid).values(**value)

    def death_sql(self, cause):
        return update('rework.worker').where(id=self.wid).values(
            deathinfo=cause,
            running=False,
            finished=utcnow()
        )

    def die_if_ancestor_died(self):
        if not self.ppid:
            return
        if not has_ancestor_pid(self.ppid):
            # Our ancestor does not exist any more
            # this is an ambiguous signal that we also must
            # go to bed.
            with self.engine.begin() as cn:
                self.death_sql('ancestor died').do(cn)
            raise SystemExit(f'Worker {os.getpid()} exiting.')

    def ask_shutdown(self):
        with self.engine.begin() as cn:
            update('rework.worker').where(id=self.wid).values(
                shutdown=True
            ).do(cn)

    def shutdown_asked(self):
        return select('shutdown').table('rework.worker').where(
            id=self.wid
        ).do(self.engine).scalar()

    def die_if_shutdown(self):
        if self.shutdown_asked():
            with self.engine.begin() as cn:
                self.death_sql('explicit shutdown').do(cn)
            raise SystemExit(f'Worker {os.getpid()} exiting.')

    @contextmanager
    def running_status(self):
        with self.engine.begin() as cn:
            self.running_sql(True).do(cn)
        try:
            yield
        finally:
            with self.engine.begin() as cn:
                self.running_sql(False).do(cn)

    def run(self):
        if self.debugport:
            import pystuck
            pystuck.run_server(port=self.debugport)

        try:
            with self.running_status():
                self.main_loop()
        except Exception:
            with self.engine.begin() as cn:
                update('rework.worker').where(id=self.wid).values(
                    traceback=traceback.format_exc()
                ).do(cn)
            raise
        except SystemExit as exit:
            raise

    def heartbeat(self):
        self.die_if_ancestor_died()

        mem = memory_usage(os.getpid())
        if (self.maxmem and mem > self.maxmem):
            self.ask_shutdown()

        self.die_if_shutdown()

    def main_loop(self):
        runs = 0
        while True:
            self.heartbeat()
            task = Task.fromqueue(self.engine, self.wid, self.domain)
            while task:
                task.run()

                # run count
                runs += 1
                if self.maxruns and runs >= self.maxruns:
                    return

                self.heartbeat()
                task = Task.fromqueue(self.engine, self.wid, self.domain)

            time.sleep(1)

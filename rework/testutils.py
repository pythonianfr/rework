from contextlib import contextmanager

from rework.helper import guard, kill
from rework.monitor import Monitor


@contextmanager
def workers(engine, numworkers=1, maxruns=0, maxmem=0, domain='default', debug=False):
    monitor = Monitor(engine, domain, numworkers, maxruns, maxmem, debug)
    monitor.reap_dead_workers()
    with engine.connect() as cn:
        cn.execute('delete from rework.task')
        cn.execute('delete from rework.worker')
    procs = monitor.ensure_workers()

    # wait till' they are all running
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda r: r.scalar() == numworkers)
    try:
        yield (monitor, procs)
    finally:
        for pid, in engine.execute(
                'select pid from rework.worker where running = true'
        ).fetchall():
            kill(pid)
        monitor.reap_dead_workers()
        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)


def scrub(anstr, subst='X'):
    out = []
    digit = False
    for char in anstr:
        if char.isdigit():
            if not digit:
                digit = True
        else:
            if digit:
                digit = False
                out.append('<{}>'.format(subst))
            out.append(char)
    # trailing digits ...
    if digit:
        out.append('<{}>'.format(subst))
    return ''.join(out).strip()


def tasks(engine):
    return engine.execute('select * from rework.task').fetchall()

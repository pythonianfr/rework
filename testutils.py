import threading
import time
from contextlib import contextmanager

from rework.monitor import ensure_workers, reap_dead_workers


def wait_true(func, timeout=6):
    outcome = []

    def loop():
        start = time.time()
        while True:
            if (time.time() - start) > timeout:
                return
            output = func()
            if output:
                outcome.append(output)
                return
            time.sleep(.1)

    th = threading.Thread(target=loop)
    th.daemon = True
    th.start()
    th.join()
    assert outcome
    return outcome[0]


def guard(engine, sql, expr, timeout=6):

    def check():
        with engine.connect() as cn:
            return expr(cn.execute(sql))

    return wait_true(check, timeout)


@contextmanager
def test_workers(engine, numworkers=1):
    reap_dead_workers(engine)
    with engine.connect() as cn:
        cn.execute('delete from rework.task')
        cn.execute('delete from rework.worker')
    procs = ensure_workers(engine, numworkers)

    # wait till' they are all running
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda r: r.scalar() == numworkers,
          3)
    yield [wid for wid, _proc in procs]
    for _wid, proc in procs:
        proc.kill()
    reap_dead_workers(engine)


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


def workers(engine):
    return engine.execute('select * from rework.worker').fetchall()

from contextlib import contextmanager

from rework import api


@contextmanager
def workers(engine, numworkers=1, minworkers=None,
            maxruns=0, maxmem=0,
            domain='default', debug=False,
            start_timeout=30):
    with engine.begin() as cn:
        cn.execute('delete from rework.task')
        cn.execute('delete from rework.worker')
    with api.workers(
            engine, domain,
            minworkers, numworkers,
            maxruns, maxmem,
            debug,
            start_timeout=start_timeout
    ) as mon:
        mon.wait_all_started()
        yield mon


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

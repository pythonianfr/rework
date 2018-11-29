from contextlib import contextmanager

from rework.monitor import Monitor


@contextmanager
def workers(engine, numworkers=1, minworkers=None,
            maxruns=0, maxmem=0,
            domain='default', debug=False):
    """context manager to set up a test monitor for a given domain

    It can be configured with `numworkers`, `minworkers`, `maxruns`,
    `maxmem`, `domain` and `debug`.

    Here's an example usage:

    .. code-block:: python

        with workers(engine, numworkers=2) as monitor:
            t1 = api.schedule(engine, 'compute_sum', [1, 2, 3, 4, 5])
            t2 = api.schedule(engine, 'compute_sum', 'abcd')

            t1.join()
            assert t1.output == 15
            t2.join()
            assert t2.traceback.endswith(
                "TypeError: unsupported operand type(s) "
                "for +: 'int' and 'str'"
            )

            assert len(monitor.wids) == 2

    At the end of the block, the monitor-controlled workers are
    cleaned up.

    However the database entries are still there.

    """
    with engine.begin() as cn:
        cn.execute('delete from rework.task')
        cn.execute('delete from rework.worker')
    monitor = Monitor(engine, domain, minworkers, numworkers, maxruns, maxmem, debug)
    monitor.register()
    monitor.ensure_workers()

    yield monitor
    monitor.killall()
    monitor.unregister()


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

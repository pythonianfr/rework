from datetime import datetime
from pathlib import Path
import threading

import pytest

from rework import api
from rework.schema import worker
from rework.monitor import Monitor
from rework.task import Task, TimeOut
from rework.worker import running_status, shutdown_asked
from rework.helper import guard, wait_true
from rework.testutils import scrub, workers


def test_basic_task_operations(engine):
    api.schedule(engine, 'print_sleep_and_go_away', 21,
                 metadata={'user': 'Joe'})

    known = [
        (name, Path(path).name)
        for name, path in engine.execute(
                'select name, path from rework.operation order by name'
        ).fetchall()
    ]
    assert [
        ('allocate_and_leak_mbytes', 'tasks.py'),
        ('capture_logs', 'tasks.py'),
        ('flush_captured_stdout', 'tasks.py'),
        ('infinite_loop', 'tasks.py'),
        ('log_swarm', 'tasks.py'),
        ('normal_exception', 'tasks.py'),
        ('print_sleep_and_go_away', 'tasks.py'),
        ('raw_input', 'tasks.py'),
        ('run_in_non_default_domain', 'tasks.py'),
        ('stderr_swarm', 'tasks.py'),
        ('unstopable_death', 'tasks.py'),
    ] == known

    mon = Monitor(engine)
    wid = mon.new_worker()
    t = Task.fromqueue(engine, wid)
    t.run()
    assert t.output == 42
    assert t.metadata == {'user': 'Joe'}

    cdate = t._propvalue('created')
    now = datetime.now()
    assert now.year == cdate.year
    assert now.month == cdate.month

    t2 = Task.byid(engine, t.tid)
    assert (t2.tid, t2.operation) == (t.tid, t.operation)

    t3 = Task.byid(engine, 42000)
    assert t3 is None

    with pytest.raises(Exception) as err:
        api.schedule(engine, 'no_such_task')
    assert err.value.args[0] == 'No operation was found for these parameters'


def test_basic_worker_operations(engine):
    mon = Monitor(engine)
    wid = mon.new_worker()

    with running_status(engine, wid, None):
        assert engine.execute(
            'select count(id) from rework.worker where running = true'
        ).scalar() == 1

    assert engine.execute(
        'select count(id) from rework.worker where running = true'
    ).scalar() == 0


def test_basic_worker_task_execution(engine):
    t = api.schedule(engine, 'print_sleep_and_go_away', 21)
    assert t.state == 'queued'

    guard(engine, "select count(id) from rework.task where status = 'queued'",
          lambda res: res.scalar() == 1)
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 0)

    mon = Monitor(engine, 'default', maxworkers=1)
    mon.ensure_workers()

    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 1)

    guard(engine, "select count(id) from rework.task where status = 'running'",
          lambda res: res.scalar() == 1)

    t.join()
    assert t.output == 42
    assert t.state == 'done'

    guard(engine, "select count(id) from rework.task where status = 'running'",
          lambda res: res.scalar() == 0)


def test_monitor_base(engine):
    with workers(engine) as mon:
        assert engine.execute(
            'select count(id) from rework.monitor where id = {}'.format(mon.monid)
        ).scalar() == 1

        res = engine.execute(
            'select options from rework.monitor where id = {}'.format(mon.monid)
        ).scalar()
        assert {
            'maxmem': 0,
            'maxruns': 0,
            'debugport': 0,
            'maxworkers': 1,
            'minworkers': 1
        } == res

    # generic monitor assertions
    guard(engine, 'select count(id) from rework.monitor where id = {}'.format(mon.monid),
          lambda r: r.scalar() == 0)


def test_domain(engine):
    with workers(engine, maxruns=1) as mon:
        wid = mon.wids[0]
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 1)

        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == False)

        assert t1.status == 'queued'
        assert t2.status == 'done'

    with workers(engine, maxruns=1, domain='nondefault') as mon:
        wid = mon.wids[0]
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 1)

        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == False)

        assert t1.status == 'done'
        assert t2.status == 'queued'


def test_domain_map(engine, cleanup):
    with engine.begin() as cn:
        cn.execute('delete from rework.operation')

    api.freeze_operations(engine, domain='nondefault',
                          domain_map={'nondefault': 'fancy'}
    )

    with workers(engine, maxruns=1, domain='fancy'):
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t1.join()
        assert t1.status == 'done'


def test_task_rawinput(engine):
    with workers(engine):
        t = api.schedule(engine, 'raw_input', rawinputdata=b'Babar')
        t.join()
        assert t.raw_output == b'Babar and Celeste'


def test_worker_shutdown(engine):
    with workers(engine) as mon:
        wid = mon.wids[0]
        assert not shutdown_asked(engine, wid)

        with engine.begin() as cn:
            cn.execute(
                worker.update().where(worker.c.id == wid).values(
                    shutdown=True
                )
            )
        assert shutdown_asked(engine, wid)
        guard(engine, 'select shutdown from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)

        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)

        assert u'explicit shutdown' == engine.execute(
            'select deathinfo from rework.worker where id = %(wid)s', wid=wid
        ).scalar()


def test_worker_kill(engine):
    with workers(engine) as mon:
        wid = mon.wids[0]

        with engine.begin() as cn:
            cn.execute(
                worker.update().where(worker.c.id == wid).values(
                    kill=True
                )
            )
        guard(engine, 'select kill from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)

        mon.preemptive_kill()

        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)

        assert engine.execute(
            'select deathinfo from rework.worker where id = %(wid)s', wid=wid
        ).scalar().startswith('preemptive kill')


def test_worker_max_runs(engine):
    with workers(engine, maxruns=2) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t.join()

        assert t.output == 'aa'
        assert not shutdown_asked(engine, wid)

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t.join()

        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == False)

    with workers(engine, maxruns=1) as mon:
        wid = mon.wids[0]

        t1 = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 'a')

        t1.join()

        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == False)

        assert t2.state == 'queued'
        assert t2.worker is None

        end = t2._propvalue('finished')
        assert end is None


def test_worker_max_mem(engine):
    with workers(engine, maxmem=100) as mon:
        wid = mon.wids[0]

        t1 = api.schedule(engine, 'allocate_and_leak_mbytes', 50)
        t1.join()

        assert engine.execute(
            'select mem from rework.worker where id = {}'.format(wid)
        ).scalar() == 0
        mon.track_memory()
        assert engine.execute(
            'select mem from rework.worker where id = {}'.format(wid)
        ).scalar() > 50

        t2 = api.schedule(engine, 'allocate_and_leak_mbytes', 100)
        t2.join()
        guard(engine, 'select shutdown from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)
        assert shutdown_asked(engine, wid)


def test_task_abortion(engine):
    with workers(engine) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'infinite_loop')
        guard(engine, 'select count(id) from rework.task where worker = {}'.format(wid),
              lambda res: res.scalar() == 1)

        assert t.state == 'running'

        with pytest.raises(TimeOut) as err:
            t.join(timeout=.1)
        assert err.value.args[0] == t

        t.abort()
        assert t.aborted
        # this is potentially racy but might work most of the time
        assert t.state == 'aborting'

        mon.preemptive_kill()
        t.join()
        assert t.state == 'aborted'

        # one dead worker
        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda res: not res.scalar())

        diagnostic = engine.execute(
            'select deathinfo from rework.worker where id = {}'.format(wid)
        ).scalar()

        assert 'preemptive kill at <X>-<X>-<X> <X>:<X>:<X>.<X>+<X>:<X>' == scrub(diagnostic)

        start = t._propvalue('created')
        end = t._propvalue('finished')
        assert end > start


def test_worker_unplanned_death(engine):
    with workers(engine) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'unstopable_death')

        deadlist = wait_true(mon.reap_dead_workers)
        assert wid in deadlist

        guard(engine, 'select deathinfo from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == 'Unaccounted death (hard crash)')

        assert t.state == 'done'

        start = t._propvalue('created')
        end = t._propvalue('finished')
        assert end > start


def test_task_error(engine):
    with workers(engine):
        t = api.schedule(engine, 'normal_exception')
        t.join()
        assert t.traceback.strip().endswith('oops')
        assert t.state == 'failed'

        start = t._propvalue('created')
        end = t._propvalue('finished')
        assert end > start


def test_task_logging_capture(engine):
    with engine.begin() as cn:
        cn.execute('delete from rework.task')

    with workers(engine, 2):
        t1 = api.schedule(engine, 'capture_logs')
        t2 = api.schedule(engine, 'capture_logs')

        t1.join()
        t2.join()

        t1logs = [scrub(logline) for id_, logline in t1.logs()]
        assert [
            u'my_app_logger:ERROR: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X>',
            u'stdout:INFO: <X>-<X>-<X> <X>:<X>:<X>: I want to be captured',
            u'my_app_logger:DEBUG: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X> also'
        ] == t1logs

        t2logs = [scrub(logline) for id_, logline in t2.logs()]
        assert [
            u'my_app_logger:ERROR: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X>',
            u'stdout:INFO: <X>-<X>-<X> <X>:<X>:<X>: I want to be captured',
            u'my_app_logger:DEBUG: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X> also'
        ] == t2logs

        t3 = api.schedule(engine, 'capture_logs')
        t3.join()

        logids = [lid for lid, logline_ in t3.logs()]
        assert 2 == len(t3.logs(fromid=logids[0]))


def test_logging_stress_test(engine):
    with engine.begin() as cn:
        cn.execute('delete from rework.log')

    with workers(engine):
        t = api.schedule(engine, 'log_swarm')

        t.join()
        records = engine.execute(
            'select id, line from rework.log where task = {}'.format(t.tid)
        ).fetchall()

        # we check that there is a constant offset between the
        # log id and the idx that is emitted by the task code
        # => ordering has been preserved
        offsets = [lid - int(line.rsplit(',')[-1].strip())
                   for lid, line in records]
        assert all(offsets[0] == offset for offset in offsets)

        assert len(list(t.logs())) == 249
        assert len(list(t.logs(fromid=245))) == 4 + offsets[0]


def test_process_lock(engine):
    with workers(engine):
        t = api.schedule(engine, 'stderr_swarm')

        def join():
            Task.byid(t.engine, t.tid).join(timeout=2)

        thr = threading.Thread(target=join)
        thr.start()
        thr.join()
        assert t.state == 'done'


def test_captured_stdout(engine):
    with workers(engine):
        t = api.schedule(engine, 'flush_captured_stdout')
        t.join()
        assert t.state == 'done'
        logs = t.logs()
        assert len(logs) == 5

        assert 'Hello World' in logs[0][1]
        third = logs[1][1]
        assert third.endswith(
            'This is an unfinished statement which could go on '
            'for a long time, but I have had enough'
        )
        fourth = logs[2][1]
        assert fourth.endswith('A truly multiline')
        fifth = logs[3][1]
        assert fifth.endswith('statement.')


def test_cleanup_unstarted(engine):
    with engine.begin() as cn:
        cn.execute('delete from rework.task')
        cn.execute('delete from rework.worker')

    mon = Monitor(engine, 'default', None, 1, 1, 0, False)
    mon.register()
    mon.ensure_workers()

    nworkers = engine.execute('select count(*) from rework.worker').scalar()
    assert nworkers == 1

    t = api.schedule(engine, 'raw_input', b'foo')
    t.join()

    mon.killall(msg=None)
    mon.cleanup_unstarted()

    assert engine.execute('select count(*) from rework.worker').scalar() == 0
    assert engine.execute('select count(*) from rework.task').scalar() == 0

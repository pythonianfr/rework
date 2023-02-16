from datetime import datetime
from pathlib import Path
import threading
import time

import pytest
from sqlhelp import insert, update

from rework import api
from rework.monitor import Monitor
from rework.task import Task, TimeOut
from rework.worker import Worker
from rework.helper import (
    cpu_usage,
    delta_isoformat,
    guard,
    kill_process_tree,
    memory_usage,
    parse_delta,
    wait_true
)
from rework.testutils import scrub, workers


def test_helper():
    with pytest.raises(ValueError):
        cpu_usage(-1)

    with pytest.raises(ValueError):
        memory_usage(-1)


def test_basic_task_operations(engine, cleanup):
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
        ('fancy_inputs_outputs', 'tasks.py'),
        ('flush_captured_stdout', 'tasks.py'),
        ('infinite_loop', 'tasks.py'),
        ('infinite_loop_long_timeout', 'tasks.py'),
        ('infinite_loop_timeout', 'tasks.py'),
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

    cdate = t._propvalue('queued')
    now = datetime.now()
    assert now.year == cdate.year
    assert now.month == cdate.month

    t2 = Task.byid(engine, t.tid)
    assert (t2.tid, t2.operation) == (t.tid, t.operation)

    t3 = Task.byid(engine, 42000)
    assert (t3.tid, t3.operation) == (42000, None)

    with pytest.raises(Exception) as err:
        api.schedule(engine, 'no_such_task')
    assert scrub(err.value.args[0]) == (
        'No operation was found for these parameters: '
        'operation=`no_such_task` domain=`None` host=`<X>.<X>.<X>.<X>`'
    )


def test_monitor_step(engine, cleanup):
    mon = Monitor(engine)
    api.schedule(engine, 'print_sleep_and_go_away', 21,
                 metadata={'user': 'Joe'})

    stats = mon.step()
    mon.wait_all_started()

    assert len(stats.new) == 2
    assert all(
        isinstance(w, int)
        for w in stats.new
    )

    # simulate a hard crash
    for _, proc in mon.workers.items():
        if proc.poll() is None:
            kill_process_tree(proc.pid)

    dead = mon.reap_dead_workers()
    assert sorted(dead) == sorted(stats.new)

    stats2 = mon.step()
    assert stats2.new != stats.new
    assert len(stats2.new) == 2
    assert all(
        isinstance(w, int)
        for w in stats2.new
    )

    mon.killall()


def test_task_input_output(engine, cleanup):
    with workers(engine) as mon:
        t = api.schedule(engine, 'raw_input', rawinputdata=b'Hello Babar')
        t.join()

        with pytest.raises(TypeError):
            t.input
        ri = t.raw_input
        with pytest.raises(TypeError):
            t.output
        ro = t.raw_output
        assert ri == b'Hello Babar'
        assert ro == b'Hello Babar and Celeste'

        t = api.schedule(engine, 'unstopable_death')
        wait_true(mon.reap_dead_workers)

        assert t.input is None
        assert t.raw_input is None
        assert t.output is None
        assert t.raw_output is None


def test_basic_worker_operations(engine, cleanup):
    mon = Monitor(engine)
    wid = mon.new_worker()
    worker = Worker(engine.url, wid)

    with worker.running_status():
        assert engine.execute(
            'select count(id) from rework.worker where running = true'
        ).scalar() == 1

    assert engine.execute(
        'select count(id) from rework.worker where running = true'
    ).scalar() == 0


def test_failed_pending_start(engine, cleanup):
    from rework.api import workers

    with workers(engine, maxworkers=2, minworkers=2, start_timeout=0) as mon:
        stat1 = mon.step()
        pending = mon.pending_start.copy()
        assert len(pending) == 2
        assert len(mon.wids) == 2

        time.sleep(.1)
        stat2 = mon.step()
        assert len(mon.pending_start) == 2
        assert len(mon.wids) == 2

        assert mon.pending_start != pending
        out = [
            (run, kill, info[:15])
            for run, kill, info in engine.execute(
                    'select running, kill, deathinfo from rework.worker '
                    'where id in %(wids)s',
                    wids=tuple(pending.keys())
            )
        ]
        assert out == [
            (False, True, 'preemptive kill'),
            (False, True, 'preemptive kill')
        ]


def test_basic_worker_task_execution(engine, cleanup):
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


def test_monitor_base(engine, cleanup):
    with workers(engine) as mon:
        assert engine.execute(
            f'select count(id) from rework.monitor where id = {mon.monid}'
        ).scalar() == 1

        res = engine.execute(
            f'select options from rework.monitor where id = {mon.monid}'
        ).scalar()
        assert {
            'maxmem': 0,
            'maxruns': 0,
            'debugport': 0,
            'maxworkers': 1,
            'minworkers': 1
        } == res

    # generic monitor assertions
    guard(engine, f'select count(id) from rework.monitor where id = {mon.monid}',
          lambda r: r.scalar() == 0)


def test_domain(engine, cleanup):
    with workers(engine, maxruns=1) as mon:
        wid = mon.wids[0]
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 1)

        guard(engine, f'select running from rework.worker where id = {wid}',
              lambda r: r.scalar() == False)

        assert t1.status == 'queued'
        assert t2.status == 'done'

    with workers(engine, maxruns=1, domain='nondefault') as mon:
        wid = mon.wids[0]
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 1)

        guard(engine, f'select running from rework.worker where id = {wid}',
              lambda r: r.scalar() == False)

        assert t1.status == 'done'
        assert t2.status == 'queued'


def test_worker_two_runs_nondfefault_domain(engine, cleanup):
    with workers(engine, maxruns=2, domain='nondefault') as mon:
        t1 = api.schedule(engine, 'run_in_non_default_domain')
        t2 = api.schedule(engine, 'run_in_non_default_domain')
        t3 = api.schedule(engine, 'run_in_non_default_domain')

        t1.join()
        t2.join()
        assert t1.status == 'done'
        assert t2.status == 'done'
        assert t3.status == 'queued'


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


def test_task_rawinput(engine, cleanup):
    with workers(engine):
        t = api.schedule(engine, 'raw_input', rawinputdata=b'Babar')
        t.join()
        assert t.raw_output == b'Babar and Celeste'


def test_worker_shutdown(engine, cleanup):
    with workers(engine) as mon:
        wid = mon.wids[0]
        worker = Worker(engine.url, wid)
        assert not worker.shutdown_asked()

        with engine.begin() as cn:
            update('rework.worker').where(id=wid).values(
                shutdown=True
            ).do(cn)
        assert worker.shutdown_asked()
        guard(engine, f'select shutdown from rework.worker where id = {wid}',
              lambda r: r.scalar() == True)

        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)

        assert u'explicit shutdown' == engine.execute(
            'select deathinfo from rework.worker where id = %(wid)s', wid=wid
        ).scalar()


def test_worker_kill(engine, cleanup):
    with workers(engine) as mon:
        wid = mon.wids[0]

        with engine.begin() as cn:
            update('rework.worker').where(id=wid).values(
                kill=True
            ).do(cn)
        guard(engine, f'select kill from rework.worker where id = {wid}',
              lambda r: r.scalar() == True)

        mon.preemptive_kill()

        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)

        assert engine.execute(
            'select deathinfo from rework.worker where id = %(wid)s', wid=wid
        ).scalar().startswith('preemptive kill')


def test_worker_max_runs(engine, cleanup):
    with workers(engine, maxruns=2) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t.join()

        assert t.output == 'aa'
        worker = Worker(engine.url, wid)
        assert not worker.shutdown_asked()

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t.join()

        guard(engine, f'select running from rework.worker where id = {wid}',
              lambda r: r.scalar() == False)

    with workers(engine, maxruns=1) as mon:
        wid = mon.wids[0]

        t1 = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        t2 = api.schedule(engine, 'print_sleep_and_go_away', 'a')

        t1.join()

        guard(engine, f'select running from rework.worker where id = {wid}',
              lambda r: r.scalar() == False)

        assert t2.state == 'queued'
        assert t2.worker is None

        end = t2._propvalue('finished')
        assert end is None


def test_worker_max_mem(engine, cleanup):
    with workers(engine, maxmem=100) as mon:
        wid = mon.wids[0]

        t1 = api.schedule(engine, 'allocate_and_leak_mbytes', 50)
        t1.join()

        assert engine.execute(
            f'select mem from rework.worker where id = {wid}'
        ).scalar() == 0
        mon.track_resources()
        assert engine.execute(
            f'select mem from rework.worker where id = {wid}'
        ).scalar() > 50
        t2 = api.schedule(engine, 'allocate_and_leak_mbytes', 100)
        t2.join()
        guard(engine, f'select shutdown from rework.worker where id = {wid}',
              lambda r: r.scalar() == True)
        worker = Worker(engine.url, wid)
        assert worker.shutdown_asked()


def test_task_abortion(engine, cleanup):
    with workers(engine) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'infinite_loop', True)
        guard(engine, f'select count(id) from rework.task where worker = {wid}',
              lambda res: res.scalar() == 1)

        assert t.state == 'running'

        with pytest.raises(TimeOut) as err:
            t.join(timeout=.1)
        assert err.value.args[0] == t

        # check cpu usage
        mon.track_resources()
        cpu = engine.execute(
            f'select cpu from rework.worker where id = {wid}'
        ).scalar()
        assert cpu > 0

        t.abort()
        assert t.aborted
        # this is potentially racy but might work most of the time
        assert t.state == 'aborting'

        mon.preemptive_kill()
        t.join()
        assert t.state == 'aborted'
        assert t.deathinfo.startswith('preemptive kill')

        # one dead worker
        guard(engine, f'select running from rework.worker where id = {wid}',
              lambda res: not res.scalar())

        diagnostic = engine.execute(
            f'select deathinfo from rework.worker where id = {wid}'
        ).scalar()

        assert 'preemptive kill at <X>-<X>-<X> <X>:<X>:<X>.<X>+<X>:<X>' == scrub(diagnostic)

        queued = t._propvalue('queued')
        started = t._propvalue('started')
        finished = t._propvalue('finished')
        assert finished > started > queued


def test_worker_unplanned_death(engine, cleanup):
    with workers(engine) as mon:
        wid = mon.wids[0]

        t = api.schedule(engine, 'unstopable_death')

        deadlist = wait_true(mon.reap_dead_workers)
        assert wid in deadlist

        guard(engine, f'select deathinfo from rework.worker where id = {wid}',
              lambda r: r.scalar() == 'Unaccounted death (hard crash)')

        assert t.state == 'aborted'

        start = t._propvalue('queued')
        end = t._propvalue('finished')
        assert end > start
        assert t.deathinfo == 'Unaccounted death (hard crash)'


def test_killed_task(engine, cleanup):
    with workers(engine) as mon:
        t = api.schedule(engine, 'infinite_loop')
        t.join('running')

    assert t.state == 'aborted'
    assert t.traceback is None

    try:
        with workers(engine) as mon:
            t = api.schedule(engine, 'infinite_loop')
            t.join('running')
            raise Exception('kill the monitor')
    except:
        pass

    assert t.state == 'aborted'
    assert 'kill the monitor' in t.traceback


def test_task_error(engine, cleanup):
    with workers(engine):
        t = api.schedule(engine, 'normal_exception')
        t.join()
        assert t.traceback.strip().endswith('oops')
        assert t.state == 'failed'

        start = t._propvalue('queued')
        end = t._propvalue('finished')
        assert end > start


def test_task_logging_capture(engine, cleanup):
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


def test_logging_stress_test(engine, cleanup):
    with workers(engine):
        t = api.schedule(engine, 'log_swarm')

        t.join()
        records = engine.execute(
            f'select id, line from rework.log where task = {t.tid}'
        ).fetchall()

        # we check that there is a constant offset between the
        # log id and the idx that is emitted by the task code
        # => ordering has been preserved
        offsets = [lid - int(line.rsplit(',')[-1].strip())
                   for lid, line in records]
        assert all(offsets[0] == offset for offset in offsets)

        assert len(list(t.logs())) == 249
        assert len(list(t.logs(fromid=245))) == 4 + offsets[0]


def test_process_lock(engine, cleanup):
    with workers(engine):
        t = api.schedule(engine, 'stderr_swarm')

        def join():
            Task.byid(t.engine, t.tid).join(timeout=2)

        thr = threading.Thread(target=join)
        thr.start()
        thr.join()
        assert t.state == 'done'


def test_captured_stdout(engine, cleanup):
    with workers(engine):
        t = api.schedule(engine, 'flush_captured_stdout')
        t.join()
        assert t.state == 'done'
        logs = t.logs()
        assert len(logs) == 7

        logs = [line.split(':')[-1] for lid, line in logs]
        assert logs == [
            ' Hello World',
            ' This is an unfinished statement which could go on for a long time, but I have had enough',
            ' A truly multiline',
            ' statement.',
            ' ',
            ' Honor the space.',
            '  (hi) '
        ]


def test_cleanup_unstarted(engine, cleanup):
    mon = Monitor(engine, 'default', None, 1, 1, 0, False)
    mon.register()
    mon.ensure_workers()

    with engine.begin() as cn:
        insert('rework.worker').values(
            host='127.0.0.1',
            domain='default'
        ).do(cn)  # unborn worker

    nworkers = engine.execute('select count(*) from rework.worker').scalar()
    assert nworkers == 2

    t = api.schedule(engine, 'raw_input', b'foo')
    t.join()

    mon.killall(msg=None)
    deleted = mon.cleanup_unstarted()
    assert deleted == 1

    assert engine.execute('select count(*) from rework.worker').scalar() == 1
    assert engine.execute('select count(*) from rework.task').scalar() == 1

    deleted = mon.cleanup_unstarted()
    assert deleted == 0


def test_more_unstarted(engine, cleanup):
    with workers(engine) as mon:
        nworkers = engine.execute('select count(*) from rework.worker').scalar()
        assert nworkers == 1

        with engine.begin() as cn:
            insert('rework.worker').values(
                host='127.0.0.1',
                domain='default'
            ).do(cn)  # unborn worker

        nworkers = engine.execute('select count(*) from rework.worker').scalar()
        assert nworkers == 2

        t1 = api.schedule(engine, 'raw_input', b'foo')
        t2 = api.schedule(engine, 'raw_input', b'foo')
        t1.join()
        t2.join()
        assert engine.execute('select count(*) from rework.task').scalar() == 2

        nworkers = engine.execute('select count(*) from rework.worker').scalar()
        assert nworkers == 2
        mon.cleanup_unstarted()
        nworkers = engine.execute('select count(*) from rework.worker').scalar()
        assert nworkers == 1

        assert engine.execute('select count(*) from rework.task').scalar() == 2


def test_timeout(engine, cleanup):
    # first, a small unittest on utility functions
    d1 = datetime(2018, 1, 1)
    d2 = datetime(2018, 3, 3, 12, 45, 30)
    delta = d2 - d1
    iso = delta_isoformat(delta)
    assert iso == 'P61DT0H0M45930S'
    delta_out = parse_delta(iso)
    assert delta == delta_out

    with workers(engine, numworkers=3) as mon:
        t1 = api.schedule(engine, 'infinite_loop_timeout')
        t2 = api.schedule(engine, 'infinite_loop_timeout')
        t3 = api.schedule(engine, 'infinite_loop_long_timeout')
        t1.join('running')
        t2.join('running')
        t3.join('running')

        time.sleep(1)  # make sure we're going to time out
        mon.track_timeouts()
        assert t1.state == 'aborting'
        assert t2.state == 'aborting'
        assert t3.state == 'running'

        mon.preemptive_kill()
        assert t1.state == 'aborted'
        assert t2.state == 'aborted'
        assert t3.state == 'running'


def test_scheduler(engine, cleanup):
    sid = api.prepare(
        engine,
        'run_in_non_default_domain',
        rule='* * * * * *',
        domain='nondefault',
        _anyrule=True
    )

    res = engine.execute(
        'select id, domain, inputdata, host, metadata, rule '
        'from rework.sched where sched.id=%(sid)s',
        sid=sid
    ).fetchall()
    assert res == [(sid, 'nondefault', None, None, None, '* * * * * *')]

    with workers(engine, domain='nondefault') as mon:
        mon.step()
        assert str(mon.scheduler) == (
            "<scheduler for nondefault ->\n"
            "[('run_in_non_default_domain', 'nondefault', None, None, None, '* * * * * *')]>"
        )


def test_scheduled_overlap(engine, cleanup):
    api.prepare(
        engine,
        'infinite_loop',
        rule='* * * * * *',
        _anyrule=True
    )
    api.prepare(
        engine,
        'infinite_loop_long_timeout',
        rule='* * * * * *',
        _anyrule=True
    )
    with workers(engine, numworkers=2) as mon:
        mon.step()
        time.sleep(2)

    nbtasks = engine.execute(
        'select count(t.id) '
        'from rework.task as t, rework.operation as o '
        'where t.operation = o.id and '
        '      o.name = \'infinite_loop\''
    ).scalar()
    assert nbtasks == 1
    nbtasks = engine.execute(
        'select count(t.id) '
        'from rework.task as t, rework.operation as o '
        'where t.operation = o.id and '
        '      o.name = \'infinite_loop_long_timeout\''
    ).scalar()
    assert nbtasks == 1


def test_with_outputs(engine, cleanup):
    with workers(engine):
        t = api.schedule(
            engine,
            'fancy_inputs_outputs',
            {
                'myfile': b'hello world',
                'foo': 42,
                'bar': 'Babar'
            }
        )
        t.join()

    assert t.output == {
        'message': 'file length: 11,foo: 42,bar: Babar'
    }

    with workers(engine):
        t = api.schedule(
            engine,
            'fancy_inputs_outputs',
            {
                'myfile': b'hello world',
                'foo': 42,
                'bar': 'Celeste'
            }
        )
        t.join()

    assert t.traceback.strip().endswith(
        'ValueError: unknown inputs: blogentry'
    )

from functools import partial
from datetime import datetime
from pathlib import Path

from rework import api
from rework.schema import worker
from rework.task import Task
from rework.worker import running_status, shutdown_asked
from rework.monitor import new_worker, ensure_workers, reap_dead_workers
from rework.helper import kill, read_proc_streams, guard, wait_true, memory_usage
from rework.testutils import scrub, workers

# our test tasks
from . import tasks


def test_basic_task_operations(engine):
    api.freeze_operations(engine)

    api.schedule(engine, 'print_sleep_and_go_away', 21,
                 metadata={'user': 'Joe'})

    expected = [(name, Path(path).name)
                for name, path in engine.execute(
                        'select name, path from rework.operation order by name'
                ).fetchall()
    ]
    assert [
        ('allocate_and_leak_mbytes', 'tasks.py'),
        ('capture_logs', 'tasks.py'),
        ('infinite_loop', 'tasks.py'),
        ('log_swarm', 'tasks.py'),
        ('normal_exception', 'tasks.py'),
        ('print_sleep_and_go_away', 'tasks.py'),
        ('unstopable_death', 'tasks.py')
    ] == expected

    wid = new_worker(engine)
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


def test_basic_worker_operations(engine):
    wid = new_worker(engine)

    with running_status(engine, wid):
        assert engine.execute(
            'select count(id) from rework.worker where running = true'
        ).scalar() == 1

    assert engine.execute(
        'select count(id) from rework.worker where running = true'
    ).scalar() == 0


def test_basic_worker_task_execution(engine):
    api.freeze_operations(engine)
    t = api.schedule(engine, 'print_sleep_and_go_away', 21)
    assert t.state == 'queued'

    guard(engine, "select count(id) from rework.task where status = 'queued'",
          lambda res: res.scalar() == 1)
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 0)

    proc = ensure_workers(engine, 1, 0, 0)[0][1]

    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 1)

    guard(engine, "select count(id) from rework.task where status = 'running'",
          lambda res: res.scalar() == 1)

    guard(engine, 'select output from rework.task where id = {}'.format(t.tid),
          lambda res: res.scalar())

    assert t.output == 42
    assert t.state == 'done'

    logs = []
    for log in read_proc_streams(proc):
        logs.append(log)
        if len(logs) > 3:
            break
    kill(proc.pid)
    # What's going on there on windows ?
    # We actually killed the parent process of the real worker process
    # hence the real worker detects his parent just died
    # and can write himself off the list.
    guard(engine, "select count(id) from rework.task where status = 'running'",
          lambda res: res.scalar() == 0)

    assert [
        ('stdout', 'Hello, world'),
        ('stdout', 'I am running within task <X>'),
        ('stdout', 'Saving computation to task.output'),
        ('stdout', 'And now I am done.'),
    ] == list((stream, scrub(line.decode('utf-8')))
              for stream, line in logs)


def test_worker_shutdown(engine):
    with workers(engine) as wids:
        wid = wids[0]
        assert not shutdown_asked(engine, wid)

        with engine.connect() as cn:
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


def test_worker_max_runs(engine):
    api.freeze_operations(engine)
    with workers(engine, maxruns=2) as wids:
        wid = wids[0]

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')

        finished = lambda t: t.status == 'done'
        wait_true(partial(finished, t))

        assert t.output == 'aa'
        assert not shutdown_asked(engine, wid)

        t = api.schedule(engine, 'print_sleep_and_go_away', 'a')
        wait_true(partial(finished, t))

        guard(engine, 'select shutdown from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)
        assert shutdown_asked(engine, wid)


def test_worker_max_mem(engine):
    mem = memory_usage()
    api.freeze_operations(engine)

    with workers(engine, maxmem=100) as wids:
        wid = wids[0]

        t = api.schedule(engine, 'allocate_and_leak_mbytes', 100)

        finished = lambda t: t.status == 'done'
        wait_true(partial(finished, t))

        guard(engine, 'select shutdown from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)
        assert shutdown_asked(engine, wid)


def test_task_abortion(engine):
    api.freeze_operations(engine)

    with workers(engine) as wids:
        wid = wids[0]

        t = api.schedule(engine, 'infinite_loop')
        guard(engine, 'select count(id) from rework.task where worker = {}'.format(wid),
              lambda res: res.scalar() == 1)

        assert t.state == 'running'

        t.abort()
        assert t.aborted
        # this is potentially racy but might work most of the time
        assert t.state == 'aborting'

        guard(engine, "select count(id) from rework.task "
              "where status = 'done' and worker = {}".format(wid),
              lambda res: res.scalar() == 1)

        assert t.state == 'aborted'

        # one dead worker
        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda res: not res.scalar())

        diagnostic = engine.execute(
            'select deathinfo from rework.worker where id = {}'.format(wid)
        ).scalar()

        assert 'Task <X> aborted' == scrub(diagnostic)


def test_worker_unplanned_death(engine):
    api.freeze_operations(engine)

    with workers(engine) as wids:
        wid = wids[0]

        t = api.schedule(engine, 'unstopable_death')

        deadlist = wait_true(partial(reap_dead_workers, engine))
        assert wid in deadlist

        guard(engine, 'select deathinfo from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == 'Unaccounted death (hard crash)')

        assert t.state == 'done'


def test_task_error(engine):
    api.freeze_operations(engine)

    with workers(engine):

        t = api.schedule(engine, 'normal_exception')

        tb = guard(engine, 'select traceback from rework.task where id = {}'.format(t.tid),
                   lambda r: r.scalar())

        assert tb.strip().endswith('oops')
        assert t.traceback == tb
        assert t.state == 'failed'


def test_task_logging_capture(engine):
    api.freeze_operations(engine)
    with engine.connect() as cn:
        cn.execute('delete from rework.task')

    with workers(engine, 2):
        t1 = api.schedule(engine, 'capture_logs')
        t2 = api.schedule(engine, 'capture_logs')

        finished = lambda t: t.status == 'done'
        wait_true(partial(finished, t1))
        wait_true(partial(finished, t2))

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
        wait_true(partial(finished, t3))

        logids = [lid for lid, logline_ in t3.logs()]
        assert 2 == len(t3.logs(fromid=logids[0]))


def test_logging_stress_test(engine):
    api.freeze_operations(engine)
    with engine.connect() as cn:
        cn.execute('delete from rework.log')

    with workers(engine):
        t = api.schedule(engine, 'log_swarm')

        wait_true(partial(lambda t: t.status == 'done', t))
        records = engine.execute(
            'select id, line from rework.log where task = {}'.format(t.tid)
        ).fetchall()

        # we check that there is a constant offset between the
        # log id and the idx that is emitted by the task code
        # => ordering has been preserved
        offsets = [lid - int(line.rsplit(',')[-1].strip())
                   for lid, line in records]
        assert all(offsets[0] == offset for offset in offsets)

import time
from functools import partial
import logging

from rework import api
from rework.schema import worker
from rework.task import Task
from rework.worker import running_status, shutdown_asked
from rework.monitor import new_worker, ensure_workers, reap_dead_workers
from rework.helper import kill, read_proc_streams
from rework.testutils import guard, scrub, wait_true, test_workers


@api.task
def print_sleep_and_go_away(task):
    print('Hello, world')
    time.sleep(.2)
    print('I am running within task', task.tid)
    time.sleep(.2)
    print('Saving computation to task.output')
    task.save_output(2 * task.input)
    print('And now I am done.')


@api.task
def infinite_loop(task):
    while True:
        time.sleep(1)


@api.task
def unstopable_death(task):
    import os
    os._exit(0)


@api.task
def normal_exception(task):
    raise Exception('oops')


@api.task
def capture_logs(task):
    logger = logging.getLogger('my_app_logger')
    logger.debug('uncaptured %s', 42)
    with task.capturelogs(std=True):
        logger.error('will be captured %s', 42)
        print('I want to be captured')
        logger.debug('will be captured %s also', 1)
    logger.debug('uncaptured %s', 42)
    print('This will be lost')


@api.task
def log_swarm(task):
    logger = logging.getLogger('APP')
    with task.capturelogs():
        for i in range(1, 250):
            logger.info('I will fill your database, %s', i)


def test_basic_task_operations(engine):
    api.freeze_operations(engine)
    api.schedule(engine, 'print_sleep_and_go_away', 21)
    wid = new_worker(engine)
    t = Task.fromqueue(engine, wid)
    t.run()
    assert t.output == 42


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

    guard(engine, "select count(id) from rework.task where status = 'queued'",
          lambda res: res.scalar() == 1)
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 0)

    proc = ensure_workers(engine, 1)[0][1]

    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 1)

    guard(engine, "select count(id) from rework.task where status = 'running'",
          lambda res: res.scalar() == 1)

    guard(engine, 'select output from rework.task where id = {}'.format(t.tid),
          lambda res: res.scalar())

    assert t.output == 42

    logs = []
    for log in read_proc_streams(proc):
        logs.append(log)
        if len(logs) > 3:
            break
    kill(proc.pid)
    # What's going on there ?
    # We actually killed the parent process of the real worker process
    # (because of obscure details, there is a middle-man),
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
    with test_workers(engine) as wids:
        wid = wids[0]
        assert not shutdown_asked(engine, wid)

        with engine.connect() as cn:
            cn.execute(
                worker.update().where(worker.c.id == wid).values(
                    shutdown=True
                )
            )
        guard(engine, 'select shutdown from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == True)

        guard(engine, 'select count(id) from rework.worker where running = true',
              lambda r: r.scalar() == 0)


def test_task_abortion(engine):
    api.freeze_operations(engine)

    with test_workers(engine) as wids:
        wid = wids[0]

        t = api.schedule(engine, 'infinite_loop')
        guard(engine, 'select count(id) from rework.task where worker = {}'.format(wid),
              lambda res: res.scalar() == 1)

        t.abort()
        assert t.aborted

        guard(engine, "select count(id) from rework.task "
              "where status = 'done' and worker = {}".format(wid),
              lambda res: res.scalar() == 1)
        # one dead worker
        guard(engine, 'select running from rework.worker where id = {}'.format(wid),
              lambda res: not res.scalar())

        diagnostic = engine.execute(
            'select deathinfo from rework.worker where id = {}'.format(wid)
        ).scalar()

        assert 'Task <X> aborted' == scrub(diagnostic)


def test_worker_unplanned_death(engine):
    api.freeze_operations(engine)

    with test_workers(engine) as wids:
        wid = wids[0]

        api.schedule(engine, 'unstopable_death')

        deadlist = wait_true(partial(reap_dead_workers, engine))
        assert wid in deadlist

        guard(engine, 'select deathinfo from rework.worker where id = {}'.format(wid),
              lambda r: r.scalar() == 'Unaccounted death (hard crash)')


def test_task_error(engine):
    api.freeze_operations(engine)

    with test_workers(engine):

        t = api.schedule(engine, 'normal_exception')

        tb = guard(engine, 'select traceback from rework.task where id = {}'.format(t.tid),
                   lambda r: r.scalar())

        assert tb.strip().endswith('oops')
        assert t.traceback == tb


def test_task_logging_capture(engine):
    api.freeze_operations(engine)
    with engine.connect() as cn:
        cn.execute('delete from rework.task')

    with test_workers(engine, 2):
        t1 = api.schedule(engine, 'capture_logs')
        t2 = api.schedule(engine, 'capture_logs')

        finished = lambda t: t.status == 'done'
        wait_true(partial(finished, t1))
        wait_true(partial(finished, t2))

        assert [
            (1, t1.tid, 'my_app_logger:ERROR: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X>'),
            (2, t1.tid, 'stdout:INFO: <X>-<X>-<X> <X>:<X>:<X>: I want to be captured'),
            (3, t1.tid, 'my_app_logger:DEBUG: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X> also'),
            (4, t2.tid, 'my_app_logger:ERROR: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X>'),
            (5, t2.tid, 'stdout:INFO: <X>-<X>-<X> <X>:<X>:<X>: I want to be captured'),
            (6, t2.tid, 'my_app_logger:DEBUG: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X> also')
        ] == [(lid, tid, scrub(line)) for lid, tid, line in engine.execute(
            'select id, task, line from rework.log order by id, task').fetchall()
        ]


def test_logging_stress_test(engine):
    api.freeze_operations(engine)
    with engine.connect() as cn:
        cn.execute('delete from rework.log')

    with test_workers(engine):
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

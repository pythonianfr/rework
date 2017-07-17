import time

from rework import api
from rework.schema import worker
from rework.task import grab_task
from rework.worker import running_status, shutdown_asked
from rework.monitor import new_worker, ensure_workers
from rework.helper import kill, read_proc_streams
from rework.testutils import guard, scrub


@api.task
def print_sleep_and_go_away(task):
    print('Hello, world')
    time.sleep(.2)
    print('I am running within task', task.tid)
    time.sleep(.2)
    print('Saving computation to task.output')
    task.save_output(42)
    print('And now I am done.')


@api.task
def infinite_loop(task):
    while True:
        time.sleep(1)


def test_basic_task_operations(engine):
    api.freeze_operations(engine)
    api.schedule(engine, 'print_sleep_and_go_away')
    wid = new_worker(engine)
    t = grab_task(engine, wid)
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
    t = api.schedule(engine, 'print_sleep_and_go_away')

    guard(engine, "select count(id) from rework.task where status = 'queued'",
          lambda res: res.scalar() == 1)
    guard(engine, 'select count(id) from rework.worker where running = true',
          lambda res: res.scalar() == 0)

    proc = ensure_workers(engine, 1)[0]

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
    ensure_workers(engine, 1)

    wid = guard(engine, 'select id from rework.worker where running = true',
                lambda r: r.scalar(),
                3)
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

    ensure_workers(engine, 1)

    wid = guard(engine, 'select id from rework.worker where running = true',
                lambda res: res.scalar())

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

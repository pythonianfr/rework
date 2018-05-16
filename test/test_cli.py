import pytest

from rework import api
from rework.testutils import scrub, workers
from rework.helper import guard


def test_list_operations(engine, cli):
    with workers(engine):
        r = cli('list-operations', engine.url)

        assert """
<X> host(<X>) `<X>.<X>.<X>.<X>` path(print_sleep_and_go_away)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(run_in_non_default_domain)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(raw_input)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(infinite_loop)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(unstopable_death)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(normal_exception)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(allocate_and_leak_mbytes)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(capture_logs)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(log_swarm)
<X> host(<X>) `<X>.<X>.<X>.<X>` path(stderr_swarm)
""".strip() == scrub(r.output).strip()


def test_debug_port(engine, cli):
    with workers(engine, numworkers=3, debug=True) as mon:
        r = cli('list-workers', engine.url)
        assert '6666' in r.output
        assert '6667' in r.output
        assert '6668' in r.output

        with pytest.raises(AssertionError):
            mon.grab_debug_port(6666, 0)

        # port = mon.grab_debug_port(6666, 0)
        # assert port == 6669

        killtarget = mon.wids[0]
        cli('kill-worker', engine.url, killtarget)
        killed = mon.preemptive_kill()

        assert len(killed) == 1
        assert killed[0] == killtarget
        assert len(mon.wids) == 2

        guard(engine,
              'select running from rework.worker where id = {}'.format(killtarget),
              lambda r: not r.scalar())

        r = cli('list-workers', engine.url)
        assert '[dead] debugport = 6666' in r.output

        port = mon.grab_debug_port(6666, 0)
        assert port == 6666 # recycled

        procs = mon.ensure_workers()
        assert procs
        guard(engine, 'select running from rework.worker where id = {}'.format(procs[0]),
              lambda r: r.scalar())

        r = cli('list-workers', engine.url)
        assert '[dead] debugport = 6666' in r.output
        assert '(idle)] debugport = 6666' in r.output
        # proper recycling did happen

    with workers(engine, numworkers=3, debug=True):
        r = cli('list-workers', engine.url)
        assert '6666' in r.output
        assert '6667' in r.output
        assert '6668' in r.output


def test_abort_task(engine, cli):
    url = engine.url
    with workers(engine):
        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running (idle)]' == scrub(r.output)

        t = api.schedule(engine, 'infinite_loop')
        t.join('running')  # let the worker pick up the task

        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running #<X>]' == scrub(r.output)

        r = cli('list-tasks', url)
        assert '<X> infinite_loop running [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]' == scrub(r.output)

        r = cli('abort-task', url, t.tid)
        t.join()

        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] Task <X> aborted' == scrub(r.output)

        r = cli('list-tasks', url)
        assert '<X> infinite_loop aborted [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]' == scrub(r.output)


def test_kill_worker(engine, cli):
    url = engine.url
    with engine.connect() as cn:
        cn.execute('delete from rework.worker')

    with workers(engine) as mon:
        t = api.schedule(engine, 'infinite_loop')
        t.join('running')  # let the worker pick up the task

        r = cli('kill-worker', url, mon.wids[0])
        mon.preemptive_kill()

        r = cli('list-workers', url)
        assert ('<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] preemptive kill '
                'at <X>-<X>-<X> <X>:<X>:<X>.<X>'
        ) == scrub(r.output)

        r = cli('list-tasks', url)
        assert '<X> infinite_loop done [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]' == scrub(r.output)


def test_debug_worker(engine, cli):
    url = engine.url
    with engine.connect() as cn:
        cn.execute('delete from rework.worker')

    with workers(engine, debug=True):
        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running (idle)] debugport = <X>' == scrub(r.output)


def test_shutdown_worker(engine, cli):
    url = engine.url
    with workers(engine) as mon:
        cli('shutdown-worker', url, mon.wids[0])

        guard(engine, 'select running from rework.worker where id = {}'.format(mon.wids[0]),
              lambda res: res.scalar() == 0)

        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] explicit shutdown' == scrub(r.output)


def test_task_logs(engine, cli):
    with workers(engine):
        t = api.schedule(engine, 'capture_logs')
        t.join()

        r = cli('log-task', engine.url, t.tid)
        assert (
            '\x1b[<X>mmy_app_logger:ERROR: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X>\n'
            '\x1b[<X>mstdout:INFO: <X>-<X>-<X> <X>:<X>:<X>: I want to be captured\n'
            '\x1b[<X>mmy_app_logger:DEBUG: <X>-<X>-<X> <X>:<X>:<X>: will be captured <X> also'
        ) == scrub(r.output)


def test_vacuum(engine, cli):
    r = cli('vacuum', engine.url, '--workers', '--tasks')
    assert r.output == 'vacuum deletes workers or tasks, not both at the same time\n'

    r = cli('vacuum', engine.url)
    assert r.output == 'to cleanup old workers or tasks please use --workers or --tasks\n'

    with engine.connect() as cn:
        cn.execute('delete from rework.worker')

    def run_stuff():
        with workers(engine, numworkers=2):
            r = cli('list-workers', engine.url)
            assert r.output.count('running') == 2

            t1 = api.schedule(engine, 'print_sleep_and_go_away', 1)
            t2 = api.schedule(engine, 'print_sleep_and_go_away', 2)
            t3 = api.schedule(engine, 'print_sleep_and_go_away', 3)

            t1.join() or t2.join() or t3.join()
            r = cli('list-tasks', engine.url)
            assert r.output.count('done') == 3

    run_stuff()

    r = cli('vacuum', engine.url, '--tasks')
    assert r.output == 'deleted 3 tasks\n'

    run_stuff()

    r = cli('vacuum', engine.url, '--workers')
    assert r.output == 'deleted 2 workers\n'

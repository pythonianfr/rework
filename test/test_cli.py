import os
import time
from pathlib import Path

import pytest
from rework import api
from rework.testutils import scrub, workers
from rework.helper import guard, wait_true


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
<X> host(<X>) `<X>.<X>.<X>.<X>` path(flush_captured_stdout)
""".strip() == scrub(r.output).strip()


def test_register_operations(engine, cli):
    r = cli('list-operations', engine.url)
    assert 'boring_task' not in r.output
    assert 'scrap_sites' not in r.output

    api.__task_registry__.clear()

    newtaskspath = Path(__file__).parent / 'newtasks.py'
    r = cli('register-operations', engine.url, newtaskspath)
    assert 'registered 2 new operation (0 already known)' in r.output
    r = cli('register-operations', engine.url, newtaskspath)
    assert 'registered 0 new operation (2 already known)' in r.output

    r = cli('list-operations', engine.url)
    assert 'boring_task' in r.output
    assert 'scrap_sites' in r.output

    # cleanup
    def cleanup():
        api.__task_registry__.clear()
        with engine.begin() as cn:
            cn.execute("delete from rework.operation where name in "
                       "('boring_task', 'scrap_sites')")

    cleanup()

    # per-domain
    r = cli('register-operations', engine.url, newtaskspath, domain='default')
    assert 'registered 1 new operation (0 already known)' in r.output
    r = cli('list-operations', engine.url)
    assert 'boring_task' in r.output
    assert 'scrap_sites' not in r.output

    r = cli('register-operations', engine.url, newtaskspath, domain='scrappers')
    assert 'registered 1 new operation (0 already known)' in r.output
    r = cli('list-operations', engine.url)
    assert 'boring_task' in r.output
    assert 'scrap_sites' in r.output

    cleanup()

    # per-domain + asdomain
    r = cli('register-operations', engine.url, newtaskspath,
            domain='default', asdomain='cloudhost')
    assert 'registered 1 new operation (0 already known)' in r.output
    r = cli('list-operations', engine.url)
    assert 'boring_task' in r.output
    assert 'scrap_sites' not in r.output

    r = cli('register-operations', engine.url, newtaskspath,
            domain='scrappers', asdomain='cloudscrappers')
    assert 'registered 1 new operation (0 already known)' in r.output
    r = cli('list-operations', engine.url)
    assert 'boring_task' in r.output
    assert 'scrap_sites' in r.output

    # error situations

    cleanup()

    r = cli('register-operations', engine.url, newtaskspath,
            domain='nu-such-domain')
    assert 'registered 0 new operation (0 already known)' in r.output

    r = cli('register-operations', engine.url, newtaskspath, domain='default')
    r = cli('register-operations', engine.url, newtaskspath)  # all domains
    assert 'registered 1 new operation (1 already known)' in r.output

    # keep the other tests sane !
    cleanup()
    from . import tasks


def test_list_monitors(engine, cli):
    with workers(engine):
        r = cli('list-monitors', engine.url)

        assert (
            '<X> <X>-<X>-<X> <X>:<X>:<X>+<X>  '
            'default options(maxmem=<X>, maxruns=<X>, debugport=<X>, '
            'maxworkers=<X>, minworkers=<X>)'
        ) == scrub(r.output)


def test_list_workers(engine, cli):
    with workers(engine):
        r = cli('list-workers', engine.url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running (idle)]' == scrub(r.output)

    from rework import schema
    sql = schema.worker.insert().values(host='12345', domain='default')
    with engine.begin() as cn:
        cn.execute(sql)

    r = cli('list-workers', engine.url)
    assert (
        '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] Forcefully killed by the monitor. \n'
        '<X> <nopid>@<X> <X> Mb [unstarted]'
    )== scrub(r.output)


def test_debug_port(engine, cli):
    with workers(engine, numworkers=3, debug=True) as mon:
        r = cli('list-workers', engine.url)
        assert '6666' in r.output
        assert '6667' in r.output
        assert '6668' in r.output

        with pytest.raises(AssertionError):
            mon.grab_debug_port(0)

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

        port = mon.grab_debug_port(0)
        assert port == 6666 # recycled

        stats = mon.ensure_workers()
        assert stats.new
        guard(engine, 'select running from rework.worker where id = {}'.format(stats.new[0]),
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


def test_minworkers(engine, cli):
    with workers(engine, minworkers=1, numworkers=4) as mon:
        r = cli('list-workers', engine.url)
        assert r.output.count('running (idle)') == 1

        # progressive ramp up, one task
        t1 = api.schedule(engine, 'infinite_loop')
        t1.join(target='running')

        new = mon.ensure_workers().new
        assert len(new) == 0

        r = cli('list-workers', engine.url)
        assert r.output.count('\n') == 1
        assert r.output.count('running (idle)') == 0
        assert scrub(r.output).count('running #<X>') == 1

        # now with three tasks
        t2 = api.schedule(engine, 'infinite_loop')
        t3 = api.schedule(engine, 'infinite_loop')

        new = mon.ensure_workers().new
        assert len(new) == 2

        t2.join(target='running')
        t3.join(target='running')
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 3
        assert r.output.count('running (idle)') == 0

        # just one useless turn for fun
        new = mon.ensure_workers().new
        assert len(new) == 0
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 3
        assert r.output.count('running (idle)') == 0

        # t4 should run and t5 remain in queue
        t4 = api.schedule(engine, 'infinite_loop')
        t5 = api.schedule(engine, 'infinite_loop')

        new = mon.ensure_workers().new
        assert len(new) == 1
        t4.join(target='running')
        assert t5.status == 'queued'

        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 4
        assert r.output.count('running (idle)') == 0
        r = cli('list-tasks', engine.url)
        assert r.output.count('running') == 4
        assert r.output.count('queued') == 1

        # ramp down
        t1.abort(); t2.abort()
        mon.preemptive_kill()
        t1.join(); t2.join()

        mon.ensure_workers()
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 3
        assert r.output.count('running (idle)') == 0

        mon.ensure_workers()
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 3
        assert r.output.count('running (idle)') == 0

        t3.abort(); t4.abort()
        mon.preemptive_kill()
        t3.join(); t4.join()

        mon.ensure_workers()
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 1
        assert r.output.count('running (idle)') == 0

        t5.abort()
        mon.preemptive_kill()
        t5.join()
        mon.ensure_workers()
        r = cli('list-workers', engine.url)
        assert scrub(r.output).count('running #<X>') == 0
        assert r.output.count('running (idle)') == 1


def test_shrink_minworkers(engine, cli):
    with engine.begin() as cn:
        cn.execute('delete from rework.worker')
    with workers(engine, minworkers=0, numworkers=4) as mon:
        r = cli('list-workers', engine.url)
        assert r.output.count('running (idle)') == 0

        # ramp up, more tasks than maxworkers
        # we want to saturate the monitor
        tasks = {}
        for idx in range(6):
            tasks[idx] = api.schedule(engine, 'print_sleep_and_go_away', 1)

        assert [task.state for task in tasks.values()] == [
            'queued', 'queued', 'queued', 'queued', 'queued', 'queued']

        stat1 = mon.ensure_workers()
        assert len(stat1.new) == 4

        # occupy a worker
        looping = api.schedule(engine, 'infinite_loop')

        for t in tasks.values():
            t.join()

        assert [task.state for task in tasks.values()] == [
            'done', 'done', 'done', 'done', 'done', 'done']

        stat2 = mon.ensure_workers()
        assert len(stat2.new) == 0
        assert len(stat2.shrink) == 1
        assert len(mon.wids) == 4

        # wait for the first shutdown to happen
        guard(engine,
              'select running from rework.worker '
              'where id = {}'.format(stat2.shrink[0]),
              lambda r: not r.scalar())

        # give 2 times a chance to shutdown a spare worker
        r = cli('list-workers', engine.url)
        assert r.output.count('running') == 3
        assert r.output.count('idle') == 2

        for _ in range(1, 3):
            stat = mon.ensure_workers()
            shuttingdown = stat.shrink[0]
            assert shuttingdown in mon.wids
            guard(engine,
                  'select running from rework.worker '
                  'where id = {}'.format(shuttingdown),
                  lambda r: not r.scalar())

        guard(engine,
              'select count(*) from rework.worker '
              'where shutdown = true and running = false',
              lambda r: r.scalar() == 3)

        def shrinking():
            mon.ensure_workers()
            return len(mon.wids) == 1

        wait_true(shrinking)

        # finish the show
        looping.abort()
        mon.preemptive_kill()
        looping.join()
        guard(engine,
              'select count(*) from rework.worker '
              'where running = true',
              lambda r: r.scalar() == 0)


def test_abort_task(engine, cli):
    url = engine.url
    with workers(engine) as mon:
        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running (idle)]' == scrub(r.output)

        t = api.schedule(engine, 'infinite_loop')
        t.join('running')  # let the worker pick up the task

        r = cli('list-workers', url)
        assert '<X> <X>@<X>.<X>.<X>.<X> <X> Mb [running #<X>]' == scrub(r.output)

        r = cli('list-tasks', url)
        assert (
            '<X> infinite_loop running [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]'
            ' → [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]'
        )== scrub(r.output)

        r = cli('abort-task', url, t.tid)
        mon.preemptive_kill()
        t.join()

        r = cli('list-workers', url)
        assert ('<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] preemptive kill at '
                '<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>:<X>') == scrub(r.output)

        r = cli('list-tasks', url)
        assert (
            '<X> infinite_loop aborted [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>] '
            '→ [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>] '
            '→ [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]'
        ) == scrub(r.output)


def test_kill_worker(engine, cli):
    url = engine.url
    with engine.begin() as cn:
        cn.execute('delete from rework.worker')

    with workers(engine) as mon:
        t = api.schedule(engine, 'infinite_loop')
        t.join('running')  # let the worker pick up the task

        r = cli('kill-worker', url, mon.wids[0])
        mon.preemptive_kill()

        r = cli('list-workers', url)
        assert ('<X> <X>@<X>.<X>.<X>.<X> <X> Mb [dead] preemptive kill '
                'at <X>-<X>-<X> <X>:<X>:<X>.<X>+<X>:<X>'
        ) == scrub(r.output)

        r = cli('list-tasks', url)
        assert (
            '<X> infinite_loop done [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>] '
            '→ [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>] '
            '→ [<X>-<X>-<X> <X>:<X>:<X>.<X>+<X>]'
        ) == scrub(r.output)


def test_debug_worker(engine, cli):
    url = engine.url
    with engine.begin() as cn:
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

    with engine.begin() as cn:
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

    ntasks = engine.execute('select count(*) from rework.task').scalar()
    assert ntasks == 3

    r = cli('vacuum', engine.url, '--workers')
    assert r.output == 'deleted 2 workers\n'

    ntasks = engine.execute('select count(*) from rework.task').scalar()
    assert ntasks == 0

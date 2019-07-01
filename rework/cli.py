# coding: utf-8
from __future__ import print_function

import imp
from time import sleep
import tzlocal
from pathlib import Path

import click
from colorama import init, Fore, Style
from pkg_resources import iter_entry_points

from sqlalchemy import create_engine
from sqlhelp import update

from rework import api
from rework.helper import (
    cleanup_tasks,
    cleanup_workers,
    find_dburi,
    utcnow
)
from rework.worker import run_worker
from rework.task import Task
from rework.monitor import Monitor


TZ = tzlocal.get_localzone()


class DeprecatingGroup(click.Group):

    def get_command(self, ctx, cmd_name):
        if cmd_name == 'deploy':
            print(
                '`deploy` is deprecated, please use `monitor` instead'
            )
            cmd_name = 'monitor'
        return super(DeprecatingGroup, self).get_command(ctx, cmd_name)


@click.command(cls=DeprecatingGroup)
def rework():
    pass


@rework.command(name='init-db')
@click.argument('dburi')
@click.pass_context
def init_db(ctx, dburi):
    " initialize a postgres database with everything needed "
    engine = create_engine(find_dburi(dburi))
    schema.init(engine, drop=True)


@rework.command(name='register-operations')
@click.argument('dburi')
@click.argument('module', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.option('--domain')
@click.option('--asdomain')
def register_operations(dburi, module, domain=None, asdomain=None):
    """register operations from a python module containing
    python functions decorated with `api.task`

    It is possible to filter by domain and also to specify a
    subtitution domain.

    The registered operations will be relative to the current host.
    """
    mod = imp.load_source('operations', module)
    engine = create_engine(find_dburi(dburi))
    ok, ko = api.freeze_operations(engine, domain)

    print('registered {} new operation ({} already known)'.format(
        len(ok), len(ko)
    ))


@rework.command(name='new-worker')
@click.argument('dburi')
@click.argument('worker_id', type=int)
@click.argument('ppid', type=int)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
@click.option('--domain', default='default')
@click.option('--debug-port', type=int, default=0)
def new_worker(**config):
    " spawn a new worker -- this is a purely *internal* command "
    run_worker(**config)


@rework.command()
@click.argument('dburi')
@click.option('--maxworkers', type=int, default=2)
@click.option('--minworkers', type=int)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
@click.option('--domain', default='default')
@click.option('--debug', is_flag=True, default=False)
def monitor(dburi, **config):
    " start a monitor controlling min/max workers "
    engine = create_engine(find_dburi(dburi))
    monitor = Monitor(engine, **config)
    monitor.run()


@rework.command(name='list-operations')
@click.argument('dburi')
def list_operations(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = 'select id, host, name, path from rework.operation'
    for oid, hostid, opname, modpath in engine.execute(sql):
        print(Fore.WHITE + '{}'.format(oid), end=' ')
        print(Fore.GREEN + 'host({}) `{}` path({})'.format(oid, hostid, opname, modpath))
    print(Style.RESET_ALL)


@rework.command(name='list-workers')
@click.argument('dburi')
def list_workers(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = ('select id, host, pid, mem, debugport, running, shutdown, traceback, '
           'deathinfo, created, started, finished '
           'from rework.worker order by id, running')
    for (wid, host, pid, mem, debugport, running, shutdown, traceback,
         deathinfo, created, started, finished) in engine.execute(sql):
        color = Fore.GREEN
        dead = not running and pid
        activity = '(idle)'
        if dead:
            color = Fore.RED
        else:
            color = Fore.MAGENTA
            sql = "select id from rework.task where status = 'running' and worker = %(worker)s"
            tid = engine.execute(sql, worker=wid).scalar()
            if tid:
                activity = '#{}'.format(tid)
        print(wid,
              Fore.GREEN + '{}@{}'.format(pid or '<nopid>', host),
              '{} Mb'.format(mem),
              color + ('[running {}]'.format(activity) if running else '[dead]' if dead else '[unstarted]'),
              end=' ')
        if debugport:
            print(Fore.RED + 'debugport = {}'.format(debugport), end=' ')

        print(Fore.WHITE + '[{}]'.format(
            created.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
              end=' ')
        if started:
            print(Fore.WHITE + '→ [{}]'.format(
                started.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')
        if finished:
            print(Fore.WHITE + '→ [{}]'.format(
                finished.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')

        if dead:
            if deathinfo:
                print(Fore.YELLOW + deathinfo, end=' ')
            if traceback:
                print(Fore.YELLOW + traceback, end=' ')
        print(Style.RESET_ALL)


@rework.command(name='list-monitors')
@click.argument('dburi')
def list_monitors(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = ('select id, domain, options, lastseen from rework.monitor')
    now = utcnow().astimezone(TZ)
    for mid, domain, options, lastseen in engine.execute(sql):
        color = Fore.GREEN
        delta = (now - lastseen).total_seconds()
        if delta > 60:
            color = Fore.RED
        elif delta > 10:
            color = Fore.MAGENTA
        print(mid,
              color + lastseen.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S%z'), end=' ')
        print(Style.RESET_ALL, end=' ')
        print(domain, 'options({})'.format(
            ', '.join('{}={}'.format(k, v) for k, v in options.items()))
        )


@rework.command(name='shutdown-worker')
@click.argument('dburi')
@click.argument('worker-id')
def shutdown_worker(dburi, worker_id):
    """ ask a worker to shut down as soon as it becomes idle

    If you want to immediately and unconditionally terminate
    a worker, use `rework kill-worker`
    """
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        update('rework.worker').where(id=worker_id).values(
            shutdown=True
        ).do(cn)


@rework.command(name='kill-worker')
@click.argument('dburi')
@click.argument('worker-id')
def kill_worker(dburi, worker_id):
    """ ask to preemptively kill a given worker to its monitor

    If you want to not risk interrupting any ongoing work,
    you should use `rework shutdown` instead.
    """
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        update('rework.worker').where(id=worker_id).values(
            kill=True
        ).do(cn)


status_color = {
    'done': Fore.GREEN,
    'aborted': Fore.RED,
    'aborting': Fore.RED,
    'failed': Fore.RED,
    'running': Fore.YELLOW,
    'queued': Fore.MAGENTA
}


@rework.command('list-tasks')
@click.argument('dburi')
@click.option('--tracebacks/--no-tracebacks', default=False)
@click.option('--logcount/--no-logcount', default=False)
def list_tasks(dburi, tracebacks=False, logcount=False):
    init()
    engine = create_engine(find_dburi(dburi))
    opmap = dict(engine.execute('select id, name from rework.operation').fetchall())
    sql = ('select id from rework.task order by id')
    for tid, in engine.execute(sql):
        task = Task.byid(engine, tid)
        stat = task.state
        print(Style.RESET_ALL + str(tid),
              Fore.GREEN + opmap[task.operation],
              status_color[stat] + stat, end=' ')
        if logcount:
            sql = 'select count(*) from rework.log where task = %(tid)s'
            count = engine.execute(sql, {'tid': task.tid}).scalar()
            print(Style.RESET_ALL + '{} log lines'.format(count), end=' ')
        finished = task._propvalue('finished')
        started = task._propvalue('started')
        print(Fore.WHITE + '[{}]'.format(
            task._propvalue('queued').strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
              end=' ')
        if started:
            print(Fore.WHITE + '→ [{}]'.format(
                started.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')
        if finished:
            print(Fore.WHITE + '→ [{}]'.format(
                finished.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')

        if tracebacks and task.traceback:
            print(Fore.YELLOW + task.traceback, end='')
        print()


@rework.command('log-task')
@click.argument('dburi')
@click.argument('taskid', type=int)
@click.option('--watch', is_flag=True, default=False)
@click.option('--fromid', type=int)
def log_task(dburi, taskid, fromid=None, watch=False):
    engine = create_engine(find_dburi(dburi))
    task = Task.byid(engine, taskid)

    def watchlogs(fromid):
        lid = fromid
        for lid, line in task.logs(fromid=fromid):
            print(Fore.GREEN + line.strip())
        return lid

    if not watch:
        watchlogs(fromid)
        if task.traceback:
            print(task.traceback)
        return

    while True:
        fromid = watchlogs(fromid)
        sleep(1)
        if task.status == 'done':
            break

    print(Style.RESET_ALL)


@rework.command('abort-task')
@click.argument('dburi')
@click.argument('taskid', type=int)
def abort_task(dburi, taskid):
    """ immediately abort the given task

    This will be done by doing a preemptive kill on
    its associated worker.
    """
    engine = create_engine(find_dburi(dburi))
    task = Task.byid(engine, taskid)
    task.abort()


@rework.command('vacuum')
@click.argument('dburi')
@click.option('--workers', is_flag=True, default=False)
@click.option('--tasks', is_flag=True, default=False)
@click.option('--finished', type=click.DateTime())
def vacuum(dburi, workers=False, tasks=False, finished=None):
    " delete non-runing workers or finished tasks "
    if not (workers or tasks):
        print('to cleanup old workers or tasks '
              'please use --workers or --tasks')
        return
    if workers and tasks:
        print('vacuum deletes workers or tasks, not both '
              'at the same time')
        return

    engine = create_engine(find_dburi(dburi))
    if finished is None:
        finished = utcnow()
    if workers:
        count = cleanup_workers(engine, finished)
        print('deleted {} workers'.format(count))

    if tasks:
        count = cleanup_tasks(engine, finished)
        print('deleted {} tasks'.format(count))


for ep in iter_entry_points('rework.subcommands'):
    rework.add_command(ep.load())

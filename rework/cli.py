from __future__ import print_function

from time import sleep

import click
from colorama import init, Fore, Style
from pkg_resources import iter_entry_points
from click_plugins import with_plugins

from sqlalchemy import create_engine

from rework import schema
from rework.worker import run_worker
from rework.task import Task
from rework.monitor import run_monitor


@with_plugins(iter_entry_points('rework.subcommands'))
@click.group()
def rework():
    pass


@rework.command(name='init-db')
@click.argument('dburi')
def init_db(dburi):
    engine = create_engine(dburi)
    schema.reset(engine)
    schema.init(engine)


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
    run_worker(**config)


@rework.command()
@click.argument('dburi')
@click.option('--maxworkers', type=int, default=2)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
@click.option('--domain', default='default')
@click.option('--debug', is_flag=True, default=False)
def deploy(**config):
    run_monitor(**config)


@rework.command(name='list-operations')
@click.argument('dburi')
def list_operations(dburi):
    init()
    engine = create_engine(dburi)
    sql = 'select id, host, name, path from rework.operation'
    for oid, hostid, opname, modpath in engine.execute(sql):
        print(Fore.WHITE + '{}'.format(oid), end=' ')
        print(Fore.GREEN + 'host({}) `{}` path({})'.format(oid, hostid, opname, modpath))
    print(Style.RESET_ALL)


@rework.command(name='list-workers')
@click.argument('dburi')
def list_workers(dburi):
    init()
    engine = create_engine(dburi)
    sql = ('select id, host, pid, mem, debugport, running, shutdown, traceback, deathinfo '
           'from rework.worker order by id, running')
    for wid, host, pid, mem, debugport, running, shutdown, traceback, deathinfo in engine.execute(sql):
        color = Fore.GREEN
        dead = not running and (shutdown or traceback or deathinfo)
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
              Fore.GREEN + '{}@{}'.format(pid, host),
              '{} Mb'.format(mem),
              color + ('[running {}]'.format(activity) if running else '[dead]' if dead else '[unstarted]'),
              end=' ')
        if debugport:
            print(Fore.RED + 'debugport = {}'.format(debugport))
        if dead:
            if deathinfo:
                print(Fore.YELLOW + deathinfo, end=' ')
            if traceback:
                print(Fore.YELLOW + traceback, end=' ')
        print(Style.RESET_ALL)


@rework.command(name='shutdown-worker')
@click.argument('dburi')
@click.argument('worker-id')
def shutdown_worker(dburi, worker_id):
    engine = create_engine(dburi)
    with engine.connect() as cn:
        worker = schema.worker
        cn.execute(worker.update().where(
            worker.c.id == worker_id
        ).values(shutdown=True))


@rework.command(name='kill-worker')
@click.argument('dburi')
@click.argument('worker-id')
def kill_worker(dburi, worker_id):
    engine = create_engine(dburi)
    with engine.connect() as cn:
        worker = schema.worker
        cn.execute(worker.update().where(
            worker.c.id == worker_id
        ).values(kill=True))


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
    engine = create_engine(dburi)
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
        print(Fore.WHITE + '[{}]'.format(
            task._propvalue('created').strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
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
    engine = create_engine(dburi)
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
    engine = create_engine(dburi)
    task = Task.byid(engine, taskid)
    task.abort()


@rework.command('vacuum')
@click.argument('dburi')
@click.option('--workers', is_flag=True, default=False)
@click.option('--tasks', is_flag=True, default=False)
def vacuum(dburi, workers=False, tasks=False):
    if not (workers or tasks):
        print('to cleanup old workers or tasks '
              'please use --workers or --tasks')
        return
    if workers and tasks:
        print('vacuum deletes workers or tasks, not both '
              'at the same time')
        return

    engine = create_engine(dburi)
    if workers:
        with engine.connect() as cn:
            count = cn.execute('with deleted as '
                               '(delete from rework.worker where running = false returning 1) '
                               'select count(*) from deleted'
            ).scalar()
            print('deleted {} workers'.format(count))

    if tasks:
        with engine.connect() as cn:
            count = cn.execute("with deleted as "
                               "(delete from rework.task where status = 'done' returning 1) "
                               "select count(*) from deleted"
            ).scalar()
            print('deleted {} tasks'.format(count))

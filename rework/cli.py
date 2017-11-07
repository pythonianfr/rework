from __future__ import print_function

import click

from colorama import init, Fore, Style

from sqlalchemy import create_engine

from rework import schema
from rework.worker import run_worker
from rework.task import Task
from rework.monitor import run_monitor


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
def new_worker(**config):
    run_worker(**config)


@rework.command()
@click.argument('dburi')
@click.option('--maxworkers', type=int, default=2)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
def deploy(**config):
    run_monitor(**config)


@rework.command(name='list-workers')
@click.argument('dburi')
def list_workers(dburi):
    init()
    engine = create_engine(dburi)
    sql = ('select id, host, pid, mem, running, shutdown, traceback, deathinfo '
           'from rework.worker order by id, running')
    for wid, host, pid, mem, running, shutdown, traceback, deathinfo in engine.execute(sql):
        color = Fore.GREEN
        dead = not running and (shutdown or traceback or deathinfo)
        if dead:
            color = Fore.RED
        else:
            color = Fore.MAGENTA
        print(wid,
              Fore.GREEN + '{}@{}'.format(pid, host),
              '{} Mb'.format(mem),
              color + ('[running]' if running else '[dead]' if dead else '[unstarted]'),
              end=' ')
        if dead:
            if deathinfo:
                print(Fore.YELLOW + deathinfo, end=' ')
            if traceback:
                print(Fore.YELLOW + traceback, end=' ')
        print(Style.RESET_ALL)


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

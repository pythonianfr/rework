from __future__ import print_function

import click

from colorama import init, Fore, Style

from sqlalchemy import create_engine

from rework import schema
from rework.worker import run_worker
from rework.monitor import run_monitor


@click.group()
def rework():
    pass


@rework.command()
@click.argument('dburi')
def init_db(dburi):
    engine = create_engine(dburi)
    schema.reset(engine)
    schema.init(engine)


@rework.command()
@click.argument('dburi')
@click.argument('worker_id', type=int)
@click.argument('ppid', type=int)
@click.option('--polling-period', type=int, default=1)
def new_worker(**config):
    run_worker(**config)


@rework.command()
@click.argument('dburi')
@click.option('--maxworkers', type=int, default=2)
def deploy(**config):
    run_monitor(**config)


@rework.command(name='list-workers')
@click.argument('dburi')
def list_workers(dburi):
    init()
    engine = create_engine(dburi)
    sql = ('select id, host, pid, running, shutdown, traceback, deathinfo '
           'from rework.worker order by id, running')
    for wid, host, pid, running, shutdown, traceback, deathinfo in engine.execute(sql):
        color = Fore.GREEN
        dead = not running and (shutdown or traceback or deathinfo)
        if dead:
            color = Fore.RED
        else:
            color = Fore.MAGENTA
        print(wid,
              Fore.GREEN + '{}@{}'.format(pid, host),
              color + ('[running]' if running else '[dead]' if dead else '[unstarted]'),
              end=' ')
        if dead:
            if deathinfo:
                print(Fore.YELLOW + deathinfo, end=' ')
            if traceback:
                print(Fore.YELLOW + traceback, end=' ')
        print(Style.RESET_ALL)

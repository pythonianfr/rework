import click

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

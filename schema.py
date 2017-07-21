from sqlalchemy import MetaData, Table, Column, ForeignKey, String, Integer, Boolean
from sqlalchemy.dialects.postgresql import ENUM, BYTEA
from sqlalchemy.schema import CreateSchema


meta = MetaData()


worker = Table(
    'worker', meta,
    Column('id', Integer, primary_key=True),
    Column('host', String, nullable=False),
    Column('pid', Integer),
    Column('running', Boolean, nullable=False, default=False),
    Column('shutdown', Boolean, nullable=False, default=False),
    Column('traceback', String),
    Column('deathinfo', String),
    schema='rework'
)


operation = Table(
    'operation', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('path', String, nullable=False),
    schema='rework'
)


task = Table(
    'task', meta,
    Column('id', Integer, primary_key=True),
    Column('operation', Integer,
           ForeignKey('rework.operation.id', ondelete='cascade'),
           nullable=False),
    Column('input', BYTEA),
    Column('output', BYTEA),
    Column('traceback', String),
    Column('worker', Integer, ForeignKey('rework.worker.id', ondelete='cascade')),
    Column('status', ENUM('queued', 'running', 'done', name='status')),
    Column('abort', Boolean, nullable=False, default=False),
    schema='rework'
)


log = Table(
    'log', meta,
    Column('id', Integer, primary_key=True),
    Column('task', Integer, ForeignKey('rework.task.id', ondelete='cascade')),
    Column('tstamp', Integer, nullable=False),
    Column('line', String, nullable=False),
    schema='rework'
)


def init(engine):
    engine.execute(CreateSchema('rework'))
    meta.create_all(engine)


def reset(engine):
    engine.execute('drop schema if exists rework cascade')
    meta.drop_all(engine)

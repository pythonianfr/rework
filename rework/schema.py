from sqlalchemy import (
    MetaData, Table, Column, ForeignKey, String, Integer, Boolean,
    DateTime, UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import ENUM, BYTEA, JSONB
from sqlalchemy.schema import CreateSchema


meta = MetaData()


monitor = Table(
    'monitor', meta,
    Column('id', Integer, primary_key=True),
    Column('domain', String, nullable=False, index=True),
    Column('options', JSONB(none_as_null=True)),
    Column('lastseen', DateTime(timezone=True), default=func.now(), index=True),
    schema='rework'
)


worker = Table(
    'worker', meta,
    Column('id', Integer, primary_key=True),
    Column('host', String, nullable=False),
    Column('domain', String, nullable=False, default='default'),
    Column('pid', Integer),
    Column('mem', Integer, default=0),
    Column('debugport', Integer),
    Column('running', Boolean, nullable=False, default=False, index=True),
    Column('shutdown', Boolean, nullable=False, default=False),
    Column('kill', Boolean, nullable=False, default=False),
    Column('traceback', String),
    Column('deathinfo', String),
    schema='rework'
)


operation = Table(
    'operation', meta,
    Column('id', Integer, primary_key=True),
    Column('host', String, nullable=False, index=True),
    Column('name', String, nullable=False, index=True),
    Column('path', String, nullable=False),
    Column('domain', String, nullable=False, default='default'),
    UniqueConstraint('host', 'name', 'path', name='unique_operation'),
    schema='rework'
)


task = Table(
    'task', meta,
    Column('id', Integer, primary_key=True),
    Column('operation', Integer,
           ForeignKey('rework.operation.id', ondelete='cascade'),
           index=True, nullable=False),
    Column('created', DateTime(timezone=True), server_default=func.now()),
    Column('input', BYTEA),
    Column('output', BYTEA),
    Column('traceback', String),
    Column('worker', Integer,
           ForeignKey('rework.worker.id', ondelete='cascade'),
           index=True),
    Column('status', ENUM('queued', 'running', 'done', name='rework.status'),
           index=True),
    Column('abort', Boolean, nullable=False, default=False),
    Column('metadata', JSONB(none_as_null=True)),
    schema='rework'
)


log = Table(
    'log', meta,
    Column('id', Integer, primary_key=True),
    Column('task', Integer,
           ForeignKey('rework.task.id', ondelete='cascade'),
           index=True),
    Column('tstamp', Integer, nullable=False),
    Column('line', String, nullable=False),
    schema='rework'
)


def init(engine):
    with engine.begin() as cn:
        cn.execute(CreateSchema('rework'))
        for table in (monitor, worker, operation, task, log):
            table.create(cn)


def reset(engine):
    with engine.begin() as cn:
        for table in (log, task, worker, operation, monitor):
            table.drop(cn, checkfirst=True)
        cn.execute('drop schema if exists rework cascade')

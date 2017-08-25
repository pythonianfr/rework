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
    Column('worker', Integer, ForeignKey('rework.worker.id', ondelete='cascade')),
    Column('status', ENUM('queued', 'running', 'done', name='status')),
    schema='rework'
)


def init(engine):
    engine.execute(CreateSchema('rework'))
    meta.create_all(engine)


def reset(engine):
    engine.execute('drop schema if exists rework cascade')
    meta.drop_all(engine)

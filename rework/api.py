import sys
from pickle import dumps

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from rework.helper import host
from rework.schema import task as taskentity, operation
from rework.task import __task_registry__, Task


def task(func):
    __task_registry__[func.__name__] = func
    return func


def schedule(engine, opname, inputdata=None,
             hostid=None, module=None,
             metadata=None):
    if metadata:
        assert isinstance(metadata, dict)

    if hostid is None:
        hostid = host()

    sql = select([operation.c.id]
    ).where(operation.c.name == opname
    ).where(operation.c.host == hostid)

    if module is not None:
        sql = sql.where(operation.c.modname == module)

    with engine.connect() as cn:
        opids = cn.execute(sql).fetchall()
        if len(opids) > 1:
            raise ValueError('Ambiguous operation selection')
        if not len(opids):
            raise Exception('No operation was found for these parameters')
        opid = opids[0][0]
        sql = taskentity.insert()
        value = {
            'operation': opid,
            'input': dumps(inputdata, protocol=2) if inputdata is not None else None,
            'status': 'queued',
            'metadata': metadata
        }
        tid = cn.execute(sql, **value).inserted_primary_key[0]

    return Task(engine, tid, opid)


def freeze_operations(engine):
    sql = operation.insert()
    values = []
    hostid = host()
    for name, func in __task_registry__.items():
        funcmod = func.__module__
        module = sys.modules[funcmod]
        modpath = module.__file__
        # python2
        if modpath.endswith('pyc'):
            modpath = modpath[:-1]
        modpath = str(Path(modpath).resolve())
        values.append({
            'host': hostid,
            'name': name,
            'path': modpath
        })
    for value in values:
        with engine.connect() as cn:
            try:
                cn.execute(sql, value)
            except IntegrityError:
                pass

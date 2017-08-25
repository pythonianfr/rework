import sys
from pickle import dumps

from rework.schema import task as taskentity, operation
from rework.task import __task_registry__, Task


def task(func):
    __task_registry__[func.__name__] = func
    return func


def schedule(engine, operation, inputdata=None):
    assert operation in __task_registry__
    with engine.connect() as cn:
        opid = cn.execute('select id from rework.operation where name = %(name)s',
                          name=operation).scalar()
        sql = taskentity.insert()
        value = {
            'operation': opid,
            'input': dumps(inputdata) if inputdata is not None else None,
            'status': 'queued'
        }
        tid = cn.execute(sql, **value).inserted_primary_key[0]

    return Task(engine, tid, opid)


def freeze_operations(engine):
    sql = operation.insert()
    values = []
    for name, func in __task_registry__.items():
        funcmod = func.__module__
        module = sys.modules[funcmod]
        modpath = module.__file__
        values.append({
            'name': name,
            'path': modpath
        })
    with engine.connect() as cn:
        cn.execute(sql, values)

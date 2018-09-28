import sys
from pickle import dumps

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from rework.helper import host
from rework.schema import task as taskentity, operation
from rework.task import __task_registry__, Task


def task(*args, **kw):
    assert args and callable(args[0]) or 'domain' in kw, "Use either @task or @task(domain='domain')"
    domain = 'default'

    def register_task(func):
        __task_registry__[(domain, func.__name__)] = func
        return func

    if args and callable(args[0]):
        return register_task(args[0])

    domain = kw.pop('domain')
    return register_task


def schedule(engine, opname, inputdata=None,
             rawinputdata=None,
             hostid=None, module=None,
             domain=None,
             metadata=None):
    if metadata:
        assert isinstance(metadata, dict)

    if inputdata is not None:
        rawinputdata = dumps(inputdata, protocol=2)

    if hostid is None:
        hostid = host()

    sql = select([operation.c.id]
    ).where(operation.c.name == opname
    ).where(operation.c.host == hostid)

    if module is not None:
        sql = sql.where(operation.c.modname == module)

    if domain is not None:
        sql = sql.where(operation.c.domain == domain)

    with engine.begin() as cn:
        opids = cn.execute(sql).fetchall()
        if len(opids) > 1:
            raise ValueError('Ambiguous operation selection')
        if not len(opids):
            raise Exception('No operation was found for these parameters')
        opid = opids[0][0]
        sql = taskentity.insert()
        value = {
            'operation': opid,
            'input': rawinputdata,
            'status': 'queued',
            'metadata': metadata
        }
        tid = cn.execute(sql, **value).inserted_primary_key[0]

    return Task(engine, tid, opid)


def freeze_operations(engine, domain=None, domain_map=None):
    values = []
    hostid = host()

    if domain_map:
        domain = domain_map.get(domain, domain)

    for (fdomain, fname), func in __task_registry__.items():
        if domain_map:
            fdomain = domain_map.get(fdomain, fdomain)
        if domain is not None and domain != fdomain:
            continue
        funcmod = func.__module__
        module = sys.modules[funcmod]
        modpath = module.__file__
        # python2
        if modpath.endswith('pyc'):
            modpath = modpath[:-1]
        modpath = str(Path(modpath).resolve())
        values.append({
            'host': hostid,
            'name': fname,
            'path': modpath,
            'domain': fdomain
        })

    sql = operation.insert()
    recorded = []
    alreadyknown = []
    for value in values:
        with engine.begin() as cn:
            try:
                cn.execute(sql, value)
                recorded.append(value)
            except IntegrityError:
                alreadyknown.append(value)

    return recorded, alreadyknown

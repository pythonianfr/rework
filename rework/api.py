import sys
from pickle import dumps
from datetime import timedelta

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from rework.helper import host, delta_isoformat
from rework.schema import task as taskentity, operation
from rework.task import __task_registry__, Task


def task(*args, **kw):
    """decorator to turn a python function into a rework operation
    that will be executable as a task

    There are two ways to use it:

    .. code-block:: python

        from rework import api

        @api.task
        def mytask(task):
            pass

        @api.task(domain='somedomain', timeout=timedelta(minutes=3))
        def mytask(task):
            pass

    If you want to specify either a non-default domain or a timeout
    parameter, the second notation (with keywords) must be used.

    All operation functions must take a single `task` parameter.

    """
    msg = "Use either @task or @task(domain='domain')"
    if args:
        assert callable(args[0]), msg
    else:
        assert 'domain' in kw or 'timeout' in kw, msg
    domain = 'default'
    timeout = None

    def register_task(func):
        __task_registry__[(domain, func.__name__)] = (func, timeout)
        return func

    if args and callable(args[0]):
        return register_task(args[0])

    domain = kw.pop('domain', domain)
    timeout = kw.pop('timeout', None)
    assert domain or timeout
    if timeout is not None:
        msg = 'timeout must be a timedelta object'
        assert isinstance(timeout, timedelta), msg

    return register_task


def schedule(engine,
             opname,
             inputdata=None,
             rawinputdata=None,
             hostid=None,
             module=None,
             domain=None,
             metadata=None):
    """schedule an operation to be run by a worker

    It returns a `Task` object.
    The operation name is the only mandatory parameter.

    The `domain` can be specified to avoid an ambiguity if an
    operation is defined within several domains.

    An `inputdata` object can be given. It can be any picklable python
    object. It will be available through `task.input`.

    Alternatively `rawinputdata` can be provided. It must be a byte
    string. It can be useful to transmit file contents and avoid the
    pickling overhead. It will be available through `task.rawinput`.

    Lastly, `metadata` can be provided as a json-serializable python
    dictionary. It can contain anything.

    """
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

    for (fdomain, fname), (func, timeout) in __task_registry__.items():
        if domain_map:
            fdomain = domain_map.get(fdomain, fdomain)
        if domain is not None and domain != fdomain:
            continue
        if timeout is not None:
            timeout = delta_isoformat(timeout)
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
            'domain': fdomain,
            'timeout': timeout
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

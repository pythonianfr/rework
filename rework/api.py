import sys
from pickle import dumps
from datetime import timedelta
from contextlib import contextmanager
import json
import traceback
from pathlib import Path

from sqlalchemy.exc import IntegrityError
from sqlhelp import select, insert

from rework.helper import (
    BetterCronTrigger,
    delta_isoformat,
    filterinput,
    host,
    InputEncoder,
    inputspec,
    pack_inputs
)
from rework.task import (
    __task_inputs__,
    __task_registry__,
    Task
)
from rework.monitor import Monitor


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

        @api.task(inputs={'myfile': file, 'foo': int, 'bar': str})

    If you want to specify either a non-default domain or a timeout
    parameter, the second notation (with keywords) must be used.

    All operation functions must take a single `task` parameter.

    """
    msg = "Use either @task or @task(domain='domain', timeout=..., inputs=...)"
    if args:
        assert callable(args[0]), msg
    else:
        assert 'domain' in kw or 'timeout' in kw or 'inputs' in kw, msg
    domain = 'default'
    timeout = None
    inputs = None

    def register_task(func):
        __task_registry__[(domain, func.__name__)] = (func, timeout)
        if inputs is None:
            return func

        msg = 'inputs must be a tuple'
        assert isinstance(inputs, tuple), msg

        __task_inputs__[(domain, func.__name__)] = inputs
        return func

    if args and callable(args[0]):
        return register_task(args[0])

    domain = kw.pop('domain', domain)
    timeout = kw.pop('timeout', None)
    inputs = kw.pop('inputs', None)
    assert domain or timeout or inputs
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

    if rawinputdata is None:
        spec = filterinput(inputspec(engine), opname, domain, hostid)
        if spec is not None:
            rawinputdata = pack_inputs(spec, inputdata)
        elif inputdata is not None:
            rawinputdata = dumps(inputdata, protocol=2)

    q = select('id').table('rework.operation').where(
        name=opname
    )

    if hostid is not None:
        q.where(host=hostid)

    if module is not None:
        q.where(modname=module)

    if domain is not None:
        q.where(domain=domain)

    with engine.begin() as cn:
        opids = q.do(cn).fetchall()
        if len(opids) > 1:
            if hostid is None:
                return schedule(
                    engine,
                    opname,
                    rawinputdata=rawinputdata,
                    hostid=host(),
                    module=module,
                    domain=domain,
                    metadata=metadata
                )
            raise ValueError('Ambiguous operation selection')
        if not len(opids):
            raise Exception(
                f'No operation was found for these parameters: '
                f'operation=`{opname}` domain=`{domain}` host=`{host()}`'
            )
        opid = opids[0][0]
        q = insert(
            'rework.task'
        ).values(
            operation=opid,
            input=rawinputdata,
            status='queued',
            metadata=json.dumps(metadata)
        )
        tid = q.do(cn).scalar()

    return Task(engine, tid, opid)


def prepare(engine,
            opname,
            rule='* * * * * *',
            domain='default',
            inputdata=None,
            host=None,
            metadata=None,
            rawinputdata=None):
    if metadata:
        assert isinstance(metadata, dict)

    # validate the rules
    BetterCronTrigger.from_extended_crontab(rule)

    spec = filterinput(inputspec(engine), opname, domain, host)
    if rawinputdata is None and inputdata:
        if spec is not None:
            rawinputdata = pack_inputs(spec, inputdata)
        else:
            rawinputdata = dumps(inputdata, protocol=2)

    q = select('id').table('rework.operation').where(
        name=opname
    )
    with engine.begin() as cn:
        opid = q.do(cn).scalar()
        q = insert('rework.sched').values(
            operation=opid,
            domain=domain,
            inputdata=rawinputdata,
            host=host,
            metadata=json.dumps(metadata),
            rule=str(rule)
        )
        sid = q.do(cn).scalar()
    return sid


def freeze_operations(engine, domain=None, domain_map=None,
                      hostid=None):
    values = []
    if hostid is None:
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
        modpath = str(Path(modpath).resolve())
        val = {
            'host': hostid,
            'name': fname,
            'path': modpath,
            'domain': fdomain,
            'timeout': timeout,
        }
        # inputs
        if (fdomain, fname) in __task_inputs__:
            val['inputs'] = InputEncoder().encode(
                __task_inputs__[(fdomain, fname)]
            )
        values.append(val)

    recorded = []
    alreadyknown = []
    for value in values:
        with engine.begin() as cn:
            try:
                q = insert(
                    'rework.operation'
                ).values(
                    **value
                )
                q.do(cn)
                recorded.append(value)
            except IntegrityError:
                alreadyknown.append(value)

    return recorded, alreadyknown


@contextmanager
def workers(engine, domain='default',
            minworkers=0, maxworkers=1,
            maxruns=0, maxmem=0,
            debug=False, start_timeout=30):
    """context manager to set up a test monitor for a given domain

    It can be configured with `domain`, `maxworkers`, `minworkers`, `maxruns`,
    `maxmem` and `debug`.

    Here's an example usage:

    .. code-block:: python

        with workers(engine, numworkers=2) as monitor:
            t1 = api.schedule(engine, 'compute_sum', [1, 2, 3, 4, 5])
            t2 = api.schedule(engine, 'compute_sum', 'abcd')

            t1.join()
            assert t1.output == 15
            t2.join()
            assert t2.traceback.endswith(
                "TypeError: unsupported operand type(s) "
                "for +: 'int' and 'str'"
            )

            assert len(monitor.wids) == 2

    At the end of the block, the monitor-controlled workers are
    cleaned up.

    However the database entries are still there.

    """
    mon = Monitor(
        engine, domain, minworkers, maxworkers, maxruns, maxmem, debug,
        start_timeout=start_timeout
    )
    mon.register()
    mon.ensure_workers()
    try:
        yield mon
    except:
        mon.killall(
            'Something killed the monitor',
            traceback.format_exc()
        )
        raise
    finally:
        mon.killall()
        mon.unregister()

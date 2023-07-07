from pathlib import Path

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from pytest_sa_pg import db

from rework import api, cli as rcli, schema

# our test tasks
from . import tasks as _tasks  # noqa


DATADIR = Path(__file__).parent / 'data'
PORT = 2346
ENGINE = None


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, PORT, {
        'timezone': 'UTC',
        'log_timezone': 'UTC'
    })
    e = create_engine('postgresql://localhost:{}/postgres'.format(PORT))
    schema.init(e, drop=True)
    api.freeze_operations(e)
    # help the `cleanup` fixture
    # (for some reason, working with a fresh engine therein won't work)
    global ENGINE
    ENGINE = e
    return e


@pytest.fixture
def cli():
    def runner(*args, **kw):
        args = [str(a) for a in args]
        for k, v in kw.items():
            args.append('--{}'.format(k))
            args.append(str(v))
        return CliRunner().invoke(rcli.rework, args)
    return runner


@pytest.fixture
def cleanup():
    tasks = api.__task_registry__.copy()
    inputs = api.__task_inputs__.copy()
    yield
    api.__task_registry__.clear()
    api.__task_registry__.update(tasks)
    api.__task_inputs__.clear()
    api.__task_inputs__.update(inputs)
    if ENGINE:
        with ENGINE.begin() as cn:
            cn.execute('delete from rework.sched')
            cn.execute('delete from rework.task')
            cn.execute('delete from rework.operation')
            cn.execute('delete from rework.worker')
            cn.execute('delete from rework.log')
        api.freeze_operations(ENGINE)

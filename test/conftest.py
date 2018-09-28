from pathlib import Path

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from pytest_sa_pg import db

from rework import api, cli as rcli, schema

# our test tasks
from . import tasks


DATADIR = Path(__file__).parent / 'data'
PORT = 2346
ENGINE = None


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, PORT)
    e = create_engine('postgresql://localhost:{}/postgres'.format(PORT))
    schema.reset(e)
    schema.init(e)
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
    yield
    api.__task_registry__.clear()
    api.__task_registry__.update(tasks)
    if ENGINE:
        with ENGINE.begin() as cn:
            cn.execute('delete from rework.operation')
        api.freeze_operations(ENGINE)

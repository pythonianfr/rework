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


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, PORT)
    uri = 'postgresql://localhost:{}/postgres'.format(PORT)
    e = create_engine(uri)
    schema.reset(e)
    schema.init(e)
    api.freeze_operations(e)
    return e


@pytest.fixture
def cli():
    def runner(*args):
        return CliRunner().invoke(rcli.rework, [str(a) for a in args])
    return runner

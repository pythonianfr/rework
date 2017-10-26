from pathlib import Path
import pytest

from sqlalchemy import create_engine

from pytest_sa_pg import db

from rework import schema


DATADIR = Path(__file__).parent / 'data'
PORT = 2346


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, PORT)
    uri = 'postgresql://localhost:{}/postgres'.format(PORT)
    e = create_engine(uri)
    schema.reset(e)
    schema.init(e)
    return e


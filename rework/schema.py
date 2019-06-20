from pathlib import Path

from sqlhelp import sqlfile


SQLFILE = Path(__file__).parent / 'schema.sql'


def init(engine, drop=False):
    if drop:
        with engine.begin() as cn:
            cn.execute('drop schema if exists rework cascade')
            cn.execute('drop type if exists "rework.status"')

    with engine.begin() as cn:
        cn.execute(sqlfile(SQLFILE, ns='rework'))

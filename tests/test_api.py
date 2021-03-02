import pytest
from datetime import datetime as dt

from rework.helper import (
    filterinput,
    inputspec,
    host,
    unpack_inputs
)
from rework import api, input


def test_task_decorator(cleanup):

    @api.task
    def foo(task):
        pass

    @api.task(domain='babar')
    def bar(task):
        pass


    with pytest.raises(AssertionError) as werr:
        @api.task('nope')
        def nope(task):
            pass

    assert werr.value.args[0] == (
        "Use either @task or @task(domain='domain', timeout=..., inputs=...)"
    )


def reset_ops(engine):
    with engine.begin() as cn:
        cn.execute('delete from rework.operation')
    api.__task_registry__.clear()


def register_tasks():
    @api.task
    def foo(task):
        pass

    @api.task(domain='cheese')
    def cheesy(task):
        pass

    @api.task(domain='ham')
    def hammy(task):
        pass

    @api.task(inputs=(
        input.file('myfile.txt', required=True),
        input.number('weight'),
        input.datetime('birthdate'),
        input.string('name'),
        input.string('option', choices=('foo', 'bar')),
        input.string('ignoreme'))
    )
    def yummy(task):
        pass

    @api.task(inputs=())
    def noinput(task):
        pass

    @api.task(inputs=(input.moment('when'),))
    def happy_days(task):
        pass



def test_freeze_ops(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    res = [
        tuple(row)
        for row in engine.execute(
                'select name, domain, inputs from rework.operation '
                'order by domain, name'
        ).fetchall()
    ]
    assert res == [
        ('cheesy', 'cheese', None),
        ('foo', 'default', None),
        ('happy_days', 'default', [
            {'choices': None, 'name': 'when', 'required': False, 'type': 'moment'}
        ]),
        ('noinput', 'default', []),
        ('yummy', 'default', [
            {'choices': None, 'name': 'myfile.txt', 'required': True, 'type': 'file'},
            {'choices': None, 'name': 'weight', 'required': False, 'type': 'number'},
            {'choices': None, 'name': 'birthdate', 'required': False, 'type': 'datetime'},
            {'choices': None, 'name': 'name', 'required': False, 'type': 'string'},
            {'choices': ['foo', 'bar'], 'name': 'option', 'required': False, 'type': 'string'},
            {'choices': None, 'name': 'ignoreme', 'required': False, 'type': 'string'}
        ]),
        ('hammy', 'ham', None)
    ]

    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine, domain='default')
    api.freeze_operations(engine, domain='ham')

    res = engine.execute(
        'select name, domain from rework.operation order by domain, name'
    ).fetchall()
    assert res == [
        ('foo', 'default'),
        ('happy_days', 'default'),
        ('noinput', 'default'),
        ('yummy', 'default'),
        ('hammy', 'ham')
    ]


def test_prepare(engine, cleanup):
    register_tasks()
    api.freeze_operations(engine, domain='default')

    api.prepare(engine, 'foo', ' 0 0 8 * * *')
    api.prepare(engine, 'foo', ' 0 0 8 * * *')

    with pytest.raises(Exception):
        api.prepare(engine, 'foo', '* * * * * *')

    res = engine.execute('select count(*) from rework.sched').scalar()
    assert res == 2


def test_with_inputs(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    args = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9),
        'option': 'foo'
    }
    t = api.schedule(engine, 'yummy', args)
    assert t.input == args

    with pytest.raises(ValueError) as err:
        api.schedule(engine, 'yummy', {'no-such-thing': 42})
    assert err.value.args[0] == 'missing required input: `myfile.txt`'

    with pytest.raises(ValueError) as err:
        api.schedule(
            engine, 'yummy',
            {
                'myfile.txt': b'something',
                'option': 'quux'
            }
        )
    assert err.value.args[0] == "option -> value not in ['foo', 'bar']"

    with pytest.raises(ValueError) as err:
        api.schedule(
            engine, 'yummy',
            {'no-such-thing': 42,
             'option': 'foo',
             'myfile.txt': b'something'
            }
        )
    assert err.value.args[0] == 'unknown inputs: no-such-thing'

    args2 = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': '1973-5-20',
        'option': 'foo'
    }
    t = api.schedule(engine, 'yummy', args2)
    assert t.input == {
        'birthdate': dt(1973, 5, 20, 0, 0),
        'myfile.txt': b'some file',
        'name': 'Babar',
        'option': 'foo',
        'weight': 65
    }


def test_moment_input(engine, cleanup):
    register_tasks()
    api.freeze_operations(engine)
    t = api.schedule(
        engine,
        'happy_days',
        inputdata={'when': '(shifted (today) #:days 1)'}
    )
    when = t.input['when']
    assert when > dt.now()

    t = api.schedule(
        engine,
        'happy_days',
        inputdata={'when': '(date "2021-1-1 09:00")'}
    )
    when = t.input['when']
    assert when == dt(2021, 1, 1, 9, 0)


def test_prepare_with_inputs(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    args = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9),
        'option': 'foo'
    }
    api.prepare(
        engine,
        'yummy',
        rule='* * * * * *',
        _anyrule=True,
        inputdata=args,
        metadata={'user': 'Babar'}
    )

    with pytest.raises(ValueError) as err:
        api.prepare(
            engine,
            'yummy',
            inputdata={'no-such-thing': 42},
            rule='* * * * * *',
            _anyrule=True
        )
    assert err.value.args[0] == 'missing required input: `myfile.txt`'

    with pytest.raises(ValueError) as err:
        api.prepare(
            engine, 'yummy',
            inputdata={
                'no-such-thing': 42,
                'option': 'foo',
                'myfile.txt': b'something'
            },
            rule='* * * * * *',
            _anyrule=True
        )
    assert err.value.args[0] == 'unknown inputs: no-such-thing'

    args2 = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': '1973-5-20',
        'option': 'foo'
    }
    api.prepare(
        engine, 'yummy', inputdata=args2,
        rule='* * * * * *', _anyrule=True
    )

    inputdata = engine.execute(
        'select inputdata from rework.sched order by id asc limit 1'
    ).scalar()

    specs = inputspec(engine)
    spec = filterinput(specs, 'yummy')
    unpacked = unpack_inputs(spec, inputdata)
    assert unpacked == {
        'myfile.txt': b'some file',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9, 0),
        'name': 'Babar',
        'option': 'foo'
    }


def test_with_noinput(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    args = {}
    t = api.schedule(engine, 'noinput', args)
    assert t.input == {}

    args = {'foo': 42}
    with pytest.raises(ValueError):
        t = api.schedule(engine, 'noinput', args)

    t = api.schedule(engine, 'noinput')
    assert t.input is None


def test_schedule_domain(engine, cleanup):
    reset_ops(engine)
    from . import task_testenv
    from . import task_prodenv

    api.freeze_operations(engine, domain='test')
    api.freeze_operations(engine, domain='production')
    api.freeze_operations(engine, domain='production', hostid='192.168.122.42')

    with pytest.raises(ValueError) as err:
        api.schedule(engine, 'foo')
    assert err.value.args[0] == 'Ambiguous operation selection'


    api.schedule(engine, 'foo', domain='test')
    # there two of them but .schedule will by default pick the one
    # matching the *current* host
    api.schedule(engine, 'foo', domain='production')
    api.schedule(engine, 'foo', domain='production', hostid='192.168.122.42')
    api.schedule(engine, 'foo', domain='production', hostid=host())

    hosts = [
        host for host, in engine.execute(
            'select host from rework.task as t, rework.operation as op '
            'where t.operation = op.id'
        ).fetchall()
    ]
    assert hosts.count(host()) == 3
    assert hosts.count('192.168.122.42') == 1

    with pytest.raises(Exception):
        api.schedule(engine, 'foo', domain='production', hostid='172.16.0.1')

    with pytest.raises(Exception):
        api.schedule(engine, 'foo', domain='bogusdomain')

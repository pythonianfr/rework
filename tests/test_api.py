import pytest
from datetime import datetime as dt

from rework.helper import (
    convert_io,
    filterio,
    iospec,
    host,
    prepared,
    unpack_io,
    unpack_iofile,
    unpack_iofiles_length
)
from rework import api, io


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
        "Use either @task or @task(domain='domain', timeout=..., inputs=..., outputs=...)"
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

    @api.task(domain='ham', outputs=(io.string('taste'),))
    def hammy(task):
        pass

    @api.task(inputs=(
        io.file('myfile.txt', required=True),
        io.number('weight'),
        io.datetime('birthdate'),
        io.moment('sometime'),
        io.string('name'),
        io.string('option', choices=('foo', 'bar')),
        io.string('ignoreme'))
    )
    def yummy(task):
        pass

    @api.task(inputs=())
    def noinput(task):
        pass

    @api.task(inputs=(io.moment('when'),))
    def happy_days(task):
        pass

    @api.task(
        domain='non-default',
        inputs=(
        io.number('history'),
    ))
    def nr(task):
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
            {'choices': None, 'name': 'sometime', 'required': False, 'type': 'moment'},
            {'choices': None, 'name': 'name', 'required': False, 'type': 'string'},
            {'choices': ['foo', 'bar'], 'name': 'option', 'required': False, 'type': 'string'},
            {'choices': None, 'name': 'ignoreme', 'required': False, 'type': 'string'}
        ]),
        ('hammy', 'ham', None),
        ('nr', 'non-default', [
            {'choices': None, 'name': 'history', 'required': False, 'type': 'number'}
        ])
    ]

    res = [
        tuple(row)
        for row in engine.execute(
                'select name, domain, outputs from rework.operation '
                'where outputs is not null '
                'order by domain, name'
        ).fetchall()
    ]
    assert res == [
        ('hammy', 'ham', [
            {'choices': None, 'name': 'taste', 'required': False, 'type': 'string'}
        ])
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
    assert res == 1


def test_with_inputs(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    args = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9),
        'sometime': '(date "1973-5-20")',
        'option': 'foo'
    }
    t = api.schedule(engine, 'yummy', args)
    assert t.input == {
        'myfile.txt': b'some file',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9, 0),
        'sometime': dt(1973, 5, 20, 0, 0),
        'name': 'Babar',
        'option': 'foo'
    }

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
        'sometime': '(date "1973-5-20")',
        'option': 'foo'
    }
    t = api.schedule(engine, 'yummy', args2)
    assert t.input == {
        'birthdate': dt(1973, 5, 20, 0, 0),
        'sometime': dt(1973, 5, 20, 0, 0),
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


def test_convert_io(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)
    args = {
        'myfile.txt': 'some file',
        'name': 'Babar',
        'weight': '65',
        'birthdate': '1973-5-20T09:00:00',
        'sometime': '(date "1973-5-20")',
        'option': 'foo'
    }
    specs = iospec(engine)
    spec = filterio(specs, 'yummy')
    typed = convert_io(spec, args)
    assert typed == {
        'myfile.txt': b'some file',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9, 0),
        'sometime': '(date "1973-5-20")',
        'name': 'Babar',
        'option': 'foo'
    }


def test_prepare_with_inputs(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    args = {
        'myfile.txt': b'some file',
        'name': 'Babar',
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 9),
        'sometime': '(date "1973-5-20")',
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
    # second insert should be a no-op
    api.prepare(
        engine,
        'yummy',
        rule='* * * * * *',
        _anyrule=True,
        inputdata=args,
        metadata={'user': 'Babar'}
    )
    res = engine.execute('select count(*) from rework.sched').scalar()
    assert res == 1

    for name, badvalue in (
            ('name', 42),
            ('myfile.txt', 'hello'),
            ('weight', '65'),
            ('birthdate', 'lol'),
            ('sometime', 'lol')
    ):
        failargs = args.copy()
        failargs[name] = badvalue
        with pytest.raises(TypeError):
            api.prepare(
                engine,
                'yummy',
                rule='* * * * * *',
                _anyrule=True,
                inputdata=failargs,
                metadata={'user': 'Babar'}
            )

    failargs = args.copy()
    failargs['option'] = 3.14
    with pytest.raises(ValueError):
        api.prepare(
            engine,
            'yummy',
            rule='* * * * * *',
            _anyrule=True,
            inputdata=failargs,
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
    sid = api.prepare(
        engine, 'yummy', inputdata=args2,
        rule='* * * * * *', _anyrule=True
    )

    inputdata = engine.execute(
        'select inputdata from rework.sched where id = %(sid)s',
        sid=sid
    ).scalar()

    specs = iospec(engine)
    spec = filterio(specs, 'yummy')
    unpacked = unpack_io(spec, inputdata)
    assert unpacked == {
        'birthdate': dt(1973, 5, 20, 0, 0),
        'myfile.txt': b'some file',
        'name': 'Babar',
        'option': 'foo',
        'weight': 65
    }
    unpacked = unpack_io(spec, inputdata, nofiles=True)
    assert unpacked == {
        'weight': 65,
        'birthdate': dt(1973, 5, 20, 0, 0),
        'name': 'Babar',
        'option': 'foo'
    }

    unpacked_files_length = unpack_iofiles_length(spec, inputdata)
    assert unpacked_files_length == {
        'myfile.txt': 9
    }

    unpacked_file = unpack_iofile(spec, inputdata, 'myfile.txt')
    assert unpacked_file == b'some file'

    prep = prepared(engine, 'yummy', 'default')
    assert prep == [
        (4,
         (b'myfile.txt',
          b'weight',
          b'birthdate',
          b'sometime',
          b'name',
        b'option',
          b'some file',
          b'65',
          b'1973-05-20T09:00:00',
          b'(date "1973-5-20")',
          b'Babar',
          b'foo'),
         {'user': 'Babar'},
         '* * * * * *'),
        (5,
         (b'myfile.txt',
          b'weight',
          b'birthdate',
          b'name',
          b'option',
          b'some file',
          b'65',
          b'1973-05-20T00:00:00',
          b'Babar',
          b'foo'),
         None,
         '* * * * * *')
    ]


def test_prepare_inputs_nr_domain_mismatch(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)
    data = {
        'history': 0
    }
    with pytest.raises(Exception):
        sid = api.prepare(
            engine,
            'nr',
            rule='0 0 * * * *',
            inputdata=data
        )

    sid = api.prepare(
        engine,
        'nr',
        domain='non-default',
        rule='0 0 * * * *',
        inputdata=data
    )

    inputdata = engine.execute(
        'select inputdata from rework.sched where id = %(sid)s',
        sid=sid
    ).scalar()

    specs = iospec(engine)
    spec = filterio(specs, 'nr')
    unpacked = unpack_io(spec, inputdata)
    assert unpacked == {'history': 0}


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
    assert err.value.args[0] == 'Ambiguous operation selection `foo`'


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

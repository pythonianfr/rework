import pytest

from rework import api


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

    assert werr.value.args[0] == "Use either @task or @task(domain='domain')"



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


def test_freeze_ops(engine, cleanup):
    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine)

    res = engine.execute(
        'select name, domain from rework.operation order by domain, name'
    ).fetchall()
    assert res == [('cheesy', 'cheese'), ('foo', 'default'), ('hammy', 'ham')]

    reset_ops(engine)
    register_tasks()
    api.freeze_operations(engine, domain='default')
    api.freeze_operations(engine, domain='ham')

    res = engine.execute(
        'select name, domain from rework.operation order by domain, name'
    ).fetchall()
    assert res == [('foo', 'default'), ('hammy', 'ham')]


def test_schedule_domain(engine, cleanup):
    reset_ops(engine)
    from . import task_testenv
    from . import task_prodenv

    api.freeze_operations(engine, domain='test')
    api.freeze_operations(engine, domain='production')

    with pytest.raises(ValueError) as err:
        api.schedule(engine, 'foo')
    assert err.value.args[0] == 'Ambiguous operation selection'


    api.schedule(engine, 'foo', domain='test')
    api.schedule(engine, 'foo', domain='production')

    with pytest.raises(Exception):
        api.schedule(engine, 'foo', domain='bogusdomain')

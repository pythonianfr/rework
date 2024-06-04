from rework import api
from rework.testutils import workers


def test_web_scheduling(engine, cleanup, capsys):
    api.prepare(
        engine,
        'basic',
        rule='* * * * * *',
        metadata={'user': 'WEBUI'},
        _anyrule = True
    )
    with workers(engine) as mon:
        mon.step()
        printed = capsys.readouterr()
        error = (
            " can\'t adapt type \'dict\'\n"
            "[SQL: select  id from rework.operation  where name = %(name)s and modname = %(modname)s  ]"
            "\n[parameters: {\'name\': \'basic\', \'modname\': {\'user\': \'WEBUI\'}}]"
        )

        assert error not in printed[-1]

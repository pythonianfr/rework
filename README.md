REWORK
========

This is a python task scheduling and execution tool, which needs only
python and Postgres to work (using sqlalchemy).

[TOC]


# Principles

Rework might interest people who:

* want Postgres (and *only Postgres*) as a Task Queue Manager,
  Input/Output store and task log store

* have Python long-running tasks to run, with the ability to
  preemptively kill tasks

* want a tiny, self-contained tool with great functional test
  abilities.

The most common python library for such things is Celery. If you don't
mind depending on RabbitMQ and the Celery API and feature set, Celery
is for you. It is mature, probably well-tested, and largely used.


# Basic usage

## Setting up a database

You need a postgresql database. Rework will install its tables into
its own namespace schema, so you can use either a dedicated database
or an exising one, with little risk of conflict.

If you don't already have a database, create a fresh one with:

```shell
createdb jobstore
```

To install rework inside:

```shell
rework init-db postgres://babar:password@localhost:5432/jobstore
```


## Declaring and scheduling a task

All the features are covered in the [test suite][1], which can
henceforth be regarded as a reasonnable source of
documentation. However here's a simple example:

```python
from time import sleep
from rework import api
from sqlalchemy import create_engine


@api.task
def my_first_task(task):
    with task.capturelogs(std=True):
        print('I am running')
        somevalue = task.input * 2
        task.save_output(somevalue)
        print('I am done')


def main(dburi):
    engine = create_engine(dburi)

    # record the decorated tasks
    @api.freeze_operations(engine)

    # now, schedule tasks
    t1 = api.schedule(engine, 'my_first_task', 24)
    t2 = api.schedule(engine, 'my_first_task', 100)

    # wait til they are completed
    while True:
        if t1.state == 'done' and t2.state == 'done':
            break
        time.sleep(1)

    assert t1.output == 42
    assert t2.output == 200


if __name__ == '__main__':
    main('postgres://babar:password@localhost:5432/jobstore')
```

If you put this into a `test_rework.py` module and type `python
test_rework.py` it should *hang* forever. Hold on, what's missing ?

On another terminal, one needs to *start* the workers that will
execute the tasks. Do as follows:

```shell
rework deploy postgres://babar:password@localhost:5432/jobstore
```

Then, the script will quickly terminate, as both tasks have been
executed.


# Command line

If you read the previous chapter, you already know the `init-db` and
`deploy` commands.

The `rework` command, if typed without subcommand, shows its usage:

```shell
$ rework
Usage: rework [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  deploy
  init-db
  list-tasks
  list-workers
  new-worker
  tasklog
```

Of those commands, `new-worker` is for purely internal purposes, and
unless you know what you're doing, should should never use it.

One can list the tasks:

```shell
rework list-tasks postgres://babar:password@localhost:5432/jobstore
1 my_first_task done [2017-09-13 17:08:48.306970+02]
2 my_first_task done [2017-09-13 17:08:48.416770+02]
```

It is possible to monitor the output of a given task:

```shell
$ rework tasklog postgres://babar:password@localhost:5432/jobstore 1
stdout:INFO: 2017-09-13 17:08:49: I am running
stdout:INFO: 2017-09-13 17:08:49: I am done
```

The last argument `1` is the task identifier as was shown by the
`list-tasks` command.

Notice how we capture the standard output (print calls) using the
`task.capturelogs` context manager. This is completely optional of
course but quite handy. The line shown above actually capture
*standard output*, *standard error* and *all logs*. It accepts a
`level` parameter, like e.g. `capturelogs(level=logging.INFO)`.

Lastly, `list-workers` will show the currently running workers:

```shell
$ rework list-workers postgres://babar:password@localhost:5432/jobstore
1 4889896@192.168.1.2 30 Mb [running]
2 4889748@192.168.1.2 30 Mb [running]
```

[1]: https://bitbucket.org/pythonian/rework/src/default/test/test_monitor.py?fileviewer=file-view-default

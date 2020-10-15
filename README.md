REWORK
========

This is a python task scheduling and execution tool, which needs only
python and Postgres to work (using sqlalchemy).


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
    api.freeze_operations(engine)

    # now, schedule tasks
    t1 = api.schedule(engine, 'my_first_task', 'hello')
    t2 = api.schedule(engine, 'my_first_task', 100)

    # wait til they are completed
    t1.join()
    t2.join()

    assert t1.output == 'hellohello'
    assert t2.output == 200


if __name__ == '__main__':
    main('postgres://babar:password@localhost:5432/jobstore')
```

If you put this into a `test_rework.py` module and type `python
test_rework.py` it should *hang* forever. Hold on, what's missing ?

On another terminal, one needs to *start* the workers that will
execute the tasks. Do as follows:

```shell
rework monitor postgres://babar:password@localhost:5432/jobstore
```

Then, the script will quickly terminate, as both tasks have been
executed.


## Specifying inputs

Having a formal declaration of the task input can help validate them
and also, in [rework_ui][reworkui] it will provide an interactive web
form allowing subsequent launch of the task.

```python
from rework import api, input

@api.task(inputs=(
    input.file('myfile.txt', required=True),
    input.string('name', required=True),
    input.string('option', choices=('foo', 'bar')),
    input.number('weight'),
    input.datetime('birthdate'))
)
def compute_things(task):
    inp = task.input
    assert 'name' in inp
    ...
```

... and then, later:

```python
task = api.schedule(
    engine, 'compute_things',
    {'myfile.txt': b'file contents',
     'birthdate': datetime(1973, 5, 20, 9),
     'name': 'Babar',
     'weight': 65
    }
)

assert task.input == {
    'myfile.txt': b'file contents',
    'birthdate': datetime(1973, 5, 20, 9),
    'name': 'Babar',
    'weight': 65
}
```


# API

The `api` module exposes most if what is needed. The `task` module
and task objects provide the rest.


## `api` module

Three functions are provided: the `task` decorator, the
`freeze_operations` and `schedule` functions.

Defining tasks is done using the `task` decorator:

```python
from rework.api import task

@task
def my_task(task):
    pass
```

It is also possible to specify a non-default `domain`:

```python
@task(domain='scrapers')
def my_scraper(task):
    pass
```

A `timeout` parameter is also available:

```python
from datetime import timedelta

@task(timeout=timedelta(seconds=30)
def my_time_limited_task(task):
    pass
```

To make the tasks available for use, they must be recorded within
the database referential. We use `freeze_operations` for this:

```python
from sqlalchemy import create_engine
from rework.api import freeze_operations

engine = create_engine('postgres://babar:password@localhost:5432/jobstore')
api.freeze_operations(engine)
```

Finally, one can schedule tasks as such:

```python
from sqlalchemy import create_engine
from rework.api import schedule

engine = create_engine('postgres://babar:password@localhost:5432/jobstore')

task = api.schedule(engine, 'my_task', 42)
```

The `schedule` function wants these mandatory parameters:

* engine: sqlalchemy engine

* task name: string

* task input: any python picklable object


It also accepts two more options:

* domain: a domain identifier (for cases when the same service is
  available under several domains and you want to force one)

* hostid: an host identifier (e.g. '192.168.1.1')

* metadata: a json-serializable dictionary (e.g. {'user': 'Babar'})


## Task objects

Task objects can be obtained from the `schedule` api call (as seen in the
previous example) or through the `task` module.

```python
from task import Task

task = task.by_id(42)
```

The task object provides:

* `.state` attribute to describe the task state (amongst: `queued`,
  `running`, `aborting`, `aborted`, `failed`, `done`)

* `.join()` method to wait synchronously for the task completion

* `.capturelogs(sync=True, level=logging.NOTSET, std=False)` method to
  record matching logs into the db (`sync` controls whether the logs
  are written synchronously, `level` specifies the capture level,
  `std` permits to also record prints as logs)

* `.input` attribute to get the task input (yields any object)

* `.save_output(<obj>)` method to store any object

* `.abort()` method to preemptively stop the task

* `.log(fromid=None)` method to retrieve the task logs (all or from a
  given log id)


# Command line

## Operations

If you read the previous chapter, you already know the `init-db` and
`monitor` commands.

The `rework` command, if typed without subcommand, shows its usage:

```shell
$ rework
Usage: rework [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  abort-task
  monitor
  init-db
  kill-worker
  list-operations
  list-tasks
  list-workers
  log-task
  new-worker
  shutdown-worker
  vacuum
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
$ rework log-task postgres://babar:password@localhost:5432/jobstore 1
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


## Extensions

It is possible to augment the `rework` command with new subcommands (or
augment, modify existing commands).

Any program doing so must define a new command and declare a setup
tools entry point named `rework:subcommand` as in e.g.:

```python

    entry_points={'rework.subcommands': [
        'view=rework_ui.cli:view'
    ]}
```

For instance, the [rework_ui][reworkui] python package provides such a
`view` subcommand to launch a monitoring webapp for a given rework job
store.

[reworkui]: https://bitbucket.org/pythonian/rework_ui

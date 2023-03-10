# REWORK

`rework` is a distributed execution system for the execution of tasks
that can belong to independant python environments and code bases,
even hosted on different computers.

The only constraint is that postgres must be accessible from all nodes
of a given `rework` installation.

## Principles

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


## Basic usage

### Setting up a database

You need a postgresql database. Rework will install its tables into
its own `rework` namespace schema, so you can use either a dedicated
database or an exising one, with little risk of conflict.

If you don't already have a database, create a fresh one with:

```shell
createdb jobstore
```

To install rework inside:

```shell
rework init-db postgres://babar:password@localhost:5432/jobstore
```


### Declaring and scheduling a task

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


### Specifying inputs

Having a formal declaration of the task input can help validate them
and also, in [rework_ui][reworkui] it will provide an interactive web
form allowing subsequent launch of the task.

```python
from rework import api, io

@api.task(inputs=(
    io.file('myfile.txt', required=True),
    io.string('name', required=True),
    io.string('option', choices=('foo', 'bar')),
    io.number('weight'),
    io.datetime('birthdate'),
    io.moment('horizon')
))
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
     'weight': 65,
     'horizon': '(shifted (today) #:days 7)'
     }
)

assert task.input == {
    'myfile.txt': b'file contents',
    'birthdate': datetime(1973, 5, 20, 9),
    'name': 'Babar',
    'weight': 65,
    'horizon': datetime(2021, 1, 7)
}
```

### Specifying outputs

As for the inputs, and for the same reasons, we can provide a spec for
the outputs.

```python
from rework import api, io

@api.task(outputs=(
    io.string('name'),
    io.datetime('birthdate')
))
def compute_things(task):
    ...
    task.save_output({
        'name': 'Babar',
        'birthdate': datetime(1931, 1, 1)
    })
```

And this will of course be fetched from the other side:

```python
t = api.schedule(engine, 'compute_things')
assert t.output == {
    'name': 'Babar',
    'birthdate': datetime(1931, 1, 1)
}
```

### Scheduling

While the base api provides a `schedule` call that schedules a task
for immediate execution, there is also a `prepare` call that allows to
define the exact moment the task ought to be executed, using a
`crontab` like notation.

Example:

```python
api.prepare(
    engine,
    'compute_things',
    {'myfile.txt': b'file contents',
     'birthdate': datetime(1973, 5, 20, 9),
     'name': 'Babar',
     'weight': 65
    },
    rule='0 0 8,12 * * *'
)
```

This would schedule the task every day at 8:00 and 12:00. The extended
crontab notation also features a field for seconds (in first
position).


### Debugging

If you need to debug some task, the standard advice is:

* write your task content in plain functions and have them unit-tested
  with e.g. `pytest`

```python
@api.task
def my_fancy_task(task):
    the_body_of_my_fancy_task(task.input)
```

* you can also you use print-based logging as shown there:

```python
@api.task
def my_fancy_task(task):
    with task.capturelogs(std=True):
        print('starting')
        # do stuff
        print('done', result)
```

* finally, it may happen that a task is "stuck" because of a deadlock,
  and in this case, starting the monitor with `--debug-port` will help:

```bash
$ pip install pystuck
$ rework monitor postgres://babar:password@localhost:5432/jobstore --debug-port=666
```

Then launching `pystuck` (possibly from another machine) is done as such:

```bash
$ pystuck -h <host> -p 666
```


### Organize tasks in code

A common pattern is to have a `project/tasks.py` module.

One can manage the tasks using the `register-operations` and
`unregister-operation` commands.

```bash
$ rework register-operations <dburi> /path/to/project/tasks.py
```

and also

```bash
``` rework unregister-operation <dburi> <opname>
delete <opname> <domain> /path/to/project/tasks.py <hostid>
really remove those [y/n]? [y/N]: y
```

This pair of operations can be used also whenever a task input or
output specifications have changed.


## API

The `api` module exposes most if what is needed. The `task` module
and task objects provide the rest.


### `api` module

Three functions are provided: the `task` decorator, the
`freeze_operations`, `schedule` and `prepare` functions.

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

# immediate executionn
task = api.schedule(engine, 'my_task', 42)

# execution every five minutes
api.prepare(engine, 'my_task', 42, rule='0 */5 * * * *')
```

The `schedule` function wants these mandatory parameters:

* `engine`: sqlalchemy engine

* `operation`: string

* `inputdata`: any python picklable object (if no input specification
  is provided, else the input formalism provides ways for numbers,
  strings, dates and files)


It also accepts two more options:

* `domain`: a domain identifier (for cases when the same service is
  available under several domains and you want to force one)

* `hostid`: an host identifier (e.g. '192.168.1.1')

* `metadata`: a json-serializable dictionary (e.g. {'user': 'Babar'})

The `prepare` function takes the same parameters as `schedule` plus a
`rule` option using `crontab` notation with seconds in first position.


### Task objects

Task objects can be obtained from the `schedule` api call (as seen in the
previous example) or through the `task` module.

```python
from task import Task

task = task.byid(engine, 42)
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


## Command line

### Operations

If you read the previous chapter, you already know the `init-db` and
`monitor` commands.

The `rework` command, if typed without subcommand, shows its usage:

```shell
$ rework
Usage: rework [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  abort-task            immediately abort the given task This will be done...
  export-scheduled
  import-scheduled
  init-db               initialize the database schema for rework in its
                        own...
  kill-worker           ask to preemptively kill a given worker to its...
  list-monitors
  list-operations
  list-scheduled
  list-tasks
  list-workers
  log-task
  monitor               start a monitor controlling min/max workers
  new-worker            spawn a new worker -- this is a purely *internal*...
  register-operations   register operations from a python module containing...
  shutdown-worker       ask a worker to shut down as soon as it becomes
                        idle...
  unregister-operation  unregister an operation (or several) using its name...
  vacuum                delete non-runing workers or finished tasks
```

Of those commands, `new-worker` is for purely internal purposes, and
unless you know what you're doing, you should never use it.

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

[1]: https://hg.sr.ht/~pythonian/rework/browse/tests/test_monitor.py?rev=tip


### Extensions

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

[reworkui]: https://hg.sr.ht/~pythonian/rework_ui

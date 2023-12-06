
Rework
======

``rework`` is a distributed execution system for the execution of
tasks that can belong to independant python environments and code
bases, even hosted on different computers.

The only constraint is that postgres must be accessible from all nodes
of a given ``rework`` installation.

Rework might interest people who:

* want Postgres (and *only Postgres*) as a Task Queue Manager,
  Input/Output store and task log store

* have Python long-running tasks to run, with the ability to
  preemptively kill tasks

* want a tiny, self-contained tool with great functional test
  abilities (writing tests for tasks is easy)


Rework provides a rich command line utility to diagnose the state of
the system.


Introduction
------------

Overview
........

To use it properly one has to understand the following concepts:

``operation`` A python function decorated with the ``task``
    decorator. The function has a single ``task`` parameter that
    allows to communicate with the system (for the purposes of input
    and output management, and log capture). It is defined within a
    ``domain`` and on a specific ``host``.

``task`` A concrete execution of an operation. Also, name of the
    decorator that indicates an ``operation``. The task can indicate
    its state and be aborted if needed. It can provide access to the
    captured logs, input and output.

``worker`` A python process spawned by a ``monitor``, that will
    execute ``tasks``. It is always associated with a ``domain`` on a
    specific ``host``.

``domain`` A label associated with ``operations``, ``tasks`` and
    ``workers``, which can be used to map operations to virtual
    environments or just help organize a logical separation of
    operations (and the associated pools of workers).

``monitor`` A python process which is responsible for the management
    of workers (start, stop and abort), whose precise amount is
    configurable, within a ``domain``.

They will be illustrated further in the documentation.


Installation
............

.. code-block:: bash

   $ pip install rework


Quick start
...........

Let's have a look at a simple example.

We need to set up a database first, which we'll name ``jobstore``.

.. code-block:: bash

   $ createdb jobstore

Rework will install its tables into its own namespace schema, so you
can use either a dedicated database (like we're doing right now) or an
exising one, with little risk of conflict.

Now we must set up the rework schema:

.. code-block:: bash

   rework init-db postgres://babar:password@localhost/jobstore

This being done, we can start writing our first task:

.. code-block:: python

   from rework import api
   from sqlalchemy import create_engine

   @api.task
   def my_first_task(task):
       with task.capturelogs(std=True):
           print('I am running')
           somevalue = task.input * 2
           task.save_output(somevalue)
           print('I am done')


   def main(uri):
       engine = create_engine(
           'postgres://babar:password@localhost/jobstore'
       )
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


Here we have defined a dummy task that will print a bunch of
sentences, double the input value and save a result back.

This has to be put into a python module, e.g. ``test_rework.py``

At this point, the rework system knows *nothing* of the task. We must
register it, as follows:

.. code-block:: bash

   $ rework register-operations postgres://babar:password@localhost/jobstore test_rework.py
   registered 1 new operation (0 already known)

From this point, we can check it is indeed registered:

.. code-block:: bash

   $ rework list-operations postgres://babar:password@localhost/jobstore
   1 host(1) ``10.211.55.3`` path(my_first_task)

Now, let's execute our script:

.. code-block:: bash

   $ python test_rework.py

It will start and hang indefinitely on the first ``join`` call. Indeed
we are missing an important step: providing ``workers`` that will
execute the tasks.

This should be made in a separate shell, since it is a blocking
operation:

.. code-block:: bash

   $ rework monitor postgres://babar:password@localhost/jobstore

Then, the script will quickly terminate, as both tasks have been
executed.

Congratulations ! You juste fired your first tasks.
We can finish this chapter with a few command line goodies.

First we'll want to know about the existing tasks:

.. code-block:: bash

   $ rework list-tasks postgres://babar:password@localhost/jobstore
   1 my_first_task done [2018-11-28 16:07:51.672672+01] → [2018-11-28 16:08:27.974392+01] → [2018-11-28 16:08:27.985432+01] 
   2 my_first_task done [2018-11-28 16:07:51.676981+01] → [2018-11-28 16:08:27.974642+01] → [2018-11-28 16:08:27.985502+01] 

It is possible to monitor the output of a given task:

.. code-block:: bash

   $ rework log-task postgres://babar:password@localhost/jobstore 1
   stdout:INFO: 2018-11-28 16:08:27: I am running
   stdout:INFO: 2018-11-28 16:08:27: I am done

The last argument ``1`` is the task identifier as was shown by the
``list-tasks`` command.

Notice how we capture the standard output (print calls) using the
``task.capturelogs`` context manager. This is completely optional of
course but quite handy. The line shown above actually capture
*standard output*, *standard error* and *all logs*. It accepts a
``level`` parameter, like e.g. ``capturelogs(level=logging.INFO)``.

Lastly, ``list-workers`` will show the currently running workers:

.. code-block:: bash

   $ rework list-workers postgres://babar:password@localhost/jobstore
   1 4124@10.211.55.3 43 Mb [running (idle)] [2018-11-28 16:08:27.438491+01] → [2018-11-28 15:08:27.967432+01] 
   2 4125@10.211.55.3 43 Mb [running (idle)] [2018-11-28 16:08:27.442869+01] → [2018-11-28 15:08:27.967397+01] 

It is now possible to stop the ``monitor`` on its separate console, with
a plain ``ctrl-c``.

After this, ``list-workers`` will provide an updated status:

.. code-block:: bash

   $ rework list-workers postgres://aurelien:aurelien@localhost/rework 
   1 4124@10.211.55.3 43 Mb [dead] [2018-11-28 16:08:27.438491+01] → [2018-11-28 15:08:27.967432+01] → [2018-11-28 16:11:09.668587+01] monitor exit 
   2 4125@10.211.55.3 43 Mb [dead] [2018-11-28 16:08:27.442869+01] → [2018-11-28 15:08:27.967397+01] → [2018-11-28 16:11:09.668587+01] monitor exit 


Specifying inputs
.................

Having a formal declaration of the task input can help validate them
and also, in `rework_ui <https://hg.sr.ht/~pythonian/rework_ui>`_ it
will provide an interactive web form allowing subsequent launches of
the task.

.. code-block:: python

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


... and then, later:

.. code-block:: python

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


Specifying outputs
..................

As for the inputs, and for the same reasons, we can provide a spec for
the outputs.

.. code-block:: python

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


And this will of course be fetched from the other side:

.. code-block:: python

   t = api.schedule(engine, 'compute_things')
   assert t.output == {
       'name': 'Babar',
       'birthdate': datetime(1931, 1, 1)
   }


Scheduling
..........

While the base api provides a ``schedule`` call that schedules a task
for immediate execution, there is also a ``prepare`` call that allows
to define the exact moment the task ought to be executed, using a
``crontab`` like notation.

Example:

.. code-block:: python

   api.prepare(
       engine,g
       'compute_things',
       {'myfile.txt': b'file contents',
       'birthdate': datetime(1973, 5, 20, 9),
       'name': 'Babar',
       'weight': 65
       },
       rule='0 15 8,12 * * *'
   )


This would schedule the task every day at 8:15 and 12:15. The extended
crontab notation also features a field for seconds (in first
position).


Debugging
.........

If you need to debug some task, the standard advice is:

* write your task content in plain functions and have them unit-tested
  with e.g. ``pytest``

.. code-block:: python

   @api.task
   def my_fancy_task(task):
       the_body_of_my_fancy_task(task.input)

* you can also you use print-based logging as shown there:

.. code-block:: python

   @api.task
   def my_fancy_task(task):
       with task.capturelogs(std=True):
           print('starting')
           # do stuff
           print('done', result)

* finally, it may happen that a task is "stuck" because of a deadlock,
  and in this case, starting the monitor with ``--debug-port`` will
  help:

.. code-block:: bash

   $ pip install pystuck
   $ rework monitor postgres://babar:password@localhost:5432/jobstore --debug-port=666

Then launching ``pystuck`` (possibly from another machine) is done as
such:

.. code-block:: bash

   $ pystuck -h <host> -p 666


Organize tasks in code
......................

A common pattern is to have a ``project/tasks.py`` module.

One can manage the tasks using the ``register-operations`` and
``unregister-operation`` commands.

.. code-block:: bash

   $ rework register-operations <dburi> /path/to/project/tasks.py

and also

.. code-block:: bash

   rework unregister-operation <dburi> <opname>
   delete <opname> <domain> /path/to/project/tasks.py <hostid>
   really remove those [y/n]? [y/N]: y

This pair of operations can be used also whenever a task input or
output specifications have changed.


API overview
------------

The ``api`` module exposes most if what is needed. The ``task`` module
and task objects provide the rest.


``api`` module
..............

Four functions are provided: the ``task`` decorator, the
``freeze_operations``, ``schedule``, ``prepare`` and ``unprepare``
functions.

Defining tasks is done using the ``task`` decorator:

.. code-block:: python

   from rework.api import task

   @task
   def my_task(task):
       pass

It is also possible to specify a non-default ``domain``:

.. code-block:: python

   @task(domain='scrapers')
   def my_scraper(task):
       pass


A ``timeout`` parameter is also available:

.. code-block:: python

   from datetime import timedelta

   @task(timeout=timedelta(seconds=30)
   def my_time_limited_task(task):
       pass


To make the tasks available for use, they must be recorded within the
database referential. We use ``freeze_operations`` for this:

.. code-block:: python

   from sqlalchemy import create_engine
   from rework.api import freeze_operations

   engine = create_engine('postgres://babar:password@localhost:5432/jobstore')
   api.freeze_operations(engine)


Finally, one can schedule tasks as such:

.. code-block:: python

   from sqlalchemy import create_engine
   from rework.api import schedule

   engine = create_engine('postgres://babar:password@localhost:5432/jobstore')

   # immediate executionn (the task will be queued)
   task = api.schedule(engine, 'my_task', 42)

   # execution every five minutes (the task will be queued at the
   # specified moments)
   api.prepare(engine, 'my_task', 42, rule='0 */5 * * * *')


The ``schedule`` function wants these mandatory parameters:

* ``engine``: sqlalchemy engine

* ``operation``: string

* ``inputdata``: any python picklable object (if no input
  specification is provided, else the input formalism provides ways
  for numbers, strings, dates and files)


It also accepts two more options:

* ``domain``: a domain identifier (for cases when the same service is
  available under several domains and you want to force one)

* ``hostid``: an host identifier (e.g. '192.168.1.1')

* ``metadata``: a json-serializable dictionary (e.g. {'user':
  'Babar'})

The ``prepare`` function takes the same parameters as ``schedule``
plus a ``rule`` option using ``crontab`` notation with seconds in
first position.


Task objects
............

Task objects can be obtained from the ``schedule`` api call (as seen
in the previous example) or through the ``task`` module.

.. code-block:: python

   from task import Task

   task = task.byid(engine, 42)


The task object provides:

* ``.state`` attribute to describe the task state (amongst:
  ``queued``, ``running``, ``aborting``, ``aborted``, ``failed``,
  ``done``)

* ``.join()`` method to wait synchronously for the task completion

* ``.capturelogs(sync=True, level=logging.NOTSET, std=False)`` method
  to record matching logs into the db (``sync`` controls whether the
  logs are written synchronously, ``level`` specifies the capture
  level, ``std`` permits to also record prints as logs)

* ``.input`` attribute to get the task input (yields any object)

* ``.save_output(<obj>)`` method to store any object

* ``.abort()`` method to preemptively stop the task

* ``.log(fromid=None)`` method to retrieve the task logs (all or from
  a given log id)


Command line
------------

Operations
..........

If you read the previous chapter, you already know the ``init-db`` and
``monitor`` commands.

The ``rework`` command, if typed without subcommand, shows its usage:

.. code-block:: shell

   $ rework
   Usage: rework [OPTIONS] COMMAND [ARGS]...

   Options:
     --help  Show this message and exit.

   Commands:
     abort-task            immediately abort the given task
     export-scheduled
     import-scheduled
     init-db               initialize the database schema for rework in its...
     kill-worker           ask to preemptively kill a given worker to its...
     list-monitors
     list-operations
     list-scheduled        list the prepared operations with their cron rule
     list-tasks
     list-workers
     log-task
     monitor               start a monitor controlling min/max workers
     new-worker            spawn a new worker -- this is a purely *internal*...
     register-operations   register operations from a python module...
     scheduled-plan        show what operation will be executed at which...
     shutdown-worker       ask a worker to shut down as soon as it becomes idle
     unprepare             remove a scheduling plan given its id
     unregister-operation  unregister an operation (or several) using its...
     vacuum                delete non-runing workers or finished tasks


Of those commands, ``new-worker`` is for purely internal purposes, and
unless you know what you're doing, you should never use it.

One can list the tasks:

.. code-block:: shell

   rework list-tasks postgres://babar:password@localhost:5432/jobstore
   1 my_first_task done [2017-09-13 17:08:48.306970+02]
   2 my_first_task done [2017-09-13 17:08:48.416770+02]


It is possible to monitor the output of a given task:

.. code-block:: shell

   $ rework log-task postgres://babar:password@localhost:5432/jobstore 1
   stdout:INFO: 2017-09-13 17:08:49: I am running
   stdout:INFO: 2017-09-13 17:08:49: I am done


The last argument ``1`` is the task identifier as was shown by the
``list-tasks`` command.

Notice how we capture the standard output (print calls) using the
``task.capturelogs`` context manager. This is completely optional of
course but quite handy. The line shown above actually capture
*standard output*, *standard error* and *all logs*. It accepts a
``level`` parameter, like e.g. ``capturelogs(level=logging.INFO)``.

Lastly, ``list-workers`` will show the currently running workers:

.. code-block:: shell

   $ rework list-workers postgres://babar:password@localhost:5432/jobstore
   1 4889896@192.168.1.2 30 Mb [running]
   2 4889748@192.168.1.2 30 Mb [running]


Extensions
----------

It is possible to augment the ``rework`` command with new subcommands
(or augment, modify existing commands).

Any program doing so must define a new command and declare a setup
tools entry point named ``rework:subcommand`` as in e.g.:

.. code-block:: python

   entry_points={'rework.subcommands': [
       'view=rework_ui.cli:view'
   ]}

For instance, the [rework_ui][reworkui] python package provides such a
``view`` subcommand to launch a monitoring webapp for a given rework
job store.
..

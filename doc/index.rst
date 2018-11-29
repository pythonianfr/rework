Rework
======

`rework` is a distributed execution system for the execution of tasks
that can belong to independant python environments and code bases,
even hosted on different computers.

The only constraint is that postgres must be accessible from all nodes
of a given `rework` installation.

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

`operation` A python function decorated with the `task` decorator. The
    function has a single `task` parameter that allows to communicate
    with the system (for the purposes of input and output management,
    and log capture). It is defined within a `domain` and on a
    specific `host`.

`task` A concrete execution of an operation. Also, name of the
    decorator that indicates an `operation`. The task can indicate its
    state and be aborted if needed. It can provide access to the
    captured logs, input and output.

`worker` A python process spawned by a `monitor`, that will execute
    `tasks`. It is always associated with a `domain` on a specific
    `host`.

`domain` A label associated with `operations`, `tasks` and `workers`,
    which can be used to map operations to virtual environments or
    just help oprganize a logical separation of operations (and the
    associated pools of workers).

`monitor` A python process which is responsible for the management of
    workers (start, stop and abort), whose precise amount is
    configurable, within a `domain`.

They will be illustrated further in the documentation.


Installation
............

.. code-block:: bash

   $ pip install rework


Quick start
...........

Let's have a look at a simple example.

We need to set up a database first, which we'll name `jobstore`.

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


   if __name__ == '__main__':
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


Here we have defined a dummy task that will print a bunch of
sentences, doubler the input value and save a result back.

This has to be put into a python module, e.g. `test_rework.py`

At this point, the rework system knows *nothing* of the task. We must
register it, as follows:

.. code-block:: bash

   $ rework register-operations postgres://babar:password@localhost/jobstore test_rework.py
   registered 1 new operation (0 already known)

From this point, we can check it is indeed registered:

.. code-block:: bash

   $ rework list-operations postgres://babar:password@localhost/jobstore
   1 host(1) `10.211.55.3` path(my_first_task)

Now, let's execute our script:

.. code-block:: bash

   $ python test_rework.py

It will start and hang indefinitely on the first `join` call. Indeed
we are missing an important step: providing `workers` that will
execute the tasks.

This should be made in a separate shell, since it is a blocking
operation:

.. code-block:: bash

   $ rework monitor postgres://babar:password@localhost/jobstore

Then, the script will quickly terminate, as both tasks have been
executed.

Congratulations ! You juste fired your first rework tasks.
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

The last argument `1` is the task identifier as was shown by the
`list-tasks` command.

Notice how we capture the standard output (print calls) using the
`task.capturelogs` context manager. This is completely optional of
course but quite handy. The line shown above actually capture
*standard output*, *standard error* and *all logs*. It accepts a
`level` parameter, like e.g. `capturelogs(level=logging.INFO)`.

Lastly, `list-workers` will show the currently running workers:

.. code-block:: bash

   $ rework list-workers postgres://babar:password@localhost/jobstore
   1 4124@10.211.55.3 43 Mb [running (idle)] [2018-11-28 16:08:27.438491+01] → [2018-11-28 15:08:27.967432+01] 
   2 4125@10.211.55.3 43 Mb [running (idle)] [2018-11-28 16:08:27.442869+01] → [2018-11-28 15:08:27.967397+01] 

It is now possible to stop the `monitor` on its separate console, with
a plain `ctrl-c`.

After this, `list-workers` will provide an updated status:

.. code-block:: bash

   $ rework list-workers postgres://aurelien:aurelien@localhost/rework 
   1 4124@10.211.55.3 43 Mb [dead] [2018-11-28 16:08:27.438491+01] → [2018-11-28 15:08:27.967432+01] → [2018-11-28 16:11:09.668587+01] monitor exit 
   2 4125@10.211.55.3 43 Mb [dead] [2018-11-28 16:08:27.442869+01] → [2018-11-28 15:08:27.967397+01] → [2018-11-28 16:11:09.668587+01] monitor exit 


API documentation
=================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. automodule:: rework.api
   :members: task, schedule

.. autoclass:: rework.task.Task
   :members: capturelogs, join, input, raw_input, save_output, abort, metadata


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`




from __future__ import print_function
import time
import logging

from rework import api


@api.task
def print_sleep_and_go_away(task):
    print('Hello, world')
    time.sleep(.2)
    print('I am running within task', task.tid)
    time.sleep(.2)
    print('Saving computation to task.output')
    task.save_output(2 * task.input)
    print('And now I am done.')


@api.task(domain='nondefault')
def run_in_non_default_domain(task):
    task.save_output('done')


@api.task
def raw_input(task):
    task.save_output(task.raw_input + b' and Celeste',
                     raw=True)


@api.task
def infinite_loop(task):
    while True:
        time.sleep(1)


@api.task
def unstopable_death(task):
    import os
    os._exit(0)


@api.task
def normal_exception(task):
    raise Exception('oops')


LEAK = None


@api.task
def allocate_and_leak_mbytes(task):
    bytestr = 'a' * (task.input * 2**20)
    global LEAK
    LEAK = bytestr


@api.task
def capture_logs(task):
    logger = logging.getLogger('my_app_logger')
    logger.debug('uncaptured %s', 42)
    with task.capturelogs(std=True):
        logger.error('will be captured %s', 42)
        print('I want to be captured')
        logger.debug('will be captured %s also', 1)
    logger.debug('uncaptured %s', 42)
    print('This will be lost')


@api.task
def log_swarm(task):
    logger = logging.getLogger('APP')
    with task.capturelogs():
        for i in range(1, 250):
            logger.info('I will fill your database, %s', i)


@api.task
def stderr_swarm(task):
    import sys
    sys.stderr.write('X' * 8192)
    sys.stderr.flush()
    task.save_output(42)


@api.task
def flush_captured_stdout(task):
    with task.capturelogs(std=True):
        print('Hello')
        import sys
        sys.stdout.flush()


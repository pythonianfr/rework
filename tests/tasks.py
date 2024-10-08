import time
import logging
from datetime import timedelta

from rework import api, io


@api.task
def basic(task):
    print('This task is a 10. So basic...')


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


@api.task(
    inputs=(
        io.file('myfile'),
        io.number('foo'),
        io.string('bar')),
    outputs=(io.string('message'),)
)
def fancy_inputs_outputs(task):
    inputs = task.input
    msg = (
        f'file length: {len(inputs["myfile"])},'
        f'foo: {inputs["foo"]},'
        f'bar: {inputs["bar"]}'
    )
    if inputs['bar'] == 'Celeste':
        task.save_output(
            {'blogentry': msg}
        )
    else:
        task.save_output(
            {'message': msg}
        )


@api.task
def infinite_loop(task):
    while True:
        if task.input:
            # allow to eat a cpu
            continue
        time.sleep(1)


@api.task(timeout=timedelta(seconds=1))
def infinite_loop_timeout(task):
    time.sleep(3600)


@api.task(timeout=timedelta(minutes=1))
def infinite_loop_long_timeout(task):
    time.sleep(3600)


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
    with task.capturelogs(std=True):
        bytestr = 'a' * (task.input * 2**20)
        global LEAK
        LEAK = bytestr
        # let's run slow to allow the monitor
        # tracker to catch the leak
        time.sleep(.1)
        print('Leaked')
        time.sleep(.4)
        print('Finished')


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
        print('Hello ', end='')
        import sys
        sys.stdout.flush()  # force Hello on one line
        print('World')

        print('', end='')  # this won't show up ever
        sys.stdout.flush()

        print('This is an unfinished statement ', end='')
        print('which could go on for a', end='')
        print(' ', end='')
        sys.stdout.flush()
        print('long time, but I have had enough')

        print('A truly multiline\nstatement.')
        print('Honor the', 'space.')
        print(' (hi) ', end='')

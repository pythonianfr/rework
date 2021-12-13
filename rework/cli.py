import imp
from time import sleep
import tzlocal
from pathlib import Path
import pickle

import click
from colorama import init, Fore, Style
from pkg_resources import iter_entry_points
import pyzstd as zstd

from sqlalchemy import create_engine
from sqlhelp import update, select

from rework import api, schema
from rework.helper import (
    cleanup_tasks,
    cleanup_workers,
    find_dburi,
    utcnow
)
from rework.worker import Worker
from rework.task import Task
from rework.monitor import Monitor


TZ = tzlocal.get_localzone()


@click.group()
def rework():
    pass


@rework.command(name='init-db')
@click.argument('dburi')
@click.pass_context
def init_db(ctx, dburi):
    " initialize a postgres database with everything needed "
    engine = create_engine(find_dburi(dburi))
    schema.init(engine, drop=True)


@rework.command(name='register-operations')
@click.argument('dburi')
@click.argument('module', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.option('--domain')
@click.option('--asdomain')
def register_operations(dburi, module, domain=None, asdomain=None):
    """register operations from a python module containing
    python functions decorated with `api.task`

    It is possible to filter by domain and also to specify a
    subtitution domain.

    The registered operations will be relative to the current host.
    """
    mod = imp.load_source('operations', module)
    engine = create_engine(find_dburi(dburi))
    ok, ko = api.freeze_operations(engine, domain)

    print(f'registered {len(ok)} new operation ({len(ko)} already known)')


@rework.command(name='unregister-operation')
@click.argument('dburi')
@click.argument('operation')
@click.option('--domain')
@click.option('--module', help='part of the module path')
@click.option('--host')
@click.option('--confirm/--no-confirm', is_flag=True, default=True)
def unregister_operation(dburi, operation, domain=None, module=None, host=None,
                         confirm=True):
    """unregister an operation (or several) using its name and other properties
    (module path, domain and host).

    """
    init()
    engine = create_engine(find_dburi(dburi))

    sql = select(
        'id', 'name', 'domain', 'path', 'host'
    ).table(
        'rework.operation'
    ).where(name=operation)
    if module:
        sql.where('path like %(module)s', module=module)
    if domain:
        sql.where(domain=domain)
    if host:
        sql.where(host=host)

    candidates = sql.do(engine).fetchall()
    if not len(candidates):
        print('Nothing to unregister')

    if confirm:
        print('preparing de-registration of:')
        for oid, name, domain, path, host in candidates:
            print(Fore.RED + 'delete', end=' ')
            print(Fore.GREEN + name, end=' ')
            print(Fore.WHITE + domain, path, host)
        if not click.confirm('really remove those [y/n]?'):
            print('Ok, nothing has been done.')
            return

    with engine.begin() as cn:
        for oid, name, domain, path, host in candidates:
            print(Fore.RED + 'delete', end=' ')
            print(Fore.GREEN + name, end=' ')
            print(Fore.WHITE + domain, path, host)
            cn.execute(
                'delete from rework.operation '
                'where id = %(oid)s',
                oid=oid
            )


@rework.command(name='new-worker')
@click.argument('dburi')
@click.argument('worker_id', type=int)
@click.argument('ppid', type=int)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
@click.option('--domain', default='default')
@click.option('--debug-port', type=int, default=0)
def new_worker(**config):
    " spawn a new worker -- this is a purely *internal* command "
    if config.get('debug_port', 0):
        try:
            import pystuck
        except ImportError:
            print('--debug-port is unsupported without `pystuck` ')
            print('try "pip install pystuck" to make it work')
            return
    worker = Worker(**config)
    worker.run()


@rework.command()
@click.argument('dburi')
@click.option('--maxworkers', type=int, default=2)
@click.option('--minworkers', type=int)
@click.option('--maxruns', type=int, default=0)
@click.option('--maxmem', type=int, default=0,
              help='shutdown on Mb consummed')
@click.option('--domain', default='default')
@click.option('--debug', is_flag=True, default=False)
@click.option('--start-timeout', type=int, default=30)
@click.option('--debugfile')
def monitor(dburi, **config):
    " start a monitor controlling min/max workers "
    engine = create_engine(find_dburi(dburi))
    monitor = Monitor(engine, **config)
    monitor.run()


@rework.command(name='list-operations')
@click.argument('dburi')
def list_operations(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = 'select id, name, domain, path, host from rework.operation'
    for oid, opname, domain, modpath, hostid in engine.execute(sql):
        print(Fore.WHITE + f'{oid}', end=' ')
        print(Fore.GREEN + f'`{opname}` {domain} path({modpath}) host({hostid})')
    print(Style.RESET_ALL)


@rework.command(name='list-workers')
@click.argument('dburi')
def list_workers(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = ('select id, host, pid, mem, debugport, running, shutdown, traceback, '
           'deathinfo, created, started, finished '
           'from rework.worker order by id, running')
    for (wid, host, pid, mem, debugport, running, shutdown, traceback,
         deathinfo, created, started, finished) in engine.execute(sql):
        color = Fore.GREEN
        dead = not running and pid
        activity = '(idle)'
        if dead:
            color = Fore.RED
        else:
            color = Fore.MAGENTA
            sql = "select id from rework.task where status = 'running' and worker = %(worker)s"
            tid = engine.execute(sql, worker=wid).scalar()
            if tid:
                activity = f'#{tid}'
        print(wid,
              Fore.GREEN + f'{pid or "<nopid>"}@{host}',
              f'{mem} Mb',
              color + (
                  f'[running {activity}]' if running else '[dead]' if dead else '[unstarted]'
              ),
              end=' ')
        if debugport:
            print(Fore.RED + f'debugport = {debugport}', end=' ')

        print(Fore.WHITE + '[{}]'.format(
            created.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
              end=' ')
        if started:
            print(Fore.WHITE + '→ [{}]'.format(
                started.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')
        if finished:
            print(Fore.WHITE + '→ [{}]'.format(
                finished.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')

        if dead:
            if deathinfo:
                print(Fore.YELLOW + deathinfo, end=' ')
            if traceback:
                print(Fore.YELLOW + traceback, end=' ')
        print(Style.RESET_ALL)


@rework.command(name='list-monitors')
@click.argument('dburi')
def list_monitors(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = ('select id, domain, options, lastseen from rework.monitor')
    now = utcnow().astimezone(TZ)
    for mid, domain, options, lastseen in engine.execute(sql):
        color = Fore.GREEN
        delta = (now - lastseen).total_seconds()
        if delta > 60:
            color = Fore.RED
        elif delta > 10:
            color = Fore.MAGENTA
        print(mid,
              color + lastseen.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S%z'), end=' ')
        print(Style.RESET_ALL, end=' ')
        print(domain, 'options({})'.format(
            ', '.join('{}={}'.format(k, v) for k, v in options.items()))
        )


@rework.command(name='shutdown-worker')
@click.argument('dburi')
@click.argument('worker-id')
def shutdown_worker(dburi, worker_id):
    """ ask a worker to shut down as soon as it becomes idle

    If you want to immediately and unconditionally terminate
    a worker, use `rework kill-worker`
    """
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        update('rework.worker').where(id=worker_id).values(
            shutdown=True
        ).do(cn)


@rework.command(name='kill-worker')
@click.argument('dburi')
@click.argument('worker-id')
def kill_worker(dburi, worker_id):
    """ ask to preemptively kill a given worker to its monitor

    If you want to not risk interrupting any ongoing work,
    you should use `rework shutdown` instead.
    """
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        update('rework.worker').where(id=worker_id).values(
            kill=True
        ).do(cn)


status_color = {
    'done': Fore.GREEN,
    'aborted': Fore.RED,
    'aborting': Fore.RED,
    'failed': Fore.RED,
    'running': Fore.YELLOW,
    'queued': Fore.MAGENTA
}


@rework.command('list-tasks')
@click.argument('dburi')
@click.option('--tracebacks/--no-tracebacks', default=False)
@click.option('--logcount/--no-logcount', default=False)
def list_tasks(dburi, tracebacks=False, logcount=False):
    init()
    engine = create_engine(find_dburi(dburi))
    opmap = dict(engine.execute('select id, name from rework.operation').fetchall())
    sql = ('select t.id, w.deathinfo, w.mem '
           'from rework.task as t left outer join rework.worker as w '
           'on t.worker = w.id '
           'order by id')
    for tid, di, mem in engine.execute(sql):
        task = Task.byid(engine, tid)
        stat = task.state
        print(Style.RESET_ALL + str(tid),
              Fore.GREEN + opmap[task.operation],
              status_color[stat] + stat, end=' ')
        if logcount:
            sql = 'select count(*) from rework.log where task = %(tid)s'
            count = engine.execute(sql, {'tid': task.tid}).scalar()
            print(Style.RESET_ALL + f'{count} log lines', end=' ')
        finished = task._propvalue('finished')
        started = task._propvalue('started')
        print(Fore.WHITE + '[{}]'.format(
            task._propvalue('queued').strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
              end=' ')
        if started:
            print(Fore.WHITE + '→ [{}]'.format(
                started.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')
        if finished:
            print(Fore.WHITE + '→ [{}]'.format(
                finished.strftime('%Y-%m-%d %H:%M:%S.%f%Z')),
            end=' ')

        if tracebacks and task.traceback:
            print(Fore.YELLOW + task.traceback, end='')

        if di:
            print(Fore.YELLOW + di, end=' ')
            print(Fore.YELLOW + str(mem), end='')
        print()


@rework.command('log-task')
@click.argument('dburi')
@click.argument('taskid', type=int)
@click.option('--watch', is_flag=True, default=False)
@click.option('--fromid', type=int)
def log_task(dburi, taskid, fromid=None, watch=False):
    engine = create_engine(find_dburi(dburi))
    task = Task.byid(engine, taskid)

    def watchlogs(fromid):
        lid = fromid
        for lid, line in task.logs(fromid=fromid):
            print(Fore.GREEN + line.strip())
        return lid

    if not watch:
        watchlogs(fromid)
        if task.traceback:
            print(task.traceback)
        return

    while True:
        fromid = watchlogs(fromid)
        sleep(1)
        if task.status == 'done':
            break

    print(Style.RESET_ALL)


@rework.command('abort-task')
@click.argument('dburi')
@click.argument('taskid', type=int)
def abort_task(dburi, taskid):
    """ immediately abort the given task

    This will be done by doing a preemptive kill on
    its associated worker.
    """
    engine = create_engine(find_dburi(dburi))
    task = Task.byid(engine, taskid)
    task.abort('from the command line')


@rework.command(name='list-scheduled')
@click.argument('dburi')
def list_scheduled(dburi):
    init()
    engine = create_engine(find_dburi(dburi))
    sql = (
        'select id, operation, domain, inputdata, host, metadata, rule '
        'from rework.sched'
    )
    for sid, op, dom, indata, host, meta, rule in engine.execute(sql):
        q = select('name').table('rework.operation').where(id=op)
        opname = q.do(engine).scalar()
        print(Fore.WHITE + f'{sid}', end=' ')
        print(Fore.GREEN +
              f'`{opname}` {dom} `{host or "no host"}` '
              f'`{meta or "no meta"}` "{rule}"'
        )
    print(Style.RESET_ALL)


@rework.command(name='export-scheduled')
@click.argument('dburi')
@click.option('--path', default='rework.sched')
def export_scheduled(dburi, path):
    engine = create_engine(find_dburi(dburi))
    sql = (
        'select op.name, s.domain, s.inputdata, s.host, s.metadata, s.rule '
        'from rework.sched as s, rework.operation as op '
        'where s.operation = op.id'
    )
    inputs = []
    for row in engine.execute(sql):
        inp = {}
        for k, v in row.items():
            if isinstance(v, memoryview):
                v = v.tobytes()
            inp[k] = v
        inputs.append(inp)

    Path(path).write_bytes(
        zstd.compress(
            pickle.dumps(inputs)
        )
    )
    print(f'saved {len(inputs)} entries into {path}')


@rework.command(name='import-scheduled')
@click.argument('dburi')
@click.option('--path', default='rework.sched')
def import_scheduled(dburi, path):
    engine = create_engine(find_dburi(dburi))
    inputs = pickle.loads(
        zstd.decompress(
            Path(path).read_bytes()
        )
    )
    for row in inputs:
        api.prepare(
            engine,
            row['name'],
            row['rule'],
            row['domain'],
            host=row['host'],
            metadata=row['metadata'],
            rawinputdata=row['inputdata'],
            _anyrule=True
        )
    print(f'loaded {len(inputs)} entries from {path}')


@rework.command('vacuum')
@click.argument('dburi')
@click.option('--workers', is_flag=True, default=False)
@click.option('--tasks', is_flag=True, default=False)
@click.option('--finished', type=click.DateTime())
def vacuum(dburi, workers=False, tasks=False, finished=None):
    " delete non-runing workers or finished tasks "
    if not (workers or tasks):
        print('to cleanup old workers or tasks '
              'please use --workers or --tasks')
        return
    if workers and tasks:
        print('vacuum deletes workers or tasks, not both '
              'at the same time')
        return

    engine = create_engine(find_dburi(dburi))
    if finished is None:
        finished = utcnow()
    if workers:
        count = cleanup_workers(engine, finished)
        print(f'deleted {count} workers')

    if tasks:
        count = cleanup_tasks(engine, finished)
        print(f'deleted {count} tasks')


for ep in iter_entry_points('rework.subcommands'):
    rework.add_command(ep.load())

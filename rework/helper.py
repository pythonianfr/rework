import os
import io
from threading import Thread
import socket
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import re
import json
import struct

from apscheduler.triggers.cron import CronTrigger
import pyzstd as zstd
import pytz
import psutil
from sqlalchemy.engine import url
from sqlhelp import select
from inireader import reader

from rework.io import _iobase


def utcnow():
    return datetime.utcnow().replace(tzinfo=pytz.utc)


def memory_usage(pid):
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return 0
    return int(process.memory_info().rss / float(2 ** 20))


def cpu_usage(pid):
    try:
        proc = psutil.Process(pid)
        return _cpu_tree_usage(proc)
    except psutil.NoSuchProcess:
        return 0


def _cpu_tree_usage(proc):
    try:
        cpu = proc.cpu_percent(interval=0.02)
    except psutil.NoSuchProcess:
        return 0
    for child in proc.children():
        cpu += _cpu_tree_usage(child)
    return cpu


def wait_true(func, timeout=6, sleeptime=.1):
    outcome = []

    def loop():
        start = time.time()
        while True:
            if (time.time() - start) > timeout:
                return
            output = func()
            if output:
                outcome.append(output)
                return
            time.sleep(sleeptime)

    th = Thread(target=loop)
    th.daemon = True
    th.start()
    th.join()
    assert outcome
    return outcome[0]


def guard(engine, sql, expr, timeout=6):

    def check():
        with engine.begin() as cn:
            return expr(cn.execute(sql))

    return wait_true(check, timeout)


def host():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))
    return s.getsockname()[0]


# read stuff

def ops_and_domains(engine):
    res = engine.execute(
        'select name, domain from rework.operation'
    ).fetchall()
    return set((name, domain) for name, domain in res)


# db cleanup

def cleanup_workers(engine, finished):
    with engine.begin() as cn:
        count = cn.execute(
            'with deleted as '
            '(delete from rework.worker '
            '        where running = false and '
            '              finished < %(finished)s '
            ' returning 1) '
            'select count(*) from deleted',
            finished=finished
        ).scalar()
    return count


def cleanup_tasks(engine, finished):
    with engine.begin() as cn:
        count = cn.execute(
            'with deleted as '
            '(delete from rework.task '
            '        where status = \'done\' and '
            '              finished < %(finished)s '
            ' returning 1) '
            'select count(*) from deleted',
            finished=finished
        ).scalar()
    return count


# process handling

def kill(pid, timeout=3):
    def on_terminate(proc):
        print(f'process {proc} terminated with exit code {proc.returncode}')

    # TERM then KILL
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        _, alive = psutil.wait_procs([proc], timeout=timeout, callback=on_terminate)
        if alive:
            proc.kill()
            _, alive = psutil.wait_procs([proc], timeout=timeout, callback=on_terminate)
            if alive:
                return False
    except psutil.NoSuchProcess:
        return True
    return True


def has_ancestor_pid(pid):
    parent = psutil.Process(os.getpid()).parent()
    while parent:
        if pid == parent.pid:
            return True
        parent = parent.parent()
    return False


def kill_process_tree(pid, timeout=3):
    """Terminate all the children of this process.
    inspired from https://psutil.readthedocs.io/en/latest/#terminate-my-children
    """
    try:
        procs = psutil.Process(pid).children()
    except psutil.NoSuchProcess:
        print(f'process {pid} is already dead')
        return True
    for proc in procs:
        kill_process_tree(proc.pid, timeout)
        kill(proc.pid)
    return kill(pid)


# timedelta (de)serialisation

def delta_isoformat(td):
    return f'P{td.days}DT0H0M{td.seconds}S'


_DELTA = re.compile('P(.*)DT(.*)H(.*)M(.*)S')
def parse_delta(td):
    match = _DELTA.match(td)
    if not match:
        raise Exception(f'unparseable time delta `{td}`')
    days, hours, minutes, seconds = match.groups()
    return timedelta(
        days=int(days), hours=int(hours),
        minutes=int(minutes), seconds=int(seconds)
    )


# configuration lookup

def get_cfg_path():
    if 'REWORKCFGPATH' in os.environ:
        cfgpath = Path(os.environ['REWORKCFGPATH'])
        if cfgpath.exists():
            return cfgpath
    cfgpath = Path('rework.cfg')
    if cfgpath.exists():
        return cfgpath
    cfgpath = Path('~/rework.cfg').expanduser()
    if cfgpath.exists():
        return cfgpath

    return None


def find_dburi(something):
    try:
        url.make_url(something)
    except Exception:
        pass
    else:
        return something

    # lookup in the env, then in cwd, then in the home
    cfgpath = get_cfg_path()
    if not cfgpath:
        raise Exception('could not use nor look up the db uri')

    try:
        cfg = reader(cfgpath)
        return cfg['dburi'][something]
    except Exception as exc:
        raise Exception((
            'could not find the `{}` entry in the '
            '[dburi] section of the `{}` '
            'conf file (cause: {} -> {})').format(
                something, cfgpath.resolve(),
                exc.__class__.__name__, exc)
        )


# Logging


class PGLogHandler(logging.Handler):
    maxqueue = 100

    def __init__(self, task, sync=True):
        super(PGLogHandler, self).__init__()
        self.task = task
        self.sync = sync
        self.lastflush = time.time()
        self.queue = []
        self.formatter = logging.Formatter(
            '%(name)s:%(levelname)s: %(asctime)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def emit(self, record):
        self.queue.append(record)

        if ((time.time() - self.lastflush) > 1 or
            len(self.queue) > self.maxqueue):
            self.flush()

    def flush(self):
        if not self.queue:
            return

        values = [{'task': self.task.tid,
                   'tstamp': record.created,
                   'line': self.formatter.format(record)}
                  for record in self.queue]
        self.queue = []
        self.lastflush = time.time()

        def writeback_log(values, engine):
            with engine.begin() as cn:
                sql = ('insert into rework.log '
                       '(task, tstamp, line) '
                       'values (%(task)s, %(tstamp)s, %(line)s)')
                cn.execute(sql, values)

        th = Thread(target=writeback_log,
                    args=(values, self.task.engine))
        th.daemon = True
        # fire and forget
        th.start()
        if self.sync:
            th.join()

    def close(self):
        pass


class PGLogWriter:
    __slots__ = ('stream', 'handler', 'level', 'pending')

    def __init__(self, stream, handler):
        self.stream = stream
        self.handler = handler
        if 'out' in self.stream:
            self.level = logging.INFO
        else:
            self.level = logging.WARNING
        self.pending = []

    def write(self, message):
        linefeed = '\n' in message
        if not linefeed and not message.strip('\n\r'):
            return
        self.pending.append(message)
        if linefeed:
            self.flush()

    def flush(self, force=False):
        message = ''.join(msg for msg in self.pending)
        if not message or '\n' not in message and not force:
            return

        self.pending = []
        for part in message.splitlines():
            self.handler.emit(
                    logging.LogRecord(
                        self.stream, self.level, '', -1, part, (), ()
                    )
                )


# cron handling

class BetterCronTrigger(CronTrigger):

    @classmethod
    def from_extended_crontab(cls, expr, timezone=None):
        """Create a :class:`~CronTrigger` from an extended standard crontab expression.

        See https://en.wikipedia.org/wiki/Cron for more information on
        the format accepted here.  We add an initial field there for
        seconds
        """
        values = expr.split()
        if len(values) != 6:
            raise ValueError(
                f'Wrong number of fields; got {len(values)}, expected 6'
            )

        return cls(
            second=values[0], minute=values[1], hour=values[2], day=values[3],
            month=values[4], day_of_week=values[5], timezone=timezone
        )


# json input serializer

class InputEncoder(json.JSONEncoder):

    def default(self, o):
        if getattr(o, '__json_encode__'):
            return o.__json_encode__()
        return super().default(o)


# inputs spec reader

def iospec(engine, attr='inputs'):
    assert attr in ('inputs', 'outputs')
    q = select(
        'id', 'host', 'name', 'domain', attr
    ).table('rework.operation'
    ).where(f'{attr} is not null'
    ).order('domain, name')

    out = []
    for row in q.do(engine).fetchall():
        out.append(
            (row.id,
             row.name,
             row.domain,
             row.host,
             row[attr]
            )
        )
    return out


def filterio(specs, operation, domain=None, hostid=None):
    out = []
    for sid, opname, dom, host, spec in specs:
        if opname == operation:
            if domain and domain != dom:
                continue
            if hostid and hostid != host:
                continue
            out.append(spec)

    if not len(out):
        return None

    if len(out) > 1:
        raise ValueError('Ambiguous operation selection')

    return out[0]


# binary serializer

def nary_pack(*bytestr):
    sizes = [
        struct.pack('!L', len(b))
        for b in bytestr
    ]
    sizes_size = struct.pack('!L', len(sizes))
    stream = io.BytesIO()
    stream.write(sizes_size)
    stream.write(b''.join(sizes))
    for bstr in bytestr:
        stream.write(bstr)
    return zstd.compress(
        stream.getvalue()
    )


def convert_io(spec, args):
    """Tries to convert string values into the well typed input values
    needed for prepare or schedule calls.

    May be usefull for user interfaces (or coming from json values).

    """
    if args is None and not len(spec):
        return

    typed = {}
    for field in spec:
        inp = _iobase.from_type(
            field['type'], field['name'], field['required'], field['choices']
        )
        val = inp.from_string(args)
        if val is not None:
            typed[inp.name] = val

    return typed


def pack_io(spec, args):
    if args is None and not len(spec):
        return

    raw = {}
    for field in spec:
        inp = _iobase.from_type(
            field['type'], field['name'], field['required'], field['choices']
        )
        val = inp.binary_encode(args)
        if val is not None:
            raw[inp.name] = val

    spec_keys = {field['name'] for field in spec}
    unknown_keys = set(args) - spec_keys

    if unknown_keys:
        raise ValueError(
            f'unknown inputs: {",".join(unknown_keys)}'
        )

    return nary_pack(*(
        [k.encode('utf-8') for k in raw] +
        list(raw.values()))
    )


def nary_unpack(packedbytes):
    try:
        packedbytes = zstd.decompress(packedbytes.tobytes())
    except zstd.ZstdError:
        raise TypeError('wrong input format')

    [sizes_size] = struct.unpack(
        '!L', packedbytes[:4]
    )
    payloadoffset = 4 + sizes_size * 4
    sizes = struct.unpack(
        f'!{"L"*sizes_size}',
        packedbytes[4: payloadoffset]
    )
    fmt = ''.join('%ss' % size for size in sizes)
    return struct.unpack(fmt, packedbytes[payloadoffset:])


def _raw_unpack_io(spec, packedbytes):
    byteslist = nary_unpack(packedbytes)
    middle = len(byteslist) // 2
    keys = [
        k.decode('utf-8')
        for k in byteslist[:middle]
    ]
    values = byteslist[middle:]
    return dict(zip(keys, values))


def unpack_io(spec,
              packedbytes,
              nofiles=False):
    output = _raw_unpack_io(spec, packedbytes)

    for field in spec:
        fname = field['name']
        if nofiles:
            if field['type'] == 'file':
                output.pop(fname, None)
                continue
        inp = _iobase.from_type(
            field['type'], fname, field['required'], field['choices']
        )
        val = inp.binary_decode(output)
        if val is None:
            continue

        output[inp.name] = val

    return output


def unpack_iofiles_length(spec, packedbytes):
    output = _raw_unpack_io(spec, packedbytes)

    for field in spec:
        fname = field['name']
        if field['type'] != 'file':
            output.pop(fname, None)
            continue

        inp = _iobase.from_type(
            'file', fname, field['required'], field['choices']
        )
        val = inp.binary_decode(output)
        if val is None:
            continue

        output[inp.name] = len(val)

    return output


def unpack_iofile(spec, packedbytes, name):
    output = _raw_unpack_io(spec, packedbytes)

    for field in spec:
        fname = field['name']
        if fname != name:
            output.pop(fname, None)
            continue

        assert field['type'] == 'file'

        inp = _iobase.from_type(
            'file', fname, field['required'], field['choices']
        )
        return inp.binary_decode(output)

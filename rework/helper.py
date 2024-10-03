import os
import io
import itertools as it
from threading import Thread
import socket
import time
import logging
from datetime import datetime
from pathlib import Path
import json
import struct
import tzlocal
import importlib
import sys

from icron import croniter_range
import pyzstd as zstd
import pytz
import psutil
from sqlalchemy.engine import url
from sqlhelp import select
from inireader import reader

from rework.io import _iobase


def setuplogger(level=logging.DEBUG):
    logger = logging.getLogger('rework')
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def load_source(modname, filename):
    loader = importlib.machinery.SourceFileLoader(
        modname, filename
    )
    spec = importlib.util.spec_from_file_location(
        modname, filename, loader=loader
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    loader.exec_module(module)
    return module


def utcnow():
    return datetime.utcnow().replace(tzinfo=pytz.utc)


def partition(pred, iterable):
    t1, t2 = it.tee(iterable)
    return list(filter(pred, t2)), list(it.filterfalse(pred, t1))


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

def cleanup_workers(engine, finished, domain):
    with engine.begin() as cn:
        count = cn.execute(
            'with deleted as '
            '(delete from rework.worker '
            '        where running = false and '
            '              finished < %(finished)s and'
            '              domain = %(domain)s'
            ' returning 1) '
            'select count(*) from deleted',
            finished=finished,
            domain=domain
        ).scalar()
    return count


def cleanup_tasks(engine, finished, domain, status='done'):
    with engine.begin() as cn:
        finish = ''
        if finished:
            finish = 't.finished < %(finished)s and'
        count = cn.execute(
            f'with deleted as '
            f'(delete from rework.task as t'
            f'        using rework.operation as o'
            f'        where t.status = %(status)s and '
            f'              {finish}'
            f'              t.operation = o.id and '
            f'              o.domain = %(domain)s'
            f' returning 1) '
            f'select count(*) from deleted',
            finished=finished,
            domain=domain,
            status=status
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

    def __init__(self, task):
        super(PGLogHandler, self).__init__()
        self.task = task
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


    _sql = (
        'insert into rework.log '
        '(task, tstamp, line) '
        'values (%(task)s, %(tstamp)s, %(line)s)'
    )

    def flush(self):
        if not self.queue:
            return

        values = [
            {
                'task': self.task.tid,
                'tstamp': record.created,
                'line': self.formatter.format(record)
            }
            for record in self.queue
        ]
        self.queue = []
        self.lastflush = time.time()

        with self.task.engine.begin() as cn:
            cn.execute(self._sql, values)

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

    # TextIO interface

    def write(self, message):
        linefeed = '\n' in message
        if not linefeed and not message.strip('\n\r'):
            # empty message with maybe just an \r
            return
        self.pending.append(message)
        if linefeed:
            self.flush()

    def close(self):
        self.flush(force=True)

    def flush(self, force=False):
        message = ''.join(msg for msg in self.pending)
        if not message or '\n' not in message and not force:
            return

        self.pending = []
        for part in message.splitlines():
            self.handler.emit(
                    logging.LogRecord(
                        self.stream, self.level, '', -1, part, (), None
                    )
                )


# cron handling

def iter_stamps_from_cronrules(rulemap, start, stop):
    for rule, payload in rulemap:
        for stamp in croniter_range(start, stop, rule):
            yield stamp, payload


def schedule_plan(engine, domain, delta):
    tz = tzlocal.get_localzone()
    q = select(
        's.rule', 'op.name'
    ).table('rework.sched as s', 'rework.operation as op'
    ).where('s.operation = op.id'
    ).where('s.domain = %(domain)s', domain=domain)
    out = q.do(engine).fetchall()
    now = datetime.now(tz)
    for stamp, op in sorted(iter_stamps_from_cronrules(
            out,
            now,
            now + delta)):
        yield stamp, op


# json input serializer

class InputEncoder(json.JSONEncoder):

    def default(self, o):
        if getattr(o, '__json_encode__', None):
            return o.__json_encode__()
        if isinstance(o, datetime):
            return o.isoformat()
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
        raise ValueError(f'Ambiguous operation selection `{operation}`')

    return out[0]


def prepared(engine, operation, domain):
    q = (
        'select s.id, s.inputdata, s.metadata, s.rule '
        'from rework.sched as s, rework.operation as o '
        'where s.domain = %(domain)s and '
        '      s.operation = o.id and '
        '      o.name = %(operation)s'
    )
    return [
        (sid, nary_unpack(inp), meta, rule)
        for sid, inp, meta, rule in
        engine.execute(
            q,
            operation=operation,
            domain=domain
        ).fetchall()
    ]


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
            field['type'], field['name'], field['required'], field['choices'], field['default']
        )
        val = inp.from_string(args)
        if val is not None:
            typed[inp.name] = val

    return typed


def pack_io(spec, args):
    "Prepare the args for a .prepare or .schedule api call"
    if args is None and not len(spec):
        return

    raw = {}
    for field in spec:
        inp = _iobase.from_type(
            field['type'], field['name'], field['required'], field['choices'], field.get('default')
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
            field['type'], fname, field['required'], field['choices'], field.get('default')
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
            'file', fname, field['required'], field['choices'], None
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
            'file', fname, field['required'], field['choices'], None
        )
        return inp.binary_decode(output)

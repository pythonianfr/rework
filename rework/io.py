import json
import calendar as cal
from datetime import datetime as dt

from dateutil.parser import (
    isoparse,
    parse as defaultparse,
    ParserError
)
from dateutil.relativedelta import relativedelta
from psyl import lisp


class _iobase:
    _fields = 'name', 'required', 'choices'

    def __init__(self, name, required=False, choices=None):
        self.name = name
        self.required = required
        self.choices = choices

    def __json_encode__(self):
        out = {
            name: getattr(self, name, None)
            for name in self._fields
        }
        out['type'] = self.__class__.__name__
        return out

    @staticmethod
    def from_type(atype, name, required, choices):
        return globals()[atype](name, required, choices)

    def val(self, args):
        val = args.get(self.name)
        if val is None:
            if self.required:
                raise ValueError(
                    f'missing required input: `{self.name}`'
                )
        else:
            if self.choices and val not in self.choices:
                raise ValueError(
                    f'{self.name} -> value not in {self.choices}'
                )
        return val


class number(_iobase):

    def from_string(self, args):
        return self.binary_decode(args)

    def binary_encode(self, args):
        val = self.val(args)
        if val is not None:
            if not isinstance(val, (int, float)):
                raise TypeError(
                    f'value `{repr(val)}` is not a number'
                )
            return str(val).encode('utf-8')

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is None:
            return
        try:
            return int(val)
        except ValueError:
            return float(val)


class string(_iobase):

    def from_string(self, args):
        return self.val(args)

    def binary_encode(self, args):
        val = self.val(args)
        if val is not None:
            if not isinstance(val, str):
                raise TypeError(
                    f'value `{repr(val)}` is not a string'
                )
            return val.encode('utf-8')

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is not None:
            return val.decode('utf-8')


class file(_iobase):

    def from_string(self, args):
        val = self.val(args)
        if val is not None:
            if not isinstance(val, bytes):
                return val.encode('utf-8')
            return val

    def binary_encode(self, args):
        val = self.val(args)
        if val is not None:
            if not isinstance(val, bytes):
                raise TypeError(
                    f'value `{repr(val)}` is not bytes'
                )
        return val

    def binary_decode(self, args):
        return args.get(self.name)


class datetime(_iobase):

    def from_string(self, args):
        val = self.val(args)
        if val is not None:
            return defaultparse(val)

    def binary_encode(self, args):
        val = self.val(args)
        if val is None:
            return
        if not isinstance(val, (str, dt)):
            raise TypeError(
                f'value `{repr(val)}` is not str/datetime'
            )

        if isinstance(val, str):
            try:
                val = defaultparse(val)
            except ParserError:
                raise TypeError(
                    f'value `{repr(val)}` is not a valid datetime'
                )

        val = val.isoformat().encode('utf-8')
        return val

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is None:
            return
        val = val.decode('utf-8')
        try:
            return isoparse(val)
        except ValueError:
            return defaultparse(val)


def _last_day_of_month(dt):
    return cal.monthrange(dt.year, dt.month)[1]


def _parsedatetime(strdt):
    try:
        return isoparse(strdt)
    except ValueError:
        return defaultparse(strdt)


_MOMENT_ENV = lisp.Env({
    'date': lambda strdate: _parsedatetime(strdate),
    'today': lambda: dt.now(),
    'monthstart': lambda dt: dt.replace(day=1),
    'monthend': lambda dt: dt.replace(day=_last_day_of_month(dt)),
    'yearstart': lambda dt: dt.replace(day=1, month=1),
    'yearend': lambda dt: dt.replace(day=31, month=12),
    'shifted': lambda dt, **kw: dt + relativedelta(**kw),
})


class moment(_iobase):

    def from_string(self, args):
        val = self.val(args)
        if val is not None:
            lisp.evaluate(val, env=_MOMENT_ENV)
            return val

    def binary_encode(self, args):
        val = self.val(args)
        if val is None:
            return
        try:
            # validate the expression
            lisp.evaluate(val, env=_MOMENT_ENV)
        except:
            import traceback as tb; tb.print_exc()
            raise TypeError(
                f'value `{repr(val)}` is not a valid moment expression'
            )

        return val.encode('utf-8')

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is None:
            return
        val = val.decode('utf-8')
        return lisp.evaluate(val, env=_MOMENT_ENV)

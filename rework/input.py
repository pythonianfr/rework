import json

from dateutil.parser import isoparse, parse as defaultparse


def parsedatetime(strdt):
    try:
        return isoparse(strdt)
    except ValueError:
        return defaultparse(strdt)


class inputio:
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


class number(inputio):

    def binary_encode(self, args):
        val = self.val(args)
        if val is not None:
            return str(val).encode('utf-8')

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is None:
            return
        try:
            return int(val)
        except ValueError:
            return float(val)


class string(inputio):

    def binary_encode(self, args):
        val = self.val(args)
        if val is not None:
            return val.encode('utf-8')

    def binary_decode(self, args):
        val = args.get(self.name)
        if val is not None:
            return val.decode('utf-8')


class file(inputio):

    def binary_encode(self, args):
        return self.val(args)

    def binary_decode(self, args):
        return args.get(self.name)


class datetime(inputio):

    def binary_encode(self, args):
        val = self.val(args)
        if val is None:
            return
        if isinstance(val, str):
            val = val.encode('utf-8')
        else:
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

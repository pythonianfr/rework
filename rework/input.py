import json


class _base:
    _fields = 'name', 'required', 'choices'

    def __json_encode__(self):
        out = {
            name: getattr(self, name, None)
            for name in self._fields
        }
        out['type'] = self.__class__.__name__
        return out


class number(_base):

    def __init__(self, name, required=False):
        self.name = name
        self.required = required


class string(_base):

    def __init__(self, name, required=False, choices=None):
        self.name = name
        self.required = required
        self.choices = choices


class file(_base):

    def __init__(self, name, required=False):
        self.name = name
        self.required = required

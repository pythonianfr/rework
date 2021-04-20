from warnings import warn

from rework.io import (
    number,
    string,
    file,
    datetime,
    moment
)

warn(
    'the `input` module is deprecated, use `io` instead, '
    'as in "from rework.io import ..."',
    DeprecationWarning,
    stacklevel=2
)

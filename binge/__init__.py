import os
from .binge import B
from ._version import __version__, __major__, __minor__, __micro__

_PATH = os.path.dirname(os.path.abspath(__file__))
_PATH = _PATH.split(os.path.sep)[:-1]

try:
    __doc__ = """
              {0}
              """.format(open(os.path.join(os.path.sep, os.path.sep.join(_PATH), 'README.rst'), 'r').read())
except:
    __doc__ = ""

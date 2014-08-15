#: A dict from type names as strings to the class that implements
#: them. This is used inside :py:class:`Map` to initialize new values.
TYPES = {}

from .datatype import Datatype
from .counter import Counter
from .flag import Flag
from .register import Register
from .set import Set
from .map import Map
from .errors import ContextRequired


__all__ = ['Datatype', 'Flag', 'Counter', 'Register', 'Set', 'Map', 'TYPES',
           'ContextRequired']

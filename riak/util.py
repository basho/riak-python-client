import warnings

try:
    from collections import Mapping
except ImportError:
    # compatibility with Python 2.5
    Mapping = dict


def quacks_like_dict(object):
    """Check if object is dict-like"""
    return isinstance(object, Mapping)


def deep_merge(a, b):
    """Merge two deep dicts non-destructively

    Uses a stack to avoid maximum recursion depth exceptions

    >>> a = {'a': 1, 'b': {1: 1, 2: 2}, 'd': 6}
    >>> b = {'c': 3, 'b': {2: 7}, 'd': {'z': [1, 2, 3]}}
    >>> c = merge(a, b)
    >>> from pprint import pprint; pprint(c)
    {'a': 1, 'b': {1: 1, 2: 7}, 'c': 3, 'd': {'z': [1, 2, 3]}}
    """
    assert quacks_like_dict(a), quacks_like_dict(b)
    dst = a.copy()

    stack = [(dst, b)]
    while stack:
        current_dst, current_src = stack.pop()
        for key in current_src:
            if key not in current_dst:
                current_dst[key] = current_src[key]
            else:
                if (quacks_like_dict(current_src[key])
                    and quacks_like_dict(current_dst[key])):
                    stack.append((current_dst[key], current_src[key]))
                else:
                    current_dst[key] = current_src[key]
    return dst


def deprecated(message, stacklevel=3):
    warnings.warn(message, DeprecationWarning, stacklevel=stacklevel)

QUORUMS = ['r', 'pr', 'w', 'dw', 'pw', 'rw']
QDEPMESSAGE = """
Quorum accessors on type %s are deprecated. Use request-specific
parameters or bucket properties instead.
"""


def deprecateQuorumAccessors(klass, parent=None):
    """
    Adds deprecation warnings for the quorum get_* and set_*
    accessors, informing the user to switch to the appropriate bucket
    properties or requests parameters.
    """
    for q in QUORUMS:
        __deprecateQuorumAccessor(klass, parent, q)
    return klass


def __deprecateQuorumAccessor(klass, parent, quorum):
    propname = "_%s" % quorum
    getter_name = "get_%s" % quorum
    setter_name = "set_%s" % quorum
    if not parent:
        def getter(self, val=None):
            deprecated(QDEPMESSAGE % klass.__name__)
            if val:
                return val
            return getattr(self, propname, "default")

    else:
        def getter(self, val=None):
            deprecated(QDEPMESSAGE % klass.__name__)
            if val:
                return val
            parentInstance = getattr(self, parent)
            return getattr(self, propname,
                           getattr(parentInstance, propname, "default"))

    def setter(self, value):
        deprecated(QDEPMESSAGE % klass.__name__)
        setattr(self, propname, value)
        return self

    setattr(klass, getter_name, getter)
    setattr(klass, setter_name, setter)


class lazy_property(object):
    '''
    meant to be used for lazy evaluation of an object attribute.
    property should represent non-mutable data, as it replaces itself.
    '''

    def __init__(self, fget):
        self.fget = fget
        self.func_name = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return None
        value = self.fget(obj)
        setattr(obj, self.func_name, value)
        return value

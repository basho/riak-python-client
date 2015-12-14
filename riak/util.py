from __future__ import print_function
import warnings
from collections import Mapping
from six import string_types, PY2


def quacks_like_dict(object):
    """Check if object is dict-like"""
    return isinstance(object, Mapping)


def deep_merge(a, b):
    """Merge two deep dicts non-destructively

    Uses a stack to avoid maximum recursion depth exceptions

    >>> a = {'a': 1, 'b': {1: 1, 2: 2}, 'd': 6}
    >>> b = {'c': 3, 'b': {2: 7}, 'd': {'z': [1, 2, 3]}}
    >>> c = deep_merge(a, b)
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
                if (quacks_like_dict(current_src[key]) and
                        quacks_like_dict(current_dst[key])):
                    stack.append((current_dst[key], current_src[key]))
                else:
                    current_dst[key] = current_src[key]
    return dst


def deprecated(message, stacklevel=3):
    """
    Prints a deprecation warning to the console.
    """
    warnings.warn(message, UserWarning, stacklevel=stacklevel)


class lazy_property(object):
    '''
    A method decorator meant to be used for lazy evaluation and
    memoization of an object attribute. The property should represent
    immutable data, as it replaces itself on first access.
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


def decode_index_value(index, value):
    if "_int" in bytes_to_str(index):
        return str_to_long(value)
    elif PY2:
        return str(value)
    else:
        return bytes_to_str(value)


def bytes_to_str(value, encoding='utf-8'):
    if isinstance(value, string_types) or value is None:
        return value
    elif isinstance(value, list):
        return [bytes_to_str(elem) for elem in value]
    else:
        return value.decode(encoding)


def str_to_bytes(value, encoding='utf-8'):
    if PY2 or value is None:
        return value
    elif isinstance(value, list):
        return [str_to_bytes(elem) for elem in value]
    else:
        return value.encode(encoding)


def str_to_long(value, base=10):
    if value is None:
        return None
    elif PY2:
        return long(value, base)  # noqa
    else:
        return int(value, base)

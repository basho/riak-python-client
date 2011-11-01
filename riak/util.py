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
                if quacks_like_dict(current_src[key]) and quacks_like_dict(current_dst[key]) :
                    stack.append((current_dst[key], current_src[key]))
                else:
                    current_dst[key] = current_src[key]
    return dst


def deprecated(message, stacklevel=3):
    warnings.warn(message, DeprecationWarning, stacklevel=stacklevel)

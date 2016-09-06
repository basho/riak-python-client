import six

from .datatype import Datatype
from riak.datatypes import TYPES

__all__ = ['Hll']


class Hll(Datatype):
    """A convergent datatype representing a HyperLogLog set.
    Currently strings are the only supported value type.
    Example::

        myhll.add('barista')
        myhll.add('roaster')
        myhll.add('brewer')
    """

    type_name = 'hll'
    _type_error_msg = 'Hlls can only be integers'

    def _post_init(self):
        self._adds = set()

    def _default_value(self):
        return 0

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this HyperLogLog has staged adds.
        """
        return len(self._adds) > 0

    def to_op(self):
        """
        Extracts the modification operation from the Hll.

        :rtype: dict, None
        """
        if not self._adds:
            return None
        changes = {}
        if self._adds:
            changes['adds'] = list(self._adds)
        return changes

    def add(self, element):
        """
        Adds an element to the HyperLogLog. Datatype cardinality will
        be updated when the object is saved.

        :param element: the element to add
        :type element: str
        """
        if not isinstance(element, six.string_types):
            raise TypeError("Hll elements can only be strings")
        self._adds.add(element)

    def _coerce_value(self, new_value):
        return int(new_value)

    def _check_type(self, new_value):
        return isinstance(new_value, six.integer_types)


TYPES['hll'] = Hll

from collections import Sized
from riak.datatypes.datatype import Datatype


class Register(Sized, Datatype):
    """
    A convergent datatype that represents an opaque string that is set
    with last-write-wins semantics, and may only be embedded in
    :py:class:`~riak.datatypes.Map` instances.
    """

    type_name = 'register'
    _type_error_msg = "Registers can only be strings"

    def _post_init(self):
        self._new_value = None

    def _default_value(self):
        return ""

    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original value of the register.

        :rtype: str
        """
        return self._value[:]

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this register has staged assignment.
        """
        return self._new_value is not None

    def to_op(self):
        """
        Extracts the mutation operation from the register.

        :rtype: str, None
        """
        if self._new_value is not None:
            return ('assign', self._new_value)

    def assign(self, new_value):
        """
        Assigns a new value to the register.

        :param new_value: the new value for the register
        :type new_value: str
        """
        self._raise_if_badtype(new_value)
        self._new_value = new_value

    def __len__(self):
        return len(self.value)

    def _check_type(self, new_value):
        return isinstance(new_value, basestring)


from riak.datatypes import TYPES
TYPES['register'] = Register

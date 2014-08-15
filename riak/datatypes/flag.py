from riak.datatypes.datatype import Datatype


class Flag(Datatype):
    """
    A convergent datatype that represents a boolean value that can be
    enabled or disabled, and may only be embedded in :py:class:`Map`
    instances.
    """

    type_name = 'flag'
    _type_error_msg = "Flags can only be booleans"

    def _post_init(self):
        self._op = None

    def _default_value(self):
        return False

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this flag has staged toggles.
        """
        return self._op is not None

    def enable(self):
        """
        Turns the flag on, effectively setting its value to 'True'.
        """
        self._op = 'enable'

    def disable(self):
        """
        Turns the flag off, effectively setting its value to 'False'.
        """
        self._require_context()
        self._op = 'disable'

    def to_op(self):
        """
        Extracts the mutation operation from the flag.

        :rtype: bool, None
        """
        return self._op

    def _check_type(self, new_value):
        return isinstance(new_value, bool)


from riak.datatypes import TYPES
TYPES['flag'] = Flag

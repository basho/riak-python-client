from riak import RiakError


class ContextRequired(RiakError):
    """
    This exception is raised when removals of map fields and set
    entries are attempted and the datatype hasn't been initialized
    with a context.
    """

    _default_message = ("A context is required for remove operations, "
                        "fetch the datatype first")

    def __init__(self, message=None):
        super(ContextRequired, self).__init__(message or
                                              self._default_message)

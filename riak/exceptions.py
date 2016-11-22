class BadResource(Exception):
    """
    Users of a :class:`Pool` should raise this error when the pool
    resource currently in-use is bad and should be removed from the
    pool.
    """
    pass


class ConnectionClosed(BadResource):
    """
    Users of a :class:`Pool` should raise this error when the pool
    resource currently in-use has been closed and should be removed
    from the pool.
    """
    pass

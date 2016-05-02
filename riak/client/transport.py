from contextlib import contextmanager
from riak.transports.pool import BadResource
from riak.transports.tcp import is_retryable as is_tcp_retryable
from riak.transports.http import is_retryable as is_http_retryable
import threading
from six import PY2
if PY2:
    from httplib import HTTPException
else:
    from http.client import HTTPException

#: The default (global) number of times to retry requests that are
#: retryable. This can be modified locally, per-thread, via the
#: :attr:`RiakClient.retries` property, or using the
#: :attr:`RiakClient.retry_count` method in a ``with`` statement.
DEFAULT_RETRY_COUNT = 3


class _client_locals(threading.local):
    """
    A thread-locals object used by the client.
    """
    def __init__(self):
        self.riak_retries_count = DEFAULT_RETRY_COUNT


class RiakClientTransport(object):
    """
    Methods for RiakClient related to transport selection and retries.
    """

    # These will be set or redefined by the RiakClient initializer
    protocol = 'pbc'
    _http_pool = None
    _tcp_pool = None
    _locals = _client_locals()

    def _get_retry_count(self):
        return self._locals.riak_retries_count or DEFAULT_RETRY_COUNT

    def _set_retry_count(self, value):
        if not isinstance(value, int):
            raise TypeError("retries must be an integer")
        self._locals.riak_retries_count = value

    __retries_doc = """
          The number of times retryable operations will be attempted
          before raising an exception to the caller. Defaults to
          ``3``.

          :note: This is a thread-local for safety and
                 operation-specific modification. To change the
                 default globally, modify
                 :data:`riak.client.transport.DEFAULT_RETRY_COUNT`.
          """

    retries = property(_get_retry_count, _set_retry_count, doc=__retries_doc)

    @contextmanager
    def retry_count(self, retries):
        """
        retry_count(retries)

        Modifies the number of retries for the scope of the ``with``
        statement (in the current thread).

        Example::

            with client.retry_count(10):
                client.ping()
        """
        if not isinstance(retries, int):
            raise TypeError("retries must be an integer")

        old_retries, self.retries = self.retries, retries
        try:
            yield
        finally:
            self.retries = old_retries

    @contextmanager
    def _transport(self):
        """
        _transport()

        Yields a single transport to the caller from the default pool,
        without retries.
        """
        pool = self._choose_pool()
        with pool.transaction() as transport:
            yield transport

    def _acquire(self):
        """
        _acquire()

        Acquires a connection from the default pool.
        """
        return self._choose_pool().acquire()

    def _with_retries(self, pool, fn):
        """
        Performs the passed function with retries against the given pool.

        :param pool: the connection pool to use
        :type pool: Pool
        :param fn: the function to pass a transport
        :type fn: function
        """
        skip_nodes = []

        def _skip_bad_nodes(transport):
            return transport._node not in skip_nodes

        retry_count = self.retries

        for retry in range(retry_count):
            try:
                with pool.transaction(_filter=_skip_bad_nodes) as transport:
                    try:
                        return fn(transport)
                    except (IOError, HTTPException) as e:
                        if _is_retryable(e):
                            transport._node.error_rate.incr(1)
                            skip_nodes.append(transport._node)
                            raise BadResource(e)
                        else:
                            raise
            except BadResource as e:
                if retry < (retry_count - 1):
                    continue
                else:
                    # Re-raise the inner exception
                    raise e.args[0]

    def _choose_pool(self, protocol=None):
        """
        Selects a connection pool according to the default protocol
        and the passed one.

        :param protocol: the protocol to use
        :type protocol: string
        :rtype: Pool
        """
        if not protocol:
            protocol = self.protocol
        if protocol == 'http':
            pool = self._http_pool
        elif protocol == 'tcp' or protocol == 'pbc':
            pool = self._tcp_pool
        else:
            raise ValueError("invalid protocol %s" % protocol)
        return pool


def _is_retryable(error):
    """
    Determines whether a given error is retryable according to the
    exceptions allowed to be retried by each transport.

    :param error: the error to check
    :type error: Exception
    :rtype: boolean
    """
    return is_tcp_retryable(error) or is_http_retryable(error)


def retryable(fn, protocol=None):
    """
    Wraps a client operation that can be retried according to the set
    :attr:`RiakClient.retries`. Used internally.
    """
    def wrapper(self, *args, **kwargs):
        pool = self._choose_pool(protocol)

        def thunk(transport):
            return fn(self, transport, *args, **kwargs)

        return self._with_retries(pool, thunk)

    wrapper.__doc__ = fn.__doc__
    wrapper.__repr__ = fn.__repr__

    return wrapper


def retryableHttpOnly(fn):
    """
    Wraps a retryable client operation that is only valid over HTTP.
    Used internally.
    """
    return retryable(fn, protocol='http')

import errno
import socket

from riak.transports.pool import Pool, ConnectionClosed
from riak.transports.tcp.transport import TcpTransport


class TcpPool(Pool):
    """
    A resource pool of TCP transports.
    """
    def __init__(self, client, **options):
        super(TcpPool, self).__init__()
        self._client = client
        self._options = options

    def create_resource(self):
        node = self._client._choose_node()
        return TcpTransport(node=node,
                            client=self._client,
                            **self._options)

    def destroy_resource(self, tcp):
        tcp.close()


# These are a specific set of socket errors
# that could be raised on send/recv that indicate
# that the socket is closed or reset, and is not
# usable. On seeing any of these errors, the socket
# should be closed, and the connection re-established.
CONN_CLOSED_ERRORS = (
    errno.EHOSTUNREACH,
    errno.ECONNRESET,
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ETIMEDOUT,
    errno.EBADF,
    errno.EPIPE
)


def is_retryable(err):
    """
    Determines if the given exception is something that is
    network/socket-related and should thus cause the TCP connection to
    close and the operation retried on another node.

    :rtype: boolean
    """
    if isinstance(err, ConnectionClosed):
        # NB: only retryable if we're not mid-streaming
        if err.mid_stream:
            return False
        else:
            return True
    elif isinstance(err, socket.error):
        code = err.args[0]
        return code in CONN_CLOSED_ERRORS
    else:
        return False

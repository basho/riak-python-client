"""
Copyright 2011 Greg Stein <gstein@gmail.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import httplib
import socket
import contextlib
import functools


class ConnectionManager(object):

    # Must be constructable with: connection_class(host, port)
    # Must have two attribute: host and port
    # Must have a close() method
    connection_class = None

    def __init__(self, hostports=[]):
        # We want a private copy of this list: either to detach the argument
        # default, or to detach from the caller's list.
        self.hostports = hostports[:]

        # Open a connection to each specified host/port. On single-threaded
        # systems, this will create a round-robin across all specified servers.
        # When multi-threaded, this will give us an initial set for all the
        # threads to work with (and more will be created, according to demand).
        self.conns = [self.connection_class(host, port)
                      for host, port in hostports]

    def add_hostport(self, host, port):
        self.hostports.append((host, port))

        # Open an initial connection. For single-threaded, this adds to the
        # round-robin pool. On multi-threaded, it simply gives us an extra
        # connectiong for the load-balancing across the servers.
        self.conns.append(self.connection_class(host, port))

    def remove_host(self, host, port=None):
        if port is None:
            self.hostports = [(h, p) for h, p in self.hostports
                                     if h != host]
        else:
            self.hostports.remove((host, port))

        # Now that the host/port pair has been removed from self.hostports,
        # no connections on this pair will be added in .giveback(). Thus, the
        # existing connections are all that may exist at this time. We'll
        # snapshot the list, and look for offending connections, then try and
        # remove them, being wary that race conditions may remove them before
        # we can remove it.
        for conn in self.conns[:]:
            if conn.host == host and (port is None or conn.port == port):
                try:
                    self.conns.remove(conn)
                except ValueError:
                    # Another thread removed the connection. It won't
                    # be coming back, so we have nothing to do here.
                    pass
                else:
                    # If the connection was still present (no
                    # ValueError), then we should go ahead and close
                    # it down.
                    conn.close()

    # Just in case somebody uses a host/port combo and typos...
    remove_hostport = remove_host

    def take(self):
        if len(self.conns) == 0:
            # RACE: in a multi-threaded environment, a conn might arrive in
            #       self.conns, but... no biggy. If we're bouncing up against
            #       needing a new connection, then we'll just create one.
            return self._new_connection()

        # RACE: self.conns might empty out right now, so we need to protect
        #       our access to it.
        try:
            # round-robin: take from the front, we'll append when it comes back
            return self.conns.pop(0)
        except IndexError:
            return self._new_connection()

    def giveback(self, conn):
        # Connections using a host/port pair that is NOT in self.hostports
        # should be ignored. Likely, remove_host() was called while this
        # connection was borrowed for some work.
        if (conn.host, conn.port) in self.hostports:
            self.conns.append(conn)
        else:
            # Proactively close the connection. The caller won't know whether
            # we put it into our list, or left the connection for the caller
            # to deal with (and close).
            conn.close()

    @contextlib.contextmanager
    def withconn(self):
        conn = self.take()
        try:
            yield conn
        finally:
            self.giveback(conn)

    def _new_connection(self):
        if len(self.hostports) == 0:
            raise NoHostsDefined()

        # Grab the first host/port combo. We'll put this at the end, so that
        # we do a round-robin on the host/port pairs.
        host, port = self.hostports[0]
        conn = self.connection_class(host, port)

        if len(self.hostports) == 1:
            # No rotation needed.
            return conn

        # Be careful about rotating. We want to append before
        # removing, so that we never hit a len==0 race condition
        # (which could prevent the creation of needed connections).
        self.hostports.append((host, port))

        # RACE: another thread may have appended the same host/port
        #   pair. We will add another pair. Each thread will remove
        #   one (either [0], or one that had been appened), resulting
        #   in a correct state of a single pair in the list.
        # RACE: another thread may get the host/port pair from
        #   hostports[0] before we have a chance to remove it. We
        #   don't need precision round-robin behavior; just something
        #   close.
        # RACE: another thread may have removed hostports[0] (which we
        #   are also trying to remove), but it will have placed
        #   another copy at the end before doing so. We have also
        #   added a host/port pair, and will remove one, leaving the
        #   list in a correct state.
        self.hostports.remove((host, port))

        return conn


class Socket(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.sock = None

    def maybe_connect(self):
        if self.sock is None:
            self.sock = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s.connect((self.host, self.port))
        except:
            self.close()
            raise

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None


class FactoryConnectionManager(ConnectionManager):

    def __init__(self, connection_class, hostports=[]):
        self.connection_class = connection_class
        ConnectionManager.__init__(self, hostports)


def cm_using(connection_class):
    return functools.partial(FactoryConnectionManager, connection_class)

HTTPConnectionManager = cm_using(httplib.HTTPConnection)
SocketConnectionManager = cm_using(Socket)


class NoHostsDefined(Exception):
    pass

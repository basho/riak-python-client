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
import contextlib
import functools
import socket
from gevent import monkey

from gevent.queue import Queue

class ConnectionManager(object):

    # Must be constructable with: connection_class(host, port)
    # Must have two attribute: host and port
    # Must have a close() method
    connection_class = None

    def __init__(self, hostports, pool_size):
        # We want a private copy of this list: either to detach the argument
        # default, or to detach from the caller's list.
        if type(hostports) is list:
            if len(hostports) > pool_size:
                raise Exception("pool_size cannot be larger than hostports")
        
        self.pool_size = pool_size
        self.hostports = hostports
        
        queue_limit = pool_size * len(hostports) if type(hostports) is list else pool_size
        self.queue = Queue(queue_limit)
        
        # Patch httplib if we are using that, also patch the sockets
        monkey.patch_all()
        
        if type(self.hostports) is list:
            for host, port in self.hostports:
                for i in range(0, pool_size):
                    self.queue.put(self.connection_class(host, port))
        else:
            for i in range(0, pool_size):
                self.queue.put(self.connection_class(hostports[0], hostports[1]))
    
    def checkout(self):
        """Checkout a connection from the queue - block if we have
        none in the queue till one arrives."""
        
        return self.queue.get()

    def checkin(self, conn):
        """Checkin a connection back into the queue.
        
        If it doesn't exist in the hostports list then
        close it.
        """
        
        # If we are tring to checkin a connection and the pool has
        # already been re-filled, throw away the connection silently
        if self.queue.qsize() >= self.pool_size:
            conn.close()
        
        if ((type(self.hostports) is list) and (conn.host, conn.port) in self.hostports) or (conn.host, conn.port) == self.hostports:
            self.queue.put(conn)
        else:
            # Proactively close the connection. The caller won't know whether
            # we put it into our list, or left the connection for the caller
            # to deal with (and close).
            conn.close()

    @contextlib.contextmanager
    def withconn(self):
        """Context managaer method."""
        conn = self.checkout()
        try:
            yield conn
        finally:
            self.checkin(conn)
    
    def new(self):
        """Create a new connection if we have room for it."""
        
        if self.queue.qsize() >= self.pool_size:
            raise Exception("the queue is already full")
        
        host, port = self.hostports[0] if type(self.hostports) is list else self.hostports
        
        self.queue.put(self.connection_class(host, port))

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

    def __init__(self, connection_class, hostports, pool_size=10):
        self.connection_class = connection_class
        ConnectionManager.__init__(self, hostports, pool_size)


def cm_using(connection_class):
    return functools.partial(FactoryConnectionManager, connection_class)

HTTPConnectionManager = cm_using(httplib.HTTPConnection)
SocketConnectionManager = cm_using(Socket)


class NoHostsDefined(Exception):
    pass

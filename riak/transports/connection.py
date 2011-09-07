#
# ### docco
#

import httplib
import socket
import contextlib


class ConnectionManager(object):

  # Must be constructable with: connection_class(host, port)
  # Must have two attribute: host and port
  # Must have a close() method
  connection_class = None

  def __init__(self, hostports=[]):
    self.hostports = hostports[:]
    self.conns = [ ]

  def add_hostport(self, host, port):
    self.hostports.append((host, port))

  def remove_host(self, host, port=None):
    if port is None:
      self.hostports = [(h, p) for h, p in self.hostports
                        if h != host]
    else:
      self.hostports.remove((host, port))

  # just in case somebody wants a host/port combo and typos...
  remove_hostport = remove_host

  def take(self):
    if len(self.conns) == 0:
      # RACE: in a multi-threaded environment, a conn might arrive in
      #   self.conns, but... no biggy. If we're bouncing up against
      #   needing a new connection, then we'll just create one.
      return self._new_connection()

    # RACE: self.conns might empty out right now, so we need to protect
    #   our access to it.
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

    # Be careful about rotating. We want to append before removing, so that
    # we never hit a len==0 race condition.
    self.hostports.append((host, port))

    # RACE: another thread may have appended the same host/port pair. We
    #   will add another pair. Each thread will remove one, resulting in
    #   a correct state of a single pair in the list.
    # RACE: another thread may get the host/port pair from hostports[0]
    #   before we have a chance to remove it. We don't need precision
    #   round-robin behavior; just something close.
    # RACE: another thread may have removed hostports[0], but it will have
    #   placed another copy at the end. We have added a host/port pair, and
    #   will remove one, leaving the list in a correct state.
    self.hostports.remove((host, port))

    return conn


class HTTPConnectionManager(ConnectionManager):
  connection_class = httplib.HTTPConnection


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


class SocketConnectionManager(ConnectionManager):
  connection_class = Socket


class NoHostsDefined(Exception):
  pass

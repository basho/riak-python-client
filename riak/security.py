"""
Copyright 2014 Basho Technologies, Inc.

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

import OpenSSL.SSL
import httplib
import socket
import select
import string
import datetime
from riak import RiakError
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

OPENSSL_VERSION_101G = 268439679
OPENSSL_VERSION_101 = 1000*1000*1 + 1000*0 + 1
OPENSSL_VERSION_NUM_POS = 1
OPENSSL_VERSION_DAY_POS = 4
OPENSSL_VERSION_MON_POS = 3
OPENSSL_VERSION_YEAR_POS = 7
ssldate = datetime.date(2014, 4, 1)
sslver = OpenSSL.SSL.OPENSSL_VERSION_NUMBER
# Be sure to use at least OpenSSL 1.0.1g
if (sslver < OPENSSL_VERSION_101G):
    too_old = False
    # Check the build date on older versions
    verstring = OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_VERSION)
    versions = string.split(verstring)
    # Convert version string to integer
    verdots = string.split(versions[OPENSSL_VERSION_NUM_POS], '.')
    if len(verdots) == 3:
        verint = 1000 * 1000 * verdots[0] + 1000 * verdots[1] + \
            verdots[2].translate(None, "abcdefghijklmnopqrstuvwxyz")
        # Is this at least 1.0.1 built after April 2014 (hopefully patched)
        if verint < OPENSSL_VERSION_101:
            too_old = True
        else:
            builtstr = OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_BUILT_ON)
            timestamp = string.split(builtstr)
            import calendar
            calmap = {v: k for k,v in enumerate(calendar.month_abbr)}
            day = int(timestamp[OPENSSL_VERSION_DAY_POS])
            mon = calmap[timestamp[OPENSSL_VERSION_MON_POS]]
            year = int(timestamp[OPENSSL_VERSION_YEAR_POS])
            build = datetime.date(year, mon, day)
            if build < ssldate:
                too_old = True
    else:
        too_old = True
    if too_old:
        raise RuntimeError("Found {0} version, but expected at least "
                           "OpenSSL 1.0.1g".format(verstring))


class SecurityError(RiakError):
    """
    Raised when there is an issue establishing security.
    """
    def __init__(self, message="Security error"):
        super(SecurityError, self).__init__(message)


class SecurityCreds(object):
    def __init__(self,
                 username=None,
                 password=None,
                 cacert_file=None,
                 ssl_version=OpenSSL.SSL.TLSv1_2_METHOD):
        """
        Container class for security-related settings

        :param username: Riak Security username
        :type username: str
        :param password: Riak Security password
        :type password: str
        :param cacert_file: Full path to CA Certificate File
        :type cacert_file: str
        :param ssl_version: OpenSSL security version
        :type ssl_version: int
        """
        self.username = username
        self.password = password
        self.cacert_file = cacert_file
        self.ssl_version = ssl_version


# Inspired by
# http://code.activestate.com/recipes/577548-https-httplib-client-connection-with-certificate-v/
class RiakHTTPSConnection(httplib.HTTPSConnection):
    def __init__(self,
                 host,
                 port,
                 credentials,
                 key_file=None,
                 cert_file=None,
                 timeout=None):
        """
        Class to make a HTTPS connection,
        with support for full client-based SSL Authentication

        :param host: Riak host name
        :type host: str
        :param port: Riak host port number
        :type port: int
        :param credentials: Security Credential settings
        :type  credentials: SecurityCreds
        :param key_file: PEM formatted file that contains your private key
        :type key_file: str
        :param cert_file: PEM formatted certificate chain file
        :type key_file: str
        :param timeout: Number of seconds before timing out
        :type timeout: int
        """
        httplib.HTTPSConnection.__init__(self,
                                         host,
                                         port,
                                         key_file=key_file,
                                         cert_file=cert_file)
        self.key_file = key_file
        self.cert_file = cert_file
        self.credentials = credentials
        self.timeout = timeout

    def connect(self):
        """
        Connect to a host on a given (SSL) port using PyOpenSSL.
        """
        sock = socket.create_connection((self.host, self.port), self.timeout)
        ssl_ctx = OpenSSL.SSL.Context(self.credentials.ssl_version)
        ssl_ctx.load_verify_locations(self.credentials.cacert_file)

        # attempt to upgrade the socket to SSL
        cxn = OpenSSL.SSL.Connection(ssl_ctx, sock)
        cxn.set_connect_state()
        while True:
            try:
                cxn.do_handshake()
            except OpenSSL.SSL.WantReadError:
                select.select([sock], [], [])
                continue
            except OpenSSL.SSL.Error as e:
                raise SecurityError('bad handshake - ' + str(e))
            break

        self.sock = RiakWrappedSocket(cxn, sock)


# Inspired by
# https://github.com/shazow/urllib3/blob/master/urllib3/contrib/pyopenssl.py
class RiakWrappedSocket(object):
    def __init__(self, connection, socket):
        """
        API-compatibility wrapper for Python OpenSSL's Connection-class.

        :param connection: OpenSSL connection
        :type connection: OpenSSL.SSL.Connection
        :param socket: Underlying already connected socket
        :type socket: socket
        """
        self.connection = connection
        self.socket = socket

    def fileno(self):
        return self.socket.fileno()

    def makefile(self, mode, bufsize=-1):
        return fileobject(self.connection, mode, bufsize)

    def settimeout(self, timeout):
        return self.socket.settimeout(timeout)

    def sendall(self, data):
        # SSL seems to need bytes, so force the data to byte encoding
        return self.connection.sendall(bytes(data))

    def close(self):
        return self.connection.shutdown()


# Blatantly Stolen from
# https://github.com/shazow/urllib3/blob/master/urllib3/contrib/pyopenssl.py
# which is basically a port of the `socket._fileobject` class
class fileobject(socket._fileobject):
    """
    Extension of the socket module's fileobject to use PyOpenSSL.
    """

    def read(self, size=-1):
        # Use max, disallow tiny reads in a loop as they are very inefficient.
        # We never leave read() with any leftover data from a new recv() call
        # in our internal buffer.
        rbufsize = max(self._rbufsize, self.default_bufsize)
        # Our use of StringIO rather than lists of string objects returned by
        # recv() minimizes memory usage and fragmentation that occurs when
        # rbufsize is large compared to the typical return value of recv().
        buf = self._rbuf
        buf.seek(0, 2)  # seek end
        if size < 0:
            # Read until EOF
            self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
            while True:
                try:
                    data = self._sock.recv(rbufsize)
                except OpenSSL.SSL.WantReadError:
                    continue
                if not data:
                    break
                buf.write(data)
            return buf.getvalue()
        else:
            # Read until size bytes or EOF seen, whichever comes first
            buf_len = buf.tell()
            if buf_len >= size:
                # Already have size bytes in our buffer?  Extract and return.
                buf.seek(0)
                rv = buf.read(size)
                self._rbuf = StringIO()
                self._rbuf.write(buf.read())
                return rv

            self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
            while True:
                left = size - buf_len
                # recv() will malloc the amount of memory given as its
                # parameter even though it often returns much less data
                # than that.  The returned data string is short lived
                # as we copy it into a StringIO and free it.  This avoids
                # fragmentation issues on many platforms.
                try:
                    data = self._sock.recv(left)
                except OpenSSL.SSL.WantReadError:
                    continue
                if not data:
                    break
                n = len(data)
                if n == size and not buf_len:
                    # Shortcut.  Avoid buffer data copies when:
                    # - We have no data in our buffer.
                    # AND
                    # - Our call to recv returned exactly the
                    #   number of bytes we were asked to read.
                    return data
                if n == left:
                    buf.write(data)
                    # del data  # explicit free
                    break
                assert n <= left, "recv(%d) returned %d bytes" % (left, n)
                buf.write(data)
                buf_len += n
                # del data  # explicit free
                # assert buf_len == buf.tell()
            # Moved del outside of loop to keep pyflakes happy
            if data:
                del data
            return buf.getvalue()

    def readline(self, size=-1):
        data = None
        buf = self._rbuf
        buf.seek(0, 2)  # seek end
        if buf.tell() > 0:
            # check if we already have it in our buffer
            buf.seek(0)
            bline = buf.readline(size)
            if bline.endswith('\n') or len(bline) == size:
                self._rbuf = StringIO()
                self._rbuf.write(buf.read())
                return bline
            del bline
        if size < 0:
            # Read until \n or EOF, whichever comes first
            if self._rbufsize <= 1:
                # Speed up unbuffered case
                buf.seek(0)
                buffers = [buf.read()]
                self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
                data = None
                recv = self._sock.recv
                while True:
                    try:
                        while data != "\n":
                            data = recv(1)
                            if not data:
                                break
                            buffers.append(data)
                    except OpenSSL.SSL.WantReadError:
                        continue
                    break
                return "".join(buffers)

            buf.seek(0, 2)  # seek end
            self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
            while True:
                try:
                    data = self._sock.recv(self._rbufsize)
                except OpenSSL.SSL.WantReadError:
                    continue
                if not data:
                    break
                nl = data.find('\n')
                if nl >= 0:
                    nl += 1
                    buf.write(data[:nl])
                    self._rbuf.write(data[nl:])
                    # del data
                    break
                buf.write(data)
            # Moved del outside of loop to keep pyflakes happy
            if data:
                del data
            return buf.getvalue()
        else:
            # Read until size bytes or \n or EOF seen, whichever comes first
            buf.seek(0, 2)  # seek end
            buf_len = buf.tell()
            if buf_len >= size:
                buf.seek(0)
                rv = buf.read(size)
                self._rbuf = StringIO()
                self._rbuf.write(buf.read())
                return rv
            self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
            while True:
                try:
                    data = self._sock.recv(self._rbufsize)
                except OpenSSL.SSL.WantReadError:
                        continue
                if not data:
                    break
                left = size - buf_len
                # did we just receive a newline?
                nl = data.find('\n', 0, left)
                if nl >= 0:
                    nl += 1
                    # save the excess data to _rbuf
                    self._rbuf.write(data[nl:])
                    if buf_len:
                        buf.write(data[:nl])
                        break
                    else:
                        # Shortcut.  Avoid data copy through buf when returning
                        # a substring of our first recv().
                        return data[:nl]
                n = len(data)
                if n == size and not buf_len:
                    # Shortcut.  Avoid data copy through buf when
                    # returning exactly all of our first recv().
                    return data
                if n >= left:
                    buf.write(data[:left])
                    self._rbuf.write(data[left:])
                    break
                buf.write(data)
                buf_len += n
                # assert buf_len == buf.tell()
            return buf.getvalue()

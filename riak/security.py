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
from OpenSSL import crypto
import string
import datetime
import calendar
from riak import RiakError
from distutils.version import LooseVersion

OPENSSL_VERSION_101G = 268439679
OPENSSL_VERSION_101 = "1.0.1"
OPENSSL_VERSION_NUM_POS = 1
OPENSSL_VERSION_DAY_POS = 4
OPENSSL_VERSION_MON_POS = 3
OPENSSL_VERSION_YEAR_POS = 7
ssldate = datetime.date(2014, 4, 1)
sslver = OpenSSL.SSL.OPENSSL_VERSION_NUMBER
# Be sure to use at least OpenSSL 1.0.1g
if (sslver < OPENSSL_VERSION_101G):
    # Check the build date on older versions
    verstring = OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_VERSION)
    verdots = string.split(verstring)[OPENSSL_VERSION_NUM_POS]
    builtstr = OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_BUILT_ON)
    timestamp = string.split(builtstr)
    calmap = {v: k for k, v in enumerate(calendar.month_abbr)}
    day = int(timestamp[OPENSSL_VERSION_DAY_POS])
    mon = calmap[timestamp[OPENSSL_VERSION_MON_POS]]
    year = int(timestamp[OPENSSL_VERSION_YEAR_POS])
    build = datetime.date(year, mon, day)
    if LooseVersion(verdots) < LooseVersion(OPENSSL_VERSION_101) or \
            build < ssldate:
        raise RuntimeError("Found {0} version, but expected at least "
                           "OpenSSL 1.0.1 built after {1}"
                           .format(verstring, ssldate))


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
                 key_file=None,
                 cert_file=None,
                 cacert_file=None,
                 crl_file=None,
                 ssl_version=OpenSSL.SSL.TLSv1_2_METHOD):
        """
        Container class for security-related settings

        :param username: Riak Security username
        :type username: str
        :param password: Riak Security password
        :type password: str
        :param key_file: Full path to security key file
        :type key_file: str
        :param cert_file: Full path to certificate file
        :type cert_file: str
        :param cacert_file: Full path to CA certificate file
        :type cacert_file: str
        :param crl_file: Full path to revoked certificates file
        :type crl_file: str
        :param ssl_version: OpenSSL security version
        :type ssl_version: int
        """
        self.username = username
        self.password = password
        self.key_file = key_file
        self.cert_file = cert_file
        self.cacert_file = cacert_file
        self.crl_file = crl_file
        self.ssl_version = ssl_version


def check_revoked_cert(ssl_socket, crl_file):
    """
    Determine if the server certificate has been revoked or not.

    :param ssl_socket: Secure SSL socket
    :type ssl_socket: socket
    :param crl_file: Certificate Revocation List file
    :type crl_file: str
    """
    f = open(crl_file, 'r')
    crl = crypto.load_crl(OpenSSL.SSL.FILETYPE_PEM, f.read())
    revs = crl.get_revoked()
    servcert = ssl_socket.get_peer_certificate()
    servserial = servcert.get_serial_number()
    for rev in revs:
        if servserial == long(rev.get_serial(), 16):
            raise RiakError(
                "Server certificate has been revoked")

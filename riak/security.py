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
import warnings
from riak import RiakError

OPENSSL_VERSION_101G = 268439679
sslver = OpenSSL.SSL.OPENSSL_VERSION_NUMBER
# Be sure to use at least OpenSSL 1.0.1g
if (sslver < OPENSSL_VERSION_101G) or \
        not hasattr(OpenSSL.SSL, 'TLSv1_2_METHOD'):
    verstring = OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_VERSION)
    msg = "Found {0} version, but expected at least OpenSSL 1.0.1g.  " \
          "Security may not support TLS 1.2.".format(verstring)
    warnings.warn(msg, UserWarning)


class SecurityError(RiakError):
    """
    Raised when there is an issue establishing security.
    """
    def __init__(self, message="Security error"):
        super(SecurityError, self).__init__(message)


class SecurityCreds:
    def __init__(self,
                 username=None,
                 password=None,
                 pkey_file=None,
                 pkey=None,
                 cert_file=None,
                 cert=None,
                 cacert_file=None,
                 cacert=None,
                 crl_file=None,
                 crl=None,
                 ciphers=None,
                 ssl_version=OpenSSL.SSL.TLSv1_2_METHOD):
        """
        Container class for security-related settings

        :param username: Riak Security username
        :type username: str
        :param password: Riak Security password
        :type password: str
        :param pkey_file: Full path to security key file
        :type pkey_file: str
        :param key: Loaded security key file
        :type key: :class:`~OpenSSL.crypto.PKey`
        :param cert_file: Full path to certificate file
        :type cert_file: str
        :param cert: Loaded client certificate
        :type cert: :class:`~OpenSSL.crypto.X509`
        :param cacert_file: List of full paths to CA certificate files
        :type cacert_file: str
        :param cacert: Loaded CA certificates
        :type cacert: list of :class:`~OpenSSL.crypto.X509`
        :param crl_file: Full path to revoked certificates file
        :type crl_file: str
        :param crl: Loaded revoked certificates list
        :type crl: :class:`~OpenSSL.crypto.CRL`
        :param ciphers: List of supported SSL ciphers
        :type ciphers: str
        :param ssl_version: OpenSSL security version
        :type ssl_version: int
        """
        self._username = username
        self._password = password
        self._pkey_file = pkey_file
        self._pkey = pkey
        self._cert_file = cert_file
        self._cert = cert
        self._cacert_file = cacert_file
        self._cacert = cacert
        self._crl_file = crl_file
        self._crl = crl
        self._ciphers = ciphers
        self._ssl_version = ssl_version

    @property
    def username(self):
        """
        Riak Username
        """
        return self._username

    @property
    def password(self):
        """
        Riak Password
        """
        return self._password

    @property
    def pkey(self):
        """
        Private key
        """
        return self._cached_cert('_pkey', crypto.load_privatekey)

    @property
    def cert(self):
        """
        Client Certificate
        """
        return self._cached_cert('_cert', crypto.load_certificate)

    @property
    def cacert(self):
        """
        Certifying Authority Certificate
        """
        return self._cached_cert('_cacert', crypto.load_certificate)

    @property
    def crl(self):
        """
        Certificate Revocation List
        """
        return self._cached_cert('_crl', crypto.load_crl)

    @property
    def ciphers(self):
        """
        Colon-delimited list of supported ciphers
        """
        return self._ciphers

    @property
    def ssl_version(self):
        """
        SSL Encryption Version
        """
        return self._ssl_version

    def _cached_cert(self, key, loader):
        # If the key is associated with a file, then lazily load and cache it
        key_file = getattr(self, key + "_file")
        if (getattr(self, key) is None) and (key_file is not None):
            cert_list = []
            # The _file may be a list of files
            if not isinstance(key_file, list):
                key_file = [key_file]
            for filename in key_file:
                with open(filename, 'r') as f:
                    cert_list.append(loader(OpenSSL.SSL.FILETYPE_PEM,
                                            f.read()))
            # If it is not a list, just store the first element
            if len(cert_list) == 1:
                cert_list = cert_list[0]
            setattr(self, key, cert_list)
        return getattr(self, key)

    def has_credential(self, key):
        """
        True if a credential or filename value has been supplied
        """
        internal_key = "_" + key
        return (getattr(self, internal_key) is not None) or \
            (getattr(self, internal_key + "_file") is not None)

    def check_revoked_cert(self, ssl_socket):
        if not self.has_credential('crl'):
            return True

        servcert = ssl_socket.get_peer_certificate()
        servserial = servcert.get_serial_number()
        for rev in self.crl.get_revoked():
            if servserial == long(rev.get_serial(), 16):
                raise SecurityError("Server certificate has been revoked")

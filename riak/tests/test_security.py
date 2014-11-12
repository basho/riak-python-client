# -*- coding: utf-8 -*-
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

import platform
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
from riak.tests import RUN_SECURITY, SECURITY_USER, SECURITY_PASSWD, \
    SECURITY_CACERT, SECURITY_KEY, SECURITY_CERT, SECURITY_REVOKED, \
    SECURITY_CERT_USER, SECURITY_CERT_PASSWD, SECURITY_BAD_CERT
from riak.security import SecurityCreds
from six import PY3


class SecurityTests(object):
    @unittest.skipIf(RUN_SECURITY, 'RUN_SECURITY is set')
    def test_security_disabled(self):
        creds = SecurityCreds(username=SECURITY_USER,
                              password=SECURITY_PASSWD,
                              cacert_file=SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        myBucket = client.bucket('test')
        val1 = "foobar"
        key1 = myBucket.new('x', data=val1)
        with self.assertRaises(Exception):
            key1.store()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_basic_connection(self):
        myBucket = self.client.bucket('test')
        val1 = "foobar"
        key1 = myBucket.new('x', data=val1)
        key1.store()
        myBucket.get('x')

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_bad_user(self):
        creds = SecurityCreds(username='foo', password=SECURITY_PASSWD,
                              cacert_file=SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_bad_password(self):
        creds = SecurityCreds(username=SECURITY_USER, password='foo',
                              cacert_file=SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_invalid_cert(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD,
                              cacert_file='/tmp/foo')
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_password_without_cacert(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD)
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            myBucket = client.bucket('test')
            val1 = "foobar"
            key1 = myBucket.new('x', data=val1)
            key1.store()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_cert_authentication(self):
        creds = SecurityCreds(username=SECURITY_CERT_USER,
                              password=SECURITY_CERT_PASSWD,
                              cert_file=SECURITY_CERT,
                              pkey_file=SECURITY_KEY,
                              cacert_file=SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        myBucket = client.bucket('test')
        val1 = "foobar2"
        key1 = myBucket.new('x', data=val1)
        # Certificate Authentication is currently only supported
        # by Protocol Buffers
        if self.protocol == 'pbc':
            key1.store()
            myBucket.get('x')
        else:
            with self.assertRaises(Exception):
                key1.store()
                myBucket.get('x')

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_revoked_cert(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD,
                              cacert_file=SECURITY_CACERT,
                              crl_file=SECURITY_REVOKED)
        # Curenly Python 3.x native CRL doesn't seem to work
        # as advertised
        if PY3:
            return
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_bad_ca_cert(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD,
                              cacert_file=SECURITY_BAD_CERT)
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_ciphers(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD,
                              cacert_file=SECURITY_CACERT,
                              ciphers='DHE-RSA-AES256-SHA')
        client = self.create_client(credentials=creds)
        myBucket = client.bucket('test')
        val1 = "foobar"
        key1 = myBucket.new('x', data=val1)
        key1.store()
        myBucket.get('x')

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is not set')
    def test_security_bad_ciphers(self):
        creds = SecurityCreds(username=SECURITY_USER, password=SECURITY_PASSWD,
                              cacert_file=SECURITY_CACERT,
                              ciphers='ECDHE-RSA-AES256-GCM-SHA384')
        client = self.create_client(credentials=creds)
        with self.assertRaises(Exception):
            client.get_buckets()

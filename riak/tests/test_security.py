# -*- coding: utf-8 -*-
import platform
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
from riak.tests import RUN_SECURITY, SECURITY_USER, SECURITY_PASSWD, \
    SECURITY_CACERT
from riak.security import SecurityCreds


class SecurityTests(object):
    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is undefined')
    def test_security_basic_connection(self):
        myBucket = self.client.bucket('test')
        val1 = "foobar"
        key1 = myBucket.new('x', data=val1)
        key1.store()
        fetched1 = myBucket.get('x')
        for sibling in fetched1.siblings:
            # print sibling.etag + " " +  sibling.data
            pass

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is undefined')
    @unittest.expectedFailure
    def test_security_bad_user(self):
        creds = SecurityCreds('foo', SECURITY_PASSWD, SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is undefined')
    @unittest.expectedFailure
    def test_security_bad_password(self):
        creds = SecurityCreds(SECURITY_USER, 'foo', SECURITY_CACERT)
        client = self.create_client(credentials=creds)
        client.get_buckets()

    @unittest.skipUnless(RUN_SECURITY, 'RUN_SECURITY is undefined')
    @unittest.expectedFailure
    def test_security_missing_cert(self):
        creds = SecurityCreds(SECURITY_USER, SECURITY_PASSWD, '/tmp/foo')
        client = self.create_client(credentials=creds)
        client.get_buckets()

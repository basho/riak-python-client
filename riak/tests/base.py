# -*- coding: utf-8 -*-
import logging
import os
import random
import sys

from riak.client import RiakClient
from riak.tests import HOST, PROTOCOL, PB_PORT, HTTP_PORT, SECURITY_CREDS


class IntegrationTestBase(object):

    host = None
    pb_port = None
    http_port = None
    credentials = None

    @staticmethod
    def randint():
        return random.randint(1, 999999)

    @staticmethod
    def randname(length=12):
        out = ''
        for i in range(length):
            out += chr(random.randint(ord('a'), ord('z')))
        return out

    @classmethod
    def create_client(cls, host=None, http_port=None, pb_port=None,
                      protocol=None, credentials=None, **client_args):
        host = host or HOST
        http_port = http_port or HTTP_PORT
        pb_port = pb_port or PB_PORT

        if protocol is None:
            if hasattr(cls, 'protocol') and (cls.protocol is not None):
                protocol = cls.protocol
            else:
                protocol = PROTOCOL

        cls.protocol = protocol

        credentials = credentials or SECURITY_CREDS

        if hasattr(cls, 'logging_enabled') and cls.logging_enabled:
            cls.logger.debug("RiakClient(protocol='%s', host='%s', " +
                             "pb_port='%d', http_port='%d', " +
                             "credentials='%s', client_args='%s')",
                             protocol,
                             host,
                             pb_port,
                             http_port,
                             credentials,
                             client_args)

        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          credentials=credentials,
                          pb_port=pb_port, **client_args)

    @classmethod
    def setUpClass(cls):
        cls.logging_enabled = False
        distutils_debug = os.environ.get('DISTUTILS_DEBUG', '0')
        if distutils_debug == '1':
            cls.logging_enabled = True
            cls.logger = logging.getLogger()
            cls.logger.level = logging.DEBUG
            cls.logging_stream_handler = logging.StreamHandler(sys.stdout)
            cls.logger.addHandler(cls.logging_stream_handler)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'logging_enabled') and cls.logging_enabled:
            cls.logger.removeHandler(cls.logging_stream_handler)
            cls.logging_enabled = False

    def setUp(self):
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.client = self.create_client()

    def tearDown(self):
        self.client.close()

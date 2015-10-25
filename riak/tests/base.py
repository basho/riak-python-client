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

    def create_client(self, host=None, http_port=None, pb_port=None,
                      protocol=None, credentials=None,
                      **client_args):
        host = host or self.host or HOST
        http_port = http_port or self.http_port or HTTP_PORT
        pb_port = pb_port or self.pb_port or PB_PORT

        if protocol is None:
            if hasattr(self, 'protocol') and (self.protocol is not None):
                protocol = self.protocol
            else:
                protocol = PROTOCOL

        self.protocol = protocol

        credentials = credentials or SECURITY_CREDS

        if self.logging_enabled:
            self.logger.debug("RiakClient(protocol='%s', host='%s', pb_port='%d', http_port='%d', credentials='%s', client_args='%s')", protocol, host, pb_port, http_port, credentials, client_args)

        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          credentials=credentials,
                          pb_port=pb_port, **client_args)

    def setUp(self):
        self.logging_enabled = False
        distutils_debug = os.environ.get('DISTUTILS_DEBUG', '0')
        if distutils_debug == '1':
            self.logging_enabled = True
            self.logger = logging.getLogger()
            self.logger.level = logging.DEBUG
            self.logging_stream_handler = logging.StreamHandler(sys.stdout)
            self.logger.addHandler(self.logging_stream_handler)

        self.table_name = 'GeoCheckin'
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.credentials = SECURITY_CREDS
        self.client = self.create_client()

    def tearDown(self):
        if self.logging_enabled:
            self.logger.removeHandler(self.logging_stream_handler)


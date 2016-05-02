# -*- coding: utf-8 -*-
import logging
import random

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
                      protocol=None, credentials=None, **kwargs):
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

        if hasattr(cls, 'client_options'):
            kwargs.update(cls.client_options)

        logger = logging.getLogger()
        logger.debug("RiakClient(protocol='%s', host='%s', pb_port='%d', "
                     "http_port='%d', credentials='%s', kwargs='%s')",
                     protocol, host, pb_port, http_port, credentials, kwargs)

        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          credentials=credentials,
                          pb_port=pb_port,
                          **kwargs)

    def setUp(self):
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.client = self.create_client()

    def tearDown(self):
        self.client.close()

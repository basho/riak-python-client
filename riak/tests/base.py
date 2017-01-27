# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import logging
import random
import riak

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
        riak.disable_list_exceptions = True
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.client = self.create_client()

    def tearDown(self):
        riak.disable_list_exceptions = False
        self.client.close()

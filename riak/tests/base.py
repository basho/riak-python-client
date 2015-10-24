# -*- coding: utf-8 -*-
import random

from riak.client import RiakClient
from riak.tests import HOST, PROTOCOL,PB_PORT, HTTP_PORT, SECURITY_CREDS

testrun_search_bucket = 'searchbucket'
testrun_props_bucket = 'propsbucket'
testrun_sibs_bucket = 'sibsbucket'

def setUpModule():

    c = RiakClient(host=PB_HOST, http_port=HTTP_PORT,
                   pb_port=PB_PORT, credentials=SECURITY_CREDS)

    c.bucket(testrun_sibs_bucket).allow_mult = True

    if (not SKIP_SEARCH and not RUN_YZ):
        b = c.bucket(testrun_search_bucket)
        b.enable_search()


def tearDownModule():
    c = RiakClient(host=HTTP_HOST, http_port=HTTP_PORT,
                   pb_port=PB_PORT, credentials=SECURITY_CREDS)

    c.bucket(testrun_sibs_bucket).clear_properties()
    c.bucket(testrun_props_bucket).clear_properties()

    if not SKIP_SEARCH and not RUN_YZ:
        b = c.bucket(testrun_search_bucket)
        b.clear_properties()

class BaseTestCase(object):

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

        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          credentials=credentials,
                          pb_port=pb_port, **client_args)

    def setUp(self):
        self.table_name = 'GeoCheckin'
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.search_bucket = testrun_search_bucket
        self.sibs_bucket = testrun_sibs_bucket
        self.props_bucket = testrun_props_bucket
        # self.yz = testrun_yz
        # self.yz_index = testrun_yz_index
        # self.yz_mr = testrun_yz_mr
        self.credentials = SECURITY_CREDS
        self.client = self.create_client()


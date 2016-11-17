import logging
import os
import socket
import sys

from riak.test_server import TestServer
from riak.security import SecurityCreds

USE_TEST_SERVER = int(os.environ.get('USE_TEST_SERVER', '0'))
if USE_TEST_SERVER:
    HTTP_PORT = 9000
    PB_PORT = 9002
    test_server = TestServer()
    test_server.cleanup()
    test_server.prepare()
    test_server.start()

try:
    __import__('riak.pb')
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False


def hostname_resolves(hostname):
    try:
        socket.gethostbyname(hostname)
        return 1
    except socket.error:
        return 0


distutils_debug = os.environ.get('DISTUTILS_DEBUG', '0')
if distutils_debug == '1':
    logger = logging.getLogger()
    logger.level = logging.DEBUG
    logger.addHandler(logging.StreamHandler(sys.stdout))

HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')

PROTOCOL = os.environ.get('RIAK_TEST_PROTOCOL', 'pbc')

PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))

HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))

# these ports are used to simulate errors, there shouldn't
# be anything listening on either port.
DUMMY_HTTP_PORT = int(os.environ.get('DUMMY_HTTP_PORT', '1023'))
DUMMY_PB_PORT = int(os.environ.get('DUMMY_PB_PORT', '1022'))

RUN_BTYPES = int(os.environ.get('RUN_BTYPES', '0'))
RUN_DATATYPES = int(os.environ.get('RUN_DATATYPES', '0'))
RUN_CLIENT = int(os.environ.get('RUN_CLIENT', '0'))
RUN_INDEXES = int(os.environ.get('RUN_INDEXES', '0'))
RUN_KV = int(os.environ.get('RUN_KV', '0'))
RUN_MAPREDUCE = int(os.environ.get('RUN_MAPREDUCE', '0'))
RUN_POOL = int(os.environ.get('RUN_POOL', '0'))
RUN_RESOLVE = int(os.environ.get('RUN_RESOLVE', '0'))
RUN_SEARCH = int(os.environ.get('RUN_SEARCH', '0'))
RUN_TIMESERIES = int(os.environ.get('RUN_TIMESERIES', '0'))
RUN_YZ = int(os.environ.get('RUN_YZ', '0'))

if PROTOCOL != 'pbc':
    RUN_TIMESERIES = 0

RUN_SECURITY = int(os.environ.get('RUN_SECURITY', '0'))
if RUN_SECURITY:
    h = 'riak-test'
    if hostname_resolves(h):
        HOST = PB_HOST = HTTP_HOST = h
    else:
        raise AssertionError(
                'RUN_SECURITY requires that the host name' +
                ' "riak-test" resolves to the IP address of a Riak node' +
                ' with security enabled.')

SECURITY_USER = os.environ.get('RIAK_TEST_SECURITY_USER', 'riakpass')
SECURITY_PASSWD = os.environ.get('RIAK_TEST_SECURITY_PASSWD', 'Test1234')

SECURITY_CACERT = os.environ.get('RIAK_TEST_SECURITY_CACERT',
                                 'tools/test-ca/certs/cacert.pem')
SECURITY_REVOKED = os.environ.get('RIAK_TEST_SECURITY_REVOKED',
                                  'tools/test-ca/crl/crl.pem')
SECURITY_BAD_CERT = os.environ.get('RIAK_TEST_SECURITY_BAD_CERT',
                                   'tools/test-ca/certs/badcert.pem')
# Certificate-based Authentication only supported by PBC
SECURITY_KEY = os.environ.get(
        'RIAK_TEST_SECURITY_KEY',
        'tools/test-ca/private/riakuser-client-cert-key.pem')
SECURITY_CERT = os.environ.get('RIAK_TEST_SECURITY_CERT',
                               'tools/test-ca/certs/riakuser-client-cert.pem')
SECURITY_CERT_USER = os.environ.get('RIAK_TEST_SECURITY_CERT_USER',
                                    'riakuser')

SECURITY_CIPHERS = 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:' + \
        'DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:' + \
        'AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA'

SECURITY_CREDS = None
if RUN_SECURITY:
    SECURITY_CREDS = SecurityCreds(username=SECURITY_USER,
                                   password=SECURITY_PASSWD,
                                   cacert_file=SECURITY_CACERT,
                                   ciphers=SECURITY_CIPHERS)

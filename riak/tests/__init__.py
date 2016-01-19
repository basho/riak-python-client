import os
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

RUN_SEARCH = int(os.environ.get('RUN_SEARCH', '0'))
RUN_YZ = int(os.environ.get('RUN_YZ', '0'))

RUN_INDEXES = int(os.environ.get('RUN_INDEXES', '0'))

RUN_TIMESERIES = int(os.environ.get('RUN_TIMESERIES', '0'))

RUN_POOL = int(os.environ.get('RUN_POOL', '0'))
RUN_RESOLVE = int(os.environ.get('RUN_RESOLVE', '1'))
RUN_BTYPES = int(os.environ.get('RUN_BTYPES', '1'))
RUN_DATATYPES = int(os.environ.get('RUN_DATATYPES', '1'))

RUN_SECURITY = int(os.environ.get('RUN_SECURITY', '0'))
SECURITY_USER = os.environ.get('RIAK_TEST_SECURITY_USER', 'testuser')
SECURITY_PASSWD = os.environ.get('RIAK_TEST_SECURITY_PASSWD', 'testpassword')
SECURITY_CACERT = os.environ.get('RIAK_TEST_SECURITY_CACERT',
                                 'riak/tests/resources/ca.crt')
SECURITY_REVOKED = os.environ.get('RIAK_TEST_SECURITY_REVOKED',
                                  'riak/tests/resources/server.crl')
SECURITY_BAD_CERT = os.environ.get('RIAK_TEST_SECURITY_BAD_CERT',
                                   'riak/tests/resources/bad_ca.crt')
# Certificate-based Authentication only supported by PBC
# N.B., username and password must both still be supplied
SECURITY_KEY = os.environ.get('RIAK_TEST_SECURITY_KEY',
                              'riak/tests/resources/client.key')
SECURITY_CERT = os.environ.get('RIAK_TEST_SECURITY_CERT',
                               'riak/tests/resources/client.crt')
SECURITY_CERT_USER = os.environ.get('RIAK_TEST_SECURITY_CERT_USER',
                                    'certuser')
SECURITY_CERT_PASSWD = os.environ.get('RIAK_TEST_SECURITY_CERT_PASSWD',
                                      'certpass')

SECURITY_CIPHERS = 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:' + \
        'DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:' + \
        'AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA'

SECURITY_CREDS = None
if RUN_SECURITY:
    SECURITY_CREDS = SecurityCreds(username=SECURITY_USER,
                                   password=SECURITY_PASSWD,
                                   cacert_file=SECURITY_CACERT,
                                   ciphers=SECURITY_CIPHERS)

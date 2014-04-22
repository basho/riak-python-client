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
    __import__('riak_pb')
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False

HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')

PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))

HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))

# these ports are used to simulate errors, there shouldn't
# be anything listening on either port.
DUMMY_HTTP_PORT = int(os.environ.get('DUMMY_HTTP_PORT', '1023'))
DUMMY_PB_PORT = int(os.environ.get('DUMMY_PB_PORT', '1022'))


SKIP_SEARCH = int(os.environ.get('SKIP_SEARCH', '1'))
RUN_YZ = int(os.environ.get('RUN_YZ', '0'))

SKIP_INDEXES = int(os.environ.get('SKIP_INDEXES', '1'))

SKIP_POOL = os.environ.get('SKIP_POOL')
SKIP_RESOLVE = int(os.environ.get('SKIP_RESOLVE', '0'))
SKIP_BTYPES = int(os.environ.get('SKIP_BTYPES', '0'))

RUN_SECURITY = int(os.environ.get('RUN_SECURITY', '0'))
SECURITY_USER = os.environ.get('RIAK_TEST_SECURITY_USER', 'testuser')
SECURITY_PASSWD = os.environ.get('RIAK_TEST_SECURITY_PASSWD', 'testpassword')
SECURITY_CACERT = os.environ.get('RIAK_TEST_SECURITY_CACERT',
                                 '/tmp/cacert.pem')

SECURITY_CREDS = None
if RUN_SECURITY:
    SECURITY_CREDS = SecurityCreds(username=SECURITY_USER,
                                   password=SECURITY_PASSWD,
                                   cacert_file=SECURITY_CACERT)

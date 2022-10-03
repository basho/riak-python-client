import unittest

from riak.transports.tcp import TcpTransport
from riak.node import RiakNode


class TransportsTCPTransportTests(unittest.TestCase):
    def test__server_version(self):
        node = RiakNode()
        transport = TcpTransport(node=node)
        transport.get_server_info = lambda: {'server_version': '2.1.2'}
        self.assertEqual('2.1.2', transport._server_version())
        transport.get_server_info = lambda: {'server_version': '3.0'}
        self.assertEqual('3.0', transport._server_version())

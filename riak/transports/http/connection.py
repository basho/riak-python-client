"""
Copyright 2012 Basho Technologies, Inc.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import httplib


class RiakHttpConnection(object):
    """
    Connection and low-level request methods for RiakHttpTransport.
    """

    def _request(self, method, uri, headers={}, body='', stream=False):
        """
        Given a Method, URL, Headers, and Body, perform and HTTP
        request, and return a 3-tuple containing the response status,
        response headers (as httplib.HTTPMessage), and response body.
        """
        response = None
        headers.setdefault('Accept',
                           'multipart/mixed, application/json, */*;q=0.5')
        try:
            self._connection.request(method, uri, body, headers)
            response = self._connection.getresponse()

            if stream:
                # The caller is responsible for fully reading the
                # response and closing it when streaming.
                response_body = response
            else:
                response_body = response.read()
        finally:
            if response and not stream:
                response.close()

        return response.status, response.msg, response_body

    def _connect(self):
        self._connection = self._connection_class(self._node.host,
                                                  self._node.http_port)
        # Forces the population of stats and resources before any
        # other requests are made.
        self.server_version

    def close(self):
        """
        Closes the underlying HTTP connection.
        """
        try:
            self._connection.close()
        except httplib.NotConnected:
            pass

    # These are set by the RiakHttpTransport initializer
    _connection_class = httplib.HTTPConnection
    _node = None

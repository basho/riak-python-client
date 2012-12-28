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

    def GET(self, uri, headers={}):
        return self._request('GET', uri, headers, '')

    def POST(self, uri, headers={}, body=''):
        return self._request('POST', uri, headers, body)

    def PUT(self, uri, headers={}, body=''):
        return self._request('PUT', uri, headers, body)

    def DELETE(self, uri, headers={}, body=''):
        return self.request('DELETE', uri, headers, body)

    def HEAD(self, uri, headers={}):
        return self.request('HEAD', uri, headers, '')

    def _request(self, method, uri, headers={}, body=''):
        """
        Given a Method, URL, Headers, and Body, perform and HTTP request,
        and return a 2-tuple containing a dictionary of response headers
        and the response body.
        """
        try:
            self._connection.request(method, uri, body, headers)
            response = self._connection.getresponse()

            response_headers = {'http_code': response.status}
            for (key, value) in response.getheaders():
                response_headers[key.lower()] = value

            # TODO: Support streaming responses
            response_body = response.read()
        finally:
            response.close()

        return response_headers, response_body

    def _connect(self):
        self._connection = self._connection_class(self._node.host,
                                                  self._node.http_port)

    def close(self):
        try:
            self._connection.close()
        except httplib.NotConnected:
            pass

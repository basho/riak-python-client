"""
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

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
from __future__ import with_statement

import urllib, re, csv
from cStringIO import StringIO
import httplib
import socket
import errno
try:
    import json
except ImportError:
    import simplejson as json

from transport import RiakTransport
from riak.metadata import *
from riak.mapreduce import RiakLink
from riak import RiakError
from riak.riak_index_entry import RiakIndexEntry
from riak.multidict import MultiDict
from connection import HTTPConnectionManager
import riak.util

MAX_LINK_HEADER_SIZE = 8192 - 8 # substract length of "Link: " header string and newline

class RiakHttpTransport(RiakTransport) :
    """
    The RiakHttpTransport object holds information necessary to connect to
    Riak. The Riak API uses HTTP, so there is no persistent
    connection, and the RiakClient object is extremely lightweight.
    """

    # We're using the new RiakTransport API
    api = 2

    # The ConnectionManager class that this transport prefers.
    default_cm = HTTPConnectionManager

    # How many times to retry a request
    RETRY_COUNT = 3

    def __init__(self, cm,
                 prefix='riak', mapred_prefix='mapred', client_id=None,
                 **unused_options):
        """
        Construct a new RiakClient object.
        @param string host - Hostname or IP address (default '127.0.0.1')
        @param int port - Port number (default 8098)
        @param string prefix - Interface prefix (default 'riak')
        @param string mapred_prefix - MapReduce prefix (default 'mapred')
        @param string client_id - client id to use for vector clocks
        """
        super(RiakHttpTransport, self).__init__()
        self._conns = cm
        self._prefix = prefix
        self._mapred_prefix = mapred_prefix
        self._client_id = client_id
        if not self._client_id:
            self._client_id = self.make_random_client_id()

    def __copy__(self):
        ### not implemented right now
        raise Exception('not implemented')
        ### we don't have _host and _port. will fix after some refactoring...
        return RiakHttpTransport(self._host, self._port, self._prefix,
                                 self._mapred_prefix)

    def set_client_id(self, client_id):
        self._client_id = client_id

    def get_client_id(self):
        return self._client_id

    def ping(self) :
        """
        Check server is alive over HTTP
        """
        response = self.http_request('GET', '/ping')
        return(response is not None) and (response[1] == 'OK')


    def get(self, robj, r = None, pr = None, vtag = None) :
        """
        Get a bucket/key from the server
        """
        params = {'r' : r, 'pr': pr}
        if vtag is not None:
            params['vtag'] = vtag
        url = self.build_rest_path(robj.get_bucket(), robj.get_key(),
                                   params=params)
        response = self.http_request('GET', url)
        return self.parse_body(response, [200, 300, 404])

    def put(self, robj, w = None, dw = None, pw = None, return_body = True, if_none_match=False):
        """
        Serialize put request and deserialize response
        """
       # Construct the URL...
        params = {'returnbody' : str(return_body).lower(), 'w' : w, 'dw' : dw, 'pw' : pw }
        url = self.build_rest_path(bucket=robj.get_bucket(), key=robj.get_key(),
                                   params=params)
        headers = self.build_put_headers(robj)
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        return self.do_put(url, headers, content, return_body, key=robj.get_key())

    def do_put(self, url, headers, content, return_body=False, key=None):
        if key is None:
          response = self.http_request('POST', url, headers, content)
        else:
          response = self.http_request('PUT', url, headers, content)

        if return_body:
          return self.parse_body(response, [200, 201, 300])
        else:
          self.check_http_code(response, [204])
          return None

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True, if_none_match=False):
        """Put a new object into the Riak store, returning its (new) key."""
        # Construct the URL...
        params = {'returnbody' : str(return_body).lower(), 'w' : w, 'dw' : dw, 'pw' : pw}
        url = self.build_rest_path(bucket=robj.get_bucket(), params=params)
        headers = self.build_put_headers(robj)
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        response = self.http_request('POST', url, headers, content)
        location = response[0]['location']
        idx = location.rindex('/')
        key = location[idx+1:]
        if return_body:
            vclock, [(metadata, data)] = self.parse_body(response, [201])
            return key, vclock, metadata
        else:
            self.check_http_code(response, [201])
            return key, None, None

    def delete(self, robj, rw=None, r = None, w = None, dw = None, pr = None, pw = None):
        # Construct the URL...
        params = {'rw' : rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw}
        url = self.build_rest_path(robj.get_bucket(), robj.get_key(),
                                   params=params)
        # TODO: Send vclock of robj if it exists
        # Run the operation..
        response = self.http_request('DELETE', url)
        self.check_http_code(response, [204, 404])
        return self


    def get_keys(self, bucket):
        params = {'props' : 'True', 'keys' : 'true'}
        url = self.build_rest_path(bucket, params=params)
        response = self.http_request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = json.loads(encoded_props)
            return props['keys']
        else:
            raise Exception('Error getting bucket properties.')

    def get_buckets(self):
        params = {'buckets': 'true'}
        url = self.build_rest_path(None, params=params)
        response = self.http_request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = json.loads(encoded_props)
            return props['buckets']
        else:
            raise Exception('Error getting buckets.')

    def get_bucket_props(self, bucket):
        # Run the request...
        params = {'props' : 'True', 'keys' : 'False'}
        url = self.build_rest_path(bucket, params=params)
        response = self.http_request('GET', url)

        headers = response[0]
        encoded_props = response[1]
        if headers['http_code'] == 200:
            props = json.loads(encoded_props)
            return props['props']
        else:
            raise Exception('Error getting bucket properties.')


    def set_bucket_props(self, bucket, props):
        """
        Set the properties on the bucket object given
        """
        url = self.build_rest_path(bucket)
        headers = {'Content-Type' : 'application/json'}
        content = json.dumps({'props' : props})

        # Run the request...
        response = self.http_request('PUT', url, headers, content)

        # Handle the response...
        if response is None:
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = response[0]['http_code']
        if status != 204:
            raise Exception('Error setting bucket properties.')
        return True

    def mapred(self, inputs, query, timeout=None):
        # Construct the job, optionally set the timeout...
        job = {'inputs':inputs, 'query':query}
        if timeout is not None:
            job['timeout'] = timeout

        content = json.dumps(job)

        # Do the request...
        url = "/" + self._mapred_prefix
        response = self.http_request('POST', url, {}, content)

        # Make sure the expected status code came back...
        status = response[0]['http_code']
        if status != 200:
            raise Exception('Error running MapReduce operation. Status: ' + str(status) + ' : ' + response[1])

        result = json.loads(response[1])
        return result

    def check_http_code(self, response, expected_statuses):
        status = response[0]['http_code']
        if not status in expected_statuses:
            m = 'Expected status ' + str(expected_statuses) + ', received ' + str(status) + ' : ' + response[1]
            raise Exception(m)

    def parse_body(self, response, expected_statuses):
        """
        Given the output of RiakUtils.http_request and a list of
        statuses, populate the object. Only for use by the Riak client
        library.
        @return self
        """
        # If no response given, then return.
        if response is None:
            return self

        # Make sure expected code came back
        self.check_http_code(response, expected_statuses)

        # Update the object...
        headers = response[0]
        data = response[1]
        status = headers['http_code']

        # Check if the server is down(status==0)
        if not status:
            ### we need the host/port that was used.
            m = 'Could not contact Riak Server: http://$HOST:$PORT !'
            raise RiakError(m)

        # If 404(Not Found), then clear the object.
        if status == 404:
            return None

        # If 300(Siblings), then return the list of siblings
        elif status == 300:
            # Parse and get rid of 'Siblings:' string in element 0
            siblings = data.strip().split('\n')
            siblings.pop(0)
            return siblings

        # Parse the headers...
        vclock = None
        metadata = {MD_USERMETA: {}, MD_INDEX: []}
        links = []
        for header, value in headers.iteritems():
            if header == 'content-type':
                metadata[MD_CTYPE] = value
            elif header == 'charset':
                metadata[MD_CHARSET] = value
            elif header == 'content-encoding':
                metadata[MD_CTYPE] = value
            elif header == 'etag':
                metadata[MD_VTAG] = value
            elif header =='link':
                self.parse_links(links, headers['link'])
            elif header == 'last-modified':
                metadata[MD_LASTMOD] = value
            elif header.startswith('x-riak-meta-'):
                metadata[MD_USERMETA][header.replace('x-riak-meta-', '')] = value
            elif header.startswith('x-riak-index-'):
                field = header.replace('x-riak-index-', '')
                reader = csv.reader([value], skipinitialspace=True)
                for line in reader:
                    for token in line:
                        rie = RiakIndexEntry(field, token)
                        metadata[MD_INDEX].append(rie)

            elif header == 'x-riak-vclock':
                vclock = value
        if links:
            metadata[MD_LINKS] = links

        return vclock, [(metadata, data)]

    def to_link_header(self, link):
        """
        Convert this RiakLink object to a link header string. Used internally.
        """
        header = ''
        header += '</'
        header += self._prefix + '/'
        header += urllib.quote_plus(link.get_bucket()) + '/'
        header += urllib.quote_plus(link.get_key()) + '>; riaktag="'
        header += urllib.quote_plus(link.get_tag()) + '"'
        return header

    def parse_links(self, links, linkHeaders):
        """
        Private.
        @return self
        """
        for linkHeader in linkHeaders.strip().split(','):
            linkHeader = linkHeader.strip()
            matches = re.match("</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\']+)\"", linkHeader) or \
                re.match("</(buckets)/([^/]+)/keys/([^/]+)>; ?riaktag=\"([^\']+)\"", linkHeader)
            if matches is not None:
                link = RiakLink(urllib.unquote_plus(matches.group(2)),
                                urllib.unquote_plus(matches.group(3)),
                                urllib.unquote_plus(matches.group(4)))
                links.append(link)
        return self

    def add_links_for_riak_object(self, robject, headers):
        links = robject.get_links()
        if links:
            current_header = ''
            for link in links:
                header = self.to_link_header(link)
                if len(current_header + header) > MAX_LINK_HEADER_SIZE:
                    headers.add('Link', current_header)
                    current_header = ''

                if current_header != '': header = ', ' + header
                current_header += header

            headers.add('Link', current_header)

        return headers

    def get_request(self, uri=None, params=None):
        url = self.build_rest_path(bucket=None, params=params, prefix=uri)
        return self.http_request('GET', url)

    def store_file(self, key, content_type="application/octet-stream", content=None):
        url = self.build_rest_path(prefix='luwak', key=key)
        headers = {'Content-Type' : content_type,
                   'X-Riak-ClientId' : self._client_id}

        return self.do_put(url, headers, content, key=key)

    def get_file(self, key):
        url = self.build_rest_path(prefix='luwak', key=key)
        response = self.http_request('GET', url)
        result = self.parse_body(response, [200, 300, 404])
        if result is not None:
            (vclock, data) = result
            (headers, body) = data.pop()
            return body

    def delete_file(self, key):
        url = self.build_rest_path(prefix='luwak', key=key)
        response = self.http_request('DELETE', url)
        self.parse_body(response, [204, 404])

    def post_request(self, uri=None, body=None, params=None, content_type="application/json"):
        uri = self.build_rest_path(prefix=uri, params=params)
        return self.http_request('POST', uri, {'Content-Type': content_type}, body)

    # Utility functions used by Riak library.

    def build_rest_path(self, bucket=None, key=None, params=None, prefix=None) :
        """
        Given a RiakClient, RiakBucket, Key, LinkSpec, and Params,
        construct and return a URL.
        """
        # Build 'http://hostname:port/prefix/bucket'
        path = ''
        path += '/' + (prefix or self._prefix)

        # Add '.../bucket'
        if bucket is not None:
            path += '/' + urllib.quote_plus(bucket._name)

        # Add '.../key'
        if key is not None:
            path += '/' + urllib.quote_plus(key)

        # Add query parameters.
        if params is not None:
            s = ''
            for key in params.keys():
                if params[key] is not None:
                    if s != '': s += '&'
                    s += urllib.quote_plus(key) + '=' + urllib.quote_plus(str(params[key]))
            path += '?' + s

        # Return.
        return path

    def build_put_headers(self, robj):
        """Build the headers for a POST/PUT request."""

        # Construct the headers...
        headers = MultiDict({'Accept' : 'text/plain, */*; q=0.5',
                             'Content-Type' : robj.get_content_type(),
                             'X-Riak-ClientId' : self._client_id})

        # Add the vclock if it exists...
        if robj.vclock() is not None:
            headers['X-Riak-Vclock'] = robj.vclock()

        # Create the header from metadata
        links = self.add_links_for_riak_object(robj, headers)

        for key, value in robj.get_usermeta().iteritems():
            headers['X-Riak-Meta-%s' % key] = value

        for rie in robj.get_indexes():
            key = 'X-Riak-Index-%s' % rie.get_field()
            if key in headers:
                headers[key] += ", " + rie.get_value()
            else:
                headers[key] = rie.get_value()

        return headers

    def http_request(self, method, uri, headers=None, body='') :
        """
        Given a Method, URL, Headers, and Body, perform and HTTP request,
        and return a 2-tuple containing a dictionary of response headers
        and the response body.
        """
        if headers is None:
            headers = {}
        # Run the request...
        for retry in range(self.RETRY_COUNT):
            with self._conns.withconn() as conn:
                ### should probably build this try/except into a custom
                ### contextmanager for the connection.
                try:
                    conn.request(method, uri, body, headers)
                    response = conn.getresponse()

                    try:
                        # Get the response headers...
                        response_headers = {'http_code': response.status}
                        for (key, value) in response.getheaders():
                            response_headers[key.lower()] = value

                        # Get the body...
                        response_body = response.read()
                    finally:
                      response.close()

                    return response_headers, response_body
                except socket.error, e:
                    conn.close()
                    if e[0] == errno.ECONNRESET:
                        # Grab another connection and try again.
                        continue
                    # Don't know how to handle this.
                    raise
                except httplib.HTTPException:
                    # Just close the connection and try again.
                    conn.close()
                    continue

        # No luck, even with retrying.
        raise RiakError("could not get a response")

    @classmethod
    def build_headers(cls, headers):
        return ['%s: %s' % (header, value) for header, value in headers.iteritems()]

    @classmethod
    def parse_http_headers(cls, headers) :
        """
        Parse an HTTP Header string into an asssociative array of
        response headers.
        """
        retVal = {}
        fields = headers.split("\n")
        for field in fields:
            matches = re.match("([^:]+):(.+)", field)
            if matches is None: continue
            key = matches.group(1).lower()
            value = matches.group(2).strip()
            if key in retVal.keys():
                if  isinstance(retVal[key], list):
                    retVal[key].append(value)
                else:
                    retVal[key] = [retVal[key]].append(value)
            else:
                retVal[key] = value
        return retVal

"""
Copyright 2012 Basho Technologies, Inc.
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

try:
    import simplejson as json
except ImportError:
    import json


import httplib
from xml.dom.minidom import Document
from riak.transports.transport import RiakTransport
from riak.transports.http.resources import RiakHttpResources
from riak.transports.http.connection import RiakHttpConnection
from riak.transports.http.codec import RiakHttpCodec
from riak.transports.http.stream import (
    RiakHttpKeyStream,
    RiakHttpMapReduceStream,
    RiakHttpBucketStream,
    RiakHttpIndexStream)
from riak import RiakError
from riak.util import decode_index_value


class RiakHttpTransport(RiakHttpConnection, RiakHttpResources, RiakHttpCodec,
                        RiakTransport):
    """
    The RiakHttpTransport object holds information necessary to
    connect to Riak via HTTP.
    """

    def __init__(self, node=None,
                 client=None,
                 connection_class=httplib.HTTPConnection,
                 client_id=None,
                 **unused_options):
        """
        Construct a new HTTP connection to Riak.
        """
        super(RiakHttpTransport, self).__init__()

        self._client = client
        self._node = node
        self._connection_class = connection_class
        self._client_id = client_id
        if not self._client_id:
            self._client_id = self.make_random_client_id()
        self._connect()

    def ping(self):
        """
        Check server is alive over HTTP
        """
        status, _, body = self._request('GET', self.ping_path())
        return(status is not None) and (body == 'OK')

    def stats(self):
        """
        Gets performance statistics and server information
        """
        status, _, body = self._request('GET', self.stats_path(),
                                        {'Accept': 'application/json'})
        if status == 200:
            return json.loads(body)
        else:
            return None

    # FeatureDetection API - private
    def _server_version(self):
        stats = self.stats()
        if stats is not None:
            return stats['riak_kv_version']
        # If stats is disabled, we can't assume the Riak version
        # is >= 1.1. However, we can assume the new URL scheme is
        # at least version 1.0
        elif self.riak_kv_wm_buckets:
            return "1.0.0"
        else:
            return "0.14.0"

    def get_resources(self):
        """
        Gets a JSON mapping of server-side resource names to paths
        :rtype dict
        """
        status, _, body = self._request('GET', '/',
                                        {'Accept': 'application/json'})
        if status == 200:
            return json.loads(body)
        else:
            return {}

    def get(self, robj, r=None, pr=None, timeout=None):
        """
        Get a bucket/key from the server
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'r': r, 'pr': pr, 'timeout': timeout}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        response = self._request('GET', url)
        return self._parse_body(robj, response, [200, 300, 404])

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False, timeout=None):
        """
        Puts a (possibly new) object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'returnbody': return_body, 'w': w, 'dw': dw, 'pw': pw,
                  'timeout': timeout}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        headers = self._build_put_headers(robj, if_none_match=if_none_match)
        content = bytearray(robj.encoded_data)

        if robj.key is None:
            expect = [201]
            method = 'POST'
        else:
            expect = [204]
            method = 'PUT'

        response = self._request(method, url, headers, content)
        if return_body:
            return self._parse_body(robj, response, [200, 201, 204, 300])
        else:
            self.check_http_code(response[0], expect)
            return None

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None,
               timeout=None):
        """
        Delete an object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'rw': rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw,
                  'timeout': timeout}
        headers = {}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        if self.tombstone_vclocks() and robj.vclock is not None:
            headers['X-Riak-Vclock'] = robj.vclock.encode('base64')
        response = self._request('DELETE', url, headers)
        self.check_http_code(response[0], [204, 404])
        return self

    def get_keys(self, bucket, timeout=None):
        """
        Fetch a list of keys for the bucket
        """
        url = self.key_list_path(bucket.name, timeout=timeout)
        status, _, body = self._request('GET', url)

        if status == 200:
            props = json.loads(body)
            return props['keys']
        else:
            raise RiakError('Error listing keys.')

    def stream_keys(self, bucket, timeout=None):
        url = self.key_list_path(bucket.name, keys='stream', timeout=timeout)
        status, headers, response = self._request('GET', url, stream=True)

        if status == 200:
            return RiakHttpKeyStream(response)
        else:
            raise RiakError('Error listing keys.')

    def get_buckets(self, timeout=None):
        """
        Fetch a list of all buckets
        """
        url = self.bucket_list_path(timeout=timeout)
        status, headers, body = self._request('GET', url)

        if status == 200:
            props = json.loads(body)
            return props['buckets']
        else:
            raise RiakError('Error getting buckets.')

    def stream_buckets(self, timeout=None):
        """
        Stream list of buckets through an iterator
        """
        if not self.bucket_stream():
            raise NotImplementedError('Streaming list-buckets is not '
                                      'supported')

        url = self.bucket_list_path(buckets="stream", timeout=timeout)
        status, headers, response = self._request('GET', url, stream=True)

        if status == 200:
            return RiakHttpBucketStream(response)
        else:
            raise RiakError('Error listing buckets.')

    def get_bucket_props(self, bucket):
        """
        Get properties for a bucket
        """
        # Run the request...
        url = self.bucket_properties_path(bucket.name)
        status, headers, body = self._request('GET', url)

        if status == 200:
            props = json.loads(body)
            return props['props']
        else:
            raise RiakError('Error getting bucket properties.')

    def set_bucket_props(self, bucket, props):
        """
        Set the properties on the bucket object given
        """
        url = self.bucket_properties_path(bucket.name)
        headers = {'Content-Type': 'application/json'}
        content = json.dumps({'props': props})

        # Run the request...
        status, _, _ = self._request('PUT', url, headers, content)

        if status != 204:
            raise RiakError('Error setting bucket properties.')
        return True

    def clear_bucket_props(self, bucket):
        """
        reset the properties on the bucket object given
        """
        url = self.bucket_properties_path(bucket.name)
        headers = {'Content-Type': 'application/json'}

        # Run the request...
        status, _, _ = self._request('DELETE', url, headers, None)

        if status == 204:
            return True
        elif status == 405:
            return False
        else:
            raise RiakError('Error %s clearing bucket properties.'
                            % status)

    def mapred(self, inputs, query, timeout=None):
        """
        Run a MapReduce query.
        """
        # Construct the job, optionally set the timeout...
        content = self._construct_mapred_json(inputs, query, timeout)

        # Do the request...
        url = self.mapred_path()
        headers = {'Content-Type': 'application/json'}
        status, headers, body = self._request('POST', url, headers, content)

        # Make sure the expected status code came back...
        if status != 200:
            raise RiakError(
                'Error running MapReduce operation. Headers: %s Body: %s' %
                (repr(headers), repr(body)))

        result = json.loads(body)
        return result

    def stream_mapred(self, inputs, query, timeout=None):
        content = self._construct_mapred_json(inputs, query, timeout)

        url = self.mapred_path(chunked=True)
        reqheaders = {'Content-Type': 'application/json'}
        status, headers, response = self._request('POST', url, reqheaders,
                                                  content, stream=True)

        if status == 200:
            return RiakHttpMapReduceStream(response)
        else:
            raise RiakError(
                'Error running MapReduce operation. Headers: %s Body: %s' %
                (repr(headers), repr(response.read())))

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None):
        """
        Performs a secondary index query.
        """
        if timeout == 'infinity':
            timeout = 0

        params = {'return_terms': return_terms, 'max_results': max_results,
                  'continuation': continuation, 'timeout': timeout}
        url = self.index_path(bucket, index, startkey, endkey, **params)
        status, headers, body = self._request('GET', url)
        self.check_http_code(status, [200])
        json_data = json.loads(body)
        if return_terms and u'results' in json_data:
            results = []
            for result in json_data[u'results'][:]:
                term, key = result.items()[0]
                results.append((decode_index_value(index, term), key),)
        else:
            results = json_data[u'keys'][:]

        if max_results and u'continuation' in json_data:
            return (results, json_data[u'continuation'])
        else:
            return (results, None)

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None):
        """
        Streams a secondary index query.
        """
        if not self.stream_indexes():
            raise NotImplementedError("Secondary index streaming is not "
                                      "supported on %s" %
                                      self.server_version.vstring)

        if timeout == 'infinity':
            timeout = 0

        params = {'return_terms': return_terms, 'stream': True,
                  'max_results': max_results, 'continuation': continuation,
                  'timeout': timeout}
        url = self.index_path(bucket, index, startkey, endkey, **params)
        status, headers, response = self._request('GET', url, stream=True)

        if status == 200:
            return RiakHttpIndexStream(response, index, return_terms)
        else:
            raise RiakError('Error streaming secondary index.')

    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        if index is None:
            index = 'search'

        options = {}
        if 'op' in params:
            op = params.pop('op')
            options['q.op'] = op

        options.update(params)
        url = self.solr_select_path(index, query, **options)
        status, headers, data = self._request('GET', url)
        self.check_http_code(status, [200])
        if 'json' in headers['content-type']:
            results = json.loads(data)
            return self._normalize_json_search_response(results)
        elif 'xml' in headers['content-type']:
            return self._normalize_xml_search_response(data)
        else:
            raise ValueError("Could not decode search response")

    def fulltext_add(self, index, docs):
        """
        Adds documents to the search index.
        """
        xml = Document()
        root = xml.createElement('add')
        for doc in docs:
            doc_element = xml.createElement('doc')
            for key in doc:
                value = doc[key]
                field = xml.createElement('field')
                field.setAttribute("name", key)
                text = xml.createTextNode(value)
                field.appendChild(text)
                doc_element.appendChild(field)
            root.appendChild(doc_element)
        xml.appendChild(root)

        self._request('POST', self.solr_update_path(index),
                      {'Content-Type': 'text/xml'},
                      xml.toxml().encode('utf-8'))

    def fulltext_delete(self, index, docs=None, queries=None):
        """
        Removes documents from the full-text index.
        """
        xml = Document()
        root = xml.createElement('delete')
        if docs:
            for doc in docs:
                doc_element = xml.createElement('id')
                text = xml.createTextNode(doc)
                doc_element.appendChild(text)
                root.appendChild(doc_element)
        if queries:
            for query in queries:
                query_element = xml.createElement('query')
                text = xml.createTextNode(query)
                query_element.appendChild(text)
                root.appendChild(query_element)

        xml.appendChild(root)

        self._request('POST', self.solr_update_path(index),
                      {'Content-Type': 'text/xml'},
                      xml.toxml().encode('utf-8'))

    def get_counter(self, bucket, key, **options):
        if not self.counters():
            raise NotImplementedError("Counters are not supported")

        url = self.counters_path(bucket.name, key, **options)
        status, headers, body = self._request('GET', url)

        self.check_http_code(status, [200, 404])
        if status == 200:
            return long(body.strip())
        elif status == 404:
            return None

    def update_counter(self, bucket, key, amount, **options):
        if not self.counters():
            raise NotImplementedError("Counters are not supported")

        return_value = 'returnvalue' in options and options['returnvalue']
        headers = {'Content-Type': 'text/plain'}
        url = self.counters_path(bucket.name, key, **options)
        status, headers, body = self._request('POST', url, headers,
                                              str(amount))
        if return_value and status == 200:
            return long(body.strip())
        elif status == 204:
            return True
        else:
            self.check_http_code(status, [200, 204])

    def check_http_code(self, status, expected_statuses):
        if not status in expected_statuses:
            raise RiakError('Expected status %s, received %s' %
                            (expected_statuses, status))

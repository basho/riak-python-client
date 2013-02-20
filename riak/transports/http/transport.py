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

import urllib
import re
import csv
import httplib
from riak.transports.transport import RiakTransport
from riak.transports.http.resources import RiakHttpResources
from riak.transports.http.connection import RiakHttpConnection
from riak.transports.http.search import XMLSearchResult
from riak.transports.http.stream import (
    RiakHttpKeyStream,
    RiakHttpMapReduceStream
    )
from riak.mapreduce import RiakLink
from riak import RiakError
from riak.multidict import MultiDict
from xml.etree import ElementTree
from xml.dom.minidom import Document


# subtract length of "Link: " header string and newline
MAX_LINK_HEADER_SIZE = 8192 - 8


class RiakHttpTransport(RiakHttpConnection, RiakHttpResources, RiakTransport):
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
        response = self._request('GET', self.ping_path())
        return(response is not None) and (response[1] == 'OK')

    def stats(self):
        """
        Gets performance statistics and server information
        """
        response = self._request('GET', self.stats_path(),
                                 {'Accept': 'application/json'})
        if response[0]['http_code'] is 200:
            return json.loads(response[1])
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
        response = self._request('GET', '/', {'Accept': 'application/json'})
        if response[0]['http_code'] is 200:
            return json.loads(response[1])
        else:
            return {}

    def get(self, robj, r=None, pr=None, vtag=None):
        """
        Get a bucket/key from the server
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'r': r, 'pr': pr, 'vtag': vtag}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        response = self._request('GET', url)
        return self.parse_body(robj, response, [200, 300, 404])

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):
        """
        Serialize put request and deserialize response
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'returnbody': return_body, 'w': w, 'dw': dw, 'pw': pw}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        headers = self._build_put_headers(robj)

        # TODO: use a more general 'prevent_stale_writes' semantics,
        # which is a superset of the if_none_match semantics.
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        return self.do_put(url, headers, content, robj, return_body)

    def do_put(self, url, headers, content, robj, return_body=False):
        if robj.key is None:
            response = self._request('POST', url, headers, content)
        else:
            response = self._request('PUT', url, headers, content)

        if return_body:
            return self.parse_body(robj, response, [200, 201, 204, 300])
        else:
            self.check_http_code(response, [204])
            return None

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True,
                if_none_match=False):
        """Put a new object into the Riak store, returning its (new) key."""
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'returnbody': return_body, 'w': w, 'dw': dw, 'pw': pw}
        url = self.object_path(robj.bucket.name, **params)
        headers = self._build_put_headers(robj)
        # TODO: use a more general 'prevent_stale_writes' semantics,
        # which is a superset of the if_none_match semantics.
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        response = self._request('POST', url, headers, content)
        location = response[0]['location']
        idx = location.rindex('/')
        robj.key = location[(idx + 1):]
        if return_body:
            return self.parse_body(robj, response, [201])
        else:
            self.check_http_code(response, [201])
            return None

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Delete an object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'rw': rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw}
        headers = {}
        url = self.object_path(robj.bucket.name, robj.key, **params)
        if self.tombstone_vclocks() and robj.vclock is not None:
            headers['X-Riak-Vclock'] = robj.vclock
        response = self._request('DELETE', url, headers)
        self.check_http_code(response, [204, 404])
        return self

    def get_keys(self, bucket):
        """
        Fetch a list of keys for the bucket
        """
        url = self.key_list_path(bucket.name)
        response = self._request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = json.loads(encoded_props)
            return props['keys']
        else:
            raise Exception('Error listing keys.')

    def stream_keys(self, bucket):
        url = self.key_list_path(bucket.name, keys='stream')
        headers, response = self._request('GET', url, stream=True)

        if headers['http_code'] == 200:
            return RiakHttpKeyStream(response)
        else:
            raise Exception('Error listing keys.')

    def get_buckets(self):
        """
        Fetch a list of all buckets
        """
        url = self.bucket_list_path()
        response = self._request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = json.loads(encoded_props)
            return props['buckets']
        else:
            raise Exception('Error getting buckets.')

    def get_bucket_props(self, bucket):
        """
        Get properties for a bucket
        """
        # Run the request...
        url = self.bucket_properties_path(bucket.name)
        response = self._request('GET', url)

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
        url = self.bucket_properties_path(bucket.name)
        headers = {'Content-Type': 'application/json'}
        content = json.dumps({'props': props})

        # Run the request...
        response = self._request('PUT', url, headers, content)

        # Handle the response...
        if response is None:
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = response[0]['http_code']
        if status != 204:
            raise Exception('Error setting bucket properties.')
        return True

    def clear_bucket_props(self, bucket):
        """
        reset the properties on the bucket object given
        """
        url = self.bucket_properties_path(bucket.name)
        headers = {'Content-Type': 'application/json'}

        # Run the request...
        response = self._request('DELETE', url, headers, None)

        # Handle the response...
        if response is None:
            raise Exception('Error clearing bucket properties.')

        # Check the response value...
        status = response[0]['http_code']
        if status == 204:
            return True
        elif status == 405:
            return False
        else:
            raise Exception('Error %s clearing bucket properties.'
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
        response = self._request('POST', url, headers, content)

        # Make sure the expected status code came back...
        status = response[0]['http_code']
        if status != 200:
            raise Exception(
                'Error running MapReduce operation. Headers: %s Body: %s' %
                (repr(response[0]), repr(response[1])))

        result = json.loads(response[1])
        return result

    def stream_mapred(self, inputs, query, timeout=None):
        content = self._construct_mapred_json(inputs, query, timeout)

        url = self.mapred_path(chunked=True)
        reqheaders = {'Content-Type': 'application/json'}
        headers, response = self._request('POST', url, reqheaders,
                                          content, stream=True)

        if headers['http_code'] is 200:
            return RiakHttpMapReduceStream(response)
        else:
            raise Exception(
                    'Error running MapReduce operation. Headers: %s Body: %s' %
                    (repr(headers), repr(response.read())))

    def get_index(self, bucket, index, startkey, endkey=None):
        """
        Performs a secondary index query.
        """
        url = self.index_path(bucket, index, startkey, endkey)
        response = self._request('GET', url)
        headers, data = response
        self.check_http_code(response, [200])
        json_data = json.loads(data)
        return json_data[u'keys'][:]

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
        response = self._request('GET', url)
        headers, data = response
        self.check_http_code(response, [200])
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
                      xml.toxml())

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
                      xml.toxml())

    def check_http_code(self, response, expected_statuses):
        status = response[0]['http_code']
        if not status in expected_statuses:
            raise Exception('Expected status %s, received %s : %s' %
                            (expected_statuses, status, response[1]))

    def parse_body(self, robj, response, expected_statuses):
        """
        Parse the body of an object response and populate the object.
        """
        # If no response given, then return.
        if response is None:
            return None

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
            robj.siblings = siblings
            robj.exists = True
            robj.vclock = headers['x-riak-vclock']
            return robj

        #no sibs
        robj.siblings = []

        # Parse the headers...
        links = []
        for header, value in headers.iteritems():
            if header == 'content-type':
                robj.content_type = value
            elif header == 'charset':
                robj.charset = value
            elif header == 'content-encoding':
                robj.content_encoding = value
            elif header == 'etag':
                robj.etag = value
            elif header == 'link':
                self._parse_links(links, headers['link'])
            elif header == 'last-modified':
                robj.last_modified = value
            elif header.startswith('x-riak-meta-'):
                metakey = header.replace('x-riak-meta-', '')
                robj.usermeta[metakey] = value
            elif header.startswith('x-riak-index-'):
                field = header.replace('x-riak-index-', '')
                reader = csv.reader([value], skipinitialspace=True)
                for line in reader:
                    for token in line:
                        if field.endswith("_int"):
                            token = int(token)
                        robj.add_index(field, token)
            elif header == 'x-riak-vclock':
                robj.vclock = value
            elif header == 'x-riak-deleted':
                robj.deleted = True
        if links:
            robj.links = links

        robj.set_encoded_data(data)

        robj.exists = True
        return robj

    def to_link_header(self, link):
        """
        Convert this RiakLink object to a link header string. Used internally.
        """
        url = self.object_path(link.bucket, link.key)
        header = '<%s>; riaktag="%s"' % (url, link.tag)
        return header

    def _parse_links(self, links, linkHeaders):
        oldform = "</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\"]+)\""
        newform = "</(buckets)/([^/]+)/keys/([^/]+)>; ?riaktag=\"([^\"]+)\""
        for linkHeader in linkHeaders.strip().split(','):
            linkHeader = linkHeader.strip()
            matches = (re.match(oldform, linkHeader) or
                       re.match(newform, linkHeader))
            if matches is not None:
                link = RiakLink(urllib.unquote_plus(matches.group(2)),
                                urllib.unquote_plus(matches.group(3)),
                                urllib.unquote_plus(matches.group(4)))
                links.append(link)
        return links

    def _add_links_for_riak_object(self, robject, headers):
        links = robject.links
        if links:
            current_header = ''
            for link in links:
                header = self.to_link_header(link)
                if len(current_header + header) > MAX_LINK_HEADER_SIZE:
                    headers.add('Link', current_header)
                    current_header = ''

                if current_header != '':
                    header = ', ' + header
                current_header += header

            headers.add('Link', current_header)

        return headers

    # Utility functions used by Riak library.

    def _build_put_headers(self, robj):
        """Build the headers for a POST/PUT request."""

        # Construct the headers...
        headers = MultiDict({'Accept': 'text/plain, */*; q=0.5',
                             'Content-Type': robj.content_type,
                             'X-Riak-ClientId': self._client_id})
        # Add the vclock if it exists...
        if robj.vclock is not None:
            headers['X-Riak-Vclock'] = robj.vclock

        # Create the header from metadata
        self._add_links_for_riak_object(robj, headers)

        for key, value in robj.usermeta.iteritems():
            headers['X-Riak-Meta-%s' % key] = value

        for field, value in robj.indexes:
            key = 'X-Riak-Index-%s' % field
            if key in headers:
                headers[key] += ", " + str(value)
            else:
                headers[key] = str(value)

        return headers

    def _normalize_json_search_response(self, json):
        """
        Normalizes a JSON search response so that PB and HTTP have the
        same return value
        """
        result = {}
        if u'response' in json:
            result['num_found'] = json[u'response'][u'numFound']
            result['max_score'] = float(json[u'response'][u'maxScore'])
            docs = []
            for doc in json[u'response'][u'docs']:
                resdoc = {u'id': doc[u'id']}
                if u'fields' in doc:
                    for k, v in doc[u'fields'].iteritems():
                        resdoc[k] = v
                docs.append(resdoc)
            result['docs'] = docs
        return result

    def _normalize_xml_search_response(self, xml):
        """
        Normalizes an XML search response so that PB and HTTP have the
        same return value
        """
        target = XMLSearchResult()
        parser = ElementTree.XMLParser(target=target)
        parser.feed(xml)
        return parser.close()

    @classmethod
    def build_headers(cls, headers):
        return ['%s: %s' % (header, value)
                for header, value in headers.iteritems()]

    @classmethod
    def parse_http_headers(cls, headers):
        """
        Parse an HTTP Header string into an asssociative array of
        response headers.
        """
        retVal = {}
        fields = headers.split("\n")
        for field in fields:
            matches = re.match("([^:]+):(.+)", field)
            if matches is None:
                continue
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

import re
import csv
import six

from cgi import parse_header
from email import message_from_string
from email.utils import parsedate_tz, mktime_tz
from xml.etree import ElementTree
from riak import RiakError
from riak.content import RiakContent
from riak.riak_object import VClock
from riak.multidict import MultiDict
from riak.transports.http.search import XMLSearchResult
from riak.util import decode_index_value, bytes_to_str

if six.PY2:
    from urllib import unquote_plus
else:
    from urllib.parse import unquote_plus


# subtract length of "Link: " header string and newline
MAX_LINK_HEADER_SIZE = 8192 - 8


class HttpCodec(object):
    """
    Methods for HTTP transport that marshals and unmarshals HTTP
    messages.
    """

    def _parse_body(self, robj, response, expected_statuses):
        """
        Parse the body of an object response and populate the object.
        """
        # If no response given, then return.
        if response is None:
            return None

        status, headers, data = response

        # Check if the server is down(status==0)
        if not status:
            m = 'Could not contact Riak Server: http://{0}:{1}!'.format(
                self._node.host, self._node.http_port)
            raise RiakError(m)

        # Make sure expected code came back
        self.check_http_code(status, expected_statuses)

        if 'x-riak-vclock' in headers:
            robj.vclock = VClock(headers['x-riak-vclock'], 'base64')

        # If 404(Not Found), then clear the object.
        if status == 404:
            robj.siblings = []
            return None
        # If 201 Created, we need to extract the location and set the
        # key on the object.
        elif status == 201:
            robj.key = headers['location'].strip().split('/')[-1]
        # If 300(Siblings), apply the siblings to the object
        elif status == 300:
            ctype, params = parse_header(headers['content-type'])
            if ctype == 'multipart/mixed':
                if six.PY3:
                    data = bytes_to_str(data)
                boundary = re.compile('\r?\n--%s(?:--)?\r?\n' %
                                      re.escape(params['boundary']))
                parts = [message_from_string(p)
                         for p in re.split(boundary, data)[1:-1]]
                robj.siblings = [self._parse_sibling(RiakContent(robj),
                                                     part.items(),
                                                     part.get_payload())
                                 for part in parts]

                # Invoke sibling-resolution logic
                if robj.resolver is not None:
                    robj.resolver(robj)

                return robj
            else:
                raise Exception('unexpected sibling response format: {0}'.
                                format(ctype))

        robj.siblings = [self._parse_sibling(RiakContent(robj),
                                             headers.items(),
                                             data)]

        return robj

    def _parse_sibling(self, sibling, headers, data):
        """
        Parses a single sibling out of a response.
        """

        sibling.exists = True

        # Parse the headers...
        for header, value in headers:
            header = header.lower()
            if header == 'content-type':
                sibling.content_type, sibling.charset = \
                    self._parse_content_type(value)
            elif header == 'etag':
                sibling.etag = value
            elif header == 'link':
                sibling.links = self._parse_links(value)
            elif header == 'last-modified':
                sibling.last_modified = mktime_tz(parsedate_tz(value))
            elif header.startswith('x-riak-meta-'):
                metakey = header.replace('x-riak-meta-', '')
                sibling.usermeta[metakey] = value
            elif header.startswith('x-riak-index-'):
                field = header.replace('x-riak-index-', '')
                reader = csv.reader([value], skipinitialspace=True)
                for line in reader:
                    for token in line:
                        token = decode_index_value(field, token)
                        sibling.add_index(field, token)
            elif header == 'x-riak-deleted':
                sibling.exists = False

        sibling.encoded_data = data

        return sibling

    def _to_link_header(self, link):
        """
        Convert the link tuple to a link header string. Used internally.
        """
        try:
            bucket, key, tag = link
        except ValueError:
            raise RiakError("Invalid link tuple %s" % link)
        tag = tag if tag is not None else bucket
        url = self.object_path(bucket, key)
        header = '<%s>; riaktag="%s"' % (url, tag)
        return header

    def _parse_links(self, linkHeaders):
        links = []
        oldform = "</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\"]+)\""
        newform = "</(buckets)/([^/]+)/keys/([^/]+)>; ?riaktag=\"([^\"]+)\""
        for linkHeader in linkHeaders.strip().split(','):
            linkHeader = linkHeader.strip()
            matches = (re.match(oldform, linkHeader) or
                       re.match(newform, linkHeader))
            if matches is not None:
                link = (unquote_plus(matches.group(2)),
                        unquote_plus(matches.group(3)),
                        unquote_plus(matches.group(4)))
                links.append(link)
        return links

    def _add_links_for_riak_object(self, robject, headers):
        links = robject.links
        if links:
            current_header = ''
            for link in links:
                header = self._to_link_header(link)
                if len(current_header + header) > MAX_LINK_HEADER_SIZE:
                    headers.add('Link', current_header)
                    current_header = ''

                if current_header != '':
                    header = ', ' + header
                current_header += header

            headers.add('Link', current_header)

        return headers

    def _build_put_headers(self, robj, if_none_match=False):
        """Build the headers for a POST/PUT request."""

        # Construct the headers...
        if robj.charset is not None:
            content_type = ('%s; charset="%s"' %
                            (robj.content_type, robj.charset))
        else:
            content_type = robj.content_type

        headers = MultiDict({'Content-Type': content_type,
                             'X-Riak-ClientId': self._client_id})

        # Add the vclock if it exists...
        if robj.vclock is not None:
            headers['X-Riak-Vclock'] = robj.vclock.encode('base64')

        # Create the header from metadata
        self._add_links_for_riak_object(robj, headers)

        for key in robj.usermeta.keys():
            headers['X-Riak-Meta-%s' % key] = robj.usermeta[key]

        for field, value in robj.indexes:
            key = 'X-Riak-Index-%s' % field
            if key in headers:
                headers[key] += ", " + str(value)
            else:
                headers[key] = str(value)

        if if_none_match:
            headers['If-None-Match'] = '*'

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
                resdoc = {}
                if u'_yz_rk' in doc:
                    # Is this a Riak 2.0 result?
                    resdoc = doc
                else:
                    # Riak Search 1.0 Legacy assumptions about format
                    resdoc[u'id'] = doc[u'id']
                    if u'fields' in doc:
                        for k, v in six.iteritems(doc[u'fields']):
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

    def _parse_content_type(self, value):
        """
        Split the content-type header into two parts:
        1) Actual main/sub encoding type
        2) charset

        :param value: Complete MIME content-type string
        """
        content_type, params = parse_header(value)
        if 'charset' in params:
            charset = params['charset']
        else:
            charset = None
        return content_type, charset

    def _decode_datatype(self, dtype, value):
        if not dtype == 'map':
            return value
        map = {}
        for key in value:
            field = self._map_key_to_pair(key)
            map[field] = self._decode_datatype(field[1], value[key])
        return map

    def _map_key_to_pair(self, key):
        name, _, type = key.rpartition('_')
        return (name, type)

    def _map_pair_to_key(self, pair):
        return "_".join(pair)

    def _encode_dt_op(self, dtype, op):
        if dtype in ('counter', 'register'):
            # ('increment', some_int)
            # ('assign', some_str)
            return dict([op])
        elif dtype == 'flag':
            return op
        elif dtype == 'set':
            set_op = {}
            if 'adds' in op:
                set_op['add_all'] = op['adds']
            if 'removes' in op:
                set_op['remove_all'] = op['removes']
            return set_op
        elif dtype == 'hll':
            hll_op = {}
            if 'adds' in op:
                hll_op['add_all'] = op['adds']
            return hll_op
        elif dtype == 'map':
            map_op = {}
            for fop in op:
                fopname = fop[0]
                fopkey = self._map_pair_to_key(fop[1])
                if fopname in ('add', 'remove'):
                    map_op.setdefault(fopname, []).append(fopkey)
                elif fopname == 'update':
                    updates = map_op.setdefault(fopname, {})
                    updates[fopkey] = self._encode_dt_op(fop[1][1], fop[2])
            return map_op

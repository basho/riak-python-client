from riak.transports import RiakHttpTransport
from xml.etree import ElementTree
from xml.dom.minidom import Document


class RiakSearch(object):
    def __init__(self, client, transport_class=None,
                 host="127.0.0.1", port=8098):
        if transport_class is None:
            transport_class = RiakHttpTransport

        api = getattr(transport_class, 'api', 1)
        if api >= 2:
            hostports = [(host, port), ]
            self._cm = transport_class.default_cm(hostports)
            self._transport = transport_class(self._cm, prefix="/solr")
        else:
            # The old code which attempted to use api==1 would actually
            # throw a NameError, so it was obviously never used. We will
            # simply raise an error here, intead of a gentle warning.
            raise DeprecationWarning('please upgrade the transport to the '
                                     'new API')

        self._client = client
        self._decoders = {"text/xml": ElementTree.fromstring}

    def get_decoder(self, content_type):
        decoder = (self._client.get_decoder(content_type)
                   or self._decoders[content_type])
        if not decoder:
            decoder = self.decode

        return decoder

    def decode(self, data):
        return data

    def add(self, index, *docs):
        xml = Document()
        root = xml.createElement('add')
        for doc in docs:
            doc_element = xml.createElement('doc')
            for key, value in doc.iteritems():
                field = xml.createElement('field')
                field.setAttribute("name", key)
                text = xml.createTextNode(value)
                field.appendChild(text)
                doc_element.appendChild(field)
            root.appendChild(doc_element)
        xml.appendChild(root)

        url = "/solr/%s/update" % index
        self._transport.post_request(uri=url, body=xml.toxml(),
                                     content_type="text/xml")

    index = add

    def delete(self, index, docs=None, queries=None):
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

        url = "/solr/%s/update" % index
        self._transport.post_request(uri=url, body=xml.toxml(),
                                     content_type="text/xml")

    remove = delete

    def search(self, index, query, **params):
        return self._client._transport.search(index, query, **params)

    select = search

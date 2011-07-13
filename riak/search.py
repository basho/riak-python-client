from riak.transports import RiakHttpTransport

class RiakSearch:
    def __init__(self, client, transport_class=None,
                 host="127.0.0.1", port=8098, client_id=None):
        if not transport_class:
            self._transport = RiakHttpTransport(host,
                                                port,
                                                "/solr",
                                                client_id)
        else:
            self._transport = transport_class(host, port, client_id=client_id)

        self._client = client
 
    def add(self, doc):
        pass

    def delete(self, doc):
        pass

    def search(self, index, query, **params):
        options = {'q': query, 'wt': 'json'}
        options.update(params)
        headers, results = self._transport.search(index, options)
        decoder = self._client.get_decoder(headers['content-type'])

        if decoder:
            return decoder(results)
        else:
            return results

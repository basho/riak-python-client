import re

from six import PY2
from riak import RiakError
from riak.util import lazy_property, bytes_to_str

if PY2:
    from urllib import quote_plus, urlencode
else:
    from urllib.parse import quote_plus, urlencode


class HttpResources(object):
    """
    Methods for HttpTransport related to URL generation, i.e.
    creating the proper paths.
    """

    def ping_path(self):
        return mkpath(self.riak_kv_wm_ping)

    def stats_path(self):
        return mkpath(self.riak_kv_wm_stats)

    def mapred_path(self, **options):
        return mkpath(self.riak_kv_wm_mapred, **options)

    def bucket_list_path(self, bucket_type=None, **options):
        query = {'buckets': True}
        query.update(options)
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type),
                          "buckets", **query)
        elif self.riak_kv_wm_buckets:
            return mkpath("/buckets", **query)
        else:
            return mkpath(self.riak_kv_wm_raw, **query)

    def bucket_properties_path(self, bucket, bucket_type=None, **options):
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type), "buckets",
                          quote_plus(bucket), "props", **options)
        elif self.riak_kv_wm_buckets:
            return mkpath("/buckets", quote_plus(bucket),
                          "props", **options)
        else:
            query = options.copy()
            query.update(props=True, keys=False)
            return mkpath(self.riak_kv_wm_raw, quote_plus(bucket), **query)

    def bucket_type_properties_path(self, bucket_type, **options):
        return mkpath("/types", quote_plus(bucket_type), "props",
                      **options)

    def key_list_path(self, bucket, bucket_type=None, **options):
        query = {'keys': True, 'props': False}
        query.update(options)
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type), "buckets",
                          quote_plus(bucket), "keys", **query)
        if self.riak_kv_wm_buckets:
            return mkpath("/buckets", quote_plus(bucket), "keys",
                          **query)
        else:
            return mkpath(self.riak_kv_wm_raw, quote_plus(bucket), **query)

    def object_path(self, bucket, key=None, bucket_type=None, **options):
        if key:
            key = quote_plus(key)
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type), "buckets",
                          quote_plus(bucket), "keys", key, **options)
        elif self.riak_kv_wm_buckets:
            return mkpath("/buckets", quote_plus(bucket), "keys",
                          key, **options)
        else:
            return mkpath(self.riak_kv_wm_raw, quote_plus(bucket), key,
                          **options)

    def index_path(self, bucket, index, start, finish=None, bucket_type=None,
                   **options):
        if not self.riak_kv_wm_buckets:
            raise RiakError("Indexes are unsupported by this Riak node")
        if finish is not None:
            finish = quote_plus(str(finish))
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type),
                          "buckets", quote_plus(bucket),
                          "index", quote_plus(index), quote_plus(str(start)),
                          finish, **options)
        else:
            return mkpath("/buckets", quote_plus(bucket),
                          "index", quote_plus(index), quote_plus(str(start)),
                          finish, **options)

    def search_index_path(self, index=None, **options):
        """
        Builds a Yokozuna search index URL.

        :param index: optional name of a yz index
        :type index: string
        :param options: optional list of additional arguments
        :type index: dict
        :rtype URL string
        """
        if not self.yz_wm_index:
            raise RiakError("Yokozuna search is unsupported by this Riak node")
        if index:
            quote_plus(index)
        return mkpath(self.yz_wm_index, "index", index, **options)

    def search_schema_path(self, index, **options):
        """
        Builds a Yokozuna search Solr schema URL.

        :param index: a name of a yz solr schema
        :type index: string
        :param options: optional list of additional arguments
        :type index: dict
        :rtype URL string
        """
        if not self.yz_wm_schema:
            raise RiakError("Yokozuna search is unsupported by this Riak node")
        return mkpath(self.yz_wm_schema, "schema", quote_plus(index),
                      **options)

    def solr_select_path(self, index, query, **options):
        if not self.riak_solr_searcher_wm and not self.yz_wm_search:
            raise RiakError("Search is unsupported by this Riak node")
        qs = {'q': query, 'wt': 'json', 'fl': '*,score'}
        qs.update(options)
        if index:
            index = quote_plus(index)
        return mkpath("/solr", index, "select", **qs)

    def solr_update_path(self, index):
        if not self.riak_solr_searcher_wm:
            raise RiakError("Riak Search 1 is unsupported by this Riak node")
        if index:
            index = quote_plus(index)
        return mkpath(self.riak_solr_indexer_wm, index, "update")

    def counters_path(self, bucket, key, **options):
        if not self.riak_kv_wm_counter:
            raise RiakError("Counters are unsupported by this Riak node")

        return mkpath(self.riak_kv_wm_buckets, quote_plus(bucket), "counters",
                      quote_plus(key), **options)

    def datatypes_path(self, bucket_type, bucket, key=None, **options):
        if not self.bucket_types():
            raise RiakError("Datatypes are unsupported by this Riak node")
        if key:
            key = quote_plus(key)

        return mkpath("/types", quote_plus(bucket_type), "buckets",
                      quote_plus(bucket), "datatypes", key, **options)

    def preflist_path(self, bucket, key, bucket_type=None, **options):
        """
        Generate the URL for bucket/key preflist information

        :param bucket: Name of a Riak bucket
        :type bucket: string
        :param key: Name of a Key
        :type key: string
        :param bucket_type: Optional Riak Bucket Type
        :type bucket_type: None or string
        :rtype URL string
        """
        if not self.riak_kv_wm_preflist:
            raise RiakError("Preflists are unsupported by this Riak node")
        if self.riak_kv_wm_bucket_type and bucket_type:
            return mkpath("/types", quote_plus(bucket_type),
                          "buckets", quote_plus(bucket),
                          "keys", quote_plus(key),
                          "preflist", **options)
        else:
            return mkpath("/buckets", quote_plus(bucket),
                          "keys", quote_plus(key),
                          "preflist", **options)

    # Feature detection overrides
    def bucket_types(self):
        return self.riak_kv_wm_bucket_type is not None

    def index_term_regex(self):
        if self.riak_kv_wm_bucket_type is not None:
            return True
        else:
            return super(HttpResources, self).index_term_regex()

    # Resource root paths
    @lazy_property
    def riak_kv_wm_bucket_type(self):
        if 'riak_kv_wm_bucket_type' in self.resources:
            return "/types"

    @lazy_property
    def riak_kv_wm_buckets(self):
        if 'riak_kv_wm_buckets' in self.resources:
            return "/buckets"

    @lazy_property
    def riak_kv_wm_raw(self):
        return self.resources.get('riak_kv_wm_raw') or "/riak"

    @lazy_property
    def riak_kv_wm_link_walker(self):
        return self.resources.get('riak_kv_wm_linkwalker') or "/riak"

    @lazy_property
    def riak_kv_wm_mapred(self):
        return self.resources.get('riak_kv_wm_mapred') or "/mapred"

    @lazy_property
    def riak_kv_wm_ping(self):
        return self.resources.get('riak_kv_wm_ping') or "/ping"

    @lazy_property
    def riak_kv_wm_stats(self):
        return self.resources.get('riak_kv_wm_stats') or "/stats"

    @lazy_property
    def riak_solr_searcher_wm(self):
        return self.resources.get('riak_solr_searcher_wm')

    @lazy_property
    def riak_solr_indexer_wm(self):
        return self.resources.get('riak_solr_indexer_wm')

    @lazy_property
    def riak_kv_wm_counter(self):
        return self.resources.get('riak_kv_wm_counter')

    @lazy_property
    def riak_kv_wm_preflist(self):
        return self.resources.get('riak_kv_wm_preflist')

    @lazy_property
    def yz_wm_search(self):
        return self.resources.get('yz_wm_search')

    @lazy_property
    def yz_wm_extract(self):
        return self.resources.get('yz_wm_extract')

    @lazy_property
    def yz_wm_schema(self):
        return self.resources.get('yz_wm_schema')

    @lazy_property
    def yz_wm_index(self):
        return self.resources.get('yz_wm_index')

    @lazy_property
    def resources(self):
        return self.get_resources()


def mkpath(*segments, **query):
    """
    Constructs the path & query portion of a URI from path segments
    and a dict.
    """
    # Remove empty segments (e.g. no key specified)
    segments = [bytes_to_str(s) for s in segments if s is not None]
    # Join the segments into a path
    pathstring = '/'.join(segments)
    # Remove extra slashes
    pathstring = re.sub('/+', '/', pathstring)

    # Add the query string if it exists
    _query = {}
    for key in query:
        if query[key] in [False, True]:
            _query[key] = str(query[key]).lower()
        elif query[key] is not None:
            if PY2 and isinstance(query[key], unicode):  # noqa
                _query[key] = query[key].encode('utf-8')
            else:
                _query[key] = query[key]

    if len(_query) > 0:
        pathstring += "?" + urlencode(_query)

    if not pathstring.startswith('/'):
        pathstring = '/' + pathstring

    return pathstring

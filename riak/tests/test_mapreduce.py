# -*- coding: utf-8 -*-

from riak.mapreduce import RiakLink
from riak import RiakKeyFilter, key_filter


class LinkTests(object):
    def test_store_and_get_links(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new_binary("test_store_and_get_links", '2') \
            .add_link(bucket.new("foo1")) \
            .add_link(bucket.new("foo2"), "tag") \
            .add_link(bucket.new("foo3"), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("test_store_and_get_links")
        links = obj.get_links()
        self.assertEqual(len(links), 3)
        for l in links:
            if (l.get_key() == "foo1"):
                self.assertEqual(l.get_tag(), "bucket")
            elif (l.get_key() == "foo2"):
                self.assertEqual(l.get_tag(), "tag")
            elif (l.get_key() == "foo3"):
                self.assertEqual(l.get_tag(), "tag2!@#%^&*)")
            else:
                self.assertEqual("unknown key", l.get_key())

    def test_set_links(self):
        # Create the object
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).set_links([bucket.new("foo1"),
            (bucket.new("foo2"), "tag"),
            RiakLink("bucket", "foo2", "tag2")]).store()
        obj = bucket.get("foo")
        links = sorted(obj.get_links(), key=lambda x: x.get_key())
        self.assertEqual(len(links), 3)
        self.assertEqual(links[0].get_key(), "foo1")
        self.assertEqual(links[1].get_key(), "foo2")
        self.assertEqual(links[1].get_tag(), "tag")
        self.assertEqual(links[2].get_key(), "foo2")
        self.assertEqual(links[2].get_tag(), "tag2")

    def test_set_links_all_links(self):
        bucket = self.client.bucket("bucket")
        foo1 = bucket.new("foo", 1)
        foo2 = bucket.new("foo2", 2).store()
        links = [RiakLink("bucket", "foo2")]
        foo1.set_links(links, True)
        links = foo1.get_links()
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].get_key(), "foo2")

    def test_link_walking(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2) \
            .add_link(bucket.new("foo1", "test1").store()) \
            .add_link(bucket.new("foo2", "test2").store(), "tag") \
            .add_link(bucket.new("foo3", "test3").store(), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("foo")
        results = obj.link("bucket").run()
        self.assertEqual(len(results), 3)
        results = obj.link("bucket", "tag").run()
        self.assertEqual(len(results), 1)


class ErlangMapReduceTests(object):
    def test_erlang_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 2).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map(["riak_kv_mapreduce", "map_object_value"]) \
            .reduce(["riak_kv_mapreduce", "reduce_set_union"]) \
            .run()
        self.assertEqual(len(result), 2)


class JSMapReduceTests(object):
    def test_javascript_source_map(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        mr = self.client.add("bucket", "foo")
        result = mr.map(
            "function (v) { return [JSON.parse(v.values[0].data)]; }").run()
        self.assertEqual(result, [2])

        # test ASCII-encodable unicode is accepted
        mr.map(u"function (v) { return [JSON.parse(v.values[0].data)]; }")

        # test non-ASCII-encodable unicode is rejected
        self.assertRaises(TypeError, mr.map,
            u"function (v) { /* æ */ return [JSON.parse(v.values[0].data)]; }")

        # test non-ASCII-encodable string is rejected
        self.assertRaises(TypeError, mr.map,
            "function (v) { /* æ */ return [JSON.parse(v.values[0].data)]; }")

    def test_javascript_named_map(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .map("Riak.mapValuesJson") \
            .run()
        self.assertEqual(result, [2])

    def test_javascript_source_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("function (v) { return [1]; }") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [3])

    def test_javascript_named_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [9])

    def test_javascript_bucket_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket_%s" % self.randint())
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add(bucket.get_name()) \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [9])

    def test_javascript_arg_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo", 5) \
            .add("bucket", "foo", 10) \
            .add("bucket", "foo", 15) \
            .add("bucket", "foo", -15) \
            .add("bucket", "foo", -5) \
            .map("function(v, arg) { return [arg]; }") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [10])

    def test_key_filters(self):
        bucket = self.client.bucket("kftest")
        bucket.new("basho-20101215", 1).store()
        bucket.new("google-20110103", 2).store()
        bucket.new("yahoo-20090613", 3).store()

        result = self.client \
            .add("kftest") \
            .add_key_filters([["tokenize", "-", 2]]) \
            .add_key_filter("ends_with", "0613") \
            .map("function (v, keydata) { return [v.key]; }") \
            .run()

        self.assertEqual(result, ["yahoo-20090613"])

    def test_key_filters_f_chain(self):
        bucket = self.client.bucket("kftest")
        bucket.new("basho-20101215", 1).store()
        bucket.new("google-20110103", 2).store()
        bucket.new("yahoo-20090613", 3).store()

        # compose a chain of key filters using f as the root of
        # two filters ANDed together to ensure that f can be the root
        # of multiple chains
        filters = key_filter.tokenize("-", 1).eq("yahoo") \
            & key_filter.tokenize("-", 2).ends_with("0613")

        result = self.client \
            .add("kftest") \
            .add_key_filters(filters) \
            .map("function (v, keydata) { return [v.key]; }") \
            .run()

        self.assertEqual(result, ["yahoo-20090613"])

    def test_key_filters_with_search_query(self):
        mapreduce = self.client.search("kftest", "query")
        self.assertRaises(Exception, mapreduce.add_key_filters,
                          [["tokenize", "-", 2]])
        self.assertRaises(Exception, mapreduce.add_key_filter,
                          "ends_with", "0613")

    def test_map_reduce_from_object(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        obj = bucket.get("foo")
        result = obj.map("Riak.mapValuesJson").run()
        self.assertEqual(result, [2])


class MapReduceAliasTests(object):
    """This tests the map reduce aliases"""

    def test_map_values(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new_binary('one', data='value_1').store()
        bucket.new_binary('two', data='value_2').store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort()

        self.assertEqual(result, ["value_1", "value_2"])

    def test_map_values_json(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data={'val': 'value_1'}).store()
        bucket.new('two', data={'val': 'value_2'}).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort(key=lambda x: x['val'])

        self.assertEqual(result, [{'val': "value_1"}, {'val': "value_2"}])

    def test_reduce_sum(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sum().run()

        self.assertEqual(result, [3])

    def test_reduce_min(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_min().run()

        self.assertEqual(result, [1])

    def test_reduce_max(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_max().run()

        self.assertEqual(result, [2])

    def test_reduce_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort().run()

        self.assertEqual(result, ["value1", "value2"])

    def test_reduce_sort_custom(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort("""function(x,y) {
           if(x == y) return 0;
           return x > y ? -1 : 1;
        }""").run()

        self.assertEqual(result, ["value2", "value1"])

    def test_reduce_numeric_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_numeric_sort().run()

        self.assertEqual(result, [1, 2])

    def test_reduce_limit(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_limit(1).run()

        self.assertEqual(result, [1])

    def test_reduce_slice(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_slice(1, 2).run()

        self.assertEqual(result, [2])

    def test_filter_not_found(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Make sure "three" does not exist
        bucket.get('three').delete()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')\
                        .add('bucket', 'three')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .filter_not_found()\
                   .run()

        self.assertEqual(sorted(result), [1, 2])

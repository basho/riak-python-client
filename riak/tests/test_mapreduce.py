# -*- coding: utf-8 -*-

from riak.mapreduce import RiakMapReduce
from riak import key_filter, RiakError


class LinkTests(object):
    def test_store_and_get_links(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new(key=self.key_name, encoded_data='2',
                   content_type='application/octet-stream') \
            .add_link(bucket.new("foo1")) \
            .add_link(bucket.new("foo2"), "tag") \
            .add_link(bucket.new("foo3"), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get(self.key_name)
        links = obj.links
        self.assertEqual(len(links), 3)
        for bucket, key, tag in links:
            if (key == "foo1"):
                self.assertEqual(bucket, self.bucket_name)
            elif (key == "foo2"):
                self.assertEqual(tag, "tag")
            elif (key == "foo3"):
                self.assertEqual(tag, "tag2!@#%^&*)")
            else:
                self.assertEqual(key, "unknown key")

    def test_set_links(self):
        # Create the object
        bucket = self.client.bucket(self.bucket_name)
        o = bucket.new(self.key_name, 2)
        o.links = [(self.bucket_name, "foo1", None),
                   (self.bucket_name, "foo2", "tag"),
                   ("bucket", "foo2", "tag2")]
        o.store()
        obj = bucket.get(self.key_name)
        links = sorted(obj.links, key=lambda x: x[1])
        self.assertEqual(len(links), 3)
        self.assertEqual(links[0][1], "foo1")
        self.assertEqual(links[1][1], "foo2")
        self.assertEqual(links[1][2], "tag")
        self.assertEqual(links[2][1], "foo2")
        self.assertEqual(links[2][2], "tag2")

    def test_link_walking(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2) \
            .add_link(bucket.new("foo1", "test1").store()) \
            .add_link(bucket.new("foo2", "test2").store(), "tag") \
            .add_link(bucket.new("foo3", "test3").store(), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("foo")
        results = obj.link(self.bucket_name).run()
        self.assertEqual(len(results), 3)
        results = obj.link(self.bucket_name, "tag").run()
        self.assertEqual(len(results), 1)


class ErlangMapReduceTests(object):
    def test_erlang_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        bucket.new("bar", 2).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add(self.bucket_name, "foo") \
            .add(self.bucket_name, "bar") \
            .add(self.bucket_name, "baz") \
            .map(["riak_kv_mapreduce", "map_object_value"]) \
            .reduce(["riak_kv_mapreduce", "reduce_set_union"]) \
            .run()
        self.assertEqual(len(result), 2)

    def test_erlang_source_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        strfun_allowed = True
        # Run the map...
        try:
            result = self.client \
                .add(self.bucket_name, "foo") \
                .add(self.bucket_name, "bar") \
                .add(self.bucket_name, "baz") \
                .map("""fun(Object, _KD, _A) ->
            Value = riak_object:get_value(Object),
            [Value]
        end.""", {'language': 'erlang'}).run()
        except RiakError as e:
            if e.value.startswith('May have tried'):
                strfun_allowed = False
        if strfun_allowed:
            self.assertEqual(result, ['2', '3', '4'])

    def test_client_exceptional_paths(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        bucket.new("bar", 2).store()
        bucket.new("baz", 4).store()

        #adding a b-key pair to a bucket input
        with self.assertRaises(ValueError):
            mr = self.client.add(self.bucket_name)
            mr.add(self.bucket_name, 'bar')

        #adding a b-key pair to a query input
        with self.assertRaises(ValueError):
            mr = self.client.search(self.bucket_name, 'fleh')
            mr.add(self.bucket_name, 'bar')

        #adding a key filter to a query input
        with self.assertRaises(ValueError):
            mr = self.client.search(self.bucket_name, 'fleh')
            mr.add_key_filter("tokenize", "-", 1)


class JSMapReduceTests(object):
    def test_javascript_source_map(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        # Run the map...
        mr = self.client.add(self.bucket_name, "foo")
        result = mr.map(
            "function (v) { return [JSON.parse(v.values[0].data)]; }").run()
        self.assertEqual(result, [2])

        # test ASCII-encodable unicode is accepted
        mr.map(u"function (v) { return [JSON.parse(v.values[0].data)]; }")

        # test non-ASCII-encodable unicode is rejected
        self.assertRaises(TypeError, mr.map,
                          u"""
                          function (v) {
                          /* æ */
                            return [JSON.parse(v.values[0].data)];
                          }""")

        # test non-ASCII-encodable string is rejected
        self.assertRaises(TypeError, mr.map,
                          """function (v) {
                               /* æ */
                               return [JSON.parse(v.values[0].data)];
                             }""")

    def test_javascript_named_map(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add(self.bucket_name, "foo") \
            .map("Riak.mapValuesJson") \
            .run()
        self.assertEqual(result, [2])

    def test_javascript_source_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add(self.bucket_name, "foo") \
            .add(self.bucket_name, "bar") \
            .add(self.bucket_name, "baz") \
            .map("function (v) { return [1]; }") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [3])

    def test_javascript_named_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add(self.bucket_name, "foo") \
            .add(self.bucket_name, "bar") \
            .add(self.bucket_name, "baz") \
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
            .add(bucket.name) \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [9])

    def test_javascript_arg_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add(self.bucket_name, "foo", 5) \
            .add(self.bucket_name, "foo", 10) \
            .add(self.bucket_name, "foo", 15) \
            .add(self.bucket_name, "foo", -15) \
            .add(self.bucket_name, "foo", -5) \
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
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("foo", 2).store()
        obj = bucket.get("foo")
        result = obj.map("Riak.mapValuesJson").run()
        self.assertEqual(result, [2])

    def test_mr_list_add(self):
        bucket = self.client.bucket(self.bucket_name)
        for x in range(20):
            bucket.new('baz' + str(x),
                       'bazval' + str(x)).store()
        mr = self.client.add(self.bucket_name, ['baz' + str(x)
                                                for x in range(2, 5)])
        results = mr.map_values().run()
        results.sort()
        self.assertEqual(results,
                         [u'"bazval2"',
                          u'"bazval3"',
                          u'"bazval4"'])

    def test_mr_list_add_two_buckets(self):
        bucket = self.client.bucket(self.bucket_name)
        name2 = self.randname()
        for x in range(10):
            bucket.new('foo' + str(x),
                       'fooval' + str(x)).store()
        bucket = self.client.bucket(name2)
        for x in range(10):
            bucket.new('bar' + str(x),
                       'barval' + str(x)).store()

        mr = self.client.add(self.bucket_name, ['foo' + str(x)
                                                for x in range(2, 4)])
        mr.add(name2, ['bar' + str(x)
                       for x in range(5, 7)])
        results = mr.map_values().run()
        results.sort()

        self.assertEqual(results,
                         [u'"barval5"',
                          u'"barval6"',
                          u'"fooval2"',
                          u'"fooval3"'])

    def test_mr_list_add_mix(self):
        bucket = self.client.bucket("bucket_a")
        for x in range(10):
            bucket.new('foo' + str(x),
                       'fooval' + str(x)).store()
        bucket = self.client.bucket("bucket_b")
        for x in range(10):
            bucket.new('bar' + str(x),
                       'barval' + str(x)).store()

        mr = self.client.add('bucket_a', ['foo' + str(x)
                                          for x in range(2, 4)])
        mr.add('bucket_b', 'bar9')
        mr.add('bucket_b', 'bar2')
        results = mr.map_values().run()
        results.sort()

        self.assertEqual(results,
                         [u'"barval2"',
                          u'"barval9"',
                          u'"fooval2"',
                          u'"fooval3"'])


class MapReduceAliasTests(object):
    """This tests the map reduce aliases"""

    def test_map_values(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', encoded_data='value_1',
                   content_type='text/plain').store()
        bucket.new('two', encoded_data='value_2',
                   content_type='text/plain').store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort()

        self.assertEqual(result, ["value_1", "value_2"])

    def test_map_values_json(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data={'val': 'value_1'}).store()
        bucket.new('two', data={'val': 'value_2'}).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort(key=lambda x: x['val'])

        self.assertEqual(result, [{'val': "value_1"}, {'val': "value_2"}])

    def test_reduce_sum(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sum().run()

        self.assertEqual(result, [3])

    def test_reduce_min(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_min().run()

        self.assertEqual(result, [1])

    def test_reduce_max(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_max().run()

        self.assertEqual(result, [2])

    def test_reduce_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort().run()

        self.assertEqual(result, ["value1", "value2"])

    def test_reduce_sort_custom(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort("""function(x,y) {
           if(x == y) return 0;
           return x > y ? -1 : 1;
        }""").run()

        self.assertEqual(result, ["value2", "value1"])

    def test_reduce_numeric_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_numeric_sort().run()

        self.assertEqual(result, [1, 2])

    def test_reduce_limit(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_limit(1).run()

        self.assertEqual(result, [1])

    def test_reduce_slice(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_slice(1, 2).run()

        self.assertEqual(result, [2])

    def test_filter_not_found(self):
        # Add a value to the bucket
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add(self.bucket_name, 'one')\
                        .add(self.bucket_name, 'two')\
                        .add(self.bucket_name, self.key_name)

        # Use the map_values alias
        result = mr.map_values_json()\
                   .filter_not_found()\
                   .run()

        self.assertEqual(sorted(result), [1, 2])


class MapReduceStreamTests(object):
    def test_stream_results(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        mr = RiakMapReduce(self.client).add(self.bucket_name, 'one')\
                                       .add(self.bucket_name, 'two')
        mr.map_values_json()
        results = []
        for phase, data in mr.stream():
            results.extend(data)

        self.assertEqual(sorted(results), [1, 2])

    def test_stream_cleanoperationsup(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        mr = RiakMapReduce(self.client).add(self.bucket_name, 'one')\
                                       .add(self.bucket_name, 'two')
        mr.map_values_json()
        try:
            for phase, data in mr.stream():
                raise RuntimeError("woops")
        except RuntimeError:
            pass

        # This should not raise an exception
        obj = bucket.get('one')
        self.assertEqual('1', obj.encoded_data)

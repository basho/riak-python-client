# -*- coding: utf-8 -*-
import unittest

from riak.tests import RUN_YZ
from riak.tests.base import IntegrationTestBase
from riak.tests.comparison import Comparison
from riak.tests.yz_setup import yzSetUp, yzTearDown


def wait_for_yz_index(bucket, key, index=None):
    """
    Wait until Solr index has been updated and a value returns from a query.

    :param bucket: Bucket to which indexed value is written
    :type bucket: RiakBucket
    :param key: Key to which value was written
    :type key: str
    """
    while len(bucket.search('_yz_rk:' + key, index=index)['docs']) == 0:
        pass


# YZ index on bucket of the same name
testrun_yz = {'btype': None, 'bucket': 'yzbucket', 'index': 'yzbucket'}
# YZ index on bucket of a different name
testrun_yz_index = {'btype': None,
                    'bucket': 'yzindexbucket',
                    'index': 'yzindex'}


def setUpModule():
    yzSetUp(testrun_yz, testrun_yz_index)


def tearDownModule():
    yzTearDown(testrun_yz, testrun_yz_index)


@unittest.skipUnless(RUN_YZ, 'RUN_YZ is 0')
class YZSearchTests(IntegrationTestBase, unittest.TestCase, Comparison):
    def test_yz_search_from_bucket(self):
        bucket = self.client.bucket(testrun_yz['bucket'])
        bucket.new("user", {"user_s": "Z"}).store()
        wait_for_yz_index(bucket, "user")
        results = bucket.search("user_s:Z")
        self.assertEqual(1, len(results['docs']))
        # TODO: check that docs return useful info
        result = results['docs'][0]
        self.assertIn('_yz_rk', result)
        self.assertEqual(u'user', result['_yz_rk'])
        self.assertIn('_yz_rb', result)
        self.assertEqual(testrun_yz['bucket'], result['_yz_rb'])
        self.assertIn('score', result)
        self.assertIn('user_s', result)
        self.assertEqual(u'Z', result['user_s'])

    def test_yz_search_index_using_bucket(self):
        bucket = self.client.bucket(testrun_yz_index['bucket'])
        bucket.new("feliz",
                   {"name_s": "Felix", "species_s": "Felis catus"}).store()
        wait_for_yz_index(bucket, "feliz", index=testrun_yz_index['index'])
        results = bucket.search('name_s:Felix',
                                index=testrun_yz_index['index'])
        self.assertEqual(1, len(results['docs']))

    def test_yz_search_index_using_wrong_bucket(self):
        bucket = self.client.bucket(testrun_yz_index['bucket'])
        bucket.new("feliz",
                   {"name_s": "Felix", "species_s": "Felis catus"}).store()
        wait_for_yz_index(bucket, "feliz", index=testrun_yz_index['index'])
        with self.assertRaises(Exception):
            bucket.search('name_s:Felix')

    def test_yz_get_search_index(self):
        index = self.client.get_search_index(testrun_yz['bucket'])
        self.assertEqual(testrun_yz['bucket'], index['name'])
        self.assertEqual('_yz_default', index['schema'])
        self.assertEqual(3, index['n_val'])
        with self.assertRaises(Exception):
            self.client.get_search_index('NOT' + testrun_yz['bucket'])

    def test_yz_delete_search_index(self):
        # expected to fail, since there's an attached bucket
        with self.assertRaises(Exception):
            self.client.delete_search_index(testrun_yz['bucket'])
        # detatch bucket from index then delete
        b = self.client.bucket(testrun_yz['bucket'])
        b.set_property('search_index', '_dont_index_')
        self.assertTrue(self.client.delete_search_index(testrun_yz['bucket']))
        # create it again
        self.client.create_search_index(testrun_yz['bucket'], '_yz_default', 3)
        b = self.client.bucket(testrun_yz['bucket'])
        b.set_property('search_index', testrun_yz['bucket'])
        # Wait for index to apply
        indexes = []
        while testrun_yz['bucket'] not in indexes:
            indexes = [i['name'] for i in self.client.list_search_indexes()]

    def test_yz_list_search_indexes(self):
        indexes = self.client.list_search_indexes()
        self.assertIn(testrun_yz['bucket'], [item['name'] for item in indexes])
        self.assertLessEqual(1, len(indexes))

    def test_yz_create_schema(self):
        content = """<?xml version="1.0" encoding="UTF-8" ?>
        <schema name="test" version="1.5">
        <fields>
           <field name="_yz_id" type="_yz_str" indexed="true" stored="true"
            multiValued="false" required="true" />
           <field name="_yz_ed" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_pn" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_fpn" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_vtag" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_rk" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_rb" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_rt" type="_yz_str" indexed="true" stored="true"
            multiValued="false" />
           <field name="_yz_err" type="_yz_str" indexed="true"
            multiValued="false" />
        </fields>
        <uniqueKey>_yz_id</uniqueKey>
        <types>
            <fieldType name="_yz_str" class="solr.StrField"
             sortMissingLast="true" />
        </types>
        </schema>"""
        schema_name = self.randname()
        self.assertTrue(self.client.create_search_schema(schema_name, content))
        schema = self.client.get_search_schema(schema_name)
        self.assertEqual(schema_name, schema['name'])
        self.assertEqual(content, schema['content'])

    def test_yz_create_bad_schema(self):
        bad_content = """
        <derp nope nope, how do i computer?
        """
        with self.assertRaises(Exception):
            self.client.create_search_schema(self.randname(), bad_content)

    def test_yz_search_queries(self):
        bucket = self.client.bucket(testrun_yz['bucket'])
        bucket.new("Z", {"username_s": "Z", "name_s": "ryan",
                         "age_i": 30}).store()
        bucket.new("R", {"username_s": "R", "name_s": "eric",
                         "age_i": 34}).store()
        bucket.new("F", {"username_s": "F", "name_s": "bryan fink",
                         "age_i": 32}).store()
        bucket.new("H", {"username_s": "H", "name_s": "brett",
                         "age_i": 14}).store()
        wait_for_yz_index(bucket, "H")
        # multiterm
        results = bucket.search("username_s:(F OR H)")
        l = len(results['docs'])
        self.assertTrue(l == 2 or l == 3)
        # boolean
        results = bucket.search("username_s:Z AND name_s:ryan")
        self.assertEqual(1, len(results['docs']))
        # range
        results = bucket.search("age_i:[30 TO 33]")
        self.assertEqual(2, len(results['docs']))
        # phrase
        results = bucket.search('name_s:"bryan fink"')
        self.assertEqual(1, len(results['docs']))
        # wildcard
        results = bucket.search('name_s:*ryan*')
        self.assertEqual(2, len(results['docs']))
        # regexp
        results = bucket.search('name_s:/br.*/')
        l = len(results['docs'])
        self.assertTrue(l == 2 or l == 3)
        # Parameters:
        # limit
        results = bucket.search('username_s:*', rows=2)
        self.assertEqual(2, len(results['docs']))
        # sort
        results = bucket.search('username_s:*', sort="age_i asc")
        self.assertEqual(14, int(results['docs'][0]['age_i']))

    def test_yz_search_utf8(self):
        bucket = self.client.bucket(testrun_yz['bucket'])
        body = {"text_ja": u"私はハイビスカスを食べるのが 大好き"}
        bucket.new(self.key_name, body).store()
        wait_for_yz_index(bucket, self.key_name)
        results = bucket.search(u"text_ja:大好き AND  _yz_rk:{0}".
                                format(self.key_name))
        self.assertEqual(1, len(results['docs']))

    def test_yz_multivalued_fields(self):
        bucket = self.client.bucket(testrun_yz['bucket'])
        body = {"groups_ss": ['a', 'b', 'c']}
        bucket.new(self.key_name, body).store()
        wait_for_yz_index(bucket, self.key_name)
        results = bucket.search('groups_ss:* AND _yz_rk:{0}'.
                                format(self.key_name))
        self.assertEqual(1, len(results['docs']))
        doc = results['docs'][0]
        self.assertIn('groups_ss', doc)
        field = doc['groups_ss']
        self.assertIsInstance(field, list)
        self.assertItemsEqual(['a', 'b', 'c'], field)

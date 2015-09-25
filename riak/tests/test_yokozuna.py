# -*- coding: utf-8 -*-
"""
Copyright 2015 Basho Technologies, Inc.

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

import platform
from . import RUN_YZ
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


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


class YZSearchTests(object):
    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_from_bucket(self):
        bucket = self.client.bucket(self.yz['bucket'])
        bucket.new("user", {"user_s": "Z"}).store()
        wait_for_yz_index(bucket, "user")
        results = bucket.search("user_s:Z")
        self.assertEqual(1, len(results['docs']))
        # TODO: check that docs return useful info
        result = results['docs'][0]
        self.assertIn('_yz_rk', result)
        self.assertEqual(u'user', result['_yz_rk'])
        self.assertIn('_yz_rb', result)
        self.assertEqual(self.yz['bucket'], result['_yz_rb'])
        self.assertIn('score', result)
        self.assertIn('user_s', result)
        self.assertEqual(u'Z', result['user_s'])

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_index_using_bucket(self):
        bucket = self.client.bucket(self.yz_index['bucket'])
        bucket.new("feliz",
                   {"name_s": "Felix", "species_s": "Felis catus"}).store()
        wait_for_yz_index(bucket, "feliz", index=self.yz_index['index'])
        results = bucket.search('name_s:Felix', index=self.yz_index['index'])
        self.assertEqual(1, len(results['docs']))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_index_using_wrong_bucket(self):
        bucket = self.client.bucket(self.yz_index['bucket'])
        bucket.new("feliz",
                   {"name_s": "Felix", "species_s": "Felis catus"}).store()
        wait_for_yz_index(bucket, "feliz", index=self.yz_index['index'])
        with self.assertRaises(Exception):
            bucket.search('name_s:Felix')

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_get_search_index(self):
        index = self.client.get_search_index(self.yz['bucket'])
        self.assertEqual(self.yz['bucket'], index['name'])
        self.assertEqual('_yz_default', index['schema'])
        self.assertEqual(3, index['n_val'])
        with self.assertRaises(Exception):
            self.client.get_search_index('NOT' + self.yz['bucket'])

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_delete_search_index(self):
        # expected to fail, since there's an attached bucket
        with self.assertRaises(Exception):
            self.client.delete_search_index(self.yz['bucket'])
        # detatch bucket from index then delete
        b = self.client.bucket(self.yz['bucket'])
        b.set_property('search_index', '_dont_index_')
        self.assertTrue(self.client.delete_search_index(self.yz['bucket']))
        # create it again
        self.client.create_search_index(self.yz['bucket'], '_yz_default', 3)
        b = self.client.bucket(self.yz['bucket'])
        b.set_property('search_index', self.yz['bucket'])
        # Wait for index to apply
        indexes = []
        while self.yz['bucket'] not in indexes:
            indexes = [i['name'] for i in self.client.list_search_indexes()]

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_list_search_indexes(self):
        indexes = self.client.list_search_indexes()
        self.assertIn(self.yz['bucket'], [item['name'] for item in indexes])
        self.assertLessEqual(1, len(indexes))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
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

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_create_bad_schema(self):
        bad_content = """
        <derp nope nope, how do i computer?
        """
        with self.assertRaises(Exception):
            self.client.create_search_schema(self.randname(), bad_content)

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_queries(self):
        bucket = self.client.bucket(self.yz['bucket'])
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
        self.assertEqual(2, len(results['docs']))
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
        self.assertEqual(2, len(results['docs']))
        # Parameters:
        # limit
        results = bucket.search('username_s:*', rows=2)
        self.assertEqual(2, len(results['docs']))
        # sort
        results = bucket.search('username_s:*', sort="age_i asc")
        self.assertEqual(14, int(results['docs'][0]['age_i']))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_utf8(self):
        bucket = self.client.bucket(self.yz['bucket'])
        body = {"text_ja": u"私はハイビスカスを食べるのが 大好き"}
        bucket.new(self.key_name, body).store()
        wait_for_yz_index(bucket, self.key_name)
        results = bucket.search(u"text_ja:大好き AND  _yz_rk:{0}".
                                format(self.key_name))
        self.assertEqual(1, len(results['docs']))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_multivalued_fields(self):
        bucket = self.client.bucket(self.yz['bucket'])
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

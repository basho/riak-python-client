# -*- coding: utf-8 -*-
import os
import platform
import time
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

RUN_YZ = int(os.environ.get('RUN_YZ', '0'))


class YZSearchTests(object):
    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_from_bucket(self):
        bucket = self.client.bucket('yztest')
        bucket.new("user", {"user_s": "Z"}).store()
        time.sleep(1)
        results = bucket.search("user_s:Z")
        self.assertEquals(1, len(results['docs']))
        # TODO: check that docs return useful info
        result = results['docs'][0]
        self.assertEquals(True, result.has_key('_yz_rk'))
        self.assertEquals(u'user', result['_yz_rk'])
        self.assertEquals(True, result.has_key('_yz_rb'))
        self.assertEquals(u'yztest', result['_yz_rb'])
        self.assertEquals(True, result.has_key('score'))
        self.assertEquals(True, result.has_key('user_s'))
        self.assertEquals(u'Z', result['user_s'])

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_get_search_index(self):
        index = self.client.get_search_index('yztest')
        self.assertEquals('yztest', index['name'])
        self.assertEquals('_yz_default', index['schema'])
        self.assertRaises(Exception, self.client.get_search_index, 'NOTyztest')

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_delete_search_index(self):
        testrun_yz_bucket = 'yztest'
        # expected to fail, since there's an attached bucket
        self.assertRaises(Exception,
            self.client.delete_search_index, testrun_yz_bucket)
        # detatch bucket from index then delete
        b = self.client.bucket(testrun_yz_bucket)
        b.set_property('yz_index', '')
        resp = self.client.delete_search_index(testrun_yz_bucket)
        self.assertEquals(True, resp)
        # create it again
        self.client.create_search_index(testrun_yz_bucket)
        b = self.client.bucket(testrun_yz_bucket)
        b.set_property('yz_index', testrun_yz_bucket)
        time.sleep(1) # wait for index to apply

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_list_search_indexes(self):
        indexes = self.client.list_search_indexes()
        self.assertEquals(1, len(indexes))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_create_schema(self):
        content = """<?xml version="1.0" encoding="UTF-8" ?>
        <schema name="test" version="1.5">
        <fields>
           <field name="_yz_id" type="_yz_str" indexed="true" stored="true" required="true" />
           <field name="_yz_ed" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_pn" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_fpn" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_vtag" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_node" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_rk" type="_yz_str" indexed="true" stored="true"/>
           <field name="_yz_rb" type="_yz_str" indexed="true" stored="true"/>
        </fields>
        <uniqueKey>_yz_id</uniqueKey>
        <types>
            <fieldType name="_yz_str" class="solr.StrField" sortMissingLast="true" />
        </types>
        </schema>"""
        schema_name = 'yzgoodschema'
        resp = self.client.create_search_schema(schema_name, content)
        self.assertEquals(True, resp)
        schema = self.client.get_search_schema(schema_name)
        self.assertEquals(schema_name, schema['name'])
        self.assertEquals(content, schema['content'])

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_create_bad_schema(self):
        bad_content = """
        <derp nope nope, how do i computer?
        """
        self.assertRaises(Exception, self.client.create_search_schema,
                          'yzbadschema', bad_content)
    

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_queries(self):
        bucket = self.client.bucket('yztest')
        bucket.new("Z", {"username_s": "Z", "name_s": "ryan",  "age_i":30}).store()
        bucket.new("R", {"username_s": "R", "name_s": "eric",  "age_i":34}).store()
        bucket.new("F", {"username_s": "F", "name_s": "bryan fink", "age_i":32}).store()
        bucket.new("H", {"username_s": "H", "name_s": "brett", "age_i":14}).store()
        time.sleep(1)
        # multiterm
        results = bucket.search("username_s:(F OR H)")
        self.assertEquals(2, len(results['docs']))
        # boolean
        results = bucket.search("username_s:Z AND name_s:ryan")
        self.assertEquals(1, len(results['docs']))
        # range
        results = bucket.search("age_i:[30 TO 33]")
        self.assertEquals(2, len(results['docs']))
        # phrase
        results = bucket.search('name_s:"bryan fink"')
        self.assertEquals(1, len(results['docs']))
        # wildcard
        results = bucket.search('name_s:*ryan*')
        self.assertEquals(2, len(results['docs']))
        # regexp
        results = bucket.search('name_s:/br.*/')
        self.assertEquals(2, len(results['docs']))
        # Parameters:
        # limit
        results = bucket.search('username_s:*', rows=2)
        self.assertEquals(2, len(results['docs']))
        # sort
        results = bucket.search('username_s:*', sort="age_i asc")
        self.assertEquals(14, int(results['docs'][0]['age_i']))

    @unittest.skipUnless(RUN_YZ, 'RUN_YZ is undefined')
    def test_yz_search_utf8(self):
        bucket = self.client.bucket('yztest')
        body = {"text_ja" : u"私はハイビスカスを食べるのが 大好き"}
        bucket.new("shift_jis", body).store()
        # TODO: fails due to lack of direct PB unicode support
        # results = bucket.search(u"text_ja:大好き")
        # self.assertEquals(1, len(results['docs']))

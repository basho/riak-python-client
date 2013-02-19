# -*- coding: utf-8 -*-
import os
import platform
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

SKIP_SEARCH = int(os.environ.get('SKIP_SEARCH', '0'))


class EnableSearchTests(object):
    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_bucket_search_enabled(self):
        bucket = self.client.bucket(self.bucket_name)
        self.assertFalse(bucket.search_enabled())

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_enable_search_commit_hook(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.clear_properties()
        self.assertFalse(self.create_client().
                         bucket(self.search_bucket).
                         search_enabled())
        bucket.enable_search()
        self.assertTrue(self.create_client().
                        bucket(self.search_bucket).
                        search_enabled())

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_disable_search_commit_hook(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.clear_properties()
        bucket.enable_search()
        self.assertTrue(self.create_client().bucket(self.search_bucket)
                            .search_enabled())
        bucket.disable_search()
        self.assertFalse(self.create_client().bucket(self.search_bucket)
                             .search_enabled())
        bucket.enable_search()


class SolrSearchTests(object):
    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_add_document_to_index(self):
        self.client.solr.add(self.search_bucket,
                             {"id": "doc", "username": "tony"})
        results = self.client.solr.search(self.search_bucket,
                                          "username:tony")
        self.assertEquals("tony", results['docs'][0]['username'])

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_add_multiple_documents_to_iindex(self):
        self.client.solr.add(self.search_bucket,
                             {"id": "dizzy", "username": "dizzy"},
                             {"id": "russell", "username": "russell"})
        results = self.client.solr\
            .search(self.search_bucket, "username:russell OR username:dizzy")
        self.assertEquals(2, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_id(self):
        self.client.solr.add(self.search_bucket,
                             {"id": "dizzy", "username": "dizzy"},
                             {"id": "russell", "username": "russell"})
        self.client.solr.delete(self.search_bucket, docs=["dizzy"])
        results = self.client.solr\
            .search(self.search_bucket, "username:russell OR username:dizzy")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_query(self):
        self.client.solr.add(self.search_bucket,
                             {"id": "dizzy", "username": "dizzy"},
                             {"id": "russell", "username": "russell"})
        self.client.solr\
            .delete(self.search_bucket,
                    queries=["username:dizzy", "username:russell"])
        results = self.client.solr\
            .search(self.search_bucket, "username:russell OR username:dizzy")
        self.assertEquals(0, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_query_and_id(self):
        self.client.solr.add(self.search_bucket,
                             {"id": "dizzy", "username": "dizzy"},
                             {"id": "russell", "username": "russell"})
        self.client.solr.delete(self.search_bucket,
                                docs=["dizzy"],
                                queries=["username:russell"])
        results = self.client.solr\
            .search(self.search_bucket,
                    "username:russell OR username:dizzy")
        self.assertEquals(0, len(results['docs']))


class SearchTests(object):
    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_from_bucket(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_with_params_from_bucket(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage", wt="xml")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_with_params(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.solr.search(self.search_bucket,
                                          "username:roidrage", wt="xml")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search(self):
        bucket = self.client.bucket(self.search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.solr.search(self.search_bucket,
                                          "username:roidrage")
        self.assertEquals(1, len(results["docs"]))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_search_integration(self):
        # Create some objects to search across...
        bucket = self.client.bucket(self.search_bucket)
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        bucket.new("two", {"foo": "two", "bar": "green"}).store()
        bucket.new("three", {"foo": "three", "bar": "blue"}).store()
        bucket.new("four", {"foo": "four", "bar": "orange"}).store()
        bucket.new("five", {"foo": "five", "bar": "yellow"}).store()

        # Run some operations...
        results = self.client.solr.search(self.search_bucket,
                                          "foo:one OR foo:two")
        if (len(results) == 0):
            print "\n\nNot running test \"testSearchIntegration()\".\n"
            print """Please ensure that you have installed the Riak
            Search hook on bucket \"searchbucket\" by running
            \"bin/search-cmd install searchbucket\".\n\n"""
            return
        self.assertEqual(len(results['docs']), 2)
        query = "(foo:one OR foo:two OR foo:three OR foo:four) AND\
                 (NOT bar:green)"
        results = self.client.solr.search(self.search_bucket, query)

        self.assertEqual(len(results['docs']), 3)

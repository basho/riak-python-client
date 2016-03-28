# -*- coding: utf-8 -*-
from __future__ import print_function

import unittest

from riak.tests import RUN_SEARCH, RUN_YZ
from riak.tests.base import IntegrationTestBase

testrun_search_bucket = 'searchbucket'


def setUpModule():
    if RUN_SEARCH and not RUN_YZ:
        c = IntegrationTestBase.create_client()
        b = c.bucket(testrun_search_bucket)
        b.enable_search()
        c.close()


def tearDownModule():
    if RUN_SEARCH and not RUN_YZ:
        c = IntegrationTestBase.create_client()
        b = c.bucket(testrun_search_bucket)
        b.clear_properties()
        c.close()


@unittest.skipUnless(RUN_SEARCH, 'RUN_SEARCH is 0')
class EnableSearchTests(IntegrationTestBase, unittest.TestCase):
    def test_bucket_search_enabled(self):
        bucket = self.client.bucket(self.bucket_name)
        self.assertFalse(bucket.search_enabled())

    def test_enable_search_commit_hook(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.clear_properties()

        c = self.create_client()
        self.assertFalse(c.bucket(testrun_search_bucket).search_enabled())
        c.close()

        bucket.enable_search()

        c = self.create_client()
        self.assertTrue(c.bucket(testrun_search_bucket).search_enabled())
        c.close()

    def test_disable_search_commit_hook(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.clear_properties()
        bucket.enable_search()

        c = self.create_client()
        self.assertTrue(c.bucket(testrun_search_bucket).search_enabled())
        c.close()

        bucket.disable_search()

        c = self.create_client()
        self.assertFalse(c.bucket(testrun_search_bucket).search_enabled())
        c.close()

        bucket.enable_search()


@unittest.skipUnless(RUN_SEARCH, 'RUN_SEARCH is 0')
class SolrSearchTests(IntegrationTestBase, unittest.TestCase):
    def test_add_document_to_index(self):
        self.client.fulltext_add(testrun_search_bucket,
                                 [{"id": "doc", "username": "tony"}])
        results = self.client.fulltext_search(testrun_search_bucket,
                                              "username:tony")
        self.assertEqual("tony", results['docs'][0]['username'])

    def test_add_multiple_documents_to_index(self):
        self.client.fulltext_add(
            testrun_search_bucket,
            [{"id": "dizzy", "username": "dizzy"},
             {"id": "russell", "username": "russell"}])
        results = self.client.fulltext_search(
            testrun_search_bucket, "username:russell OR username:dizzy")
        self.assertEqual(2, len(results['docs']))

    def test_delete_documents_from_search_by_id(self):
        self.client.fulltext_add(
            testrun_search_bucket,
            [{"id": "dizzy", "username": "dizzy"},
             {"id": "russell", "username": "russell"}])
        self.client.fulltext_delete(testrun_search_bucket, docs=["dizzy"])
        results = self.client.fulltext_search(
            testrun_search_bucket, "username:russell OR username:dizzy")
        self.assertEqual(1, len(results['docs']))

    def test_delete_documents_from_search_by_query(self):
        self.client.fulltext_add(
            testrun_search_bucket,
            [{"id": "dizzy", "username": "dizzy"},
             {"id": "russell", "username": "russell"}])
        self.client.fulltext_delete(
            testrun_search_bucket,
            queries=["username:dizzy", "username:russell"])
        results = self.client.fulltext_search(
            testrun_search_bucket, "username:russell OR username:dizzy")
        self.assertEqual(0, len(results['docs']))

    def test_delete_documents_from_search_by_query_and_id(self):
        self.client.fulltext_add(
            testrun_search_bucket,
            [{"id": "dizzy", "username": "dizzy"},
             {"id": "russell", "username": "russell"}])
        self.client.fulltext_delete(
            testrun_search_bucket,
            docs=["dizzy"],
            queries=["username:russell"])
        results = self.client.fulltext_search(
            testrun_search_bucket,
            "username:russell OR username:dizzy")
        self.assertEqual(0, len(results['docs']))


@unittest.skipUnless(RUN_SEARCH, 'RUN_SEARCH is 0')
class SearchTests(IntegrationTestBase, unittest.TestCase):
    def test_solr_search_from_bucket(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage")
        self.assertEqual(1, len(results['docs']))

    def test_solr_search_with_params_from_bucket(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage", wt="xml")
        self.assertEqual(1, len(results['docs']))

    def test_solr_search_with_params(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.fulltext_search(
            testrun_search_bucket,
            "username:roidrage", wt="xml")
        self.assertEqual(1, len(results['docs']))

    def test_solr_search(self):
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.fulltext_search(testrun_search_bucket,
                                              "username:roidrage")
        self.assertEqual(1, len(results["docs"]))

    def test_search_integration(self):
        # Create some objects to search across...
        bucket = self.client.bucket(testrun_search_bucket)
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        bucket.new("two", {"foo": "two", "bar": "green"}).store()
        bucket.new("three", {"foo": "three", "bar": "blue"}).store()
        bucket.new("four", {"foo": "four", "bar": "orange"}).store()
        bucket.new("five", {"foo": "five", "bar": "yellow"}).store()

        # Run some operations...
        results = self.client.fulltext_search(testrun_search_bucket,
                                              "foo:one OR foo:two")
        if (len(results) == 0):
            print("\n\nNot running test \"testSearchIntegration()\".\n")
            print("""Please ensure that you have installed the Riak
            Search hook on bucket \"searchbucket\" by running
            \"bin/search-cmd install searchbucket\".\n\n""")
            return
        self.assertEqual(len(results['docs']), 2)
        query = "(foo:one OR foo:two OR foo:three OR foo:four) AND\
                 (NOT bar:green)"
        results = self.client.fulltext_search(testrun_search_bucket, query)

        self.assertEqual(len(results['docs']), 3)

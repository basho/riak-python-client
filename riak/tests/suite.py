import unittest
import riak.tests.test_server_test
import os.path

def additional_tests():
    top_level = os.path.join(os.path.dirname(__file__), "../../")
    start_dir = os.path.dirname(__file__)
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().discover(start_dir,
        top_level_dir=top_level))
    return suite

import unittest


class MiscTests(unittest.TestCase):
    def test_timeout_validation(self):
        from riak.client.operations import _validate_timeout
        # valid cases
        try:
            _validate_timeout(None)
            _validate_timeout(None, infinity_ok=True)
            _validate_timeout('infinity', infinity_ok=True)
            _validate_timeout(1234)
            _validate_timeout(1234567898765432123456789)
        except ValueError:
            self.fail('_validate_timeout() unexpectedly raised ValueError')
        # invalid cases
        with self.assertRaises(ValueError):
            _validate_timeout('infinity')
        with self.assertRaises(ValueError):
            _validate_timeout('infinity-foo')
        with self.assertRaises(ValueError):
            _validate_timeout('foobarbaz')
        with self.assertRaises(ValueError):
            _validate_timeout('1234')
        with self.assertRaises(ValueError):
            _validate_timeout(0)
        with self.assertRaises(ValueError):
            _validate_timeout(12.34)

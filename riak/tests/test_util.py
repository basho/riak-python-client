import datetime
import unittest

from riak.util import is_timeseries_supported, \
    datetime_from_unix_time_millis, \
    unix_time_millis


class UtilUnitTests(unittest.TestCase):
    # NB:
    # 144379690 secs, 987 msecs past epoch
    # 144379690987 total msecs past epoch
    def test_conv_ms_timestamp_to_datetime_and_back(self):
        if is_timeseries_supported():
            # this is what would be stored in Riak TS
            v = 144379690987
            dt = datetime_from_unix_time_millis(v)

            # This is how Python represents the above
            utp = 144379690.987000
            dtp = datetime.datetime.utcfromtimestamp(utp)
            self.assertEqual(dt, dtp)

            utm = unix_time_millis(dt)
            self.assertEqual(v, utm)
        else:
            pass

    def test_conv_datetime_to_unix_millis(self):
        # This is the "native" Python unix timestamp including
        # microseconds, as float. timedelta "total_seconds()"
        # returns a value like this
        if is_timeseries_supported():
            v = 144379690.987000
            d = datetime.datetime.utcfromtimestamp(v)
            utm = unix_time_millis(d)
            self.assertEqual(utm, 144379690987)
        else:
            pass

    def test_unix_millis_validation(self):
        v = 144379690.987
        with self.assertRaises(ValueError):
            datetime_from_unix_time_millis(v)

    def test_unix_millis_small_value(self):
        if is_timeseries_supported():
            # this is what would be stored in Riak TS
            v = 1001
            dt = datetime_from_unix_time_millis(v)

            # This is how Python represents the above
            utp = 1.001
            dtp = datetime.datetime.utcfromtimestamp(utp)
            self.assertEqual(dt, dtp)

            utm = unix_time_millis(dt)
            self.assertEqual(v, utm)
        else:
            pass

    def test_is_timeseries_supported(self):
        v = (2, 7, 10)
        self.assertEqual(True, is_timeseries_supported(v))
        v = (2, 7, 11)
        self.assertEqual(True, is_timeseries_supported(v))
        v = (2, 7, 12)
        self.assertEqual(True, is_timeseries_supported(v))
        v = (3, 3, 6)
        self.assertEqual(False, is_timeseries_supported(v))
        v = (3, 4, 3)
        self.assertEqual(False, is_timeseries_supported(v))
        v = (3, 4, 4)
        self.assertEqual(True, is_timeseries_supported(v))
        v = (3, 4, 5)
        self.assertEqual(True, is_timeseries_supported(v))
        v = (3, 5, 0)
        self.assertEqual(False, is_timeseries_supported(v))
        v = (3, 5, 1)
        self.assertEqual(True, is_timeseries_supported(v))

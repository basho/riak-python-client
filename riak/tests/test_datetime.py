# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import datetime
import unittest

from riak.util import epoch, epoch_tz, \
        unix_time_millis

# NB: without tzinfo, this is UTC
ts0 = datetime.datetime(2015, 1, 1, 12, 1, 2, 987000)
ts0_ts = 1420113662987
ts0_ts_pst = 1420142462987


class DatetimeUnitTests(unittest.TestCase):
    def test_get_unix_time_without_tzinfo(self):
        self.assertIsNone(epoch.tzinfo)
        self.assertIsNotNone(epoch_tz.tzinfo)
        self.assertIsNone(ts0.tzinfo)
        utm = unix_time_millis(ts0)
        self.assertEqual(utm, ts0_ts)

    def test_get_unix_time_with_tzinfo(self):
        try:
            import pytz
            tz = pytz.timezone('America/Los_Angeles')
            ts0_pst = tz.localize(ts0)
            utm = unix_time_millis(ts0_pst)
            self.assertEqual(utm, ts0_ts_pst)
        except ImportError:
            pass

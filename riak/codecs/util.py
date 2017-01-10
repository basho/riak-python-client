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

import riak.pb.messages


def parse_pbuf_msg(msg_code, data):
    pbclass = riak.pb.messages.MESSAGE_CLASSES.get(msg_code, None)
    if pbclass is None:
        return None
    pbo = pbclass()
    pbo.ParseFromString(data)
    return pbo

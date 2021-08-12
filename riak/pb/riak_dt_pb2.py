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

# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: riak_dt.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection

from six import *

DESCRIPTOR = _descriptor.FileDescriptor(
  name="riak_dt.proto",
  package="",
  serialized_pb=b'\n\rriak_dt.proto\"\x85\x01\n\x08MapField\x12\x0c\n\x04name\x18\x01 \x02(\x0c\x12$\n\x04type\x18\x02 \x02(\x0e\x32\x16.MapField.MapFieldType\"E\n\x0cMapFieldType\x12\x0b\n\x07\x43OUNTER\x10\x01\x12\x07\n\x03SET\x10\x02\x12\x0c\n\x08REGISTER\x10\x03\x12\x08\n\x04\x46LAG\x10\x04\x12\x07\n\x03MAP\x10\x05\"\x98\x01\n\x08MapEntry\x12\x18\n\x05\x66ield\x18\x01 \x02(\x0b\x32\t.MapField\x12\x15\n\rcounter_value\x18\x02 \x01(\x12\x12\x11\n\tset_value\x18\x03 \x03(\x0c\x12\x16\n\x0eregister_value\x18\x04 \x01(\x0c\x12\x12\n\nflag_value\x18\x05 \x01(\x08\x12\x1c\n\tmap_value\x18\x06 \x03(\x0b\x32\t.MapEntry\"\xcf\x01\n\nDtFetchReq\x12\x0e\n\x06\x62ucket\x18\x01 \x02(\x0c\x12\x0b\n\x03key\x18\x02 \x02(\x0c\x12\x0c\n\x04type\x18\x03 \x02(\x0c\x12\t\n\x01r\x18\x04 \x01(\r\x12\n\n\x02pr\x18\x05 \x01(\r\x12\x14\n\x0c\x62\x61sic_quorum\x18\x06 \x01(\x08\x12\x13\n\x0bnotfound_ok\x18\x07 \x01(\x08\x12\x0f\n\x07timeout\x18\x08 \x01(\r\x12\x15\n\rsloppy_quorum\x18\t \x01(\x08\x12\r\n\x05n_val\x18\n \x01(\r\x12\x1d\n\x0finclude_context\x18\x0b \x01(\x08:\x04true\"x\n\x07\x44tValue\x12\x15\n\rcounter_value\x18\x01 \x01(\x12\x12\x11\n\tset_value\x18\x02 \x03(\x0c\x12\x1c\n\tmap_value\x18\x03 \x03(\x0b\x32\t.MapEntry\x12\x11\n\thll_value\x18\x04 \x01(\x04\x12\x12\n\ngset_value\x18\x05 \x03(\x0c\"\x9a\x01\n\x0b\x44tFetchResp\x12\x0f\n\x07\x63ontext\x18\x01 \x01(\x0c\x12#\n\x04type\x18\x02 \x02(\x0e\x32\x15.DtFetchResp.DataType\x12\x17\n\x05value\x18\x03 \x01(\x0b\x32\x08.DtValue\"<\n\x08\x44\x61taType\x12\x0b\n\x07\x43OUNTER\x10\x01\x12\x07\n\x03SET\x10\x02\x12\x07\n\x03MAP\x10\x03\x12\x07\n\x03HLL\x10\x04\x12\x08\n\x04GSET\x10\x05\"\x1e\n\tCounterOp\x12\x11\n\tincrement\x18\x01 \x01(\x12\"&\n\x05SetOp\x12\x0c\n\x04\x61\x64\x64s\x18\x01 \x03(\x0c\x12\x0f\n\x07removes\x18\x02 \x03(\x0c\"\x16\n\x06GSetOp\x12\x0c\n\x04\x61\x64\x64s\x18\x01 \x03(\x0c\"\x15\n\x05HllOp\x12\x0c\n\x04\x61\x64\x64s\x18\x01 \x03(\x0c\"\xd1\x01\n\tMapUpdate\x12\x18\n\x05\x66ield\x18\x01 \x02(\x0b\x32\t.MapField\x12\x1e\n\ncounter_op\x18\x02 \x01(\x0b\x32\n.CounterOp\x12\x16\n\x06set_op\x18\x03 \x01(\x0b\x32\x06.SetOp\x12\x13\n\x0bregister_op\x18\x04 \x01(\x0c\x12\"\n\x07\x66lag_op\x18\x05 \x01(\x0e\x32\x11.MapUpdate.FlagOp\x12\x16\n\x06map_op\x18\x06 \x01(\x0b\x32\x06.MapOp\"!\n\x06\x46lagOp\x12\n\n\x06\x45NABLE\x10\x01\x12\x0b\n\x07\x44ISABLE\x10\x02\"@\n\x05MapOp\x12\x1a\n\x07removes\x18\x01 \x03(\x0b\x32\t.MapField\x12\x1b\n\x07updates\x18\x02 \x03(\x0b\x32\n.MapUpdate\"\x88\x01\n\x04\x44tOp\x12\x1e\n\ncounter_op\x18\x01 \x01(\x0b\x32\n.CounterOp\x12\x16\n\x06set_op\x18\x02 \x01(\x0b\x32\x06.SetOp\x12\x16\n\x06map_op\x18\x03 \x01(\x0b\x32\x06.MapOp\x12\x16\n\x06hll_op\x18\x04 \x01(\x0b\x32\x06.HllOp\x12\x18\n\x07gset_op\x18\x05 \x01(\x0b\x32\x07.GSetOp\"\xf1\x01\n\x0b\x44tUpdateReq\x12\x0e\n\x06\x62ucket\x18\x01 \x02(\x0c\x12\x0b\n\x03key\x18\x02 \x01(\x0c\x12\x0c\n\x04type\x18\x03 \x02(\x0c\x12\x0f\n\x07\x63ontext\x18\x04 \x01(\x0c\x12\x11\n\x02op\x18\x05 \x02(\x0b\x32\x05.DtOp\x12\t\n\x01w\x18\x06 \x01(\r\x12\n\n\x02\x64w\x18\x07 \x01(\r\x12\n\n\x02pw\x18\x08 \x01(\r\x12\x1a\n\x0breturn_body\x18\t \x01(\x08:\x05\x66\x61lse\x12\x0f\n\x07timeout\x18\n \x01(\r\x12\x15\n\rsloppy_quorum\x18\x0b \x01(\x08\x12\r\n\x05n_val\x18\x0c \x01(\r\x12\x1d\n\x0finclude_context\x18\r \x01(\x08:\x04true\"\x9b\x01\n\x0c\x44tUpdateResp\x12\x0b\n\x03key\x18\x01 \x01(\x0c\x12\x0f\n\x07\x63ontext\x18\x02 \x01(\x0c\x12\x15\n\rcounter_value\x18\x03 \x01(\x12\x12\x11\n\tset_value\x18\x04 \x03(\x0c\x12\x1c\n\tmap_value\x18\x05 \x03(\x0b\x32\t.MapEntry\x12\x11\n\thll_value\x18\x06 \x01(\x04\x12\x12\n\ngset_value\x18\x07 \x03(\x0c\x42#\n\x17\x63om.basho.riak.protobufB\x08RiakDtPB')  # NOQA E501


_MAPFIELD_MAPFIELDTYPE = _descriptor.EnumDescriptor(
  name="MapFieldType",
  full_name="MapField.MapFieldType",
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name="COUNTER", index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="SET", index=1, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="REGISTER", index=2, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="FLAG", index=3, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="MAP", index=4, number=5,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=82,
  serialized_end=151,
)

_DTFETCHRESP_DATATYPE = _descriptor.EnumDescriptor(
  name="DataType",
  full_name="DtFetchResp.DataType",
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name="COUNTER", index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="SET", index=1, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="MAP", index=2, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="HLL", index=3, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="GSET", index=4, number=5,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=735,
  serialized_end=795,
)

_MAPUPDATE_FLAGOP = _descriptor.EnumDescriptor(
  name="FlagOp",
  full_name="MapUpdate.FlagOp",
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name="ENABLE", index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name="DISABLE", index=1, number=2,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1093,
  serialized_end=1126,
)


_MAPFIELD = _descriptor.Descriptor(
  name="MapField",
  full_name="MapField",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="name", full_name="MapField.name", index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="type", full_name="MapField.type", index=1,
      number=2, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _MAPFIELD_MAPFIELDTYPE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=18,
  serialized_end=151,
)


_MAPENTRY = _descriptor.Descriptor(
  name="MapEntry",
  full_name="MapEntry",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="field", full_name="MapEntry.field", index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="counter_value", full_name="MapEntry.counter_value", index=1,
      number=2, type=18, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="set_value", full_name="MapEntry.set_value", index=2,
      number=3, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="register_value", full_name="MapEntry.register_value", index=3,
      number=4, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="flag_value", full_name="MapEntry.flag_value", index=4,
      number=5, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="map_value", full_name="MapEntry.map_value", index=5,
      number=6, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=154,
  serialized_end=306,
)


_DTFETCHREQ = _descriptor.Descriptor(
  name="DtFetchReq",
  full_name="DtFetchReq",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="bucket", full_name="DtFetchReq.bucket", index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="key", full_name="DtFetchReq.key", index=1,
      number=2, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="type", full_name="DtFetchReq.type", index=2,
      number=3, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="r", full_name="DtFetchReq.r", index=3,
      number=4, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="pr", full_name="DtFetchReq.pr", index=4,
      number=5, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="basic_quorum", full_name="DtFetchReq.basic_quorum", index=5,
      number=6, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="notfound_ok", full_name="DtFetchReq.notfound_ok", index=6,
      number=7, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="timeout", full_name="DtFetchReq.timeout", index=7,
      number=8, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="sloppy_quorum", full_name="DtFetchReq.sloppy_quorum", index=8,
      number=9, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="n_val", full_name="DtFetchReq.n_val", index=9,
      number=10, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="include_context", full_name="DtFetchReq.include_context", index=10,
      number=11, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=True,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=309,
  serialized_end=516,
)


_DTVALUE = _descriptor.Descriptor(
  name="DtValue",
  full_name="DtValue",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="counter_value", full_name="DtValue.counter_value", index=0,
      number=1, type=18, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="set_value", full_name="DtValue.set_value", index=1,
      number=2, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="map_value", full_name="DtValue.map_value", index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="hll_value", full_name="DtValue.hll_value", index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="gset_value", full_name="DtValue.gset_value", index=4,
      number=5, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=518,
  serialized_end=638,
)


_DTFETCHRESP = _descriptor.Descriptor(
  name="DtFetchResp",
  full_name="DtFetchResp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="context", full_name="DtFetchResp.context", index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="type", full_name="DtFetchResp.type", index=1,
      number=2, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="value", full_name="DtFetchResp.value", index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _DTFETCHRESP_DATATYPE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=641,
  serialized_end=795,
)


_COUNTEROP = _descriptor.Descriptor(
  name="CounterOp",
  full_name="CounterOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="increment", full_name="CounterOp.increment", index=0,
      number=1, type=18, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=797,
  serialized_end=827,
)


_SETOP = _descriptor.Descriptor(
  name="SetOp",
  full_name="SetOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="adds", full_name="SetOp.adds", index=0,
      number=1, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="removes", full_name="SetOp.removes", index=1,
      number=2, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=829,
  serialized_end=867,
)


_GSETOP = _descriptor.Descriptor(
  name="GSetOp",
  full_name="GSetOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="adds", full_name="GSetOp.adds", index=0,
      number=1, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=869,
  serialized_end=891,
)


_HLLOP = _descriptor.Descriptor(
  name="HllOp",
  full_name="HllOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="adds", full_name="HllOp.adds", index=0,
      number=1, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=893,
  serialized_end=914,
)


_MAPUPDATE = _descriptor.Descriptor(
  name="MapUpdate",
  full_name="MapUpdate",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="field", full_name="MapUpdate.field", index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="counter_op", full_name="MapUpdate.counter_op", index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="set_op", full_name="MapUpdate.set_op", index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="register_op", full_name="MapUpdate.register_op", index=3,
      number=4, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="flag_op", full_name="MapUpdate.flag_op", index=4,
      number=5, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="map_op", full_name="MapUpdate.map_op", index=5,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _MAPUPDATE_FLAGOP,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=917,
  serialized_end=1126,
)


_MAPOP = _descriptor.Descriptor(
  name="MapOp",
  full_name="MapOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="removes", full_name="MapOp.removes", index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="updates", full_name="MapOp.updates", index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1128,
  serialized_end=1192,
)


_DTOP = _descriptor.Descriptor(
  name="DtOp",
  full_name="DtOp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="counter_op", full_name="DtOp.counter_op", index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="set_op", full_name="DtOp.set_op", index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="map_op", full_name="DtOp.map_op", index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="hll_op", full_name="DtOp.hll_op", index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="gset_op", full_name="DtOp.gset_op", index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1195,
  serialized_end=1331,
)


_DTUPDATEREQ = _descriptor.Descriptor(
  name="DtUpdateReq",
  full_name="DtUpdateReq",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="bucket", full_name="DtUpdateReq.bucket", index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="key", full_name="DtUpdateReq.key", index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="type", full_name="DtUpdateReq.type", index=2,
      number=3, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="context", full_name="DtUpdateReq.context", index=3,
      number=4, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="op", full_name="DtUpdateReq.op", index=4,
      number=5, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="w", full_name="DtUpdateReq.w", index=5,
      number=6, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="dw", full_name="DtUpdateReq.dw", index=6,
      number=7, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="pw", full_name="DtUpdateReq.pw", index=7,
      number=8, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="return_body", full_name="DtUpdateReq.return_body", index=8,
      number=9, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="timeout", full_name="DtUpdateReq.timeout", index=9,
      number=10, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="sloppy_quorum", full_name="DtUpdateReq.sloppy_quorum", index=10,
      number=11, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="n_val", full_name="DtUpdateReq.n_val", index=11,
      number=12, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="include_context", full_name="DtUpdateReq.include_context", index=12,
      number=13, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=True,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1334,
  serialized_end=1575,
)


_DTUPDATERESP = _descriptor.Descriptor(
  name="DtUpdateResp",
  full_name="DtUpdateResp",
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name="key", full_name="DtUpdateResp.key", index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="context", full_name="DtUpdateResp.context", index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="counter_value", full_name="DtUpdateResp.counter_value", index=2,
      number=3, type=18, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="set_value", full_name="DtUpdateResp.set_value", index=3,
      number=4, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="map_value", full_name="DtUpdateResp.map_value", index=4,
      number=5, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="hll_value", full_name="DtUpdateResp.hll_value", index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name="gset_value", full_name="DtUpdateResp.gset_value", index=6,
      number=7, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1578,
  serialized_end=1733,
)

_MAPFIELD.fields_by_name["type"].enum_type = _MAPFIELD_MAPFIELDTYPE
_MAPFIELD_MAPFIELDTYPE.containing_type = _MAPFIELD;  # NOQA E703
_MAPENTRY.fields_by_name["field"].message_type = _MAPFIELD
_MAPENTRY.fields_by_name["map_value"].message_type = _MAPENTRY
_DTVALUE.fields_by_name["map_value"].message_type = _MAPENTRY
_DTFETCHRESP.fields_by_name["type"].enum_type = _DTFETCHRESP_DATATYPE
_DTFETCHRESP.fields_by_name["value"].message_type = _DTVALUE
_DTFETCHRESP_DATATYPE.containing_type = _DTFETCHRESP;  # NOQA E703
_MAPUPDATE.fields_by_name["field"].message_type = _MAPFIELD
_MAPUPDATE.fields_by_name["counter_op"].message_type = _COUNTEROP
_MAPUPDATE.fields_by_name["set_op"].message_type = _SETOP
_MAPUPDATE.fields_by_name["flag_op"].enum_type = _MAPUPDATE_FLAGOP
_MAPUPDATE.fields_by_name["map_op"].message_type = _MAPOP
_MAPUPDATE_FLAGOP.containing_type = _MAPUPDATE;  # NOQA E703
_MAPOP.fields_by_name["removes"].message_type = _MAPFIELD
_MAPOP.fields_by_name["updates"].message_type = _MAPUPDATE
_DTOP.fields_by_name["counter_op"].message_type = _COUNTEROP
_DTOP.fields_by_name["set_op"].message_type = _SETOP
_DTOP.fields_by_name["map_op"].message_type = _MAPOP
_DTOP.fields_by_name["hll_op"].message_type = _HLLOP
_DTOP.fields_by_name["gset_op"].message_type = _GSETOP
_DTUPDATEREQ.fields_by_name["op"].message_type = _DTOP
_DTUPDATERESP.fields_by_name["map_value"].message_type = _MAPENTRY
DESCRIPTOR.message_types_by_name["MapField"] = _MAPFIELD
DESCRIPTOR.message_types_by_name["MapEntry"] = _MAPENTRY
DESCRIPTOR.message_types_by_name["DtFetchReq"] = _DTFETCHREQ
DESCRIPTOR.message_types_by_name["DtValue"] = _DTVALUE
DESCRIPTOR.message_types_by_name["DtFetchResp"] = _DTFETCHRESP
DESCRIPTOR.message_types_by_name["CounterOp"] = _COUNTEROP
DESCRIPTOR.message_types_by_name["SetOp"] = _SETOP
DESCRIPTOR.message_types_by_name["GSetOp"] = _GSETOP
DESCRIPTOR.message_types_by_name["HllOp"] = _HLLOP
DESCRIPTOR.message_types_by_name["MapUpdate"] = _MAPUPDATE
DESCRIPTOR.message_types_by_name["MapOp"] = _MAPOP
DESCRIPTOR.message_types_by_name["DtOp"] = _DTOP
DESCRIPTOR.message_types_by_name["DtUpdateReq"] = _DTUPDATEREQ
DESCRIPTOR.message_types_by_name["DtUpdateResp"] = _DTUPDATERESP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class MapField(_message.Message):
    DESCRIPTOR = _MAPFIELD


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class MapEntry(_message.Message):
    DESCRIPTOR = _MAPENTRY


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtFetchReq(_message.Message):
    DESCRIPTOR = _DTFETCHREQ


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtValue(_message.Message):
    DESCRIPTOR = _DTVALUE


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtFetchResp(_message.Message):
    DESCRIPTOR = _DTFETCHRESP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class CounterOp(_message.Message):
    DESCRIPTOR = _COUNTEROP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class SetOp(_message.Message):
    DESCRIPTOR = _SETOP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class GSetOp(_message.Message):
    DESCRIPTOR = _GSETOP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class HllOp(_message.Message):
    DESCRIPTOR = _HLLOP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class MapUpdate(_message.Message):
    DESCRIPTOR = _MAPUPDATE


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class MapOp(_message.Message):
    DESCRIPTOR = _MAPOP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtOp(_message.Message):
    DESCRIPTOR = _DTOP


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtUpdateReq(_message.Message):
    DESCRIPTOR = _DTUPDATEREQ


@add_metaclass(_reflection.GeneratedProtocolMessageType)
class DtUpdateResp(_message.Message):
    DESCRIPTOR = _DTUPDATERESP


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(
    descriptor_pb2.FileOptions(),
    b"\n\027com.basho.riak.protobufB\010RiakDtPB",
)

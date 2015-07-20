"""
Copyright 2013 Basho Technologies, Inc.

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


def default_resolver(riak_object):
    """
    The default conflict-resolution function, which does nothing. To
    implement a resolver, define a function that sets the
    :attr:`siblings <riak.riak_object.RiakObject.siblings>` property
    on the passed :class:`RiakObject <riak.riak_object.RiakObject>`
    instance to a list containing a single :class:`RiakContent
    <riak.content.RiakContent>` object.

    :param riak_object: an object-in-conflict that will be resolved
    :type riak_object: :class:`RiakObject <riak.riak_object.RiakObject>`
    """
    pass


def last_written_resolver(riak_object):
    """
    A conflict-resolution function that resolves by selecting the most
    recently-modified sibling by timestamp.

    :param riak_object: an object-in-conflict that will be resolved
    :type riak_object: :class:`RiakObject <riak.riak_object.RiakObject>`
    """
    riak_object.siblings = [max(riak_object.siblings,
                                key=lambda x: x.last_modified), ]

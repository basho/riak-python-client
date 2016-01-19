"""
Copyright 2015 Basho Technologies, Inc.

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

from collections import Mapping
from riak.util import lazy_property
from .datatype import Datatype
from riak.datatypes import TYPES


class TypedMapView(Mapping):
    """
    Implements a sort of view over a :class:`Map`, filtered by the embedded
    datatype.
    """

    def __init__(self, parent, datatype):
        self.map = parent
        self.datatype = datatype

    # Mapping API
    def __getitem__(self, key):
        """
        Fetches an item from the parent :class:`Map` scoped by this view's
        datatype.

        :param key: the key of the item
        :type key: str
        :rtype: :class:`~riak.datatypes.Datatype`
        """
        return self.map[(key, self.datatype)]

    def __iter__(self):
        """
        Iterates over all keys in the :class:`Map` scoped by this view's
        datatype.
        """
        for key in self.map.value:
            name, datatype = key
            if datatype == self.datatype:
                yield name

    def __len__(self):
        """
        Returns the number of keys in this map scoped by this view's datatype.
        """
        return len(iter(self))

    def __contains__(self, key):
        """
        Determines whether the given key with this view's datatype is in the
        parent :class:`Map`.
        """
        return (key, self.datatype) in self.map

    # From the MutableMapping API
    def __delitem__(self, key):
        """
        Removes the key with this view's datatype from the parent :class:`Map`.
        """
        del self.map[(key, self.datatype)]


class Map(Mapping, Datatype):
    """A convergent datatype that acts as a key-value datastructure. Keys
    are pairs of ``(name, datatype)`` where ``name`` is a string and
    ``datatype`` is the datatype name. Values are other convergent
    datatypes, represented by any concrete type in this module.

    You cannot set values in the map directly (it does not implement
    ``__setitem__``), but you may add new empty values or access
    non-existing values directly via bracket syntax. If a key is not in the
    original value of the map when accessed, fetching the key will cause
    its associated value to be created.::

        map[('name', 'register')]

    Keys and their associated values may be deleted from the map as
    you would in a dict::

        del map[('emails', 'set')]

    Convenience accessors exist that partition the map's keys by
    datatype and implement the :class:`~collections.Mapping`
    behavior as well as supporting deletion::

        map.sets['emails']
        map.registers['name']
        del map.counters['likes']
    """

    type_name = 'map'
    _type_error_msg = "Map must be a dict with (name, type) keys"

    def _default_value(self):
        return dict()

    def _post_init(self):
        self._removes = set()
        self._updates = {}

    @lazy_property
    def counters(self):
        """
        Filters keys in the map to only those of counter types. Example::

            map.counters['views'].increment()
            del map.counters['points']
        """
        return TypedMapView(self, 'counter')

    @lazy_property
    def flags(self):
        """
        Filters keys in the map to only those of flag types. Example::

            map.flags['confirmed'].enable()
            del map.flags['attending']
        """
        return TypedMapView(self, 'flag')

    @lazy_property
    def maps(self):
        """
        Filters keys in the map to only those of map types. Example::

            map.maps['emails'].registers['home'].set("user@example.com")
            del map.maps['spam']
        """
        return TypedMapView(self, 'map')

    @lazy_property
    def registers(self):
        """
        Filters keys in the map to only those of register types. Example::

            map.registers['username'].set_value("riak-user")
            del map.registers['access_key']
        """
        return TypedMapView(self, 'register')

    @lazy_property
    def sets(self):
        """
        Filters keys in the map to only those of set types. Example::

            map.sets['friends'].add("brett")
            del map.sets['favorites']
        """
        return TypedMapView(self, 'set')

    def __contains__(self, key):
        """
        A map contains a key if that key exists in the original value
        or has been added or mutated.

        :rtype: bool
        """
        self._check_key(key)
        return (key in self._value) or (key in self._updates)

    # collections.Mapping API
    def __getitem__(self, key):
        """
        Fetches a convergent datatype at the given key.

        .. note: If the key is not in the map, a new empty datatype
           will be inserted at that key and returned. If the key was
           previously deleted, that mutation will be discarded.

        :param key: the key of the value to fetch
        :type key: tuple
        :rtype: :class:`Datatype` matching the datatype in the key
        """
        self._check_key(key)
        if key in self._value:
            return self._value[key]
        else:
            # If the key does not exist, we assume they are wanting to
            # create a new one with that name/type.
            if key not in self._updates:
                self._updates[key] = TYPES[key[1]](context=self.context)
            return self._updates[key]

    def __iter__(self):
        """
        Iterates over the *immutable* original value of the map.
        """
        return iter(self.value)

    def __len__(self):
        """
        Returns the size of the original value of the map.
        """
        return len(self._value)

    def __delitem__(self, key):
        """
        Deletes a key from the map. If you have previously mutated the
        datatype associated with this key, those mutations will be
        discarded.

        .. note: You may delete keys that are not entries in the map.
           If the Riak server does not find the entry in the set, an
           error may be returned to the client. For safety, always
           submit removal operations with a context.

        :param key: the key to remove
        :type key: tuple
        """
        # NB: deleting a key only marks it deleted, and you can delete
        # things that don't appear in the value!
        self._check_key(key)
        self._require_context()
        self._removes.add(key)

    def _check_key(self, key):
        """
        Ensures well-formedness of a key.
        """
        if not len(key) == 2:
            raise TypeError('invalid key: %r' % key)
        elif key[1] not in TYPES:
            raise TypeError('invalid datatype: %s' % key[1])

    # Datatype API
    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original map's value. Nested values are
        pure Python values as returned by :attr:`Datatype.value` from
        the nested types.

        :rtype: dict
        """
        pvalue = {}
        for key in self._value:
            pvalue[key] = self._value[key].value
        return pvalue

    @Datatype.modified.getter
    def modified(self):
        """
        Whether the map has staged local modifications.
        """
        if self._removes:
            return True
        for v in self._value:
            if self._value[v].modified:
                return True
        for v in self._updates:
            if self._updates[v].modified:
                return True
        return False

    def to_op(self):
        """
        Extracts the modification operation(s) from the map.

        :rtype: list, None
        """
        removes = [('remove', r) for r in self._removes]
        value_updates = list(self._extract_updates(self._value))
        new_updates = list(self._extract_updates(self._updates))
        all_updates = removes + value_updates + new_updates
        if all_updates:
            return all_updates
        else:
            return None

    def _check_type(self, value):
        for key in value:
            try:
                self._check_key(key)
            except:
                return False
        return True

    def _coerce_value(self, new_value):
        cvalue = {}
        for key in new_value:
            cvalue[key] = TYPES[key[1]](value=new_value[key],
                                        context=self._context)
        return cvalue

    def _extract_updates(self, d):
        for key in d:
            if d[key].modified:
                yield ('update', key, d[key].to_op())


TYPES['map'] = Map

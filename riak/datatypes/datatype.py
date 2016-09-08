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


from .errors import ContextRequired
from . import TYPES


class Datatype(object):
    """
    Base class for all convergent datatype wrappers. You will not use
    this class directly, but it does define some methods are common to
    all datatype wrappers.
    """

    #: The string "name" of this datatype. Each datatype should set this.
    type_name = None

    #: The message included in the exception raised when the value is of
    #: incorrect type. See also :meth:`_check_type`.
    _type_error_msg = "Invalid value type"

    def __init__(self, bucket=None, key=None, value=None, context=None):
        self.bucket = bucket
        self.key = key
        self._context = context
        if value is not None:
            self._set_value(value)
        else:
            self._set_value(self._default_value())
        self._post_init()

    # Properties

    @property
    def value(self):
        """
        The pure, immutable value of this datatype, as a Python value,
        which is unique for each datatype.

        **NB**: Do not use this property to mutate data, as it will not
        have any effect. Use the methods of the individual type to affect
        changes. This value is guaranteed to be independent of any internal
        data representation.
        """
        return self._value

    @property
    def context(self):
        """
        The opaque context for this type, if it was previously fetched.

        :rtype: str
        """
        if self._context:
            return self._context[:]

    @property
    def modified(self):
        """
        Whether this datatype has staged local modifications.

        :rtype: bool
        """
        raise NotImplementedError

    # Lifecycle methods

    def reload(self, **params):
        """
        Reloads the datatype from Riak.

        .. warning: This clears any local modifications you might have
           made.

        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool
        :rtype: :class:`Datatype`
        """
        if not self.bucket:
            raise ValueError('bucket property not assigned')

        if not self.key:
            raise ValueError('key property not assigned')

        dtype, value, context = self.bucket._client._fetch_datatype(
            self.bucket, self.key, **params)

        if not dtype == self.type_name:
            raise TypeError("Expected datatype {} but "
                            "got datatype {}".format(self.__class__,
                                                     TYPES[dtype]))

        self.clear()
        self._context = context
        self._set_value(value)
        return self

    def delete(self, **params):
        """
        Deletes the datatype from Riak. See :meth:`RiakClient.delete()
        <riak.client.RiakClient.delete>` for options.
        """
        self.clear()
        self._context = None
        self._set_value(self._default_value())
        self.bucket._client.delete(self, **params)
        return self

    def update(self, **params):
        """
        Sends locally staged mutations to Riak.

        :param w: W-value, wait for this many partitions to respond
         before returning to client.
        :type w: integer
        :param dw: DW-value, wait for this many partitions to
         confirm the write before returning to client.
        :type dw: integer
        :param pw: PW-value, require this many primary partitions to
                   be available before performing the put
        :type pw: integer
        :param return_body: if the newly stored object should be
                            retrieved, defaults to True
        :type return_body: bool
        :param include_context: whether to return the new opaque
          context when `return_body` is `True`
        :type include_context: bool
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: a subclass of :class:`~riak.datatypes.Datatype`
        """
        if not self.modified:
            raise ValueError("No operation to perform")

        params.setdefault('return_body', True)
        self.bucket._client.update_datatype(self, **params)
        self.clear()

        return self

    store = update

    def clear(self):
        """
        Removes all locally staged mutations.
        """
        self._post_init()

    def to_op(self):
        """
        Extracts the mutation operation from this datatype, if any.
        Each type must implement this method, returning the
        appropriate operation, or `None` if there is no queued
        mutation.
        """
        raise NotImplementedError

    # Private stuff

    def _check_type(self, new_value):
        """
        Checks that initial values of the type are appropriate. Each
        type must implement this method.

        :rtype: bool
        """
        raise NotImplementedError

    def _coerce_value(self, new_value):
        """
        Coerces the input value into the internal representation for
        the type. Datatypes may override this method.
        """
        return new_value

    def _raise_if_badtype(self, new_value):
        if not self._check_type(new_value):
            raise TypeError(self._type_error_msg)

    def __str__(self):
        return str(self.value)

    def _set_value(self, value):
        self._raise_if_badtype(value)
        self._value = self._coerce_value(value)

    def _default_value(self):
        """
        Returns what the initial value of an empty datatype should be.
        """
        raise NotImplementedError

    def _post_init(self):
        """
        Called at the end of :meth:`__init__` so that subclasses can tweak
        their own setup without overriding the constructor.
        """
        pass

    def _require_context(self):
        """
        Raises an exception if the context is not present
        """
        if not self._context:
            raise ContextRequired()

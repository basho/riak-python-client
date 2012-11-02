"""
Copyright 2012 Basho Technologies, Inc.

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

from contextlib import contextmanager
import threading


# This file is a rough port of the Innertube Ruby library
class BadResource(StandardError):
    """
    Users of a Pool should raise this error when the pool element
    currently in-use is bad and should be removed from the pool.
    """
    pass


class Element(object):
    """
    A member of the Pool, a container for the actual resource being
    pooled and a marker for whether the resource is currently claimed.
    """
    def __init__(self, obj):
        """
        Creates a new Element, wrapping the passed object as the
        pooled resource.
        :param obj: the resource to wrap
        :type obj: object
        """

        """The wrapped pool resource."""
        self.object = obj
        """Whether the resource is currently in use."""
        self.claimed = False


class Pool(object):
    """
    A thread-safe, reentrant resource pool, ported from the
    "Innertube" Ruby library. Pool should be subclassed to implement
    the create_resource and destroy_resource functions that are
    responsible for creating and cleaning up the resources in the
    pool, respectively. Claiming a resource of the pool for a block of
    code is done using a with statement on the take method. The take
    method also allows filtering of the pool and supplying a default
    value to be used as the resource if no elements are free.

    Example:

        from riak.Pool import Pool, BadResource
        class ListPool(Pool):
            def create_resource(self):
                return []

            def destroy_resource(self):
                # Lists don't need to be cleaned up
                pass

        pool = ListPool()
        with pool.take() as resource:
            resource.append(1)
        with pool.take() as resource2:
            print repr(resource2) # should be [1]
    """

    def __init__(self):
        """
        Creates a new Pool. This should be called manually if you
        override the __init__ method in a subclass.
        """
        self.lock = threading.Lock()
        self.elements = list()

    @contextmanager
    def take(self, _filter=None, default=None):
        """
        Claims a resource from the pool for use in a thread-safe,
        reentrant manner (as part of a with statement). Resources are
        created as needed when all members of the pool are claimed or
        the pool is empty.

        :param _filter: a filter that can be used to select a member
            of the pool
        :type _filter: callable
        :param default: a value that will be used instead of calling
            create_resource if a new resource needs to be created
        """
        element = None
        if not callable(_filter):
            def _filter(obj):
                return True
        with self.lock:
            for e in self.elements:
                if not e.claimed and _filter(e.object):
                    element = e
                    break
            if element is None:
                if default is not None:
                    element = Element(default)
                else:
                    element = Element(self.create_resource())
                self.elements.append(element)
            element.claimed = True
        try:
            yield element.object
        except BadResource:
            self.delete_element(element)
            raise
        finally:
            element.claimed = False

    def delete_element(self, element):
        """
        Deletes the element from the pool and destroys the associated
        resource. Not usually needed by users of the pool, but called
        internally when BadResource is raised.

        :param element: the element to remove
        :type element: Element
        """
        with self.lock:
            self.elements.remove(element)
        self.destroy_resource(element.object)
        del element

    def create_resource(self):
        """
        Implemented by subclasses to allocate a new resource for use
        in the pool.
        """
        raise NotImplemented

    def destroy_resource(self, obj):
        """
        Called when removing a resource from the pool so that it can
        be cleanly deallocated. Subclasses should implement this
        method if additional cleanup is needed beyond normal GC. The
        default implementation is a no-op.

        :param obj: the resource being removed
        """
        pass

from __future__ import print_function

import threading

from contextlib import contextmanager


class BadResource(Exception):
    """
    Users of a :class:`Pool` should raise this error when the pool
    resource currently in-use is bad and should be removed from the
    pool.

    :param mid_stream: did this exception happen mid-streaming op?
    :type mid_stream: boolean
    """
    def __init__(self, ex, mid_stream=False):
        super(BadResource, self).__init__(ex)
        self.mid_stream = mid_stream


class ConnectionClosed(BadResource):
    """
    Users of a :class:`Pool` should raise this error when the pool
    resource currently in-use has been closed and should be removed
    from the pool.

    :param mid_stream: did this exception happen mid-streaming op?
    :type mid_stream: boolean
    """
    def __init__(self, ex, mid_stream=False):
        super(ConnectionClosed, self).__init__(ex, mid_stream)


class Resource(object):
    """
    A member of the :class:`Pool`, a container for the actual resource
    being pooled and a marker for whether the resource is currently
    claimed.
    """
    def __init__(self, obj, pool):
        """
        Creates a new Resource, wrapping the passed object as the
        pooled resource.

        :param obj: the resource to wrap
        :type obj: object
        """

        """The wrapped pool resource."""
        self.object = obj

        """Whether the resource is currently in use."""
        self.claimed = False

        """The pool that this resource belongs to."""
        self.pool = pool

        """True if this Resource errored."""
        self.errored = False

    def release(self):
        """
        Releases this resource back to the pool it came from.
        """
        if self.errored:
            self.pool.delete_resource(self)
        else:
            self.pool.release(self)


class Pool(object):
    """
    A thread-safe, reentrant resource pool, ported from the
    "Innertube" Ruby library. Pool should be subclassed to implement
    the create_resource and destroy_resource functions that are
    responsible for creating and cleaning up the resources in the
    pool, respectively. Claiming a resource of the pool for a block of
    code is done using a with statement on the transaction method. The
    transaction method also allows filtering of the pool and supplying
    a default value to be used as the resource if no resources are
    free.

    Example::

        from riak.transports.pool import Pool
        class ListPool(Pool):
            def create_resource(self):
                return []

            def destroy_resource(self):
                # Lists don't need to be cleaned up
                pass

        pool = ListPool()
        with pool.transaction() as resource:
            resource.append(1)
        with pool.transaction() as resource2:
            print(repr(resource2)) # should be [1]
    """

    def __init__(self):
        """
        Creates a new Pool. This should be called manually if you
        override the :meth:`__init__` method in a subclass.
        """
        self.lock = threading.RLock()
        self.releaser = threading.Condition(self.lock)
        self.resources = list()

    def acquire(self, _filter=None, default=None):
        """
        acquire(_filter=None, default=None)

        Claims a resource from the pool for manual use. Resources are
        created as needed when all members of the pool are claimed or
        the pool is empty. Most of the time you will want to use
        :meth:`transaction`.

        :param _filter: a filter that can be used to select a member
            of the pool
        :type _filter: callable
        :param default: a value that will be used instead of calling
            :meth:`create_resource` if a new resource needs to be created
        :rtype: Resource
        """
        if not _filter:
            def _filter(obj):
                return True
        elif not callable(_filter):
            raise TypeError("_filter is not a callable")

        resource = None
        with self.lock:
            for e in self.resources:
                if not e.claimed and _filter(e.object):
                    resource = e
                    break
            if resource is None:
                if default is not None:
                    resource = Resource(default, self)
                else:
                    resource = Resource(self.create_resource(), self)
                self.resources.append(resource)
            resource.claimed = True
        return resource

    def release(self, resource):
        """release(resource)

        Returns a resource to the pool. Most of the time you will want
        to use :meth:`transaction`, but if you use :meth:`acquire`,
        you must release the acquired resource back to the pool when
        finished. Failure to do so could result in deadlock.

        :param resource: Resource
        """
        with self.releaser:
            resource.claimed = False
            self.releaser.notify_all()

    @contextmanager
    def transaction(self, _filter=None, default=None, yield_resource=False):
        """
        transaction(_filter=None, default=None)

        Claims a resource from the pool for use in a thread-safe,
        reentrant manner (as part of a with statement). Resources are
        created as needed when all members of the pool are claimed or
        the pool is empty.

        :param _filter: a filter that can be used to select a member
            of the pool
        :type _filter: callable
        :param default: a value that will be used instead of calling
            :meth:`create_resource` if a new resource needs to be created
        :param yield_resource: set to True to yield the Resource object
            itself
        :type yield_resource: boolean
        """
        resource = self.acquire(_filter=_filter, default=default)
        try:
            if yield_resource:
                yield resource
            else:
                yield resource.object
            if resource.errored:
                self.delete_resource(resource)
        except BadResource:
            self.delete_resource(resource)
            raise
        finally:
            self.release(resource)

    def delete_resource(self, resource):
        """
        Deletes the resource from the pool and destroys the associated
        resource. Not usually needed by users of the pool, but called
        internally when BadResource is raised.

        :param resource: the resource to remove
        :type resource: Resource
        """
        with self.lock:
            self.resources.remove(resource)
        self.destroy_resource(resource.object)
        del resource

    def __iter__(self):
        """
        Iterator callback to iterate over the resources of the pool.
        """
        return PoolIterator(self)

    def clear(self):
        """
        Removes all resources from the pool, calling :meth:`delete_resource`
        with each one so that the resources are cleaned up.
        """
        for resource in self:
            self.delete_resource(resource)

    def create_resource(self):
        """
        Implemented by subclasses to allocate a new resource for use
        in the pool.
        """
        raise NotImplementedError

    def destroy_resource(self, obj):
        """
        Called when removing a resource from the pool so that it can
        be cleanly deallocated. Subclasses should implement this
        method if additional cleanup is needed beyond normal GC. The
        default implementation is a no-op.

        :param obj: the resource being removed
        """
        pass


class PoolIterator(object):
    """
    Iterates over a snapshot of the pool in a thread-safe manner,
    eventually touching all resources that were known when the
    iteration started.

    Note that if claimed resources are not released for long periods,
    the iterator may hang, waiting for those last resources to be
    released. The iteration and pool functionality is only meant to be
    used internally within the client, and resources will be claimed
    per client operation, making this an unlikely event (although
    still possible).
    """

    def __init__(self, pool):
        with pool.lock:
            self.targets = pool.resources[:]
        self.unlocked = []
        self.lock = pool.lock
        self.releaser = pool.releaser

    def __iter__(self):
        return self

    def next(self):
        # Python 2.x version
        if len(self.targets) == 0:
            raise StopIteration
        if len(self.unlocked) == 0:
            self.__claim_resources()
        return self.unlocked.pop(0)

    def __next__(self):
        # Python 3.x version
        return self.next()

    def __claim_resources(self):
        with self.lock:
            with self.releaser:
                if self.__all_claimed():
                    self.releaser.wait()
            for resource in self.targets:
                if not resource.claimed:
                    self.targets.remove(resource)
                    self.unlocked.append(resource)
                    resource.claimed = True

    def __all_claimed(self):
        for resource in self.targets:
            if not resource.claimed:
                return False
        return True

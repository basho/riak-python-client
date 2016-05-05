# Riak Python Client Release Notes

## [2.5.2 Release](https://github.com/basho/riak-python-client/issues?q=milestone%3Ariak-python-client-2.5.2)

* Miscellaneous fixes for term-to-binary encoding of messages for Riak TS.
* [Ensure `six` is not required during installation](https://github.com/basho/riak-python-client/pull/459)

## [2.5.0 Release - Deprecated](https://github.com/basho/riak-python-client/issues?q=milestone%3Ariak-python-client-2.5.0)

* *NOTE*: due to the `basho-erlastic` dependency, this version will not install correctly. Please use `2.5.2`.
* [Socket Enhancements](https://github.com/basho/riak-python-client/pull/453) - Resolves [#399](https://github.com/basho/riak-python-client/issues/399)
* [Add multi-put](https://github.com/basho/riak-python-client/pull/452)
* [Add support for term-to-binary encoding](https://github.com/basho/riak-python-client/pull/448) *Note:* This requires at least version `1.3.0` of Riak TS.

## 2.4.2 Patch Release - 2016-02-20

* [Fix SSL host name](https://github.com/basho/riak-python-client/pull/436)
* [Use `riak-client-tools`](https://github.com/basho/riak-python-client/issues/434)

## 2.4.1 Patch Release - 2016-02-03

* [Riak TS: Millisecond precision](https://github.com/basho/riak-python-client/issues/430)
* [Fix release process](https://github.com/basho/riak-python-client/issues/429)

## 2.4.0 Feature Release - 2016-01-13

This release enhances Riak Time Series functionality.

* [Encapsulate table description](https://github.com/basho/riak-python-client/pull/422)

## 2.3.0 Feature Release - 2015-12-14

Release 2.3.0 features support for new
[time series](https://github.com/basho/riak-python-client/pull/416)
functionality.

This is release retires support for Python 2.6.x but adds support for
Python 3.5.x.

There are also many bugfixes and new enhancements:

* [Protocol buffers are now integrated into the Python Client]
  (https://github.com/basho/riak-python-client/pull/418)
* [Support for Preflists and Write-Once bucket types]
  (https://github.com/basho/riak-python-client/pull/414)
* [Support Riak 2.1.1]
  (https://github.com/basho/riak-python-client/pull/407)
* [Native SSL support for Python 2.7.9+]
  (https://github.com/basho/riak-python-client/pull/397)


## 2.2.0 Feature Release - 2014-12-18

Release 2.2.0 features support for
[Python 3](https://github.com/basho/riak-python-client/pull/379),
specifically 3.3 and 3.4.  This version uses the native SSL security instead
of [pyOpenSSL](http://pypi.python.org/pypi/pyOpenSSL) which is required
for the Python 2 series.

This release also includes many bugfixes and enhancements, most
notably:

* [Fixed an issue with the implementation of `Mapping.__iter__`]
  (https://github.com/basho/riak-python-client/pull/367)
* [Test client certificate generation updated]
  (https://github.com/basho/riak-python-client/pull/373)
* [Protocol Buffers had a socket.send issue]
  (https://github.com/basho/riak-python-client/pull/382)
* [Support for bucket types in Map/Reduce jobs added]
  (https://github.com/basho/riak-python-client/pull/385)
* [Race condition in `RiakBucket` creation fixed]
  (https://github.com/basho/riak-python-client/pull/386)
* [Data Types can now be deleted]
  (https://github.com/basho/riak-python-client/pull/387)
* [2i Range Queries with a zero end index now work]
  (https://github.com/basho/riak-python-client/pull/388)


## 2.1.0 Feature Release - 2014-09-03

Release 2.1.0 features support for Riak 2.0 capabilities including:

* Bucket Types
* Riak Data Types (CRDTs)
* Search 2.0 (codename Yokozuna)
* Security: SSL/TLS, Authentication, and Authorization

As a result of the new security features, the package now depends on
[pyOpenSSL](http://pypi.python.org/pypi/pyOpenSSL) and will warn if
your version of OpenSSL is too old.

This release also includes many bugfixes and enhancements, most
notably:

* The default protocol is now 'pbc', not 'http'.
* When used correctly, streaming requests no longer result in leaks
  from the connection pool.
* The size of the multiget worker pool can be set when initializing
  the client.
* Secondary index queries can now iterate over all pages in a query.
* The number of times a request is retried after network failure is
  now configurable.
* The additional request options `basic_quorum` and `notfound_ok` are
  now supported.

## 2.0.3 Patch Release - 2014-03-06

Release 2.0.3 includes support for 1.4.4's 2I regexp feature and fixes
a few bugs:

* Docs generation now uses the version from the top-level package.
* Some internal uses of the deprecated RiakClient.solr were removed.
* More errors will be caught and propagated properly from multiget
  requests, preventing deadlocks on the caller side.

## 2.0.2 Patch release - 2013-11-18

Release 2.0.2 includes support for the 1.4.1+ "timeout" option on
secondary index queries.

## 2.0.1 Patch release - 2013-08-28

Release 2.0.1 includes a minor compatibility fix for Python 2.6 and an
updated README.

## 2.0.0 Feature Release - 2013-07-30

Release 2.0 is the culmination of many months of rearchitecting the
client. Highlights:

* Automatic connection to multiple nodes, with request retries,
  through a thread-safe connection pool.
* All Riak 1.3 and 1.4 features, including bucket properties,
  paginating and streaming secondary indexes, CRDT counters,
  client-specified timeouts, and more.
* Cleaner, more Pythonic access to RiakObject and RiakBucket
  attributes, favoring properties over methods where possible.
* Simpler representations of links (3-tuples) and index entries
  (2-tuples).
* Streaming requests (keys, buckets, MapReduce, 2i) are now exposed as
  iterators.
* Feature detection prevents sending requests to hosts that can't
  handle them.
* Better handling of siblings -- you don't have to request them
  individually anymore -- and registrable resolver functions.
* A new `multiget` operation that fetches a collection of keys using
  a pool background threads.
* A more resilient, repeatable test suite that generates buckets and
  key names that are essentially random.
* Last but not least, a brand new, more detailed documentation site!

Other features:

* Added an encoder/decoder pair to support `text/plain`.
* The Travis CI build will now install the latest Riak to run the
  suite against.

Other bugfixes:

* The `charset` metadata can now be received via the `Content-Type`
  header on HTTP.
* Objects with empty keys and buckets with empty names cannot be
  created or accessed, as they are unaddressable over HTTP.
* Performance and compatibility of `TestServer` was improved.
* Non-ASCII request bodies are better supported on HTTP.
* Enabling and disabling search indexing on a bucket now uses the
  `search` bucket property.

## 1.5.2 Patch Release - 2013-01-31

Release 1.5.2 fixes some bugs and adds HTTPS/SSL support.

* Added support for HTTPS.
* Fixed writing of the `app.config` for the `TestServer`.
* Reorganized the tests into multiple files and cases.
* Some methods on `RiakObject` were made private where appropriate.
* The version comparison used in feature detection was loosened to
  support pre-release versions of Riak.
* Prevent fetching the `protobuf` package from Google Code.
* Prefer `simplejson` over `json` when present.

## 1.5.1 Patch Release - 2012-10-24

Release 1.5.1 fixes one bug and some documentation errors.

* Fix bug where `http_status` is used instead of `http_code`.
* Fix documentation of `RiakMapReduce.index` method.
* Fix documentation of `RiakClient.__init__` method.

## 1.5.0 Feature Release - 2012-08-29

Release 1.5.0 is a feature release that supports Riak 1.2.

Noteworthy features:

* Riak 1.2 features are now supported, including Search and 2I queries
  over Protocol Buffers transport. The Protocol Buffers message
  definitions now exist as a separate package, available on
  [PyPi](http://pypi.python.org/pypi/riak_pb/1.2.0).

  **NOTE:** The return value of search queries over HTTP and MapReduce
  were changed to be compatible with the results returned from the
  Protocol Buffers interface.
* The client will use a version-based feature detection scheme to
  enable or disable various features, including the new Riak 1.2
  features. This enables compatibility with older nodes during a
  rolling upgrade, or usage of the newer client with older clusters.

Noteworthy bugfixes:

* The code formatting and style was adjusted to fit PEP8 standards.
* All classes in the package are now "new-style".
* The PW accessor methods on RiakClient now get and set the right
  instance variable.
* Various fixes were made to the TestServer and it will throw an
  exception when it fails to start.

## 1.4.1 Patch Release - 2012-06-19

Noteworthy features:

* New Riak objects support Riak-created random keys

Noteworthy bugfixes:

* Map Reduce queries now use "application/json" as the Content-Type

## 1.4.0 Feature Release - 2012-03-30

Release 1.4.0 is a feature release comprising over 117 individual
commits.

Noteworthy features:

* Python 2.6 and 2.7 are supported. On 2.6, the unittest2 package is
  required to run the test suite.
* Google's official protobuf package (2.4.1 or later) is now a
  dependency. The package from downloads.basho.com/support is no
  longer necessary.
* Travis-CI is enabled on the client. Go to
  http://travis-ci.org/basho/riak-python-client for build status.
* Riak 1.0+ features, namely secondary indexes and primary quora
  (PR/PW), are supported.
* `if_none_match` is a valid request option when storing objects, and
  will prevent the write when set to `True` if the key already exists.
* Links can be set wholesale using the `set_links()` method.
* Transport-specific options can be passed through when creating a
  `Client` object.
* A connection manager was added that will (when manipulated manually)
  allow connections to multiple Riak nodes. This will be fully
  integrated in a future release.

Noteworthy bugfixes:

* Links now use the proper URL-encoding in HTTP headers, preventing
  problems with explosion from multiple encoding passes.
* Many fixes were applied to make the Protocol Buffers transport more
  stable.
* `RiakObject.get_content_type()` will behave properly when content
  type is not set.
* Deprecated transport classes were removed since their functionality
  had folded into the primary transports.
* A temporary fix was made for unicode bucket/key names which raises
  an error when they are used and cannot be coerced to ASCII.
* The Erlang sources/beams for the TestServer are now included in the
  package.
* MapReduce failures will now produce a more useful error message and
  be handled properly when no results are returned.

There are lots of other great fixes from our wonderful
community. [Check them out!](https://github.com/basho/riak-python-client/compare/1.3.0...1.4.0)

## 1.3.0 Feature Release - 2011-08-04

Release 1.3.0 is a feature release bringing a slew of updates.

Noteworthy features:

* #37: Support for the Riak Search HTTP Interface (Mathias Meyer)
* #36: Support to store large files in Luwak (Mathias Meyer)
* #35: Convenience methods to enable, disable and check search indexing
       on Riak buckets (Mathias Meyer)
* #34: Port of Ripple's test server to Python, allows faster testing
       thanks to an in-memory Riak instance (Mathias Meyer)
* #31: New transports: A Protocol Buffers connection cache
       (riak.transports.pbc.RiakPbcCacheTransport), a transport to reuse the
       underlying TCP connections by setting SO_REUSEADDR on the socket
       (riak.transports.http.RiakHttpReuseTransport), and one that tries to
       reuse connections to the same host (riak.transports.http.RiakHttpPoolTransport)
       (Gilles Devaux)

Fixes:

* #33: Respect maximum link header size when using HTTP. Link header is now
       split up into multiple headers when it exceeds the maximum size of 8192 bytes.
       (Mathias Meyer)
* #41: Connections potentially not returned to the protocol buffers connection
       pool. (Reid Draper)
* #42: Reset protocol buffer connection up on connection error (Brett Hoerner)

## 1.2.2 Patch Release - 2011-06-22

Release 1.2.2 is a minor patch release.

Noteworthy fixes and improvements:

* #29: Add an nicer API for using key filters with MapReduce (Eric Moritz)
* #13 and #24: Let Riak generate a key when none is specified (Mark Erdmann)
* #28: Function aliases for the Riak built-in MapReduce functions (Eric Moritz)
* #20: Add a convenience method to create Riak object directly from file (Ana Nelson)
* #16: Support return\_body parameter when creating a new object (Stefan Praszalowicz, Andy Gross)
* #17: Storing an object fails when it doesn't exist in Riak (Eric Moritz, Andy Gross)
* #18: Ensure that a default content type is set when none specified (Andy Gross)
* #22: Fix user meta data support (Mathias Meyer)
* #23: Fix links to the wiki (Mikhail Sobolev)
* #25: Enable support for code coverage when running tests (Mikhail Sobolev)
* #26: Debian packaging (Dmitry Rozhkov)

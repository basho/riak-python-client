# Riak Python Client Release Notes

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

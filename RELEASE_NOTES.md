# Riak Python Client Release Notes

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

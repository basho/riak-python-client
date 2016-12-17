.. _security-label:

.. currentmodule:: riak.security

========
Security
========

Riak 2.0 supports authentication and authorization over encrypted
channels via OpenSSL. This is useful to prevent accidental collisions
between environments (e.g., pointing application software under active
development at the production cluster) and offers protection against
some malicious attacks, although Riak still should not be exposed
directly to any unsecured network.

Several important caveats when enabling security:

* There is no support yet for auditing. This is on the roadmap for a future
  release.
* Two deprecated features will not work if security is enabled: link
  walking and Riak Search 1.0.
* There are restrictions on Erlang modules exposed to MapReduce jobs when
  security is enabled.
* Enabling security requires applications be designed to transition
  gracefully based on the server response or applications will need to be
  halted before security is enabled and brought back online with support
  for the new security features.

--------------------
Server Configuration
--------------------

The server must first be configured to `enable security
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Security-Basics>`_,
users and `security sources
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/>`_ 
must be created, `permissions
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Managing-Permissions>`_
applied and the correct certificates must be installed.  An overview
can be found at `Authentication and Authorization
<http://docs.basho.com/riak/2.0.0/ops/running/authz/>`_.

--------------------
Client Configuration
--------------------

.. note:: OpenSSL 1.0.1g or later (or patched version built after
          2014-04-01) is required for `pyOpenSSL
          <http://pypi.python.org/pypi/pyOpenSSL/>`_, which is used
          for secure transport in the Riak client. Earlier versions
          may not support TLS 1.2, the recommended security protocol.

On the client, simply create a
:class:`SecurityCreds` object with just a username,
password and CA Certificate file. That would then need to be passed
into the :class:`~riak.client.RiakClient` initializer::

     creds = SecurityCreds('riakuser',
                           'riakpass',
                           cacert_file='/path/to/ca.crt')
     client = RiakClient(credentials=creds)

The ``credentials`` argument of a :class:`~riak.client.RiakClient` constructor
is a :class:`SecurityCreds` object. If you specify a dictionary
instead, it will be turned into this type::

    creds = {'username': 'riakuser',
             'password': 'riakpass',
             'cacert_file': '/path/to/ca.crt'}
    client = RiakClient(credentials=creds)

.. note:: A Certifying Authority (CA) Certificate must always be
          supplied to :class:`SecurityCreds` by specifying the path to
          a CA certificate file via the ``cacert_file`` argument or by
          setting the ``cacert`` argument to an `OpenSSL.crypto.X509
          <http://pythonhosted.org/pyOpenSSL/api/crypto.html#x509-objects>`_
          object. This mitigates MITM (man-in-the-middle) attacks by
          ensuring correct certificate validation.

--------------------
Authentication Types
--------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Trust and PAM Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The most basic authentication would be `Trust-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Trust-based-Authentication>`_
which is done exclusively on the server side by adding the appropriate
``trust`` security source:

.. code:: bash

   riak-admin security add-source all 127.0.0.1/32 trust

`PAM-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#PAM-based-Authentication>`_
is another server-side solution which can be added by a ``pam``
security source with the name of the service:

.. code:: bash

   riak-admin security add-source all 127.0.0.1/32 pam service=riak_pam

Even if you are using Trust authentication or the PAM module doesn't
require a password, you must supply one to the client API. From the
client's perspective, these are equivalent to Password authentication.

^^^^^^^^^^^^^^^^^^^^^^^
Password Authentication
^^^^^^^^^^^^^^^^^^^^^^^

The next level of security would be simply a username and password for
`Password-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Password-based-Authentication>`_.
The server needs to first have a user and a ``password`` security source:

.. code:: bash

    riak-admin security add-user riakuser password=captheorem4life
    riak-admin security add-source riakuser 127.0.0.1/32 password

On the client, simply create a :class:`~SecurityCreds` object or dict
with just a username and password. That would then need to be passed
into the :class:`~riak.client.RiakClient` initializer::

     creds = {'username': 'riakuser',
              'password': 'riakpass',
              'cacert_file': '/path/to/ca.crt'}
     client = RiakClient(credentials=creds)
     myBucket = client.bucket('test')
     val1 = "#SeanCribbsHoldingThings"
     key1 = myBucket.new('hashtag', data=val1)
     key1.store()

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Client Certificate Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are using the **Protocol Buffers** transport you could also add
a layer of security by using `Certificate-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Certificate-based-Authentication>`_.
This time the server requires a ``certificate`` security source::

   riak-admin security add-source riakuser 127.0.0.1/32 certificate

When the ``certificate`` source is used, the Riak username must match
the common name, aka ``CN``, that you specified when you generated your
certificate.  You can add a ``certificate`` source to any number of clients.

The :class:`SecurityCreds` must then include the include a client
certificate file and a private key file, too::

    creds = {'username': 'riakuser',
             'password': 'riakpass',
             'cacert_file': '/path/to/ca.crt',
             'cert_file': '/path/to/client.crt',
             'pkey_file': '/path/to/client.key'}

.. note:: Username and password are still required for certificate-based
          authentication, although the password is ignored.

Optionally, the certificate or private key may be supplied as a string::

    with open('/path/to/client.key', 'r') as f:
        preloaded_pkey = f.read()
    with open('/path/to/client.crt', 'r') as f:
        preloaded_cert = f.read()
    creds = {'username': 'riakuser',
             'password': 'riakpass',
             'cert': preloaded_cert,
             'pkey': prelocated_pkey}

------------------
Additional options
------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Certificate revocation lists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Another security option available is a Certificate Revocation List (CRL).
It lists server certificates which, for whatever reason, are no longer
valid.  For example, it is discovered that the certificate authority (CA)
had improperly issued a certificate, or if a private-key is thought to
have been compromised.  The most common reason for revocation is the user
no longer being in sole possession of the private key (e.g., the token
containing the private key has been lost or stolen)::

     creds = {'username': 'riakuser',
              'password': 'riakpass',
              'cacert_file': '/path/to/ca.crt',
              'crl_file': '/path/to/server.crl'}

^^^^^^^^^^^^^^
Cipher options
^^^^^^^^^^^^^^

The last interesting setting on :class:`SecurityCreds` is the
``ciphers`` option which is a colon-delimited list of supported
ciphers for encryption::

    creds = {'username': 'riakuser',
             'password': 'riakpass',
             'ciphers': 'ECDHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA'}

A more detailed discussion can be found at `Security Ciphers
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Security-Ciphers>`_.

--------------------
SecurityCreds object
--------------------

.. autoclass:: SecurityCreds

   .. autoattribute:: username
   .. autoattribute:: password
   .. autoattribute:: cacert
   .. autoattribute:: crl
   .. autoattribute:: cert
   .. autoattribute:: pkey
   .. autoattribute:: ciphers
   .. autoattribute:: ssl_version

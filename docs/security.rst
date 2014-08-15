.. _security-label:

========
Security
========

Riak 2.0 adds authentication and authorization by optionally encrypting traffic
betwen the client and server via OpenSSL.
This is useful to prevent accidental collisions between environments (e.g.,
pointing application software under active development at the production
cluster) and offers protection against malicious attack, although Riak still
should not be exposed directly to any unsecured network.

Several important caveats when enabling security:

   * There is no support yet for auditing. This is on the roadmap for a future
     release.
   * Two deprecated features will not work if security is enabled: link
     walking and Riak's original full-text search tool.
   * There are restrictions on Erlang modules exposed to MapReduce jobs when
     security is enabled.
   * Enabling security requires applications be designed to transition
     gracefully based on the server response or applications will need to be
     halted before security is enabled and brought back online with support
     for the new security features.

----------------------
Security Configuration
----------------------

The server must first be configured to `enable security
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Security-Basics>`_,
users and `security sources
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/>`_ 
must be created, `permissions
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Managing-Permissions>`_
applied and the correct certificates must be installed.  An overview
can be found at `Authentication and Authorization
<http://docs.basho.com/riak/2.0.0/ops/running/authz/>`_.

.. note:: OpenSSL 1.0.1g or later (or patched version built after 2014-04-01)
          is required for **pyOpenSSL**.

--------------------
Authentication Types
--------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^
Trust-based authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^

The most basic authorization would be
`Trust-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Trust-based-Authentication>`_
which is done exclusively on the server side by adding the appropriate
``trust`` security source::

   riak-admin security add-source all 127.0.0.1/32 trust

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Password-based authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The next level of security would be simply a username and password for
`Password-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Password-based-Authentication>`_.
The server needs to first have a ``password`` security source::

   riak-admin security add-source riakuser 127.0.0.1/32 password

One the client, simply create a
:py:class:`~riak.security.SecurityCreds` object with just a username and
password.  That would then need to be passed into the
:py:class:`~riak.client.RiakClient` initializer::

     creds = SecurityCreds('testuser',
                           'testpass')
     client = RiakClient(credentials=creds)
     myBucket = client.bucket('test')
     val1 = "#SeanCribbsHoldingThings"
     key1 = myBucket.new('hashtag', data=val1)
     key1.store()

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Certificate-based authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are using the **protocol buffer** transport you could also add a layer
of security by using`Certificate-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Certificate-based-Authentication>`_.
This time the server requires a ``certificate`` security source::

   riak-admin security add-source riakuser 127.0.0.1/32 certificate

When the ``certificate`` source is used, ``riakuser`` must also be entered as
the common name, aka ``CN``, that you specified when you generated your
certificate.  You can add a ``certificate`` source to any number of clients,
as long as their ``CN`` and Riak username match.

The :py:class:`~riak.security.SecurityCreds` must include a Certification
Authority (CA)-issued certificate may be provided, too::

     creds = SecurityCreds('testuser',
                           'testpass',
                           cert_file='/path/to/client.crt',
                           pkey_file='/path/to/client.key')

.. note:: Username and password are still required for certificate-based
          authentication.

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


     creds = SecurityCreds('testuser',
                           'testpass',
                           cacert_file='/path/to/ca.crt',
                           crl_file='/path/to/server.crl')

^^^^^^^^^^^^^^
Cipher options
^^^^^^^^^^^^^^

The last interesting setting on
:py:class:`~riak.security.SecurityCreds` is the cipher option which
is a colon-delimited list of supported ciphers for encryption::

        creds = SecurityCreds('testuser',
                              'testpass',
                              ciphers='ECDHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA')

A more detailed discussion can be found at `Security Ciphers
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Security-Ciphers>`_.

--------------------
SecurityCreds object
--------------------

.. autoclass:: riak.security.SecurityCreds
   :members:

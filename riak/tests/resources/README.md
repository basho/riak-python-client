**DO NOT USE THESE IN PRODUCTION**

This directory has certificates and a key for testing Riak authentication.

* server.key - a private key for a Riak server (PEM format)
* server.crt - the certificate for server.key (PEM format)
* ca.crt - a certificate for the CA that issued server.crt (PEM format)
* empty_ca.crt - a certificate for a CA that has and cannot ever issue a 
  certificate (I deleted its private key)
* client.crt - certificate for client authenication (PEM format)

**DO NOT USE THESE IN PRODUCTION**

Generation of values inspired by https://github.com/basho-labs/riak-ruby-ca


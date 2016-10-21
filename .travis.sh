#!/usr/bin/env bash
set -o errexit

flake8 --exclude=riak/pb riak *.py

sudo riak-admin security disable

python setup.py test

sudo riak-admin security enable

if [[ $RIAK_TEST_PROTOCOL == 'pbc' ]]
then
    export RUN_SECURITY=1
    python setup.py test --test-suite riak.tests.test_security
else
    echo '[INFO]: security tests run on PB protocol only'
fi

RIAK_CONF = ${RIAK_DIR}/etc/riak.conf
# ADVANCED_CONF = ${RIAK_DIR}/etc/advanced.config
# RIAK = ${RIAK_DIR}/bin/riak
RIAK_ADMIN = ${RIAK_DIR}/bin/riak-admin
CERTS_DIR = $(shell pwd)/../riak/tests/resources
unexport PYENV_VERSION

preconfigure:
	@../setup.py preconfigure --riak-conf=${RIAK_CONF}

configure:
	@../setup.py configure --riak-admin=${RIAK_ADMIN}

compile:
	@../setup.py develop

lint:
	@pip install --upgrade pep8 flake8
	@cd ..; pep8 riak *.py
	@cd ..; flake8 riak *.py
	@openssl verify -CAfile ${CERTS_DIR}/ca.crt ${CERTS_DIR}/client.crt
	@openssl verify -CAfile ${CERTS_DIR}/ca.crt ${CERTS_DIR}/server.crt

test: setup test_normal test_security

test_normal:
	@echo "Testing Riak Python Client (without security)"
	@../setup.py disable_security --riak-admin=${RIAK_ADMIN}
	@RIAK_TEST_PROTOCOL='pbc' RUN_YZ=0 RUN_DATATYPES=1 RUN_INDEXES=1 ./tox_runner.sh ..
	@RIAK_TEST_PROTOCOL='http' RUN_YZ=0 RUN_DATATYPES=1 RUN_INDEXES=1 ./tox_runner.sh ..

test_security:
	@echo "Testing Riak Python Client (with security)"
	@../setup.py enable_security --riak-admin=${RIAK_ADMIN}
	@RIAK_TEST_PROTOCOL='pbc' RUN_YZ=0 RUN_INDEXES=1 RUN_SECURITY=1 RUN_POOL=0 RUN_RESOLVE=0 ./tox_runner.sh ..
	@RIAK_TEST_PROTOCOL='http' RUN_YZ=0 RUN_INDEXES=1 RUN_SECURITY=1 RUN_POOL=0 RUN_RESOLVE=0 RIAK_TEST_HTTP_PORT=18098 ./tox_runner.sh ..

# These are required to actually build all the Python versions:
# * pip install tox
# * pyenv - https://github.com/yyuu/pyenv
# And two pyenv plugins:
# * pyenv virtualenv - https://github.com/yyuu/pyenv-virtualenv
# * pyenv alias - https://github.com/s1341/pyenv-alias
setup:
	./tox_setup.sh

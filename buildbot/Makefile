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
	@cd ..; pep8 --exclude=riak/pb riak *.py
	@cd ..; flake8 --exclude=riak/pb riak *.py
	@openssl verify -CAfile ${CERTS_DIR}/ca.crt ${CERTS_DIR}/client.crt
	@openssl verify -CAfile ${CERTS_DIR}/ca.crt ${CERTS_DIR}/server.crt

test: setup test_normal test_security

test_normal:
	@echo "Testing Riak Python Client (without security)"
	@../setup.py disable_security --riak-admin=${RIAK_ADMIN}
	@RUN_YZ=1 SKIP_DATATYPES=0 SKIP_INDEXES=0 ./tox_runner.sh ..

test_security:
	@echo "Testing Riak Python Client (with security)"
	@../setup.py enable_security --riak-admin=${RIAK_ADMIN}
	@RUN_YZ=1 SKIP_INDEXES=0 RUN_SECURITY=1 SKIP_POOL=1 SKIP_RESOLVE=1 RIAK_TEST_HTTP_PORT=18098 ./tox_runner.sh ..

setup:
	./tox_setup.sh

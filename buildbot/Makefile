ifndef RIAK_DIR
$(error RIAK_DIR is not set)
endif

PROJDIR = $(realpath $(CURDIR)/..)
TOOLS_DIR = $(PROJDIR)/tools/devrel
CA_DIR = $(PROJDIR)/tools/test-ca
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

export RIAK_HOST = localhost
export RIAK_PORT = 8087

CERTS_DIR = $(realpath $(CURDIR))/../riak/tests/resources
unexport PYENV_VERSION

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087 18098 $(CA_DIR)/certs/cacert.pem $(CA_DIR)/certs/riak-test-cert.pem $(CA_DIR)/private/riak-test-key.pem
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true false

configure_timeseries:
	@../setup.py setup_timeseries --riak-admin=$(RIAK_ADMIN)

compile:
	@echo NO-OP

lint:
	@pip install --upgrade pep8 flake8
	@cd ..; pep8 --exclude=riak/pb riak *.py
	@cd ..; flake8 --exclude=riak/pb riak *.py
	@openssl verify -CAfile $(CERTS_DIR)/ca.crt $(CERTS_DIR)/client.crt
	@openssl verify -CAfile $(CERTS_DIR)/ca.crt $(CERTS_DIR)/server.crt

# TODO test: setup test_normal test_security
test: setup test_normal

test_normal:
	@echo "Testing Riak Python Client (without security)"
	@../setup.py disable_security --riak-admin=$(RIAK_ADMIN)
	@RIAK_TEST_PROTOCOL='pbc' RIAK_TEST_PB_PORT=8087 RUN_YZ=1 RUN_DATATYPES=1 RUN_INDEXES=1 ./tox_runner.sh ..
	@RIAK_TEST_PROTOCOL='http' RIAK_TEST_HTTP_PORT=8098 RUN_YZ=1 RUN_DATATYPES=1 RUN_INDEXES=1 ./tox_runner.sh ..

test_security:
	@echo "Testing Riak Python Client (with security)"
	@../setup.py enable_security --riak-admin=$(RIAK_ADMIN)
	@RIAK_TEST_PROTOCOL='pbc' RIAK_TEST_PB_PORT=8087 RUN_YZ=1 RUN_INDEXES=1 RUN_SECURITY=1 RUN_POOL=0 RUN_RESOLVE=0 ./tox_runner.sh ..
	@RIAK_TEST_PROTOCOL='http' RIAK_TEST_HTTP_PORT=18098 RUN_YZ=1 RUN_INDEXES=1 RUN_SECURITY=1 RUN_POOL=0 RUN_RESOLVE=0 ./tox_runner.sh ..

test_timeseries:
	@echo "Testing Riak Python Client (timeseries)"
	@../setup.py disable_security --riak-admin=${RIAK_ADMIN}
	@RIAK_TEST_PROTOCOL='pbc' RUN_YZ=0 RUN_DATATYPES=0 RUN_INDEXES=1 RUN_TIMESERIES=1 ./tox_runner.sh ..

setup:
	./tox_setup.sh

RIAK_CONF = ${RIAK_DIR}/etc/riak.conf
# ADVANCED_CONF = ${RIAK_DIR}/etc/advanced.config
# RIAK = ${RIAK_DIR}/bin/riak
RIAK_ADMIN = ${RIAK_DIR}/bin/riak-admin
# CERTS_DIR = $(shell pwd)/../src/test/resources

preconfigure:
	echo "storage_backend = leveldb" >> ${RIAK_CONF}
	echo "search = on" >> ${RIAK_CONF}
	echo "listener.protobuf.internal = 127.0.0.1:8087" >> ${RIAK_CONF}
	echo "listener.http.internal = 127.0.0.1:8098" >> ${RIAK_CONF}

configure:
	@../setup.py create_bucket_types --riak-admin=${RIAK_ADMIN}

compile:
	@../setup.py build

lint:
	@pip install --upgrade pep8 pyflakes
	@cd ..; pep8 riak
	@cd ..; pyflakes riak

test:
	@RUN_YZ=1 ../setup.py test

.PHONY: all pb_compile pb_clean release install
# TODO: git submodule

all: pb_compile

clean: pb_clean

pb_compile:
	echo "==> Python (compile)"
	protoc -I riak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	python setup.py build_messages

pb_clean:
	echo "==> Python (clean)"
	rm -rf riak/pb/*.pyc riak/pb/*_pb2.py
	rm -rf riak/pb/__pycache__ __pycache__

release: pb_clean
ifeq ($(RELEASE_GPG_KEYNAME),)
	@echo "RELEASE_GPG_KEYNAME must be set to release/deploy"
else
	@echo "==> Python (release)"
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python2.7 setup.py build_messages build --build-base=riak
	@python2.7 setup.py build --build-base=python bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@rm -rf *.pyc riak_pb/*_pb2.py riak_pb/*.pyc riak_pb.egg-info python

	@echo "==> Python 3.3 (release)"
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.3 setup.py build_messages build --build-base=riak
	@python3.3 setup.py build --build-base=riak bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3

	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.4 setup.py build_messages build --build-base=riak
	@python3.4 setup.py build --build-base=riak bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3

	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.4 setup.py build_messages build --build-base=riak
	@python3.4 setup.py build --build-base=riak sdist upload -s -i $(RELEASE_GPG_KEYNAME)
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3
endif

install: pb_compile
	@echo "==> Python (install)"
	@python setup.py build_messages build --build-base=riak install

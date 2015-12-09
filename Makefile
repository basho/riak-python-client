.PHONY: all compile clean release
.PHONY: python_compile python_clean python_release python_install
.PHONY: python3_compile python3_clean python3_release python3_install

all:  python_compile python3_compile

clean: python_clean python3_clean

release: python_release python3_release

# Python 2.x specific build steps
python_compile:
	@echo "==> Python (compile)"
	@protoc -I riak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	@python2 setup.py build_messages

python_clean:
	@echo "==> Python (clean)"
	@python2 setup.py clean_messages
	@rm -rf riak/pb/*.pyc riak/pb/*_pb2.py riak/pb/*.pyc

python_release: python_clean
ifeq ($(RELEASE_GPG_KEYNAME),)
	@echo "RELEASE_GPG_KEYNAME must be set to release/deploy"
else
	@echo "==> Python (release)"
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python2.7 setup.py build_messages build --build-base=riak
	@python2.7 setup.py build --build-base=python bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@python2.7 setup.py clean --build-base=python clean_messages
	@rm -rf *.pyc riak_pb/*_pb2.py riak_pb/*.pyc riak_pb.egg-info python
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python2.7 setup.py build_messages build --build-base=riak
	@python2.7 setup.py build --build-base=python sdist upload -s -i $(RELEASE_GPG_KEYNAME)
	@python2.6 setup.py clean --build-base=python clean_messages
	@rm -rf riak_pb/*_pb2.pyc *.pyc python_riak_pb.egg-info python
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python2.6 setup.py build_messages build --build-base=riak
	@python2.6 setup.py build --build-base=riak bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
endif

python_install: python_compile
	@echo "==> Python (install)"
	@./setup.py build_messages build --build-base=riak install

# Python 3.x specific build steps
python3_compile:
	@echo "==> Python 3 (compile)"
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3 setup.py build_messages build --build-base=riak

python3_clean:
	@echo "==> Python 3 (clean)"
	@python3 setup.py clean --build-base=riak clean_messages
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3

python3_release: python3_clean
ifeq ($(RELEASE_GPG_KEYNAME),)
	@echo "RELEASE_GPG_KEYNAME must be set to release/deploy"
else
	@echo "==> Python 3 (release)"
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.4 setup.py build_messages build --build-base=riak
	@python3.4 setup.py build --build-base=riak bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@python3.4 setup.py clean --build-base=riak clean_messages
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.4 setup.py build_messages build --build-base=riak
	@python3.4 setup.py build --build-base=riak sdist upload -s -i $(RELEASE_GPG_KEYNAME)
	@python3.4 setup.py clean --build-base=riak clean_messages
	@rm -rf riak/pb/*_pb2.py riak/pb/__pycache__ __pycache__ python3_riak/pb.egg-info python3
	@protoc -Isrc --python_out=riak/pb src/*.proto
	@python3.3 setup.py build_messages build --build-base=riak
	@python3.3 setup.py build --build-base=riak bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
endif

python3_install: python3_compile
	@echo "==> Python 3 (install)"
	@python3 setup.py build_messages build --build-base=riak install

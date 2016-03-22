.PHONY: pb_clean pb_compile pb_build release release_sdist

PANDOC_VERSION := $(shell pandoc --version)

PROTOC_VERSION := $(strip $(shell protoc --version))

clean: pb_clean

pb_clean:
	@echo "==> Python (clean)"
	@rm -rf riak/pb/*_pb2.py riak/pb/*.pyc riak/pb/*.so riak/pb/__pycache__ __pycache__ py-build

pb_compile: pb_clean
ifeq ($(PROTOC_VERSION),libprotoc 2.6.1)
	sed -e '/^import/d' -e '/^option java/d' riak_pb/src/*.proto > pb/riak.proto
	protoc --proto_path=pb --python_out=riak/pb pb/riak.proto
	protoc --proto_path=pb --cpp_out=src pb/riak.proto
	# TODO: python setup.py build_messages
else
	$(error The protoc command must be version 2.6.1 ($(PROTOC_VERSION)))
endif

release_sdist:
ifeq ($(PANDOC_VERSION),)
	$(error The pandoc command is required to correctly convert README.md to rst format)
endif
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@python -c 'import pypandoc'
	@echo "==> Python (sdist release)"
	@python setup.py sdist upload -s -i $(RELEASE_GPG_KEYNAME)

release: release_sdist
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@echo "==> Python 2.7 (release)"
	@python2.7 setup.py build --build-base=py-build/2.7 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
# @echo "==> Python 3.3 (release)"
# @python3.3 setup.py build --build-base=py-build/3.3 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
# @echo "==> Python 3.4 (release)"
# @python3.4 setup.py build --build-base=py-build/3.4 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
# @echo "==> Python 3.5 (release)"
# @python3.5 setup.py build --build-base=py-build/3.5 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)

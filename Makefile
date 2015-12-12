.PHONY: pb_compile pb_clean pb_build release install
# TODO: git submodule


clean: pb_clean

pb_compile:
	echo "==> Python (compile)"
	protoc -Iriak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	python setup.py build_messages

pb_clean:
	@echo "==> Python (clean)"
	@rm -rf riak/pb/*_pb2.py riak/pb/*.pyc riak/pb/__pycache__ __pycache__ py-build

pb_build: pb_clean pb_compile
	@echo "==> Python 2.7 (build)"
	@python2.7 setup.py build --build-base=py-build/2.7
	@echo "==> Python 3.3 (build)"
	@python3.3 setup.py build --build-base=py-build/3.3
	@echo "==> Python 3.4 (build)"
	@python3.4 setup.py build --build-base=py-build/3.4
	@echo "==> Python 3.5 (build)"
	@python3.5 setup.py build --build-base=py-build/3.5

release: pb_build
ifeq ($(RELEASE_GPG_KEYNAME),)
	@echo "RELEASE_GPG_KEYNAME must be set to release/deploy"
else
	@echo "==> Python 2.7 (release)"
	@python2.7 setup.py build --build-base=py-build/2.7 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.3 (release)"
	@python3.3 setup.py build --build-base=py-build/3.3 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.4 (release)"
	@python3.4 setup.py build --build-base=py-build/3.4 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.5 (release)"
	@python3.5 setup.py build --build-base=py-build/3.5 sdist upload -s -i $(RELEASE_GPG_KEYNAME)
endif

install: pb_compile
	@echo "==> Python (install)"
	@python setup.py build_messages build --build-base=py-build install

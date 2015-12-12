.PHONY: pb_compile pb_clean release install
# TODO: git submodule

CLEAN = rm -rf riak/pb/*.pyc riak/pb/__pycache__ __pycache__ py-build

clean: pb_clean

pb_compile:
	echo "==> Python (compile)"
	protoc -Iriak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	python setup.py build_messages

pb_clean:
	@echo "==> Python (clean)"
	$(CLEAN)

release: pb_clean
ifeq ($(RELEASE_GPG_KEYNAME),)
	@echo "RELEASE_GPG_KEYNAME must be set to release/deploy"
else
	@echo "==> Python 2.7 (release)"
	@python2.7 setup.py build_messages build --build-base=py-build
	@python2.7 setup.py build --build-base=py-build bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	$(CLEAN)
	@echo "==> Python 3.3 (release)"
	@python3.3 setup.py build_messages build --build-base=py-build
	@python3.3 setup.py build --build-base=py-build bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	$(CLEAN)
	@echo "==> Python 3.4 (release)"
	@python3.4 setup.py build_messages build --build-base=py-build
	@python3.4 setup.py build --build-base=py-build bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	$(CLEAN)
	@echo "==> Python 3.5 (release)"
	@python3.5 setup.py build_messages build --build-base=py-build
	@python3.5 setup.py build --build-base=py-build sdist upload -s -i $(RELEASE_GPG_KEYNAME)
	$(CLEAN)
endif

install: pb_compile
	@echo "==> Python (install)"
	@python setup.py build_messages build --build-base=py-build install

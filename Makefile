.PHONY: pb_clean pb_compile pb_build release

clean: pb_clean

pb_clean:
	@echo "==> Python (clean)"
	@rm -rf riak/pb/*_pb2.py riak/pb/*.pyc riak/pb/__pycache__ __pycache__ py-build

pb_compile: pb_clean
	@echo "==> Python (compile)"
	@protoc -Iriak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	@python setup.py build_messages

release_sdist:
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
else
	@echo "==> Python (sdist release)"
	@python setup.py sdist upload -s -i $(RELEASE_GPG_KEYNAME)
endif

release: release_sdist
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
else
	@echo "==> Python 2.7 (release)"
	@python2.7 setup.py build --build-base=py-build/2.7 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.3 (release)"
	@python3.3 setup.py build --build-base=py-build/3.3 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.4 (release)"
	@python3.4 setup.py build --build-base=py-build/3.4 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.5 (release)"
	@python3.5 setup.py build --build-base=py-build/3.5 bdist_egg upload -s -i $(RELEASE_GPG_KEYNAME)
endif

unexport LANG
unexport LC_ADDRESS
unexport LC_COLLATE
unexport LC_CTYPE
unexport LC_IDENTIFICATION
unexport LC_MEASUREMENT
unexport LC_MESSAGES
unexport LC_MONETARY
unexport LC_NAME
unexport LC_NUMERIC
unexport LC_PAPER
unexport LC_TELEPHONE
unexport LC_TIME

PANDOC_VERSION := $(shell pandoc --version)
PROTOC_VERSION := $(shell protoc --version)

PROJDIR   = $(realpath $(CURDIR))
TOOLS_DIR = $(PROJDIR)/tools/devrel
CA_DIR    = $(PROJDIR)/tools/test-ca

.PHONY: lint
lint:
	./.runner lint

.PHONY: pb_clean
pb_clean:
	@echo "==> Python (clean)"
	@rm -rf riak/pb/*_pb2.py riak/pb/*.pyc riak/pb/__pycache__ __pycache__ py-build

.PHONY: pb_compile
pb_compile: pb_clean
ifeq ($(PROTOC_VERSION),)
	$(error The protoc command is required to parse proto files)
endif
ifneq ($(PROTOC_VERSION),libprotoc 2.5.0)
	$(error protoc must be version 2.5.0)
endif
	@echo "==> Python (compile)"
	@protoc -Iriak_pb/src --python_out=riak/pb riak_pb/src/*.proto
	@python setup.py build_messages

.PHONY: test_sdist
test_sdist:
	@python setup.py sdist

.PHONY: release_sdist
release_sdist:
ifeq ($(VERSION),)
	$(error VERSION must be set to build a release and deploy this package)
endif
ifeq ($(PANDOC_VERSION),)
	$(error The pandoc command is required to correctly convert README.md to rst format)
endif
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@python -c 'import pypandoc'
	@echo "==> Python tagging version $(VERSION)"
	# NB: Python client version strings do NOT start with 'v'. Le Sigh.
	# validate VERSION and allow pre-releases
	@./build/publish $(VERSION) validate
	@git tag --sign -a "$(VERSION)" -m "riak-python-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	@git push --tags
	@echo "==> Python (sdist release)"
	@python setup.py sdist upload --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@./build/publish $(VERSION)

.PHONY: release
release: release_sdist
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@echo "==> Python 2.7 (release)"
	@python2.7 setup.py build --build-base=py-build/2.7 bdist_egg upload --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.3 (release)"
	@python3.3 setup.py build --build-base=py-build/3.3 bdist_egg upload --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.4 (release)"
	@python3.4 setup.py build --build-base=py-build/3.4 bdist_egg upload --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.5 (release)"
	@python3.5 setup.py build --build-base=py-build/3.5 bdist_egg upload --show-response --sign --identity $(RELEASE_GPG_KEYNAME)

.PHONY: unit-test
unit-test:
	@./.runner unit-test

.PHONY: integration-test
integration-test:
	@./.runner integration-test

.PHONY: security-test
security-test:
	@./.runner security-test

.PHONY: test
test: integration-test

.PHONY: help
help:
	@echo ''
	@echo ' Targets:
	@echo ' ------------------------------------------------------------'
	@echo ' lint             - Run linter (flake8)                      '
	@echo ' test             - Run all tests                            '
	@echo ' unit-test        - Run unit tests                           '
	@echo ' integration-test - Run integration tests                    '
	@echo ' security-test    - Run integration tests (security enabled) '
	@echo ' ------------------------------------------------------------'
	@echo ''

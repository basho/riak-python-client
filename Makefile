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

PROJDIR  := $(realpath $(CURDIR))
DOCSRC   := $(PROJDIR)/docsrc
DOCTREES := $(DOCSRC)/doctrees
DOCSDIR  := $(PROJDIR)/docs

PYPI_REPOSITORY ?= pypi

all: lint test

.PHONY: lint
lint:
	$(PROJDIR)/.runner lint

.PHONY: docs
docs:
	sphinx-build -b html -d $(DOCTREES) $(DOCSRC) $(DOCSDIR)
	@echo "The HTML pages are in $(DOCSDIR)"

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
ifeq ("$(wildcard $(PROJDIR)/.python-version)","")
	$(error expected $(PROJDIR)/.python-version to exist. Run $(PROJDIR)/build/pyenv-setup)
endif
	@python -c 'import pypandoc'
	@echo "==> Python tagging version $(VERSION)"
	@$(PROJDIR)/build/publish $(VERSION) validate
	@git tag --sign -a "$(VERSION)" -m "riak-python-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	@git push --tags
	@echo "==> pypi repository: $(PYPI_REPOSITORY)"
	@echo "==> Python (sdist)"
	@python setup.py sdist upload --repository $(PYPI_REPOSITORY) --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@$(PROJDIR)/build/publish $(VERSION)

.PHONY: release
release: release_sdist
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
ifeq ("$(wildcard $(PROJDIR)/.python-version)","")
	$(error expected $(PROJDIR)/.python-version to exist. Run $(PROJDIR)/build/pyenv-setup)
endif
	@echo "==> pypi repository: $(PYPI_REPOSITORY)"
	@echo "==> Python 2.7 (bdist_egg)"
	@python2.7 setup.py build --build-base=py-build/2.7 bdist_egg upload --repository $(PYPI_REPOSITORY) --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.3 (bdist_egg)"
	@python3.3 setup.py build --build-base=py-build/3.3 bdist_egg upload --repository $(PYPI_REPOSITORY) --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.4 (bdist_egg)"
	@python3.4 setup.py build --build-base=py-build/3.4 bdist_egg upload --repository $(PYPI_REPOSITORY) --show-response --sign --identity $(RELEASE_GPG_KEYNAME)
	@echo "==> Python 3.5 (bdist_egg)"
	@python3.5 setup.py build --build-base=py-build/3.5 bdist_egg upload --repository $(PYPI_REPOSITORY) --show-response --sign --identity $(RELEASE_GPG_KEYNAME)

.PHONY: unit-test
unit-test:
	@$(PROJDIR)/.runner unit-test

.PHONY: integration-test
integration-test:
	@$(PROJDIR)/.runner integration-test

.PHONY: security-test
security-test:
	@$(PROJDIR)/.runner security-test

.PHONY: timeseries-test
timeseries-test:
	@$(PROJDIR)/.runner timeseries-test

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
	@echo ' timeseries-test  - Run timeseries integration tests         '
	@echo ' ------------------------------------------------------------'
	@echo ''

foo:
	@echo 'Usage: make {pb|docs|test}'

pb:
	test -f riakclient.proto || ln -s ../../apps/riak_kv/src/riakclient.proto
	protoc --python_out=. riakclient.proto

docs:
	mkdir -p docs
	pydoc -w riak
	mv riak.html docs

test:
	python unit_tests.py

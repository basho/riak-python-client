Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$env:RIAK_TEST_HOST = 'riak-test'
$env:RIAK_TEST_PROTOCOL = 'pbc'
$env:RIAK_TEST_PB_PORT = 10017
$env:RUN_DATATYPES = 1
$env:RUN_INDEXES = 1
$env:RUN_POOL = 1
$env:RUN_YZ = 1

flake8 --exclude=riak/pb riak commands.py setup.py version.py
if ($LastExitCode -ne 0) {
    throw 'flake8 failed!'
}

python setup.py test
if ($LastExitCode -ne 0) {
    throw 'python tests failed!'
}

#!/usr/bin/env bash

for pbin in .tox/*/bin
do
    echo $pbin
    pip="$pbin/pip"
    $pip uninstall riak_pb --yes
    $pip uninstall riak --yes
    $pip uninstall protobuf --yes
    $pip uninstall python3-riak-pb --yes
    $pip uninstall python3-protobuf --yes
    echo -----
done 

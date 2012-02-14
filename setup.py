#!/usr/bin/env python
import glob
import os
import subprocess
import platform
from setuptools import setup, find_packages

def make_docs():
    if not os.path.exists('docs'):
        os.mkdir('docs')
    subprocess.call(['pydoc', '-w', 'riak'])
    for name in glob.glob('*.html'):
        os.rename(name, 'docs/%s' % name)

def make_pb():
    subprocess.call(['protoc', '--python_out=.', './riak/transports/riakclient.proto'])

if __name__ == "__main__":
    if platform.python_version() < '2.7':
        test_require = ["unittest2"]
    else:
        test_require = []

    setup(
        name='riak',
        version='1.3.0',
        packages = find_packages(),
        requires = test_require,
        extras_require = {
            'protobuf': ['protobuf>=2.3.0'],
            },
        dependency_links = ["http://downloads.basho.com/support"],
        package_data = {
            '' : ['*.proto'],
            'riak' : ['erl_src/*']
            },
        description='Python client for Riak',
        zip_safe=True,
        include_package_data=True,
        license='Apache 2',
        platforms='Platform Independent',
        author='Basho Technologies',
        author_email='riak@basho.com',
        test_suite='riak.tests.suite',
        url='https://github.com/basho/riak-python-client'
   )

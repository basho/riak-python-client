#!/usr/bin/env python
import glob
import os
import subprocess
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
    setup(
        name='riak',
        version='1.2.1',
        packages = find_packages(),
        install_requires = ['protobuf>=2.3.0'],
        dependency_links = ["http://downloads.basho.com/support"],
        package_data = {
            '' : ['*.proto']
            },
        description='Python client for Riak',
        zip_safe=True,
        license='Apache 2',
        platforms='Platform Independent',
        author='Basho Technologies',
        author_email='riak@basho.com',
        test_suite='riak.tests.test_all',
        url='https://github.com/basho/riak-python-client'
   )

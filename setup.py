#!/usr/bin/env python
import glob
import os
import subprocess
from setuptools import setup, find_packages

#try:
#    from setuptools import setup
#except ImportError:
#    from distutils.core import setup

def make_docs():
    if not os.path.exists('docs'):
        os.mkdir('docs')
    subprocess.call(['pydoc', '-w', 'riak'])
    for name in glob.glob('*.html'):
        os.rename(name, 'docs/%s' % name)

def make_pb():
    subprocess.call(['protoc', '--python_out=./riak', 'riakclient.proto'])

if __name__ == '__main__':
    ##make_docs()
    ##make_pb()

    setup(
        name='riak_client',
        version='1.0',
        packages = find_packages(),
        install_requires = ['protobuf>=2.3.0'],
        package_data = {
            '' : ['*.proto']
        },
        description='Python client for Riak',
        license='Apache 2',
        platforms='Platform Independent',
        author='Basho Technologies',
        author_email='riak@basho.com',
        test_suite='riak.tests.test_all',
        url='https://bitbucket.org/basho/riak-python-client',
        )

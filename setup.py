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
    install_requires = {'protobuf': ['>= 2.4.0', '< 2.5.0'] }
    tests_require = []
    if platform.python_version() < '2.7':
        tests_require.append("unittest2")

    setup(
        name='riak',
        version='1.4.0',
        packages = find_packages(),
        install_requires = install_requires,
        tests_require = tests_require,
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
        author_email='clients@basho.com',
        test_suite='riak.tests.suite',
        url='https://github.com/basho/riak-python-client',
        classifiers = ['License :: OSI Approved :: Apache Software License',
                       'Intended Audience :: Developers',
                       'Operating System :: OS Independent',
                       'Topic :: Database']
   )

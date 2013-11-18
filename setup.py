#!/usr/bin/env python
import glob
import os
import subprocess
import platform
from setuptools import setup, find_packages
from version import get_version

def make_docs():
    if not os.path.exists('docs'):
        os.mkdir('docs')
    subprocess.call(['pydoc', '-w', 'riak'])
    for name in glob.glob('*.html'):
        os.rename(name, 'docs/%s' % name)

install_requires = ["riak_pb >=1.4.1, < 1.5.0"]
requires = ["riak_pb(>=1.4.1,<1.5.0)"]
tests_require = []
if platform.python_version() < '2.7':
    tests_require.append("unittest2")

setup(
    name='riak',
    version=get_version(),
    packages = find_packages(),
    requires = requires,
    install_requires = install_requires,
    tests_require = tests_require,
    package_data = {'riak' : ['erl_src/*']},
    description='Python client for Riak',
    zip_safe=True,
    options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
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

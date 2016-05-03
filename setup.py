#!/usr/bin/env python

import codecs
import sys

from setuptools import setup, find_packages
from version import get_version
from commands import setup_timeseries, build_messages

install_requires = ['six >= 1.8.0', 'basho_erlastic >= 2.1.1']
requires = ['six(>=1.8.0)', 'basho_erlastic(>= 2.1.1)']

if sys.version_info[:3] <= (2, 7, 9):
    install_requires.append("pyOpenSSL >= 0.14")
    requires.append("pyOpenSSL(>=0.14)")

if sys.version_info[:3] <= (3, 0, 0):
    install_requires.append('protobuf >=2.4.1, <2.7.0')
    requires.append('protobuf(>=2.4.1, <2.7.0)')
else:
    install_requires.append('python3_protobuf >=2.4.1, <2.6.0')
    requires.append('python3_protobuf(>=2.4.1, <2.6.0)')

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
    with codecs.open('README.rst', 'w', 'utf-8') as f:
        f.write(long_description)
except(IOError, ImportError):
    with open('README.md') as f:
        long_description = f.read()

setup(
    name='riak',
    version=get_version(),
    packages=find_packages(),
    requires=requires,
    install_requires=install_requires,
    package_data={'riak': ['erl_src/*']},
    description='Python client for Riak',
    long_description=long_description,
    zip_safe=True,
    options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
    include_package_data=True,
    license='Apache 2',
    platforms='Platform Independent',
    author='Basho Technologies',
    author_email='clients@basho.com',
    test_suite='riak.tests.suite',
    url='https://github.com/basho/riak-python-client',
    cmdclass={
        'build_messages': build_messages,
        'setup_timeseries': setup_timeseries
    },
    classifiers=['License :: OSI Approved :: Apache Software License',
                 'Intended Audience :: Developers',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.3',
                 'Programming Language :: Python :: 3.4',
                 'Programming Language :: Python :: 3.5',
                 'Topic :: Database']
    )

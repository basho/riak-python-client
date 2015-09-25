#!/usr/bin/env python
import sys
from setuptools import setup, find_packages
from version import get_version
from commands import preconfigure, configure, create_bucket_types, \
    setup_security, enable_security, disable_security

install_requires = ['six >= 1.8.0']
requires = ['six(>=1.8.0)']
if sys.version_info < (2, 7, 9):
    install_requires.append("pyOpenSSL >= 0.14")
    requires.append("pyOpenSSL(>=0.14)")
if sys.version_info < (3, ):
    install_requires.append("riak_pb >=2.0.0")
    requires.append("riak_pb(>=2.0.0)")
else:
    install_requires.append("python3_riak_pb >=2.0.0")
    requires.append("python3_riak_pb(>=2.0.0)")
tests_require = []
if sys.version_info < (2, 7):
    tests_require.append("unittest2")

setup(
    name='riak',
    version=get_version(),
    packages=find_packages(),
    requires=requires,
    install_requires=install_requires,
    tests_require=tests_require,
    package_data={'riak': ['erl_src/*']},
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
    cmdclass={'create_bucket_types': create_bucket_types,
              'setup_security': setup_security,
              'preconfigure': preconfigure,
              'configure': configure,
              'enable_security': enable_security,
              'disable_security': disable_security},
    classifiers=['License :: OSI Approved :: Apache Software License',
                 'Intended Audience :: Developers',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.3',
                 'Programming Language :: Python :: 3.4',
                 'Topic :: Database']
    )

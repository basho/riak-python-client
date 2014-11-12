#!/usr/bin/env python3
import platform
from six import PY2
from setuptools import setup, find_packages
from version import get_version
from commands import preconfigure, configure, create_bucket_types, \
    setup_security, enable_security, disable_security

install_requires = []
requires = []
if PY2:
    install_requires.append("pyOpenSSL >= 0.14")
    requires.append("pyOpenSSL(>=0.14)")
    install_requires.append("riak_pb >=2.0.0")
    requires.append("riak_pb(>=2.0.0)")
else:
    install_requires.append("python3_riak_pb >=2.0.0")
    requires.append("python3_riak_pb(>=2.0.0)")
tests_require = []
if platform.python_version() < '2.7':
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
                 'Topic :: Database']
    )

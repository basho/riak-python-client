#!/usr/bin/env python

import os
import platform
import distutils.command.build_ext
import distutils.extension
from setuptools import setup, find_packages
from version import get_version
from commands import setup_timeseries, build_messages

pb_env_key = 'PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'
install_requires = ['six >= 1.8.0']
requires = ['six(>=1.8.0)']
ext_modules = []
if platform.python_version() < '2.7.9':
    install_requires.append("pyOpenSSL >= 0.14")
    requires.append("pyOpenSSL(>=0.14)")

if platform.python_version() < '3.0':
    install_requires.append('protobuf >=2.4.1, <2.7.0')
    requires.append('protobuf(>=2.4.1, <2.7.0)')
    if os.environ.get(pb_env_key, 'unset') == 'cpp':
        ext_modules = [
            distutils.extension.Extension(
                'riak.pb._riak_pbcpp',
                sources=['src/_riak_pbcpp.cc', 'src/riak.pb.cc'],
                libraries=['protobuf'])
            ]
else:
    if os.environ.get(pb_env_key, 'unset') == 'cpp':
        raise StandardError('CPP protobuf is not supported in Python 3')
    else:
        install_requires.append('python3_protobuf >=2.4.1, <2.6.0')
        requires.append('python3_protobuf(>=2.4.1, <2.6.0)')

tests_require = []
if platform.python_version() < '2.7.0':
    tests_require.append("unittest2")

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError):
    long_description = open('README.md').read()


setup(
    name='riak',
    version=get_version(),
    packages=find_packages(),
    requires=requires,
    install_requires=install_requires,
    tests_require=tests_require,
    package_data={'riak': ['erl_src/*']},
    description='Python client for Riak',
    long_description=long_description,
    zip_safe=True,
    options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
    include_package_data=True,
    ext_modules=ext_modules,
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

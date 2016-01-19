import csv
import os
import os.path
import re
import shutil

from distutils.core import Command
from distutils.errors import DistutilsOptionError
from distutils.file_util import write_file
from distutils import log
from string import Template
from subprocess import Popen, PIPE


__all__ = ['create_bucket_types', 'build_messages',
           'setup_security', 'enable_security', 'disable_security',
           'setup_timeseries',
           'preconfigure', 'configure']


# Exception classes used by this module.
class CalledProcessError(Exception):
    """This exception is raised when a process run by check_call() or
    check_output() returns a non-zero exit status.
    The exit status will be stored in the returncode attribute;
    check_output() will also store the output in the output attribute.
    """
    def __init__(self, returncode, cmd, output=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % (self.cmd,
                                                                  self
                                                                  .returncode)


def check_output(*popenargs, **kwargs):
    """Run command with arguments and return its output as a byte string.

    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.

    The arguments are the same as for the Popen constructor.  Example:

    >>> check_output(["ls", "-l", "/dev/null"])
    'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

    The stdout argument is not allowed as it is used internally.
    To capture standard error in the result, use stderr=STDOUT.

    >>> import sys
    >>> check_output(["/bin/sh", "-c",
    ...               "ls -l non_existent_file ; exit 0"],
    ...              stderr=sys.stdout)
    'ls: non_existent_file: No such file or directory\n'
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be '
                         'overridden.')
    process = Popen(stdout=PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise CalledProcessError(retcode, cmd, output=output)
    return output

try:
    import simplejson as json
except ImportError:
    import json


class bucket_type_commands:
    def initialize_options(self):
        self.riak_admin = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        if self._check_available():
            for name in self._props:
                self._create_and_activate_type(name, self._props[name])

    def check_output(self, *args, **kwargs):
        if self.dry_run:
            log.info(' '.join(args))
            return bytearray()
        else:
            return check_output(*args, **kwargs)

    def _check_available(self):
        try:
            self.check_btype_command("list")
            return True
        except CalledProcessError:
            log.error("Bucket types are not supported on this Riak node!")
            return False

    def _create_and_activate_type(self, name, props):
        # Check status of bucket-type
        exists = False
        active = False
        try:
            status = self.check_btype_command('status', name)
        except CalledProcessError as e:
            status = e.output

        exists = ('not an existing bucket type' not in status.decode('ascii'))
        active = ('is active' in status.decode('ascii'))

        if exists or active:
            log.info("Updating {0} bucket-type with props {1}"
                     .format(repr(name), repr(props)))
            self.check_btype_command("update", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))
        else:
            log.info("Creating {0} bucket-type with props {1}"
                     .format(repr(name), repr(props)))
            self.check_btype_command("create", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))

        if not active:
            log.info('Activating {0} bucket-type'.format(repr(name)))
            self.check_btype_command("activate", name)

    def check_btype_command(self, *args):
        cmd = self._btype_command(*args)
        return self.check_output(cmd)

    def run_btype_command(self, *args):
        self.spawn(self._btype_command(*args))

    def _btype_command(self, *args):
        cmd = [self.riak_admin, "bucket-type"]
        cmd.extend(args)
        return cmd


class create_bucket_types(bucket_type_commands, Command):
    """
    Creates bucket-types appropriate for testing. By default this will create:

    * `pytest-maps` with ``{"datatype":"map"}``
    * `pytest-sets` with ``{"datatype":"set"}``
    * `pytest-counters` with ``{"datatype":"counter"}``
    * `pytest-consistent` with ``{"consistent":true}``
    * `pytest-write-once` with ``{"write_once": true}``
    * `pytest-mr`
    * `pytest` with ``{"allow_mult":false}``
    """

    description = "create bucket-types used in integration tests"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script')
    ]

    _props = {
        'pytest-maps': {'datatype': 'map'},
        'pytest-sets': {'datatype': 'set'},
        'pytest-counters': {'datatype': 'counter'},
        'pytest-consistent': {'consistent': True},
        'pytest-write-once': {'write_once': True},
        'pytest-mr': {},
        'pytest': {'allow_mult': False}
    }


class setup_timeseries(bucket_type_commands, Command):
    """
    Creates bucket-types appropriate for timeseries.
    """

    description = "create bucket-types used in timeseries tests"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script')
    ]

    _props = {
        'GeoCheckin': {
            'n_val': 3,
            'table_def': '''
                CREATE TABLE GeoCheckin (
                    geohash varchar not null,
                    user varchar not null,
                    time timestamp not null,
                    weather varchar not null,
                    temperature double,
                    PRIMARY KEY(
                        (geohash, user, quantum(time, 15, m)),
                        geohash, user, time
                    )
                )'''
        }
    }


class security_commands(object):
    def check_security_command(self, *args):
        cmd = self._security_command(*args)
        return self.check_output(cmd)

    def run_security_command(self, *args):
        self.spawn(self._security_command(*args))

    def _security_command(self, *args):
        cmd = [self.riak_admin, "security"]
        if isinstance(args, tuple):
            for elem in args:
                cmd.extend(elem)
        else:
            cmd.extend(args)
        return cmd

    def check_output(self, *args, **kwargs):
        if self.dry_run:
            log.info(' '.join(args))
            return bytearray()
        else:
            return check_output(*args, **kwargs)


class setup_security(Command, security_commands):
    """
    Sets up security for testing. By default this will create:

    * User `testuser` with password `testpassword`
    * User `certuser` with password `certpass`
    * Two security sources
    * Permissions on
        * riak_kv.get
        * riak_kv.put
        * riak_kv.delete
        * riak_kv.index
        * riak_kv.list_keys
        * riak_kv.list_buckets
        * riak_kv.mapreduce
        * riak_core.get_bucket
        * riak_core.set_bucket
        * riak_core.get_bucket_type
        * riak_core.set_bucket_type
        * search.admin
        * search.query
    """

    description = "create security settings used in integration tests"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script'),
        ('username=', None, 'test user account'),
        ('password=', None, 'password for test user account'),
        ('certuser=', None, 'certificate test user account'),
        ('certpass=', None, 'password for certificate test user account')
    ]

    _commands = [
        "add-user $USERNAME password=$PASSWORD",
        "add-source $USERNAME 127.0.0.1/32 password",
        "add-user $CERTUSER password=$CERTPASS",
        "add-source $CERTUSER 127.0.0.1/32 certificate"
    ]

    _grants = {
        "riak_kv.get": ["any"],
        "riak_kv.get_preflist": ["any"],
        "riak_kv.put": ["any"],
        "riak_kv.delete": ["any"],
        "riak_kv.index": ["any"],
        "riak_kv.list_keys": ["any"],
        "riak_kv.list_buckets": ["any"],
        "riak_kv.mapreduce": ["any"],
        "riak_core.get_bucket": ["any"],
        "riak_core.set_bucket": ["any"],
        "riak_core.get_bucket_type": ["any"],
        "riak_core.set_bucket_type": ["any"],
        "search.admin": ["index", "schema"],
        "search.query": ["index", "schema"]
    }

    def initialize_options(self):
        self.riak_admin = None
        self.username = None
        self.password = None
        self.certuser = None
        self.certpass = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")
        if self.username is None:
            self.username = 'testuser'
        if self.password is None:
            self.password = 'testpassword'
        if self.certuser is None:
            self.certuser = 'certuser'
        if self.certpass is None:
            self.certpass = 'certpass'

    def run(self):
        if self._check_available():
            for cmd in self._commands:
                # Replace the username and password if specified
                s = Template(cmd)
                newcmd = s.substitute(USERNAME=self.username,
                                      PASSWORD=self.password,
                                      CERTUSER=self.certuser,
                                      CERTPASS=self.certpass)
                log.info("Security command: {0}".format(repr(newcmd)))
                self.run_security_command(tuple(newcmd.split(' ')))
            for perm in self._grants:
                self._apply_grant(perm, self._grants[perm])

    def _check_available(self):
        try:
            self.check_security_command("status")
            return True
        except CalledProcessError:
            log.error("Security is not supported on this Riak node!")
            return False

    def _apply_grant(self, perm, targets):
        for target in targets:
            cmd = ["grant", perm, "on", target, "to", self.username]
            log.info("Granting permission {0} on {1} to {2}"
                     .format(repr(perm), repr(target), repr(self.username)))
            self.run_security_command(cmd)
            cmd = ["grant", perm, "on", target, "to", self.certuser]
            log.info("Granting permission {0} on {1} to {2}"
                     .format(repr(perm), repr(target), repr(self.certuser)))
            self.run_security_command(cmd)


class enable_security(Command, security_commands):
    """
    Actually turn on security.
    """
    description = "turn on security within Riak"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script'),
    ]

    def initialize_options(self):
        self.riak_admin = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        cmd = "enable"
        self.run_security_command(tuple(cmd.split(' ')))


class disable_security(Command, security_commands):
    """
    Actually turn off security.
    """
    description = "turn off security within Riak"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script'),
    ]

    def initialize_options(self):
        self.riak_admin = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        cmd = "disable"
        self.run_security_command(tuple(cmd.split(' ')))


class preconfigure(Command):
    """
    Sets up security configuration.

    * Update these lines in riak.conf
        * storage_backend = leveldb
        * search = on
        * listener.protobuf.internal = 127.0.0.1:8087
        * listener.http.internal = 127.0.0.1:8098
        * listener.https.internal = 127.0.0.1:18098
        * ssl.certfile = $pwd/tests/resources/server.crt
        * ssl.keyfile = $pwd/tests/resources/server.key
        * ssl.cacertfile = $pwd/tests/resources/ca.crt
        * check_crl = off
    """

    description = "preconfigure security settings used in integration tests"

    user_options = [
        ('riak-conf=', None, 'path to the riak.conf file'),
        ('host=', None, 'IP of host running Riak'),
        ('pb-port=', None, 'protocol buffers port number'),
        ('https-port=', None, 'https port number')
    ]

    def initialize_options(self):
        self.riak_conf = None
        self.host = "127.0.0.1"
        self.pb_port = "8087"
        self.http_port = "8098"
        self.https_port = "18098"

    def finalize_options(self):
        if self.riak_conf is None:
            raise DistutilsOptionError("riak-conf option not set")

    def run(self):
        self.cert_dir = os.path.dirname(os.path.realpath(__file__)) + \
            "/riak/tests/resources"
        self._update_riak_conf()

    def _update_riak_conf(self):
        http_host = self.host + ':' + self.http_port
        https_host = self.host + ':' + self.https_port
        pb_host = self.host + ':' + self.pb_port
        self._backup_file(self.riak_conf)
        conf = None
        with open(self.riak_conf, 'r', buffering=1) as f:
            conf = f.read()
        conf = re.sub(r'search\s+=\s+off', r'search = on', conf)
        conf = re.sub(r'##[ ]+ssl\.', r'ssl.', conf)
        conf = re.sub(r'ssl.certfile\s+=\s+\S+',
                      r'ssl.certfile = ' + self.cert_dir + '/server.crt',
                      conf)
        conf = re.sub(r'storage_backend\s+=\s+\S+',
                      r'storage_backend = leveldb',
                      conf)
        conf = re.sub(r'ssl.keyfile\s+=\s+\S+',
                      r'ssl.keyfile = ' + self.cert_dir + '/server.key',
                      conf)
        conf = re.sub(r'ssl.cacertfile\s+=\s+\S+',
                      r'ssl.cacertfile = ' + self.cert_dir +
                      '/ca.crt',
                      conf)
        conf = re.sub(r'#*[ ]*listener.http.internal\s+=\s+\S+',
                      r'listener.http.internal = ' + http_host,
                      conf)
        conf = re.sub(r'#*[ ]*listener.https.internal\s+=\s+\S+',
                      r'listener.https.internal = ' + https_host,
                      conf)
        conf = re.sub(r'listener.protobuf.internal\s+=\s+\S+',
                      r'listener.protobuf.internal = ' + pb_host,
                      conf)
        conf += 'check_crl = off\n'
        # Older versions of OpenSSL client library need to match on the server
        conf += 'tls_protocols.tlsv1 = on\n'
        conf += 'tls_protocols.tlsv1.1 = on\n'
        with open(self.riak_conf, 'w', buffering=1) as f:
            f.write(conf)

    def _backup_file(self, name):
        backup = name + ".bak"
        if os.path.isfile(name):
            shutil.copyfile(name, backup)
        else:
            log.info("Cannot backup missing file {0}".format(repr(name)))


class configure(Command):
    """
    Sets up security configuration.

    * Run setup_security and create_bucket_types
    """

    description = "create bucket types and security settings for testing"

    user_options = create_bucket_types.user_options + \
        setup_security.user_options

    def initialize_options(self):
        self.riak_admin = None
        self.username = None
        self.password = None

    def finalize_options(self):
        bucket = self.distribution.get_command_obj('create_bucket_types')
        bucket.riak_admin = self.riak_admin
        security = self.distribution.get_command_obj('setup_security')
        security.riak_admin = self.riak_admin
        security.username = self.username
        security.password = self.password

    def run(self):
        # Run all relevant sub-commands.
        for cmd_name in self.get_sub_commands():
            self.run_command(cmd_name)

    sub_commands = [('create_bucket_types', None), ('setup_security', None)]


class ComparableMixin(object):
    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except (AttributeError, TypeError):
            # _cmpkey not implemented, or return different type,
            # so I can't compare with "other".
            return NotImplemented

    def __lt__(self, other):
        return self._compare(other, lambda s, o: s < o)

    def __le__(self, other):
        return self._compare(other, lambda s, o: s <= o)

    def __eq__(self, other):
        return self._compare(other, lambda s, o: s == o)

    def __ge__(self, other):
        return self._compare(other, lambda s, o: s >= o)

    def __gt__(self, other):
        return self._compare(other, lambda s, o: s > o)

    def __ne__(self, other):
        return self._compare(other, lambda s, o: s != o)


class MessageCodeMapping(ComparableMixin):
    def __init__(self, code, message, proto):
        self.code = int(code)
        self.message = message
        self.proto = proto
        self.message_code_name = self._message_code_name()
        self.module_name = 'riak.pb.{0}_pb2'.format(self.proto)
        self.message_class = self._message_class()

    def _cmpkey(self):
        return self.code

    def __hash__(self):
        return self.code

    def _message_code_name(self):
        strip_rpb = re.sub(r"^Rpb", "", self.message)
        word = re.sub(r"([A-Z]+)([A-Z][a-z])", r'\1_\2', strip_rpb)
        word = re.sub(r"([a-z\d])([A-Z])", r'\1_\2', word)
        word = word.replace("-", "_")
        return "MSG_CODE_" + word.upper()

    def _message_class(self):
        try:
            pbmod = __import__(self.module_name, globals(), locals(),
                               [self.message])
            klass = pbmod.__dict__[self.message]
            return klass
        except KeyError:
            log.warn("Did not find '%s' message class in module '%s'",
                     self.message, self.module_name)
        except ImportError as e:
            log.error("Could not import module '%s', exception: %s",
                      self.module_name, e)
            raise
        return None


# NOTE: TO RUN THIS SUCCESSFULLY, YOU NEED TO HAVE THESE
#       PACKAGES INSTALLED:
#       protobuf or python3_protobuf
#       six
#
#       Run the following command to install them:
#       python setup.py install
#
# TO DEBUG: Set DISTUTILS_DEBUG=1 in the environment or run as
# 'python setup.py -vv build_messages'
class build_messages(Command):
    """
    Generates message code mappings. Add to the build process using::

        setup(cmd_class={'build_messages': build_messages})
    """

    description = "generate protocol message code mappings"

    user_options = [
        ('source=', None, 'source CSV file containing message code mappings'),
        ('destination=', None, 'destination Python source file')
    ]

    # Used in loading and generating
    _pb_imports = set()
    _messages = set()
    _linesep = os.linesep
    _indented_item_sep = ',{0}    '.format(_linesep)

    _docstring = [
        ''
        '# This is a generated file. DO NOT EDIT.',
        '',
        '"""',
        'Constants and mappings between Riak protocol codes and messages.',
        '"""',
        ''
    ]

    def initialize_options(self):
        self.source = None
        self.destination = None
        self.update_import = None

    def finalize_options(self):
        if self.source is None:
            self.source = 'riak_pb/src/riak_pb_messages.csv'
        if self.destination is None:
            self.destination = 'riak/pb/messages.py'

    def run(self):
        self.force = True
        self.make_file(self.source, self.destination,
                       self._load_and_generate, [])

    def _load_and_generate(self):
        self._format_python2_or_3()
        self._load()
        self._generate()

    def _load(self):
        with open(self.source, 'r', buffering=1) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                message = MessageCodeMapping(*row)
                self._messages.add(message)
                self._pb_imports.add(message.module_name)

    def _generate(self):
        self._contents = []
        self._generate_doc()
        self._generate_imports()
        self._generate_codes()
        self._generate_classes()
        write_file(self.destination, self._contents)

    def _generate_doc(self):
        # Write the license and docstring header
        self._contents.extend(self._docstring)

    def _generate_imports(self):
        # Write imports
        for im in sorted(self._pb_imports):
            self._contents.append("import {0}".format(im))

    def _generate_codes(self):
        # Write protocol code constants
        self._contents.extend(['', "# Protocol codes"])
        for message in sorted(self._messages):
            self._contents.append("{0} = {1}".format(message.message_code_name,
                                                     message.code))

    def _generate_classes(self):
        # Write message classes
        classes = [self._generate_mapping(message)
                   for message in sorted(self._messages)]

        classes = self._indented_item_sep.join(classes)
        self._contents.extend(['',
                               "# Mapping from code to protobuf class",
                               'MESSAGE_CLASSES = {',
                               '    ' + classes,
                               '}'])

    def _generate_mapping(self, m):
        if m.message_class is not None:
            klass = "{0}.{1}".format(m.module_name,
                                     m.message_class.__name__)
        else:
            klass = "None"
        pair = "{0}: {1}".format(m.message_code_name, klass)
        if len(pair) > 76:
            # Try to satisfy PEP8, lulz
            pair = (self._linesep + '    ').join(pair.split(' '))
        return pair

    def _format_python2_or_3(self):
        """
        Change the PB files to use full pathnames for Python 3.x
        and modify the metaclasses to be version agnostic
        """
        pb_files = set()
        with open(self.source, 'r', buffering=1) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                _, _, proto = row
                pb_files.add('riak/pb/{0}_pb2.py'.format(proto))

        for im in sorted(pb_files):
            with open(im, 'r', buffering=1) as pbfile:
                contents = 'from six import *\n' + pbfile.read()
                contents = re.sub(r'riak_pb2',
                                  r'riak.pb.riak_pb2',
                                  contents)
            # Look for this pattern in the protoc-generated file:
            #
            # class RpbCounterGetResp(_message.Message):
            #    __metaclass__ = _reflection.GeneratedProtocolMessageType
            #
            # and convert it to:
            #
            # @add_metaclass(_reflection.GeneratedProtocolMessageType)
            # class RpbCounterGetResp(_message.Message):
            contents = re.sub(
                r'class\s+(\S+)\((\S+)\):\s*\n'
                '\s+__metaclass__\s+=\s+(\S+)\s*\n',
                r'@add_metaclass(\3)\nclass \1(\2):\n', contents)

            with open(im, 'w', buffering=1) as pbfile:
                pbfile.write(contents)

import csv
import os
import os.path
import re

from distutils.core import Command
from distutils.errors import DistutilsOptionError
from distutils.file_util import write_file
from distutils import log
from subprocess import Popen, PIPE


__all__ = ['build_messages', 'setup_timeseries']


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

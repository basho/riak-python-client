"""
distutils commands for riak-python-client
"""

__all__ = ['create_bucket_types', 'setup_security', 'enable_security',
           'disable_security', 'preconfigure', 'configure']

from distutils import log
from distutils.core import Command
from distutils.errors import DistutilsOptionError
from subprocess import CalledProcessError, Popen, PIPE
from string import Template
import shutil
import re
import os.path

try:
    from subprocess import check_output
except ImportError:
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


class create_bucket_types(Command):
    """
    Creates bucket-types appropriate for testing. By default this will create:

    * `pytest-maps` with ``{"datatype":"map"}``
    * `pytest-sets` with ``{"datatype":"set"}``
    * `pytest-counters` with ``{"datatype":"counter"}``
    * `pytest-consistent` with ``{"consistent":true}``
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
        'pytest-mr': {},
        'pytest': {'allow_mult': False}
    }

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

        exists = ('not an existing bucket type' not in status)
        active = ('is active' in status)

        if exists or active:
            log.info("Updating {!r} bucket-type with props {!r}".format(name,
                                                                        props))
            self.check_btype_command("update", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))
        else:
            log.info("Creating {!r} bucket-type with props {!r}".format(name,
                                                                        props))
            self.check_btype_command("create", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))

        if not active:
            log.info('Activating {!r} bucket-type'.format(name))
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
                log.info("Security command: {!r}".format(newcmd))
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
            log.info("Granting permission {!r} on {!r} to {!r}"
                     .format(perm, target, self.username))
            self.run_security_command(cmd)
            cmd = ["grant", perm, "on", target, "to", self.certuser]
            log.info("Granting permission {!r} on {!r} to {!r}"
                     .format(perm, target, self.certuser))
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
        * listener.https.internal = 127.0.0.1:8099
        * ssl.certfile = $pwd/tests/resources/server.crt
        * ssl.keyfile = $pwd/tests/resources/server.key
        * ssl.cacertfile = $pwd/tests/resources/ca.crt
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
        self.https_port = "8099"

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
        f = open(self.riak_conf, 'r', False)
        conf = f.read()
        f.close()
        conf = re.sub(r'search\s+=\s+off', r'search = on', conf)
        conf = re.sub(r'^##\s+ssl.', 'ssl.', conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.certfile\s+=\s+\S+$',
                      r'ssl.certfile = ' + self.cert_dir + '/server.crt',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^storage_backend\s+=\s+\S+$',
                      r'storage_backend = leveldb',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.keyfile\s+=\s+\S+$',
                      r'ssl.keyfile = ' + self.cert_dir + '/server.key',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.cacertfile\s+=\s+\S+$',
                      r'ssl.cacertfile = ' + self.cert_dir +
                      '/ca.crt',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^#*\s*listener.http.internal\s+=\s+\S+',
                      r'listener.http.internal = ' + http_host,
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^#*\s*listener.https.internal\s+=\s+\S+',
                      r'listener.https.internal = ' + https_host,
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^listener.protobuf.internal\s+=\s+\S+',
                      r'listener.protobuf.internal = ' + pb_host,
                      conf, flags=re.MULTILINE)
        f = open(self.riak_conf, 'w', False)
        f.write(conf)
        f.close()

    def _backup_file(self, name):
        backup = name + ".bak"
        if os.path.isfile(name):
            shutil.copyfile(name, backup)
        else:
            log.info("Cannot backup missing file {!r}".format(name))


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

    sub_commands = [('create_bucket_types', None),
                    ('setup_security', None)
                    ]

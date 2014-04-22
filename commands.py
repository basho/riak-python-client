"""
distutils commands for riak-python-client
"""

__all__ = ['create_bucket_types']

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


class setup_security(Command):
    """
    Sets up security for testing. By default this will create:

    * User `testuser` with password `testpassword`
    * A security resource
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
        ('password=', None, 'password for test user account')
    ]

    _commands = [
        "enable",
        "add-user $USERNAME password=$PASSWORD",
        "add-source all 127.0.0.1/32 password"
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
        # Default values:
        self.username = 'testuser'
        self.password = 'testpassword'

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        if self._check_available():
            for cmd in self._commands:
                # Replace the username and password if specified
                s = Template(cmd)
                newcmd = s.substitute(USERNAME=self.username,
                                      PASSWORD=self.password)
                log.info("Security command: {!r}".format(newcmd))
                self.run_security_command(tuple(newcmd.split(' ')))
            for perm in self._grants:
                self._apply_grant(perm, self._grants[perm])

    def check_output(self, *args, **kwargs):
        if self.dry_run:
            log.info(' '.join(args))
            return bytearray()
        else:
            return check_output(*args, **kwargs)

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


class preconfig_security(Command):
    """
    Sets up security configuration.

    * Update these lines in riak.conf
        * storage_backend = leveldb
        * search = on
        * listener.protobuf.internal = 127.0.0.1:8087
        * listener.https.internal = 127.0.0.1:8098
        * ## listener.http.internal = 127.0.0.1:8098
        * ssl.certfile = $pwd/tests/resources/cert.pem
        * ssl.keyfile = $pwd/tests/resources/key.pem
        * ssl.cacertfile = $pwd/tests/resources/cacert.pem
    * Update these lines in advanced.conf
          {riak_api, [
            {certfile,  "$pwd/tests/resources/cert.pem"},
            {keyfile,    "$pwd/tests/resources/key.pem"},
            {cacertfile, "$pwd/tests/resources/cacert.pem"}
            ]}
    """

    description = "preconfigure security settings used in integration tests"

    user_options = [
        ('riak-conf=', None, 'path to the riak.conf file'),
        ('advanced-conf=', None, 'path to the advanced.conf file'),
        ('host=', None, 'IP of host running Riak'),
        ('pb-port=', None, 'protocol buffers port number'),
        ('http-port=', None, 'http port number')
    ]

    def initialize_options(self):
        self.riak_conf = None
        self.advanced_conf = None
        self.host = "127.0.0.1"
        self.pb_port = "8087"
        self.http_port = "8098"

    def finalize_options(self):
        if self.riak_conf is None:
            raise DistutilsOptionError("riak-conf option not set")
        if self.advanced_conf is None:
            raise DistutilsOptionError("advanced-conf option not set")

    def run(self):
        self.cert_dir = os.path.dirname(os.path.realpath(__file__)) + \
            "/riak/tests/resources"
        self._update_riak_conf()
        self._update_advanced_conf()

    def _update_riak_conf(self):
        http_host = self.host + ':' + self.http_port
        pb_host = self.host + ':' + self.pb_port
        self._backup_file(self.riak_conf)
        f = open(self.riak_conf, 'r', False)
        conf = f.read()
        f.close()
        conf = re.sub(r'search\s+=\s+off', r'search = on', conf)
        conf = re.sub(r'storage_backend\s+=\s+\S+',
                      r'storage_backend = leveldb', conf)
        conf = re.sub(r'^##\s+ssl.', 'ssl.', conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.certfile\s+=\s+\S+$',
                      r'ssl.certfile = ' + self.cert_dir + '/cert.pem',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.keyfile\s+=\s+\S+$',
                      r'ssl.keyfile = ' + self.cert_dir + '/key.pem',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^ssl.cacertfile\s+=\s+\S+$',
                      r'ssl.cacertfile = ' + self.cert_dir +
                      '/cacert.pem',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^listener.http.internal',
                      r'## listener.http.internal',
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^##\s+listener.https.internal',
                      'listener.https.internal', conf,
                      flags=re.MULTILINE)
        conf = re.sub(r'^listener.https.internal\s+=\s+\S+',
                      r'listener.https.internal = ' + http_host,
                      conf, flags=re.MULTILINE)
        conf = re.sub(r'^listener.protobuf.internal\s+=\s+\S+',
                      r'listener.protobuf.internal = ' + pb_host,
                      conf, flags=re.MULTILINE)
        f = open(self.riak_conf, 'w', False)
        f.write(conf)
        f.close()

    def _update_advanced_conf(self):
        self._backup_file(self.advanced_conf)
        conf = ""
        if os.path.isfile(self.advanced_conf):
            f = open(self.advanced_conf, 'r', False)
            conf = f.read()
            f.close()

        if re.search(r'{riak_api,', conf) and re.search(r'{certfile,', conf):
            # Already has certificates
            conf = re.sub(r'{certfile,\s+[^}]+}',
                          r'{certfile, "' + self.cert_dir + '/cert.pem"}',
                          conf, flags=re.MULTILINE)
            conf = re.sub(r'{keyfile,\s+[^}]+}',
                          r'{keyfile, "' + self.cert_dir + '/key.pem"}',
                          conf, flags=re.MULTILINE)
            conf = re.sub(r'{cacertfile,\s+[^}]+}',
                          r'{cacertfile, "' + self.cert_dir +
                          '/cacert.pem"}',
                          conf, flags=re.MULTILINE)
        else:
            # Add from scratch
            conf += '[\n  {riak_api, [\n'
            conf += '    {{certfile, "{0}/cert.pem"}},\n'.format(self.cert_dir)
            conf += '    {{keyfile, "{0}/key.pem"}},\n'.format(self.cert_dir)
            conf += '    {{cacertfile, "{0}/cacert.pem"}}\n' \
                .format(self.cert_dir)
            conf += '  ]}\n].\n'

        f = open(self.advanced_conf, 'w', False)
        f.write(conf)
        f.close()

    def _backup_file(self, name):
        backup = name + ".bak"
        if os.path.isfile(name):
            shutil.copyfile(name, backup)
        else:
            log.info("Cannot backup missing file {!r}".format(name))

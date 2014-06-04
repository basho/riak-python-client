"""
distutils commands for riak-python-client
"""

__all__ = ['create_bucket_types']

from distutils import log
from distutils.core import Command
from distutils.errors import DistutilsOptionError
from subprocess import CalledProcessError, Popen, PIPE

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

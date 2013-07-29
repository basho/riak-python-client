# This program is placed into the public domain.

"""
Gets the current version number.
If in a git repository, it is the current git tag.
Otherwise it is the one contained in the PKG-INFO file.

To use this script, simply import it in your setup.py file
and use the results of get_version() as your package version:

    from version import *

    setup(
        ...
        version=get_version(),
        ...
    )
"""

__all__ = ('get_version')

from os.path import dirname, isdir, join
import re
from subprocess import CalledProcessError, check_output

version_re = re.compile('^Version: (.+)$', re.M)


def get_version():
    d = dirname(__file__)

    if isdir(join(d, '.git')):
        # Get the version using "git describe".
        cmd = 'git describe --tags --match [0-9]*'.split()
        try:
            version = check_output(cmd).decode().strip()
        except CalledProcessError:
            print('Unable to get version number from git tags')
            exit(1)

        # PEP 386 compatibility
        if '-' in version:
            version = '.post'.join(version.split('-')[:2])

    else:
        # Extract the version from the PKG-INFO file.
        with open(join(d, 'PKG-INFO')) as f:
            version = version_re.search(f.read()).group(1)

    return version


if __name__ == '__main__':
    print(get_version())

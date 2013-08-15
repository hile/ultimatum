"""
Classes to process ZFS filesystems
"""

from subprocess import check_output, CalledProcessError

__all__ = [ 'snapshots', 'zpool' 'zfs' ]

SNAPSHOT_DATE_FORMAT = '%Y%m%d-%H%M%S'

class ZFSError(Exception):
    pass

def execute(cmd):
    if isinstance(cmd,basestring):
        cmd = cmd.split(' ')

    try:
        output = check_output(cmd)
    except CalledProcessError:
        raise ZFSError('Error running command %s' % ' '.join(str(x) for x in cmd))

    return [x.rstrip() for x in output.rstrip('\n').split('\n')]

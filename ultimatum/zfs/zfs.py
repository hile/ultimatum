"""
ZFS API
"""

import logging
from datetime import datetime, timedelta

from ultimatum.zfs import execute, ZFSError, SNAPSHOT_DATE_FORMAT
from ultimatum.zfs.snapshots import ZFSSnapshot

ZFS_BOOLEAN_PROPERTIES = (
    'atime',
    'checksum',
    'canmount',
    'dedup',
    'devices',
    'exec',
    'jailed',
    'mounted',
    'nbmand',
    'readonly',
    'setuid',
    'sharenfs',
    'sharesmb',
    'type',
    'utf8only',
    'vscan',
    'xattr'
)
ZFS_READONLY_PROPERTIES = (
    'available',
    'creation',
    'refcompressratio',
    'referenced',
    'type',
    'used',
    'usedbychildren',
    'usedbydataset',
    'usedbyrefreservation',
    'usedbysnapshots',
    'written',
)

# TODO - process these flags properly as in zpool.py
"""
aclinherit restricted
aclmode discard
casesensitivity sensitive
compressratio 1.00x
copies 1
logbias latency
mlslabel -
mountpoint none
normalization none
primarycache all
quota none
recordsize 128K
refquota none
refreservation none
reservation none
secondarycache all
snapdir hidden
sync standard
version 4
"""
ZFS_PROPERTIES = ZFS_BOOLEAN_PROPERTIES + ZFS_READONLY_PROPERTIES

class ZFS(object):
    def __init__(self,name):
        self.name = name

    def __repr__(self):
        return 'zfs %s' % self.name

    @property
    def snapshots(self):
        snapshots = []
        for name in execute('zfs list -Hrt snapshot -o name %s' % self.name):
            if name == '':
                continue

            snapshot = ZFSSnapshot(name)
            if snapshot.volume == self.name:
                snapshots.append(snapshot)

        return snapshots

    def create_snapshot(self,tag=None):
        if tag is None:
            tag = datetime.now().strftime(SNAPSHOT_DATE_FORMAT)
        name = '%s@%s' % (self.name,tag)
        if name in self.snapshots:
            raise ZFSError('Snapshot already exists: %s' % name)
        print 'create snapshot', name
        execute('zfs snapshot %s' % name)
        return tag

    def remove_snapshot(self,value):
        if isinstance(value, basestring):
            name = '%s@%s' % (self.name,value)
            if name not in self.snapshots:
                raise ZFSError('No such snapshot: %s' % name)
        elif isinstance(value, ZFSSnapshot):
            name = value.name

        print 'remove snapshot', name
        execute('zfs destroy %s' % name)

    def filter_snapshots(self,start,stop,date_format=SNAPSHOT_DATE_FORMAT):
        try:
            if not isinstance(start,datetime):
                start = datetime.strptime(start,date_format)
            if not isinstance(stop,datetime):
                stop = datetime.strptime(stop,date_format)

        except ValueError, emsg:
            raise ZFSError('Filter dates do not match default date format: %s' % SNAPSHOT_DATE_FORMAT)

        if start > stop:
            raise ZFSError('Invalid date range: start is after stop')

        snapshots = []
        for snapshot in self.snapshots:
            try:
                ss_date = datetime.strptime(snapshot.tag, date_format)
            except ValueError:
                # Ignore snapshot not matching date formatting
                continue

            if ss_date >= start and ss_date <= stop:
                snapshots.append(snapshot)

        return snapshots

if __name__ == '__main__':
    import sys
    fs = ZFS('media/dump')

    for ss in sorted(fs.snapshots):
        print ss

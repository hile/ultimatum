"""
ZFS API
"""

import logging
from datetime import datetime, timedelta
from subprocess import Popen, PIPE

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
ZFS_STRING_PROPERTIES =  (
    'aclinherit',
    'aclmode',
    'casesensitivity',
    'compressratio',
    'copies',
    'logbias',
    'mlslabel',
    'mountpoint',
    'normalization',
    'primarycache',
    'quota',
    'recordsize',
    'refquota',
    'refreservation',
    'reservation',
    'secondarycache',
    'snapdir',
    'sync',
    'version',
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

ZFS_PROPERTY_VALIDATORS = {

}

ZFS_PROPERTIES = ZFS_BOOLEAN_PROPERTIES + ZFS_READONLY_PROPERTIES + ZFS_STRING_PROPERTIES

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

    def get_property(self,key):
        if key not in ZFS_PROPERTIES:
            raise ZFSError('Invalid property name')

        value = None
        cmd = 'zfs get -H %s %s' % (key,self.name)
        for line in execute(cmd):
            try:
                volume, field, value, flags = line.split(None,3)
                if volume == self.name and field == key:
                    break
            except ValueError:
                continue

            value = None

        if value is None:
            return None

        if value == '-' and key in ZFS_OPTIONAL_PROPERTIES:
            return None

        if key in ZFS_BOOLEAN_PROPERTIES:
            return value == 'on'

        return value

    def set_property(self, key, value):
        skip_quotes="True"
        if key not in ZFS_PROPERTIES:
            raise ZFSError('Invalid property name')

        if key in ZFS_READONLY_PROPERTIES:
            raise ZFSError('Readonly property: %s' % key)

        if key in ZFS_PROPERTY_VALIDATORS:
            if not ZFS_PROPERTY_VALIDATORS[key](value):
                raise ZFSError('Unknown value for property %s: %s' % (key,value))

        if key in ZFS_BOOLEAN_PROPERTIES:
            value = value and 'on' or 'off'
            skip_quotes = True

        elif value == None:
            value = 'none'

        if skip_quotes:
            execute(['zfs','set','%s=%s' % (key, value), self.name])
        else:
            execute(['zfs','set','%s="%s"' % (key, value), self.name])

    def create_snapshot(self,tag=None):
        if tag is None:
            tag = datetime.now().strftime(SNAPSHOT_DATE_FORMAT)

        name = '%s@%s' % (self.name,tag)
        if name in self.snapshots:
            raise ZFSError('Snapshot already exists: %s' % name)

        execute('zfs snapshot %s' % name)
        return tag

    def remove_snapshot(self,value):
        if isinstance(value, basestring):
            name = '%s@%s' % (self.name,value)
            if name not in self.snapshots:
                raise ZFSError('No such snapshot: %s' % name)
        elif isinstance(value, ZFSSnapshot):
            name = value.name

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

    def clone_to_pool(self, pool, tag, force=False):
        """Clone filesystem to other pool

        Clone this filesystem to target zpool
        """

        if self.snapshots:
            latest = '%s' % self.snapshots[-1]
            self.create_snapshot(tag)
            src_args = ['zfs', 'send', '-i', latest, '%s@%s' % (self.name, tag)]
        else:
            self.create_snapshot(tag)
            src_args = ['zfs', 'send', '%s@%s' % (self.name, tag)]

        if force:
            dst_args = ['zfs', 'receive', '-Fd', pool.name]
        else:
            dst_args = ['zfs', 'receive', '-d', pool.name]

        src = Popen(src_args, stdout=PIPE)
        dst = Popen(dst_args, stdin= src.stdout, stdout=PIPE)
        output = dst.communicate()[0]

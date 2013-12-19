"""
Zpool API
"""

import logging
from datetime import datetime, timedelta

from ultimatum.zfs import execute, ZFSError, SNAPSHOT_DATE_FORMAT
from ultimatum.zfs.zfs import execute, ZFS

ZPOOL_READONLY_PROPERTIES = (
    'allocated',
    'capacity',
    'dedupratio',
    'expandsize',
    'free',
    'guid',
    'health',
    'size',
    'readonly',
)
ZPOOL_BOOLEAN_PROPERTIES = (
    'autoexpand',
    'autoreplace',
    'delegation',
    'listsnapshots',
    'readonly',
)
ZPOOL_STRING_PROPERTIES =  (
    'altroot',
    'bootfs',
    'cachefile',
    'comment',
    'dedupditto',
    'failmode',
    'version',
)
ZPOOL_OPTIONAL_PROPERTIES = {
    'altroot',
    'bootfs',
    'cachefile',
    'comment',
    'expandsize',
}
ZPOOL_HEALTH_STATES = (
    'DEGRADED', 'FAULTED', 'OFFLINE', 'ONLINE', 'REMOVED', 'UNAVAIL'
)
ZPOOL_PROPERTY_VALIDATORS = {
    'altroot':  lambda x: isinstance(x, basestring),
    'bootfs': lambda x: isinstance(x, basestring) and x.count('/')>0,
    'cachefile': lambda x: isinstance(x, int),
    'dedupditto': lambda x: isinstance(x, int),
    'failmode': lambda x: x in ( 'wait', 'continue', 'panic' ),
    'dedupditto': lambda x: isinstance(x, int),
    'version': lambda x: isinstance(x, int),
}
ZPOOL_PROPERTIES = ZPOOL_READONLY_PROPERTIES + ZPOOL_BOOLEAN_PROPERTIES + ZPOOL_STRING_PROPERTIES

logger = logging.getLogger(__name__)

def poolnames():
    """Pool name list

    Return list of ZPool names

    """
    names = []
    for line in execute('zpool list -H'):
        names.append(line.split()[0])
    return names

class ZPool(object):
    """ZPool object

    Abstraction of zpool commands for python

    """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'zpool %s' % self.name

    def get_property(self, property):
        """Return property value

        Return value of a zpool entry, validated and formatted

        """
        if property not in ZPOOL_PROPERTIES:
            raise ZFSError('Invalid property name')

        value = ' '.join(execute('zpool list -H -o %s %s' % (property, self.name)))

        if value == '-' and property in ZPOOL_OPTIONAL_PROPERTIES:
            return None

        if property in ZPOOL_BOOLEAN_PROPERTIES:
            return value == 'on'

        if property == 'health' and value not in ZPOOL_HEALTH_STATES:
            raise ZFSError('Unknown health state for pool %s: %s' % (self.name, value))

        return value

    def set_property(self, property, value):
        """Set property value

        Set value of a zpool entry, validated and formatted

        """
        if property not in ZPOOL_PROPERTIES:
            raise ZFSError('Invalid property name')

        if property in ZPOOL_READONLY_PROPERTIES:
            raise ZFSError('Readonly property: %s' % property)

        if property in ZPOOL_PROPERTY_VALIDATORS:
            if not ZPOOL_PROPERTY_VALIDATORS[property](value):
                raise ZFSError('Unknown value for property %s: %s' % (property, value))

        if property in ZPOOL_BOOLEAN_PROPERTIES:
            value = value and 'on' or 'off'

        elif value == None:
            value = 'none'

        execute(['zpool', 'set', '%s="%s"' % (property, value), self.name])

    @property
    def is_available(self):
        """Check if pool is is_available

        Boolean property to see if this pool is available

        """
        if self.name not in poolnames():
            return False

        return True

    @property
    def filesystems(self):
        """List of filesystems

        Return list of ZFS objects for filesystems in this pool

        """
        return [ZFS(fs) for fs in execute('zfs list -Hr -o name %s' % self.name) if fs!='']

    def import_pool(self):
        """Import zpool

        Try to import this zpool

        """
        execute('zpool import %s' % self.name)

    def export_pool(self):
        """Export zpool

        Try to export this zpool

        """
        execute('zpool export %s' % self.name)

    def create_snapshots(self, tag=None):
        """Create snapshots

        Attempt to create named snapshots for all filesystems in this pool

        If tag is not provided, we use a timestamp with SNAPSHOT_DATE_FORMAT

        """
        if tag is None:
            tag = datetime.now().strftime(SNAPSHOT_DATE_FORMAT)
        for fs in self.filesystems:
            fs.create_snapshot(tag)

    def filter_snapshots(self, start, stop, date_format=SNAPSHOT_DATE_FORMAT):
        """Filter snapshots by name

        Filter snapshots of pool's filesystems by date range

        """
        try:
            if not isinstance(start, datetime):
                start = datetime.strptime(start, date_format)
            if not isinstance(stop, datetime):
                stop = datetime.strptime(stop, date_format)

        except ValueError, emsg:
            raise ZFSError('Filter dates do not match default date format: %s' % SNAPSHOT_DATE_FORMAT)

        if start > stop:
            raise ZFSError('Invalid range: start date is after stop date')

        snapshots = []
        for fs in self.filesystems:
            snapshots.extend(fs.filter_snapshots(start, stop, date_format))

        return snapshots

"""
ZFS snapshots
"""

from ultimatum.zfs import execute, ZFSError, SNAPSHOT_DATE_FORMAT

class ZFSSnapshot(list):
    def __init__(self, name):
        self.name = name
        try:
            self.volume, self.tag = name.split('@')
        except ValueError:
            raise ZFSError('Invalid snapshot name: %s' % name)

    def __cmp__(self, other):
        if isinstance(other, basestring):
            try:
                tag = self.name.split('@')[1]
                if tag == other:
                    return 0
            except IndexError:
                pass

            return cmp(self.name, other)

        elif not isinstance(other,ZFSSnapshot):
            raise ZFSError("Can't compare ZFSSnapshot to %s object" % type(other))

        if self.volume != other.volume:
            return cmp(self.volume, other.volume)

        try:
            my_date = time.strptime(self.tag, SNAPSHOT_DATE_FORMAT)
            other_date = time.strptime(other.tag, SNAPSHOT_DATE_FORMAT)
            return cmp(my_date, other_date)
        except ValueError:
            pass

        return cmp(self.tag, other.tag)

    def __eq__(self,other):
        return self.__cmp__(other) == 0

    def __ne__(self,other):
        return self.__cmp__(other) != 0

    def __repr__(self):
        return '%s@%s' % (self.volume, self.tag)

    def rename(self,name):
        if name.count('@')==0:
            name = '%s@%s' % (self.volume, name)
        execute('zfs rename %s %s' % (self.name, name))

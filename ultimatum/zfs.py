"""
Abstraction of ZFS zpools, zfs volumes and zfs snapshots for FreeBSD users,
used by the zfs-snapshots backup script.
"""

from datetime import datetime,datetime

from subprocess import check_output,CalledProcessError,Popen,PIPE
from systematic.log import Logger,LoggerError

POOL_NAMING_SORTERS = [
    lambda x,y: cmp(datetime.strptime(x,'%Y-%m-%d.%H:%M:%S'),datetime.strptime(y,'%Y-%m-%d.%H:%M:%S')),
    lambda x,y: cmp(int(x),int(y)),
]

class ZFSError(Exception):
    def __str__(self):
        return self.args[0]

class ZFSSnapshots(dict):
    """
    Dictionary of zfs snapshots in the system.
    Each dictinary key maps to a volume and contains dictionaries
    of (volume,snapshots), i.e. self['mypool']['volumename'] == list
    """
    def __init__(self):
        self.log = Logger('zfs').default_stream
        self.reload()

    def reload(self):
        self.clear()
        try:
            cmd = ['zfs','list','-Ht','snapshot']
            output = check_output(cmd)
        except CalledProcessError:
            raise ZFSError('Error listing snapshots with %s' % cmd)

        for entry in [x.split('\t')[0] for x in output.split('\n')]:
            if entry.strip() == '': continue
            try:
                volume,snapshot = entry.split('@',1)

                try:
                    pool,volume = volume.split('/',1)
                except ValueError:
                    pool = volume
                    volume = volume

                if not pool in self.keys():
                    self[pool] = {}
                if not volume in self[pool]:
                    self[pool][volume] = []
                self[pool][volume].append(snapshot)

            except ValueError:
                raise ZFSError('Error parsing zfs snapshot output from %s' % entry)

        for pool in self.keys():
            for volume in self[pool].keys():
                for sorter in POOL_NAMING_SORTERS:
                    try:
                        self[pool][volume].sort(lambda x,y: sorter(x,y))
                        return
                    except ValueError:
                        pass

class ZFSPool(list):
    """
    Abstraction for 'zpool' items in this system
    """
    def __init__(self,name,snapshots):
        self.log = Logger('filesystems').default_stream
        self.zfs_snapshots = snapshots
        self.name = name
        if not self.available:
            self.import_pool()
        self.load()

    @property
    def available(self):
        """
        Checks if the pool is available
        """
        cmd = ['zpool','list',self.name]
        p = Popen(cmd,stdin=PIPE,stdout=PIPE,stderr=PIPE)
        p.communicate()
        return p.returncode==0

    def import_pool(self):
        """
        Import exported pool
        """
        cmd = ['zpool','import',self.name]
        try:
            check_output(cmd)
        except CalledProcessError:
            raise ZFSError('Error importing pool: %s' % self.name)

    def export_pool(self):
        """
        Export pool for removal from system
        """
        cmd = ['zpool','export',self.name]
        try:
            check_output(cmd)
        except calledprocesserror:
            raise zfserror('Error importing pool: %s' % self.name)

    def load(self):
        """
        Load list of volumes in this pool
        """
        self.__delslice__(0,len(self))
        cmd = ['zpool','list',self.name]
        try:
            check_output(cmd)
        except calledprocesserror:
            raise zfserror('pool does not exist: %s' % self.name)

        try:
            output = check_output(['zfs','list','-Hd2',self.name])
            for l in [l.strip() for l in output.split('\n')]:
                if l=='': continue
                (vol,blocks,used,total,mountpoint) = l.split('\t')
                self.append(ZFSVolume(self,vol,mountpoint))
        except CalledProcessError:
            raise ZFSError('Error listing zpool volumes')
        self.sort()

    def __str__(self):
        return self.name

    def __cmp__(self,other):
        return cmp(self.name,other.name)

    @property
    def snapshots(self):
        """
        Return dictionary (volume,snapshot) of snapshots for this pool
        """
        try:
            return self.zfs_snapshots[self.name]
        except KeyError:
            return {}

    def reload_snapshots(self):
        """
        Reload list of existing snapshots
        """
        self.zfs_snapshots.reload()

    def create_volume(self,name):
        """
        Create a new ZFS volume to this pool
        """
        volume = '%s/%s' % (self.name,name)
        if volume in self:
            raise ZFSError('Attempt to create existing volume: %s' % volume)
        try:
            check_output(['zfs','create',volume])
        except CalledProcessError:
            raise ZFSError('Error creating volume %s' % volume)
        self.load()
        return self.get_volume(name)

    def get_volume(self,name):
        """
        Return a ZFS volume from pool by name
        """
        for volume in self:
            if volume.name == name:
                return volume
        raise ZFSError('Volume not found in pool %s: %s' % (self.name,name))

class ZFSVolume(object):
    """
    Abstraction for a mounted volume in a zpool
    """
    def __init__(self,pool,volume,mountpoint):
        self.log = Logger('filesystems').default_stream
        self.pool = pool
        self.volume = volume
        self.mountpoint = mountpoint!='none' and mountpoint or None

        if self.pool.name != self.volume:
            self.name = self.volume[len(self.pool.name):].lstrip('/')
        else:
            self.name = self.volume

    def __cmp__(self,other):
        if isinstance(other,basestring):
            return cmp(self.name,other)
        if self.pool!=other.pool:
            return cmp(self.pool,other.pool)
        return cmp(self.volume,other.volume)

    def __str__(self):
        return self.volume

    @property
    def snapshots(self):
        """
        Returns list of snapshots for this volume
        """
        pool_snapshots = self.pool.snapshots
        try:
            return self.pool.snapshots[self.name]
        except KeyError:
            return []

    def destroy_snapshot(self,tag):
        """
        Destroy a snapshot of this volume
        """
        if tag not in self.snapshots:
            raise 'No such snapshot of %s: %s' % (self.volume,tag)
        try:
            check_output(['zfs','destroy','%s@%s' % (self.volume,tag)])
        except CalledProcessError:
            raise ZFSError('Error destroying snapshot for %s' % self.volume)
        self.pool.reload_snapshots()

    def create_snapshot(self,tag):
        """
        Create new snapshot of this volume with given tag
        """
        try:
            check_output(['zfs','snapshot','%s@%s' % (self.volume,tag)])
        except CalledProcessError:
            raise ZFSError('Error creating snapshot for %s' % self.volume)
        self.pool.reload_snapshots()

    def rename_snapshot(self,old,new):
        """
        Rename a snapshot for this volume
        """
        try:
            check_output(['zfs','rename','-r','%s@%s' % (self,old),'%s@%s' % (self,new)])
        except CalledProcessError:
            raise ZFSError('Error renaming snapshot %s to new' % (old,new))
        self.pool.reload_snapshots()

    def clone(self,backup_pool):
        """
        Clone this volume to given backup pool
        This cloning implementation expects snapshots tags to be incremented
        numbers without any letters in snapshot name.
        """
        self.log.debug('cloning %s to %s' % (self.volume, backup_pool.name))
        if self.snapshots:
            latest = self.snapshots[-1]
            try:
                datetime.strptime(latest,'%Y-%m-%d.%H:%M:%S')
                tag = datetime.now().strftime('%Y-%m-%d.%H:%M:%S')
            except ValueError:
                try:
                    tag = int(self.snapshots[-1])+1
                except ValueError:
                    raise ZFSError('Snapshot tag format not supported: %s' % latest)

            self.create_snapshot(tag)
            src_args = ['zfs','send','-i',latest,'%s@%s' % (self.volume,tag)]
        else:
            tag = datetime.now().strftime('%Y-%m-%d.%H:%M:%S')
            self.create_snapshot(tag)
            src_args = ['zfs','send','%s@%s' % (self.volume,tag)]

        dst_args = ['zfs','receive','-Fd',backup_pool.name]
        src = Popen(src_args,stdout=PIPE)
        dst = Popen(dst_args,stdin=src.stdout,stdout=PIPE)
        output = dst.communicate()[0]


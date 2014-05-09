
import re
import os
import glob

from seine.address import IPv4Address, IPv6Address
from seine.whois.arin import ARINReverseIPQuery, WhoisError
from systematic.log import LogEntry, LogFile, LogFileCollection, LogFileError
from systematic.sqlite import SQLiteDatabase, SQLiteError

SSHD_VIOLATIONS_DATABASE_PATH = '/var/lib/ssh/violations.db'
SQL_TABLES = [
"""CREATE TABLE IF NOT EXISTS registration (
    id              INTEGER PRIMARY KEY,
    version         INT,
    handle          TEXT,
    comment         TEXT,
    registered      DATETIME,
    updated         DATETIME
)""",
"""CREATE TABLE IF NOT EXISTS netblock (
    id              INTEGER PRIMARY KEY,
    registration    INT REFERENCES registration(id) ON DELETE CASCADE,
    description     TEXT,
    network         TEXT,
    start           TEXT,
    end             TEXT
)""",
"""CREATE UNIQUE INDEX IF NOT EXISTS netblock_registration ON netblock(registration, network)""",
"""CREATE TABLE IF NOT EXISTS login (
    id              INTEGER PRIMARY KEY,
    timestamp       DATETIME,
    registration    INT REFERENCES registration(id) ON DELETE CASCADE,
    address         TEXT,
    username        TEXT
)""",
"""CREATE UNIQUE INDEX IF NOT EXISTS attempts ON login(timestamp, address, username)"""
]


class AuthLogEntry(LogEntry):
    pass


class AuthLogFile(LogFile):
    lineloader = AuthLogEntry


class AuthLogCollection(LogFileCollection):
    loader = AuthLogFile


class SSHViolationsDatabase(SQLiteDatabase):
    def __init__(self, path=SSHD_VIOLATIONS_DATABASE_PATH):
        SQLiteDatabase.__init__(self, path, SQL_TABLES)

    def lookup_registration_id(self, address):
        try:
            address = IPv4Address(address)
        except ValueError:
            try:
                address = IPv6Address(address)
            except ValueError:
                raise ValueError('ERROR parsing address %s' % address)

        c = self.cursor
        c.execute("""SELECT registration,network FROM netblock""")
        for entry in c.fetchall():
            try:
                network = IPv4Address(entry[1])
            except ValueError:
                try:
                    network = IPv6Address(enrty[1])
                except ValueError:
                    continue

            if type(network) != type(address):
                continue

            if network.hostInNetwork('%s' % address):
                return entry[0]

        return None

    def add_netblock(self, ref):
        c = self.cursor
        c.execute("""SELECT id FROM registration WHERE handle=?""", (ref.handle,))
        r = c.fetchone()
        if r is not None:
            return None

        c.execute("""INSERT INTO registration (version, handle, comment, registered, updated) VALUES (?,?,?,?,?)""",
            (ref.version, ref.handle, ref.comment, ref.registered, ref.updated, )
        )
        self.commit()

        c.execute("""SELECT id FROM registration WHERE handle=?""", (ref.handle,))
        ref_id = int(c.fetchone()[0])

        for netblock in ref:
            if isinstance(netblock.network, IPv4Address):
                c.execute("""INSERT INTO netblock (registration, description, network, start, end) VALUES (?,?,?,?,?)""",
                    (
                        ref_id,
                        netblock.description,
                        netblock.network.cidr_address,
                        netblock.start.cidr_address,
                        netblock.end.cidr_address,
                    )
                )
            elif isinstance(netblock.network, IPv6Address):
                    (
                        ref_id,
                        netblock.description,
                        '%s' % netblock.network,
                        '%s' % netblock.start,
                        '%s' % netblock.end,
                    )

        self.commit()

        return ref_id

    def add(self, timestamp, address, username, registration):
        c = self.cursor
        c.execute("""SELECT * FROM login WHERE timestamp=? AND address=? AND username=?""",
            ( timestamp, address, username, )
        )
        r = c.fetchone()
        if r is not None:
            return None

        c.execute("""INSERT INTO login (timestamp, registration, address, username) VALUES (?,?,?,?)""",
            ( timestamp, registration, address, username, )
        )
        self.commit()

        c.execute("""SELECT * FROM login WHERE timestamp=? AND address=? AND username=?""",
            ( timestamp, address, username, )
        )
        r = c.fetchone()
        return self.as_dict(c, r)

    def update(self, paths=None):
        matcher = re.compile('^Invalid user (?P<user>[^\s]+) from (?P<address>.*)')
        if not paths:
            paths = glob.glob('/var/log/auth.log*')

        for path in paths:
            log = AuthLogFile(path)
            for entry in log:
                m = matcher.match(entry.message)
                if not m:
                    continue

                details = {
                    'timestamp': entry.time,
                    'address': m.groupdict()['address'],
                    'username': m.groupdict()['user'],
                }

                ref = self.lookup_registration_id(details['address'])
                if ref is not None:
                    details['registration'] = ref
                else:
                    self.log.debug('ARIN LOOKUP %s' % details['address'])
                    ref = ARINReverseIPQuery(details['address'])
                    details['registration'] = self.add_netblock(ref)

                self.add(**details)

    def map_netblocks(self, values):
        c = self.cursor
        c.execute("""SELECT registration,network FROM netblock ORDER BY registration""")

        registration_netblock_map = {}
        for nb in c.fetchall():
            if nb[0] not in registration_netblock_map:
                registration_netblock_map[nb[0]] = []
            try:
                address = IPv4Address(nb[1])
            except ValueError:
                try:
                    address = IPv6Address(nb[1])
                except ValueError:
                    continue

            if address not in registration_netblock_map[nb[0]]:
                registration_netblock_map[nb[0]].append(address)

        for reg in registration_netblock_map:
            registration_netblock_map[reg].sort()

        for value in values:
            value['netblocks'] = []
            if value['registration'] in registration_netblock_map:
                for nb in registration_netblock_map[value['registration']]:
                    if nb.hostInNetwork(value['address']):
                        value['netblocks'].append(nb)

        return values

    def source_address_counts(self):
        c = self.cursor
        c.execute("""SELECT COUNT(DISTINCT timestamp) AS count, registration, address FROM login GROUP BY address ORDER BY -count""")
        return self.map_netblocks([self.as_dict(c,r) for r in c.fetchall()])

    def login_attempts(self, start=None):
        c = self.cursor

        if start is not None:
            c.execute("""SELECT * FROM login WHERE timestamp >= Datetime(?) ORDER BY timestamp""", (start,))
        else:
            c.execute("""SELECT * FROM login ORDER BY timestamp""")

        return self.map_netblocks([self.as_dict(c, r) for r in c.fetchall()])


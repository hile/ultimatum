
import re
import os
import glob

from seine.address import IPv4Address, IPv6Address, parse_address
from seine.whois.arin import ARINReverseIPQuery, WhoisError
from systematic.log import LogEntry, LogFile, LogFileCollection, LogFileError
from systematic.sqlite import SQLiteDatabase, SQLiteError

SSH_LOGINS = [
    re.compile('^Accepted publickey for (?P<user>[^\s]+) from (?P<address>.*) ' +
        'port (?P<port>\d+) (?P<sshversion>.*): (?P<keytype>.*) (?P<key>.*)$'
    ),
]
SSH_ATTEMPTS = [
    re.compile('^Invalid user (?P<user>[^\s]+) from (?P<address>.*)'),
    re.compile('^Failed publickey for (?P<user>[^\s]+) from (?P<address>.*) ' +
        'port (?P<port>\d+) (?P<sshversion>.*) (?P<keytype>.*) (?P<fingerprint>.*)$'
    ),
    re.compile('^error: Received disconnect from (?P<address>[^:+]):.*: Auth fail [preauth]'),
]


SSH_CONNECT = re.compile('^Connection from (?P<address>[^\s]+) port (?P<port>\d+)$')
SSH_ACCEPT_PUBLICKEY = re.compile('^%s$' % ' '.join([
    'Accepted publickey for (?P<username>[^\s]+)',
    'from (?P<address>[^\s]+)',
    'port (?P<port>\d+)',
    '(?P<version>[^:]+):',
    '(?P<keytype>[^\s]+)',
    '(?P<fingerprint>.*)'
]))

SSH_LOGIN = re.compile('User child is on pid (?P<pid>\d+)$')
SSH_DISCONNECT = re.compile('Received disconnect from (?P<address>[^\s]+): .*: disconnected by user')
SSH_INVALID_USER = re.compile('^Invalid user (?P<username>.*) from (?P<address>[^\s]+)$')

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


class SSHSession(list):
    def __init__(self, sessioncache, entry, pid=None, parent=None, timeout=60):
        self.sessioncache = sessioncache
        self.pid = pid is not None and pid or entry.pid
        self.timeout = timeout
        self.parent = parent
        self.info = {}

        self.append(entry)

    def __repr__(self):
        return self.pid

    def append(self, entry):
        if len(self) == 0:
            self.state = 'init'
        list.append(self, entry)

        m = SSH_CONNECT.match(entry.message)
        if m:
            self.state = 'connect'
            self.info['src_address'] = m.groupdict()['address']
            self.info['src_port'] = m.groupdict()['port']
            return

        m = SSH_ACCEPT_PUBLICKEY.match(entry.message)
        if m:
            self.state = 'accepted_publickey'
            details = m.groupdict()
            for k in ( 'username', 'version', 'keytype', 'fingerprint', ):
                if k in details:
                    self.info[k] = details[k]
                else:
                    self.info[k] = 'MISSING KEY INFO %s' % k
            return

        m = SSH_LOGIN.match(entry.message)
        if m:
            if self.parent is not None:
                self.state = 'user_session'
                self.info['parent_pid'] = self.parent.pid
                self.info.update(self.parent.info.items())
            else:
                self.state = 'login'
                usersession_pid = m.groupdict()['pid']
                if usersession_pid != self.pid:
                    self.sessioncache.append(SSHSession(self.sessioncache, entry, pid=usersession_pid, parent=self))
            return

        m = SSH_DISCONNECT.match(entry.message)
        if m:
            self.state = 'logout'
            if len(self) > 0:
                self.info['session_length'] = (entry.time - self[0].time).total_seconds()
            return

        m = SSH_INVALID_USER.match(entry.message)
        if m:
            self.state = 'invalid_user'
            self.info['username'] = m.groupdict()['username']
            return

        if entry.message == 'fatal: Read from socket failed: Connection reset by peer [preauth]':
            self.state = 'preauth_connection_reset'

        elif 'src_address' in self.info:
            if entry.message == 'Connection closed by %s [preauth]' % self.info['src_address']:
                self.state = 'preauth_no_key'

            if entry.message == 'Received disconnect from %s: 11: Bye Bye [preauth]' % self.info['src_address']:
                if self.state != 'invalid_user':
                    self.state = 'preauth_disconnect'


    def match(self, entry):
        if entry.pid != self.pid:
            return False

        if self.state == 'user_session':
            return True

        if abs((entry.time - self[0].time).total_seconds()) >= self.timeout:
            return False

        return True


class SSHSessionCache(dict):
    def match(self, entry):
        if entry.pid not in self:
            return None

        for session in self[entry.pid]:
            if session.match(entry):
                return session
        return None

    def append(self, session):
        if session.pid not in self:
            self[session.pid] = []
        self[session.pid].append(session)


class AuthLogEntry(LogEntry):
    def __init__(self, *args, **kwargs):
        LogEntry.__init__(self, *args, **kwargs)

        if self.program == 'sshd':
            session = self.logfile.sessioncache.match(self)
            if session is not None:
                session.append(self)
            else:
                session = SSHSession(self.logfile.sessioncache, self)
                self.logfile.sessioncache.append(session)

class AuthLogFile(LogFile):
    lineloader = AuthLogEntry
    def __init__(self, sessioncache, *args, **kwargs):
        LogFile.__init__(self, *args, **kwargs)

        self.sessioncache = sessioncache

        self.register_iterator('failures')
        self.register_iterator('logins')

    def __match_failed__(self, entry):
        for matcher in SSH_ATTEMPTS:
            m = matcher.match(entry.message)
            if m:
                details = m.groupdict()
                entry.update_message_fields(details)
                return True

        return False

    def __match_login__(self, entry):
        for matcher in SSH_LOGINS:
            m = matcher.match(entry.message)
            if m:
                details = m.groupdict()
                if 'port' in details:
                    details['port'] = int(details['port'])
                details['address'] = parse_address(details['address'])
                entry.update_message_fields(details)
                return True

        return False

    def next_failed(self):
        return self.next_iterator_match('failures', callback=self.__match_failed__)

    def next_login(self):
        return self.next_iterator_match('logins', callback=self.__match_login__)

    @property
    def failures(self):
        return iter(self.next_failed, None)

    @property
    def logins(self):
        return iter(self.next_login, None)


class AuthLogCollection(LogFileCollection):
    loader = AuthLogFile
    def __init__(self, *args, **kwargs):
        LogFileCollection.__init__(self, *args, **kwargs)
        self.sessioncache = SSHSessionCache()

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

        c.execute("""INSERT INTO registration (version, handle, comment, registered, updated) """ +
            """VALUES (?,?,?,?,?)""",
            (ref.version, ref.handle, ref.comment, ref.registered, ref.updated, )
        )
        self.commit()

        c.execute("""SELECT id FROM registration WHERE handle=?""", (ref.handle,))
        ref_id = int(c.fetchone()[0])

        for netblock in ref:
            if isinstance(netblock.network, IPv4Address):
                c.execute("""INSERT INTO netblock (registration, description, network, start, end) """ +
                    """VALUES (?,?,?,?,?)""",
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
            auth_paths = glob.glob('/var/log/auth.log*') + glob.glob('/var/log/messages*')

        sessioncache = SSHSessionCache()
        for path in paths:
            log = AuthLogFile(sessioncache, path)
            for entry in log.failures:
                details = {
                    'timestamp': entry.time,
                    'address': entry.message_fields['address'],
                    'username': entry.message_fields['user'],
                }

                ref = self.lookup_registration_id(details['address'])
                if ref is not None:
                    details['registration'] = ref

                elif isinstance(details['address'], IPv4Address):
                    self.log.debug('ARIN LOOKUP %s' % details['address'])
                    ref = ARINReverseIPQuery(details['address'])
                    details['registration'] = self.add_netblock(ref)

                else:
                    details['registration'] = None

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
        c.execute("""SELECT COUNT(DISTINCT timestamp) AS count, registration, address """ +
            """FROM login GROUP BY address ORDER BY -count"""
        )
        return self.map_netblocks([self.as_dict(c,r) for r in c.fetchall()])

    def login_attempts(self, start=None):
        c = self.cursor

        if start is not None:
            c.execute("""SELECT * FROM login WHERE timestamp >= Datetime(?) """ +
                """ORDER BY timestamp""",
                (start,)
            )
        else:
            c.execute("""SELECT * FROM login ORDER BY timestamp""")

        return self.map_netblocks([self.as_dict(c, r) for r in c.fetchall()])


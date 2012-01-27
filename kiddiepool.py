import collections
import contextlib
import Queue as queue
import random
import socket


# Pool classes/defaults
CandidatePool = collections.deque
ConnectionPool = queue.Queue
DEFAULT_POOL_MAX = 10
DEFAULT_POOL_TIMEOUT = 2
DEFAULT_CONNECT_ATTEMPTS = 2
DEFAULT_SEND_ATTEMPTS = 2


class KiddieException(Exception):
    """Base class for Kiddie Exceptions"""


class KiddiePoolEmpty(KiddieException, queue.Empty):
    """No Kiddie connections available in pool (even after timeout)"""


class KiddiePoolMaxTries(KiddieException, socket.error):
    """Unable to connect to any Kiddie servers (even after timeout & retries)
    """


class KiddiePool(object):
    """Lazy/dumb/resilient Connection Pool Implementation

     * Lazily creates connections up to a maximum
     * Retries servers on faults
     * Rebalances by giving connections a lifetime and never culling candidate
       list (so bad servers will continue to get retried)

     `connect_attempts` is the number of times to try to connect to the
     *entire* list of hosts. Likewise `send_attempts` is the number of times to
     retry sends to the *entire* list of hosts.
    """
    def __init__(self, hosts, connect_attempts=DEFAULT_CONNECT_ATTEMPTS,
                 connection_factory=None, connection_options=None,
                 max_size=DEFAULT_POOL_MAX, pool_timeout=DEFAULT_POOL_TIMEOUT,
                 send_attempts=DEFAULT_SEND_ATTEMPTS):
        cleaned_hosts = []
        for host_pair in hosts:
            host, port = host_pair.split(':')
            cleaned_hosts.append((host, int(port)))
        random.shuffle(cleaned_hosts)
        self.candidate_pool = CandidatePool(
            cleaned_hosts, maxlen=len(cleaned_hosts))
        self.connection_pool = ConnectionPool(maxsize=max_size)
        self.pool_timeout = pool_timeout
        self.max_size = max_size
        self.full = False
        if connection_factory:
            self.connection_factory = connection_factory
        self.connection_options = connection_options or {}
        self.connect_attempts = range(connect_attempts)
        self.send_attempts = range(send_attempts)

    def _connect(self):
        """Create a new connection, retrying servers on failures

        Can take up to (retries * timeout) seconds to return.
        Raises `KiddiePoolMaxTries` after exhausting retries on list of
        hosts.
        """
        # Rotate candidate pool so next connect starts on a different host
        self.candidate_pool.rotate(1)
        candidates = list(self.candidate_pool)
        for attempt in self.connect_attempts:
            for host, port in candidates:
                try:
                    return self.connection_factory(
                            host, port, **self.connection_options)
                except socket.error:
                    continue
        raise KiddiePoolMaxTries(
            "Failed to connect to any servers after %d attempts on %r" %
            (len(self.connect_attempts), candidates))

    def get(self):
        """Get a connection from the pool, creating a new one if necessary

        Raises `KiddiePoolEmpty` if no connections are available after
        pool_timeout.

        Can block up to (retries * timeout) + pool_timeout seconds.

        Caveat: Pool isn't marked full until max_size put()s are called, so
                repeatedly calling get() without put() will happily overflow
                the pool.
        """
        conn = None
        try:
            while 1:
                conn = self.connection_pool.get_nowait()
                if conn.validate():
                    # Yay! We have a working connection!
                    break
                else:
                    # A connection from the pool was invalid, pool is not full
                    self.full = False
        except queue.Empty:
            # No available connections
            if not self.full:
                # Pool isn't maxed yet, create a new connection
                conn = self._connect()

        if conn is None:
            # All connections are checked out, block
            try:
                conn = self.connection_pool.get(self.pool_timeout)
            except queue.Empty:
                raise KiddiePoolEmpty(
                        'All %d connections checked out' % self.max_size)

        return conn

    def put(self, conn):
        """Put a connection back into the pool

        Since there's a race condition where get() can create more connections
        than max_size, silently close and drop a connection if the pool is full

        Returns instantly (no blocking)
        """
        try:
            self.connection_pool.put_nowait(conn)
        except queue.Full:
            self.full = True
            # This is an overflow connection, close it
            conn.close()

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()
        try:
            yield conn
        except Exception as e:
            conn.handle_exception(e)
            raise
        finally:
            # Regardless of whether or not the connection is valid, put it back
            # in the pool. The next get() will determine it's validity.
            self.put(conn)

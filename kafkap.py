# Copyright 2011 Urban Airship
# Copyright 2010 LinkedIn
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import Queue as queue
import random
import socket
import struct
import time
import zlib


# Pool classes/defaults
CandidatePool = collections.deque
ConnectionPool = queue.Queue
DEFAULT_POOL_MAX = 10
DEFAULT_POOL_TIMEOUT = 2
DEFAULT_CONNECT_RETRIES = 2
DEFAULT_SEND_RETRIES = 2

# Connection defaults
DEFAULT_MAX_IDLE = 60
# Non-None lifetime allows slow rebalancing after failures
DEFAULT_LIFETIME = 60 * 5
DEFAULT_TIMEOUT = 3  # connect() and send() timeout

# Kafka classes/constants/defaults
PRODUCE_REQUEST_ID = 0
DEFAULT_PARTITION = -1

# Header len is sizeof(crc32) + sizeof(byte) == 4 + 1 == 5
MESSAGE_HEADER_LEN = 5
MessageStruct = struct.Struct('>iBi')
RequestHeader = struct.Struct('>HH')
RequestMessages = struct.Struct('>ii')
EnvelopeStruct = struct.Struct('>i')


def encode_message(message):
    """<LENGTH: int> <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>"""
    return MessageStruct.pack(
            len(message) + MESSAGE_HEADER_LEN, 0, zlib.crc32(message)
        ) + message


def encode_produce_request(topic, partition, messages):
    """Encode an iterable of messages for publishing in kafka"""
    message_set = ''.join(map(encode_message, messages))

    # create the request as <REQUEST_SIZE: int> <REQUEST_ID: short>
    # <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
    data = (
            RequestHeader.pack(PRODUCE_REQUEST_ID, len(topic)) +
            topic +
            RequestMessages.pack(partition, len(message_set)) +
            message_set
        )
    return EnvelopeStruct.pack(len(data)) + data


class KafkaException(Exception):
    """Base class for Kafka Exceptions"""


class KafkaPoolEmpty(KafkaException, queue.Empty):
    """No Kafka connections available in pool (even after timeout)"""


class KafkaConnectionFailure(KafkaException, socket.error):
    """Unable to connect to any Kafka servers (even after timeout & retries)
    """


class KafkaSendFailure(KafkaException, socket.error):
    """Failed to send Kafka message after retries to every server"""


class KafkaPool(object):
    """Kafka Producer Pool Implementation

     * Lazily creates connections up to a maximum
     * Retries servers on faults

     `connect_retries` is the number of times to try to connect to the *entire*
     list of hosts. Likewise `send_retries` is the number of times to retry
     sends to the *entire* list of hosts.
    """
    def __init__(self, hosts, connect_retries=DEFAULT_CONNECT_RETRIES,
                 max_size=DEFAULT_POOL_MAX, pool_timeout=DEFAULT_POOL_TIMEOUT,
                 producer_settings=None, send_retries=DEFAULT_SEND_RETRIES):
        random.shuffle(hosts)
        self.candidate_pool = CandidatePool(hosts, maxlen=len(hosts))
        self.connection_pool = ConnectionPool(maxsize=max_size)
        self.pool_timeout = pool_timeout
        self.max_size = max_size
        self.full = False
        self.producer_settings = producer_settings or {}
        self.connect_retries = range(connect_retries)
        self.send_retries = range(send_retries)

    def _connect(self):
        """Create a new connection, retrying servers on failures

        Can take up to (retries * timeout) seconds to return.
        Raises KafkaConnectionFailure after exhausting retries on list of
        hosts.
        """
        # Rotate candidate pool so next connect starts on a different host
        self.candidate_pool.rotate(1)
        candidates = list(self.candidate_pool)
        for attempt in self.connect_retries:
            for candidate in candidates:
                host, port = candidate.split(':')
                try:
                    return KafkaProducer(host, port, **self.producer_settings)
                except socket.error:
                    continue
        raise KafkaConnectionFailure(
            "Failed to connect to any Kafka servers after %d attempts on %r" %
            (len(self.connect_retries), candidates))

    def _get(self):
        """Get a connection from the pool, creating a new one if necessary

        Raises `KafkaPoolEmpty` if no connections are available after
        pool_timeout.

        Can block up to (retries * timeout) + pool_timeout seconds.
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
                raise KafkaPoolEmpty(
                        'All %d connections checked out' % self.max_size)

        return conn

    def _put(self, producer):
        """Put a producer back into the pool

        Since there's a slight race condition where _get can create more
        connections than max_size, silently close a connection if the pool is
        full.

        Returns instantly (no blocking)
        """
        try:
            self.connection_pool.put_nowait(producer)
        except queue.Full:
            self.full = True
            # This is an overflow connection, close it
            producer.close()

    def send(self, message, topic, partition=DEFAULT_PARTITION):
        """Send a message to Kafka

        Can block up to (send_retries * producer_timeout * )
        """
        return self.sendmulti([message], topic, partition)

    def sendmulti(self, messages, topic, partition=DEFAULT_PARTITION):
        """Send multiple messages to Kafka

        Can block up to (send_retries * producer_timeout * )
        """
        e = None
        for attempt in self.send_retries:
            conn = self._get()
            try:
                conn.send(messages, topic, partition)
            except socket.error as e:
                # Dropping a connection from the pool, so it's no longer full
                self.full = False
                continue
            else:
                break
        else:
            # for-loop exited meaning attempts were exhausted
            raise KafkaSendFailure(
                    'Failed to send message to topic %s after %d attempts. '
                    'Final exception: %s' % (topic, len(self.send_retries), e))

        self._put(conn)


class KafkaProducer(object):
    """Kafka Producer Connection (no error handling)"""
    def __init__(self, host='localhost', port=9092,
                 max_idle=DEFAULT_MAX_IDLE, tcp_keepalives=True,
                 timeout=DEFAULT_TIMEOUT, lifetime=DEFAULT_LIFETIME):
        self.host = host
        self.port = int(port)
        self.max_idle = max_idle
        self.tcp_keepalives = tcp_keepalives
        self.timeout = timeout
        self.connection = None
        self.closed = False
        self.last_touch = 0
        if lifetime is None:
            # No lifetime means it's immortal
            self.lifetime = None
        else:
            # Let the connection know when it will die
            self.lifetime = time.time() + lifetime
        self.open()

    def touch(self):
        self.last_touch = time.time()

    def close(self):
        self.closed = True
        self.connection.close()

    def open(self):
        self.connection = socket.create_connection(
                (self.host, self.port), timeout=self.timeout)
        if self.tcp_keepalives:
            self.connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.touch()
        self.closed = False

    def send(self, messages, topic, partition=DEFAULT_PARTITION):
        payload = encode_produce_request(topic, partition, messages)
        self.connection.sendall(payload)
        self.touch()

    def validate(self):
        """Returns True if connection is still valid, otherwise False

        Takes into account socket status, idle time, and lifetime
        """
        if self.closed:
            # Invalid because it's closed
            return False

        now = time.time()
        if (now - self.last_touch) > self.max_idle:
            # Invalid because it's been idle too long
            return False

        if self.lifetime is not None and self.lifetime < now:
            # Invalid because it's outlived its lifetime
            return False

        return True

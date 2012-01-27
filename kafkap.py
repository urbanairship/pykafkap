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
import socket
import struct
import time
import zlib

import kiddiepool


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


class KafkaConnection(object):
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

    def handle_exception(self, e):
        """Close connection on socket errors"""
        if isinstance(e, socket.error):
            self.close()

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


class KafkaSendFailure(socket.error):
    """Failed to send Kafka message after retries to every server"""


class KafkaPool(kiddiepool.KiddiePool):

    connection_factory = KafkaConnection

    def send(self, message, topic, partition=DEFAULT_PARTITION):
        """Send a message to Kafka"""
        return self.sendmulti([message], topic, partition)

    def sendmulti(self, messages, topic, partition=DEFAULT_PARTITION):
        """Send multiple messages to Kafka"""
        e = None
        for attempt in self.send_attempts:
            try:
                with self.connection() as conn:
                    conn.send(messages, topic, partition)
            except socket.error:
                continue
            else:
                break
        else:
            # for-loop exited meaning attempts were exhausted
            raise KafkaSendFailure(
                    'Failed to send message to topic %s after %d attempts. '
                    'Last exception: %s' % (topic, len(self.send_attempts), e))

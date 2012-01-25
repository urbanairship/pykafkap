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
from datetime import datetime

import random
import socket
import struct
import zlib


from losangeles.util.stats import STATS


DEFAULT_SEND_RETRIES = 3

PRODUCE_REQUEST_ID = 0
DEFAULT_PARTITION = -1

MAX_SOCKET_IDLE_TIME = 60

# Header len is sizeof(crc32) + sizeof(byte) == 4 + 1 == 5
MESSAGE_HEADER_LEN = 5
MESSAGE_STRUCT = struct.Struct('>iBi')


class KafkaException(Exception):
    """Generic exception for Kafka issues"""


def encode_message(message):
    """<LENGTH: int> <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>"""
    return MESSAGE_STRUCT.pack(
            len(message) + MESSAGE_HEADER_LEN, 0, zlib.crc32(message)
        ) + message


class KafkaProducer(object):
    def __init__(self, hosts, retries=DEFAULT_SEND_RETRIES):
        self.hosts = hosts
        # Randomize order of kafka nodes
        random.shuffle(self.hosts)
        self.current_host = None
        self.REQUEST_KEY = 0
        self.connection = None
        self.send_retries = retries
        self.last_touch = None
        self.open()

    def close(self):
        self.connection.close()

    def open(self):
        for host in self.hosts:
            try:
                self.connection = socket.create_connection(
                        host.split(':'), timeout=2)
                self.connection.setsockopt(socket.SOL_SOCKET,
                        socket.SO_KEEPALIVE, 1)
                self.current_host = host
                self.last_touch = datetime.now()
            except socket.error:
                STATS.kafka_connect_errs.inc()
            else:
                # Connected
                break
        else:
            # else clauses only execute when for exits naturally
            # in other words: raise an exception if we didn't connect/break
            raise KafkaException(
                    'Unable to connect to any kafka servers: %r' % self.hosts)
        STATS.kafka_connects.inc()

    def send(self, messages, topic, partition=DEFAULT_PARTITION):
        payload = encode_produce_request(topic, partition, messages)
        tries = 0
        while 1:
            try:
                self.check_socket()
                self.connection.sendall(payload)
                self.last_touch = datetime.now()
            except socket.error as e:
                STATS.kafka_send_errs.inc()
                tries += 1
                if tries > self.send_retries:
                    raise KafkaException(
                        'Unable to send to any kafka hosts after %d tries: %r'
                        % (self.send_retries, e)
                    )
                self.reconnect()
            else:
                break
        STATS.kafka_sends.inc()

    def reconnect(self):
        self.close()
        self.open()

    def check_socket(self):
        delta = datetime.now() - self.last_touch
        if delta.seconds > MAX_SOCKET_IDLE_TIME:
            self.reconnect()


REQUEST_HEADER = struct.Struct('>HH')
REQUEST_MESSAGES = struct.Struct('>ii')
ENVELOPE_STRUCT = struct.Struct('>i')


def encode_produce_request(topic, partition, messages):
    """Encode an iterable of messages for publishing in kafka"""
    message_set = ''.join(map(encode_message, messages))

    # create the request as <REQUEST_SIZE: int> <REQUEST_ID: short>
    # <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
    data = (REQUEST_HEADER.pack(PRODUCE_REQUEST_ID, len(topic)) + topic +
            REQUEST_MESSAGES.pack(partition, len(message_set)) + message_set
        )
    return ENVELOPE_STRUCT.pack(len(data)) + data

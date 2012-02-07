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
import struct
import zlib

import kiddiepool


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


class KafkaSendFailure(kiddiepool.KiddieClientSendFailure):
    """Failed to send Kafka message after retries to every server"""


class KafkaClient(kiddiepool.KiddieClient):

    SendException = KafkaSendFailure

    def send(self, message, topic, partition=DEFAULT_PARTITION):
        """Send a message to Kafka"""
        return self.sendmulti([message], topic, partition)

    def sendmulti(self, messages, topic, partition=DEFAULT_PARTITION):
        """Send multiple messages to Kafka"""
        payload = encode_produce_request(topic, partition, messages)
        return self._sendall(payload)

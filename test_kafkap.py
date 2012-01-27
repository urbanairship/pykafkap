import random
import socket
import unittest

import mox

import kafkap


class TestKafkaPool(mox.MoxTestBase):

    def setUp(self):
        super(TestKafkaPool, self).setUp()
        self.mox.StubOutClassWithMocks(kafkap, 'KafkaConnection')
        self.mox.StubOutWithMock(random, 'shuffle')
        random.shuffle(mox.IgnoreArg())

    def test_hosts(self):
        self.mox.ReplayAll()

        self.assertRaises(Exception, kafkap.KafkaPool, ['lolwut'])
        self.assertRaises(Exception, kafkap.KafkaPool, 'lolwut')
        self.assertRaises(Exception, kafkap.KafkaPool, ['ok:123', None])
        self.assertRaises(Exception, kafkap.KafkaPool, ['nope:123:'])
        p = kafkap.KafkaPool(['a:1', 'b:2'])
        #XXX Testing implementation details
        self.assertTrue(('a', 1) in list(p.candidate_pool))
        self.assertTrue(('b', 2) in list(p.candidate_pool))
        self.assertEqual(len(p.candidate_pool), 2)

    def test_basic_send(self):
        conn = kafkap.KafkaConnection('localhost', 9092)
        conn.send(['message1'], 'topic1', kafkap.DEFAULT_PARTITION)
        conn.validate().AndReturn(True)
        conn.send(['message2'], 'topic2', kafkap.DEFAULT_PARTITION)

        self.mox.ReplayAll()

        # Must set connection_factory since KafkaPool.connection_factory is
        # still the unmocked (real) class
        p = kafkap.KafkaPool(['localhost:9092'],
                connection_factory=kafkap.KafkaConnection)
        p.send('message1', 'topic1')
        p.sendmulti(['message2'], 'topic2')

        self.assertFalse(p.full)
        #XXX Testing implementation details
        self.assertEqual(p.connection_pool.qsize(), 1)

    def test_send_retries(self):
        # Send 1 goes out ok
        conn = kafkap.KafkaConnection('b', 2)
        conn.send(['1'], '1', kafkap.DEFAULT_PARTITION)

        # Send 2 has troubles but makes it
        conn.validate().AndReturn(True)
        err = socket.error('wat')
        conn.send(['2'], '2', kafkap.DEFAULT_PARTITION).AndRaise(err)
        conn.handle_exception(err)
        conn.validate().AndReturn(False)
        conn = kafkap.KafkaConnection('a', 1)

        err = socket.timeout('timed out')
        conn.send(['2'], '2', kafkap.DEFAULT_PARTITION).AndRaise(err)
        conn.handle_exception(err)
        conn.validate().AndReturn(False)

        conn = kafkap.KafkaConnection('b', 2)
        conn.send(['2'], '2', kafkap.DEFAULT_PARTITION)

        self.mox.ReplayAll()

        p = kafkap.KafkaPool(['a:1', 'b:2'], send_attempts=3,
                connection_factory=kafkap.KafkaConnection)
        p.send('1', '1', kafkap.DEFAULT_PARTITION)
        p.send('2', '2', kafkap.DEFAULT_PARTITION)

    def test_send_failure(self):
        err = socket.error('mockymockmox')
        conn = kafkap.KafkaConnection('c', 3)
        conn.send(['1'], '1', 1).AndRaise(err)
        conn.handle_exception(err)
        conn.validate().AndReturn(False)

        conn = kafkap.KafkaConnection('b', 2)
        conn.send(['1'], '1', 1).AndRaise(err)
        conn.handle_exception(err)
        conn.validate().AndReturn(False)

        conn = kafkap.KafkaConnection('a', 1)
        conn.send(['1'], '1', 1).AndRaise(err)
        conn.handle_exception(err)

        self.mox.ReplayAll()

        p = kafkap.KafkaPool(['a:1', 'b:2', 'c:3'], send_attempts=3,
                connection_factory=kafkap.KafkaConnection)
        self.assertRaises(kafkap.KafkaSendFailure, p.send, '1', '1', 1)


class TestKafkaEncoding(unittest.TestCase):
    def test_message_encoding(self):
        tests = (
            ('wat', '\x00\x00\x00\x08\x00\x85qH\x04wat'),
            ('\x00', '\x00\x00\x00\x06\x00\xd2\x02\xef\x8d\x00'),
            ('x' * 5000, '\x00\x00\x13\x8d\x00\x00\xdb\xf0&' + ('x' * 5000)),
        )
        for msgin, msgout in tests:
            self.assertEqual(kafkap.encode_message(msgin), msgout)

    def test_request_encoding(self):
        tests = (
            (('1', -1, ['a']), ('\x00\x00\x00\x17\x00\x00\x00\x011\xff\xff\xff'
                '\xff\x00\x00\x00\n\x00\x00\x00\x06\x00\xe8\xb7\xbeCa')),
            (('2', 99, ['a', 'b', 'c']), ('\x00\x00\x00+\x00\x00\x00\x012\x00'
                '\x00\x00c\x00\x00\x00\x1e\x00\x00\x00\x06\x00\xe8\xb7\xbeCa'
                '\x00\x00\x00\x06\x00q\xbe\xef\xf9b\x00\x00\x00\x06\x00\x06'
                '\xb9\xdfoc')),
        )
        for args, bits in tests:
            self.assertEqual(kafkap.encode_produce_request(*args), bits)

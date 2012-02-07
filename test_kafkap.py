import random
import socket
import unittest

import mox

import kafkap
import kiddiepool


class TestKafkaPool(mox.MoxTestBase):

    def setUp(self):
        super(TestKafkaPool, self).setUp()
        self.mox.StubOutClassWithMocks(kiddiepool, 'KiddieConnection')
        self.mox.StubOutWithMock(random, 'shuffle')
        random.shuffle(mox.IgnoreArg())

    def test_hosts(self):
        self.mox.ReplayAll()

        self.assertRaises(Exception, kiddiepool.KiddiePool, ['lolwut'])
        self.assertRaises(Exception, kiddiepool.KiddiePool, 'lolwut')
        self.assertRaises(Exception, kiddiepool.KiddiePool, ['ok:123', None])
        self.assertRaises(Exception, kiddiepool.KiddiePool, ['nope:123:'])
        p = kiddiepool.KiddiePool(['a:1', 'b:2'])
        #XXX Testing implementation details
        self.assertTrue(('a', 1) in list(p.candidate_pool))
        self.assertTrue(('b', 2) in list(p.candidate_pool))
        self.assertEqual(len(p.candidate_pool), 2)

    def test_basic_send(self):
        conn = kiddiepool.KiddieConnection()

        conn.validate().AndReturn(False)
        conn.connect('localhost', 9092).AndReturn(True)
        payload1 = kafkap.encode_produce_request(
                'topic1', kafkap.DEFAULT_PARTITION, ['message1'])
        conn.sendall(payload1)

        conn.validate().AndReturn(True)
        payload2 = kafkap.encode_produce_request(
                'topic2', kafkap.DEFAULT_PARTITION, ['message2'])
        conn.sendall(payload2)

        self.mox.ReplayAll()

        # Must set connection_factory since KafkaPool.connection_factory is
        # still the unmocked (real) class
        p = kiddiepool.KiddiePool(['localhost:9092'], max_size=1,
                connection_factory=kiddiepool.KiddieConnection)
        c = kafkap.KafkaClient(p)
        c.send('message1', 'topic1')
        c.sendmulti(['message2'], 'topic2')

    def test_send_retries(self):
        conn1, conn2, conn3 = [kiddiepool.KiddieConnection() for _ in '123']
        # Send 1 goes out ok
        conn1.validate().AndReturn(False)
        conn1.connect('b', 2).AndReturn(True)
        conn1.sendall(mox.IgnoreArg())

        # Send 2 has troubles but makes it
        conn2.validate().AndReturn(False)
        conn2.connect('a', 1).AndReturn(True)
        err2 = socket.timeout('timed out')
        conn2.sendall(mox.IgnoreArg()).AndRaise(err2)
        conn2.handle_exception(err2)

        conn3.validate().AndReturn(False)
        conn3.connect('b', 2).AndReturn(True)
        conn3.sendall(mox.IgnoreArg())

        self.mox.ReplayAll()

        p = kiddiepool.KiddiePool(['a:1', 'b:2'], max_size=3,
                connection_factory=kiddiepool.KiddieConnection)
        c = kafkap.KafkaClient(p, send_attempts=3)
        c.send('1', '1', kafkap.DEFAULT_PARTITION)
        c.send('2', '2', kafkap.DEFAULT_PARTITION)

    def test_send_failure(self):
        err = socket.error('mockymockmox')

        conn1, conn2, conn3 = [kiddiepool.KiddieConnection() for _ in '123']
        conn1.validate().AndReturn(False)
        conn1.connect('c', 3).AndReturn(True)
        conn1.sendall(mox.IgnoreArg()).AndRaise(err)
        conn1.handle_exception(err)

        conn2.validate().AndReturn(False)
        conn2.connect('b', 2).AndReturn(True)
        conn2.sendall(mox.IgnoreArg()).AndRaise(err)
        conn2.handle_exception(err)

        conn3.validate().AndReturn(False)
        conn3.connect('a', 1).AndReturn(True)
        conn3.sendall(mox.IgnoreArg()).AndRaise(err)
        conn3.handle_exception(err)

        self.mox.ReplayAll()

        p = kiddiepool.KiddiePool(['a:1', 'b:2', 'c:3'], max_size=3,
                connection_factory=kiddiepool.KiddieConnection)
        c = kafkap.KafkaClient(p, send_attempts=3)
        self.assertRaises(kafkap.KafkaSendFailure, c.send, '1', '1', 1)


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


if __name__ == '__main__':
    unittest.main()

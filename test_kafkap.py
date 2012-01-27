import mox

import kafkap


class TestKafkaPool(mox.MoxTestBase):

    def setUp(self):
        super(TestKafkaPool, self).setUp()
        self.mox.StubOutClassWithMocks(kafkap, 'KafkaProducer')

    def test_basics(self):
        conn = kafkap.KafkaProducer('localhost', '9092')
        conn.send(['message1'], 'topic1', kafkap.DEFAULT_PARTITION)
        conn.validate().AndReturn(True)
        conn.send(['message2'], 'topic2', kafkap.DEFAULT_PARTITION)

        self.mox.ReplayAll()

        p = kafkap.KafkaPool(['localhost:9092'])
        p.send('message1', 'topic1')
        p.sendmulti(['message2'], 'topic2')

        self.assertFalse(p.full)
        #XXX Testing implementation details
        self.assertEqual(p.connection_pool.qsize(), 1)

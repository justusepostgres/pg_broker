"""Public confluent-kafka APIs; no protocol overrides or client patches."""
import time
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from test_broker import name, BOOTSTRAP


def test_librdkafka_produce_group_commit_restart(gateway):
    t, group = name(), name()
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    admin.create_topics([NewTopic(t, 2, 1)])[t].result(timeout=15)
    delivered = []
    producer = Producer({'bootstrap.servers': BOOTSTRAP, 'acks': 'all', 'message.timeout.ms': 10000})
    for part in range(2):
        for i in range(3):
            producer.produce(t, partition=part, key=b'key', value=f'{part}:{i}'.encode(),
                             headers=[('source', b'librdkafka')],
                             on_delivery=lambda error, msg: delivered.append((error, msg.offset())))
    assert producer.flush(15) == 0
    assert len(delivered) == 6 and all(error is None for error, _ in delivered), delivered
    config = {'bootstrap.servers': BOOTSTRAP, 'group.id': group, 'auto.offset.reset': 'earliest',
              'enable.auto.commit': False, 'partition.assignment.strategy': 'range',
              'session.timeout.ms': 6000}
    c = Consumer(config)
    try:
        c.subscribe([t])
        seen = []
        end = time.monotonic() + 20
        while len(seen) < 6 and time.monotonic() < end:
            msg = c.poll(.2)
            if msg is not None:
                assert not msg.error(), msg.error()
                seen.append(msg)
        assert len(seen) == 6
        assert all(m.key() == b'key' and m.headers() == [('source', b'librdkafka')] for m in seen)
        c.commit(asynchronous=False)
    finally:
        c.close()
    gateway.restart()
    c = Consumer(config)
    try:
        assert [p.offset for p in c.committed([TopicPartition(t, 0), TopicPartition(t, 1)], timeout=10)] == [3, 3]
        c.subscribe([t])
        assert c.poll(2) is None
    finally:
        c.close()

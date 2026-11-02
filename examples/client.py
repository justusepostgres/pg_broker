"""Ordinary kafka-python 2.0.2; Python 3.11; no protocol/version overrides."""
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

address = os.getenv('BOOTSTRAP_SERVERS', '127.0.0.1:9092')
admin = KafkaAdminClient(bootstrap_servers=address)
try:
    admin.create_topics([NewTopic('events', 2, 1)])
except TopicAlreadyExistsError:
    pass
admin.close()
producer = KafkaProducer(bootstrap_servers=address, acks='all')
producer.send('events', partition=0, key=b'order-42', value=b'created',
              headers=[('source', b'example')]).get(timeout=10)
producer.close()
consumer = KafkaConsumer('events', bootstrap_servers=address, group_id='example-reader',
                         auto_offset_reset='earliest', enable_auto_commit=False)
for record in consumer:
    print(record.topic, record.partition, record.offset, record.key, record.value, record.headers)
    # Perform your application's durable work before committing. A crash between
    # that work and this commit can replay it: make handlers duplicate-tolerant.
    consumer.commit()
    break
consumer.close()

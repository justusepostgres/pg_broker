"""Bounded real-client benchmark. Reports measurements, never a throughput SLA."""
import argparse
import json
import time
import uuid
from confluent_kafka import Producer, Consumer, TopicPartition, libversion
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='broker.pg-broker.svc.cluster.local:9092')
    parser.add_argument('--records', type=int, default=10000)
    parser.add_argument('--bytes', type=int, default=256)
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--label', default='unspecified environment')
    args = parser.parse_args()
    assert 1 <= args.records <= 1000000
    assert 8 <= args.bytes <= 65536
    assert 1 <= args.partitions <= 128
    topic = 'benchmark_' + uuid.uuid4().hex
    admin = AdminClient({'bootstrap.servers':args.bootstrap})
    admin.create_topics([NewTopic(topic,args.partitions,1)])[topic].result(30)
    producer = Producer({'bootstrap.servers':args.bootstrap,'acks':'all',
                         'queue.buffering.max.messages':1024,'queue.buffering.max.kbytes':65536,
                         'linger.ms':5,'batch.size':262144,'message.timeout.ms':60000})
    latencies=[]; failures=[]
    def delivered(start, error, msg):
        if error is not None: failures.append(str(error))
        latencies.append((time.monotonic()-start)*1000)
    began=time.monotonic()
    for index in range(args.records):
        value=index.to_bytes(8,'big')+b'x'*(args.bytes-8)
        while True:
            start=time.monotonic()
            try:
                producer.produce(topic, partition=index%args.partitions, key=index.to_bytes(8,'big'),value=value,
                                 on_delivery=lambda err,msg,start=start:delivered(start,err,msg))
                break
            except BufferError:
                # Freeing one report at a time and immediately refilling its
                # slot can degenerate into single-record batches. Drain half
                # the bounded queue before admitting another burst.
                while len(producer)>512:
                    producer.poll(.01)
        producer.poll(0)
    assert producer.flush(65)==0
    produce_seconds=time.monotonic()-began
    assert not failures, failures[:5]
    assert len(latencies)==args.records
    consumer=Consumer({'bootstrap.servers':args.bootstrap,'group.id':'bench_'+uuid.uuid4().hex,
                       'enable.auto.commit':False,'auto.offset.reset':'earliest'})
    consumer.assign([TopicPartition(topic,p,0) for p in range(args.partitions)])
    seen=bytearray(args.records); count=0; offsets=[-1]*args.partitions
    read_start=time.monotonic(); deadline=read_start+120
    try:
        while count<args.records and time.monotonic()<deadline:
            msg=consumer.poll(.1)
            if msg is None: continue
            assert not msg.error(),msg.error()
            index=int.from_bytes(msg.value()[:8],'big')
            assert 0<=index<args.records and not seen[index], 'duplicate or unexpected record'
            assert msg.key()==index.to_bytes(8,'big') and len(msg.value())==args.bytes
            assert msg.partition()==index%args.partitions
            assert msg.offset()==offsets[msg.partition()]+1
            offsets[msg.partition()]=msg.offset();seen[index]=1;count+=1
        assert count==args.records,(count,args.records)
        consumer.commit(asynchronous=False)
    finally: consumer.close()
    consume_seconds=time.monotonic()-read_start
    latencies.sort()
    def percentile(q): return round(latencies[min(len(latencies)-1,int(q*(len(latencies)-1)))],3)
    result={'result':'PASS','label':args.label,'client':'confluent-kafka 2.6.1',
            'librdkafka':libversion()[0],'topic':topic,'records':args.records,
            'value_bytes':args.bytes,'partitions':args.partitions,'acks':'all',
            'producer_window_limit':1024,'producer_linger_ms':5,'consumer_assignment':'manual',
            'producer_refill_low_watermark':512,
            'produce_seconds':round(produce_seconds,3),'produce_records_per_second':round(args.records/produce_seconds,1),
            'consume_seconds':round(consume_seconds,3),'consume_records_per_second':round(args.records/consume_seconds,1),
            'ack_latency_ms':{'p50':percentile(.5),'p95':percentile(.95),'p99':percentile(.99),'max':round(max(latencies),3)}}
    print(json.dumps(result,sort_keys=True),flush=True)
    admin.delete_topics([topic])[topic].result(30)


if __name__=='__main__': main()

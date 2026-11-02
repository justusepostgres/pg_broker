"""Cross-client acceptance against a running deployment; no protocol overrides.
Run seed once, then resume after gateway restart and again after CNPG switchover.
The checkpoint is intentionally left behind the end so every resume proves replay.
"""
import json
import os
from pathlib import Path
import sys
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata
from confluent_kafka import Producer, Consumer, TopicPartition as RdkPartition

ADDRESS = os.getenv('BOOTSTRAP_SERVERS', 'broker.pg-broker.svc.cluster.local:9092')
STATE = Path(os.getenv('SMOKE_STATE', '/tmp/pg-broker-smoke.json'))

def python_consume(state, start):
    c = KafkaConsumer(state['topic'], bootstrap_servers=ADDRESS, group_id=state['python_group'],
                      enable_auto_commit=False, auto_offset_reset='earliest',
                      session_timeout_ms=6000, heartbeat_interval_ms=1000)
    try:
        result=[]; end=time.monotonic()+40
        while len(result)<(6-start)*2 and time.monotonic()<end:
            for batch in c.poll(timeout_ms=200).values(): result.extend(batch)
        assert {(r.partition,r.offset) for r in result} == {(p,i) for p in range(2) for i in range(start,6)}, result
        assert all(r.key==b'key' and r.headers==[('source',b'smoke')] for r in result)
        c.commit({TopicPartition(state['topic'],p):OffsetAndMetadata(4,'durable checkpoint') for p in range(2)})
    finally: c.close()

def rdk_consume(state, start):
    c=Consumer({'bootstrap.servers':ADDRESS,'group.id':state['rdk_group'],
                'auto.offset.reset':'earliest','enable.auto.commit':False,
                'partition.assignment.strategy':'range','session.timeout.ms':6000})
    try:
        c.subscribe([state['topic']]);result=[];end=time.monotonic()+40
        while len(result)<(6-start)*2 and time.monotonic()<end:
            msg=c.poll(.2)
            if msg is not None:
                assert not msg.error(),msg.error()
                result.append(msg)
        assert {(r.partition(),r.offset()) for r in result}=={(p,i) for p in range(2) for i in range(start,6)}
        assert all(r.key()==b'key' and r.headers()==[('source',b'smoke')] for r in result)
        c.commit(offsets=[RdkPartition(state['topic'],p,4) for p in range(2)],asynchronous=False)
    finally:c.close()

mode=sys.argv[1]
if mode=='seed':
    suffix=uuid.uuid4().hex
    state={'topic':'cnpg_'+suffix,'python_group':'python_'+suffix,'rdk_group':'rdk_'+suffix}
    admin=KafkaAdminClient(bootstrap_servers=ADDRESS)
    admin.create_topics([NewTopic(state['topic'],2,1)]);admin.close()
    p=KafkaProducer(bootstrap_servers=ADDRESS,acks='all')
    for partition in range(2):
        for i in range(3):
            assert p.send(state['topic'],partition=partition,key=b'key',value=f'python:{i}'.encode(),headers=[('source',b'smoke')]).get(15).offset==i
    p.close()
    delivered=[]
    p=Producer({'bootstrap.servers':ADDRESS,'acks':'all','message.timeout.ms':15000})
    for partition in range(2):
        for i in range(3):p.produce(state['topic'],partition=partition,key=b'key',value=f'rdk:{i}'.encode(),headers=[('source',b'smoke')],on_delivery=lambda err,msg:delivered.append(err))
    assert p.flush(20)==0 and len(delivered)==6 and all(e is None for e in delivered),delivered
    STATE.write_text(json.dumps(state))
    start=0
elif mode=='resume':
    state=json.loads(STATE.read_text());start=4
else:raise ValueError('expected seed or resume')
python_consume(state,start)
rdk_consume(state,start)
print(json.dumps({'mode':mode,'topic':state['topic'],'clients':['kafka-python 2.0.2','confluent-kafka 2.6.1'],'records_per_client':(6-start)*2,'committed_next_offset':4,'result':'PASS'}),flush=True)

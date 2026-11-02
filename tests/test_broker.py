import concurrent.futures
import time
import uuid
import psycopg
from psycopg.types.json import Jsonb
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata
from conftest import DSN

BOOTSTRAP = '127.0.0.1:19092'

def name():
    return 'test_' + uuid.uuid4().hex

def create(partitions=1):
    topic = name()
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=10000)
    try:
        admin.create_topics([NewTopic(topic, partitions, 1)])
    finally:
        admin.close()
    return topic

def producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', retries=0, request_timeout_ms=10000, max_block_ms=10000)

def consumer(group, topics=()):
    return KafkaConsumer(*topics, bootstrap_servers=BOOTSTRAP, group_id=group,
                         enable_auto_commit=False, auto_offset_reset='earliest',
                         session_timeout_ms=6000, heartbeat_interval_ms=1000,
                         request_timeout_ms=20000, consumer_timeout_ms=5000)

def records(c, count, timeout=10):
    result = []
    deadline = time.monotonic() + timeout
    while len(result) < count and time.monotonic() < deadline:
        for batch in c.poll(timeout_ms=200).values():
            result.extend(batch)
    assert len(result) == count, (len(result), count)
    return result

def db(q, conn=None):
    if conn:
        return conn.execute('select broker_call(%s)', (Jsonb(q),)).fetchone()[0]
    with psycopg.connect(DSN) as c:
        return db(q, c)

def test_vertical_slice(gateway):
    t = create()
    p = producer()
    assert p.config['api_version'] == (0, 11, 0)
    for i in range(3):
        assert p.send(t, partition=0, key=b'key', value=str(i).encode(), timestamp_ms=1700000000000+i,
                      headers=[('source', b'test'), ('empty', b'')]).get(10).offset == i
    p.close()
    g = name()
    c = consumer(g)
    tp = TopicPartition(t, 0)
    c.assign([tp])
    batch = records(c, 3)
    assert [r.offset for r in batch] == [0, 1, 2]
    assert batch[0].key == b'key' and batch[0].headers == [('source', b'test'), ('empty', b'')]
    assert batch[2].timestamp == 1700000000002
    c.commit({tp: OffsetAndMetadata(2, 'checkpoint')})
    c.close()
    gateway.restart()
    c = consumer(g)
    c.assign([tp])
    assert c.committed(tp) == 2
    assert [r.value for r in records(c, 1)] == [b'2']
    c.close()

def test_concurrent_producers_and_groups(gateway):
    t = create(2)
    def send(worker):
        p = producer()
        results = [p.send(t, partition=0, value=f'{worker}:{i}'.encode()).get(10).offset for i in range(20)]
        p.close()
        return results
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        offsets = list(pool.map(send, range(4)))
    assert sorted(sum(offsets, [])) == list(range(80))
    assert all(a == sorted(a) for a in offsets)
    for _ in range(2):
        c = consumer(name())
        c.assign([TopicPartition(t, 0)])
        assert [r.offset for r in records(c, 80)] == list(range(80))
        c.commit()
        c.close()

def test_retention(gateway):
    t = create()
    p = producer()
    for i in range(3): p.send(t, partition=0, value=b'x').get(10)
    p.close()
    v = db(dict(op='retain', topic=t, partition=0, retention_ms=0))
    assert v['earliest'] == v['high'] == 3
    assert db(dict(op='fetch', topic=t, partition=0, offset=0, max=1000))['error'] == 1
    c = consumer(name())
    tp = TopicPartition(t, 0)
    c.assign([tp])
    assert c.beginning_offsets([tp])[tp] == c.end_offsets([tp])[tp] == 3
    c.close()

def test_topic_lifecycle(gateway):
    t = create(3)
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    assert t in admin.list_topics()
    assert len(admin.describe_topics([t])[0]['partitions']) == 3
    admin.delete_topics([t])
    assert t not in admin.list_topics()
    admin.close()

def test_group_rebalance_failure_and_restart(gateway, tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path
    t, g = create(4), name()
    workers = []
    def launch(i):
        path = tmp_path / f'{i}.json'
        log = open(tmp_path / f'{i}.log', 'w')
        proc = subprocess.Popen([sys.executable, str(Path(__file__).with_name('group_worker.py')), t, g, str(path)], stdout=log, stderr=log)
        workers.append((proc, log))
        return proc, path
    def assignment(path):
        return json.loads(path.read_text()) if path.exists() else []
    def wait(predicate, seconds=30):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if predicate(): return
            assert all(p.poll() is None for p, _ in workers)
            time.sleep(.1)
        raise AssertionError('rebalance timeout: ' + str([p.read_text() for p in tmp_path.glob('*.log')]))
    try:
        a, ap = launch(1)
        wait(lambda: assignment(ap) == [0, 1, 2, 3])
        b, bp = launch(2)
        wait(lambda: len(assignment(ap)) == len(assignment(bp)) == 2 and set(assignment(ap)).isdisjoint(assignment(bp)))
        with psycopg.connect(DSN) as conn:
            generation, member = conn.execute('select generation,leader from broker.groups where name=%s', (g,)).fetchone()
        assert db(dict(op='commit', group=g, member=member, generation=generation-1, topic=t, partition=0, offset=1))['error'] == 22
        gateway.restart()
        # Wait longer than a heartbeat; durable assignments survive the restart.
        time.sleep(2)
        assert len(assignment(ap)) == len(assignment(bp)) == 2
        b.kill(); b.wait(5)
        workers[1][1].close(); workers.pop(1)
        wait(lambda: assignment(ap) == [0, 1, 2, 3])
        assert db(dict(op='heartbeat', group=g, member=member, generation=generation))['error'] in (22, 25)
    finally:
        for proc, log in workers:
            proc.kill(); proc.wait(5); log.close()

def make_batch(value=b'held', timestamp=1700000000000):
    from kafka.record.default_records import DefaultRecordBatchBuilder
    builder = DefaultRecordBatchBuilder(2, 0, False, -1, -1, -1, 1024 * 1024)
    builder.append(0, timestamp, None, value, [])
    return bytes(builder.build())

def test_uncommitted_append_cannot_be_skipped(gateway):
    t = create()
    conn = psycopg.connect(DSN)
    try:
        assert db(dict(op='append', topic=t, partition=0, data=make_batch().hex()), conn)['offset'] == 0
        p = producer()
        future = p.send(t, partition=0, value=b'next')
        deadline = time.monotonic() + 1
        with psycopg.connect(DSN, autocommit=True) as observer:
            while time.monotonic() < deadline:
                if observer.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT public.broker_call%'").fetchone()[0]:
                    break
                time.sleep(.01)
            else:
                raise AssertionError('second append never reached the partition lock')
        assert not future.is_done
        c = consumer(name())
        tp = TopicPartition(t, 0)
        c.assign([tp])
        assert c.end_offsets([tp])[tp] == 0
        assert not c.poll(timeout_ms=100)
        conn.commit()
        assert future.get(10).offset == 1
        assert [(r.offset, r.value) for r in records(c, 2)] == [(0, b'held'), (1, b'next')]
        c.close(); p.close()
        # Rollback also rolls back the counter, leaving no gap.
        assert db(dict(op='append', topic=t, partition=0, data=make_batch().hex()), conn)['offset'] == 2
        conn.rollback()
        p = producer()
        assert p.send(t, partition=0, value=b'after-rollback').get(10).offset == 2
        p.close()
    finally:
        conn.close()

def test_batch_timestamp_lookup_and_corruption(gateway):
    t = create()
    data = make_batch(b'a', 1000) + make_batch(b'b', 2000)
    assert db(dict(op='append', topic=t, partition=0, data=data.hex()))['error'] == 0
    c = consumer(name())
    tp = TopicPartition(t, 0)
    c.assign([tp])
    assert c.offsets_for_times({tp: 1500})[tp].offset == 1
    assert [r.value for r in records(c, 2)] == [b'a', b'b']
    corrupt = bytearray(make_batch()); corrupt[-1] ^= 1
    assert db(dict(op='append', topic=t, partition=0, data=corrupt.hex()))['error'] == 2
    assert c.end_offsets([tp])[tp] == 2
    c.close()

def test_subscribed_groups_commit_independently(gateway):
    t = create(2)
    p = producer()
    for partition in range(2):
        for i in range(3):
            p.send(t, partition=partition, value=f'{partition}:{i}'.encode()).get(10)
    p.close()
    for _ in range(2):
        g = name()
        c = consumer(g, [t])
        batch = records(c, 6)
        assert {r.value for r in batch} == {f'{p}:{i}'.encode() for p in range(2) for i in range(3)}
        c.commit()
        assert all(c.committed(TopicPartition(t, p)) == 3 for p in range(2))
        c.close()
        c = consumer(g, [t])
        assert not c.poll(timeout_ms=1000)
        c.close()

def test_extension_upgrade_preserves_data(gateway):
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    database = name()
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        try:
            with psycopg.connect(make_conninfo(DSN, dbname=database), autocommit=True) as conn:
                conn.execute("CREATE EXTENSION pg_broker VERSION '0.1.0'")
                assert db(dict(op='create', topic='upgrade', partitions=1), conn)['error'] == 0
                assert db(dict(op='append', topic='upgrade', partition=0, data=make_batch().hex()), conn)['offset'] == 0
                assert db(dict(op='commit', group='upgrade-group', generation=-1, member='', topic='upgrade', partition=0, offset=1), conn)['error'] == 0
                conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.1'")
                assert db(dict(op='offset_fetch', group='upgrade-group', topic='upgrade', partition=0), conn)['offset'] == 1
                assert conn.execute("SELECT extversion FROM pg_extension WHERE extname='pg_broker'").fetchone()[0] == '0.1.1'
                assert db(dict(op='lookup', topic='upgrade', partition=0, timestamp=-1), conn)['offset'] == 1
        finally:
            admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))

def test_postgres_crash_recovery(gateway):
    import os
    import subprocess
    import pytest
    from pathlib import Path
    from conftest import ROOT
    pgdata = os.getenv('PG_BROKER_TEST_PGDATA')
    if not pgdata:
        pytest.skip('set PG_BROKER_TEST_PGDATA only for the disposable native test cluster')
    pgctl = os.environ['PG_BROKER_TEST_PG_CTL']
    t, g = create(), name()
    p = producer()
    for i in range(2): p.send(t, partition=0, value=str(i).encode()).get(10)
    p.close()
    tp = TopicPartition(t, 0)
    c = consumer(g); c.assign([tp]); records(c, 2)
    c.commit({tp: OffsetAndMetadata(1, 'before database crash')}); c.close()
    gateway.stop()
    try:
        subprocess.run([pgctl, '-D', pgdata, '-m', 'immediate', '-w', 'stop'], check=True, capture_output=True)
    finally:
        subprocess.run([pgctl, '-D', pgdata, '-l', str(ROOT / '.dev/postgres.log'), '-o', '-p 55432 -h 127.0.0.1 -k /tmp', '-w', 'start'], check=True, capture_output=True)
        gateway.start()
    c = consumer(g); c.assign([tp])
    assert c.committed(tp) == 1
    assert [r.value for r in records(c, 1)] == [b'1']
    c.close()

def test_missing_sync_standby_never_acknowledged(gateway):
    import os
    import pytest
    from psycopg import sql
    from kafka.errors import KafkaError
    from pathlib import Path
    pgdata = os.getenv('PG_BROKER_TEST_PGDATA')
    if not pgdata:
        pytest.skip('requires the explicitly selected disposable native cluster')
    t = create()
    p = producer()
    p.partitions_for(t)  # Discover the leader before making commits unavailable.
    with psycopg.connect(DSN, autocommit=True) as admin:
        assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve() == Path(pgdata).resolve()
        original = admin.execute('SHOW synchronous_standby_names').fetchone()[0]
        try:
            admin.execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (pg_broker_missing_standby)'")
            admin.execute('SELECT pg_reload_conf()')
            deadline = time.monotonic()+3
            while 'pg_broker_missing_standby' not in admin.execute('SHOW synchronous_standby_names').fetchone()[0]:
                assert time.monotonic() < deadline
                time.sleep(.05)
            start = time.monotonic()
            with pytest.raises(KafkaError):
                p.send(t, partition=0, value=b'no-replica-confirmation').get(timeout=9)
            assert time.monotonic()-start >= 4.5
        finally:
            admin.execute(sql.SQL('ALTER SYSTEM SET synchronous_standby_names = {}').format(sql.Literal(original)))
            admin.execute('SELECT pg_reload_conf()')
            p.close(timeout=2)
    # The timed-out write may have committed locally; its outcome is deliberately
    # unknown, never reported as durable. A healthy subsequent append must work.
    p = producer()
    assert p.send(t, partition=0, value=b'healthy').get(10).offset >= 0
    p.close()

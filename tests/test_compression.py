"""Storage compression must not change Kafka records or committed positions."""
import os
import subprocess

import psycopg
import pytest
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata
from psycopg import sql

from conftest import DSN, ROOT
from test_broker import create, name, producer, consumer, records, make_batch, db


def test_mixed_pglz_lz4_records_survive_crash_and_resume(gateway):
    pgdata = os.getenv('PG_BROKER_TEST_PGDATA')
    if not pgdata:
        pytest.skip('requires the explicitly selected disposable native cluster')
    with psycopg.connect(DSN, autocommit=True) as admin:
        try:
            with admin.transaction():
                admin.execute('CREATE TEMP TABLE compression_probe(data bytea COMPRESSION lz4) ON COMMIT DROP')
        except (psycopg.errors.FeatureNotSupported, psycopg.errors.InvalidParameterValue):
            if os.getenv('PG_BROKER_REQUIRE_LZ4') == '1':
                raise
            pytest.skip('PostgreSQL was built without LZ4 support')
        original = admin.execute("SELECT attcompression FROM pg_attribute WHERE attrelid='broker.batches'::regclass AND attname='data'").fetchone()[0]
        restore = {'': 'default', 'p': 'pglz', 'l': 'lz4'}[original]
        topic, group = create(), name()
        expected = []
        try:
            for algorithm in ('pglz', 'lz4'):
                admin.execute(sql.SQL('ALTER TABLE broker.batches ALTER data SET COMPRESSION {}').format(sql.Identifier(algorithm)))
                p = producer()
                try:
                    for _ in range(2):
                        offset = len(expected)
                        value = (f'{algorithm}:{offset}:'.encode() + b'payload-' * 32768)
                        key = f'key-{offset}'.encode()
                        headers = [('source', algorithm.encode()), ('empty', b''), ('source', b'duplicate')]
                        timestamp = 1700000000000 + offset
                        assert p.send(topic, partition=0, key=key, value=value,
                                      headers=headers, timestamp_ms=timestamp).get(10).offset == offset
                        expected.append((key, value, headers, timestamp))
                finally:
                    p.close(timeout=5)
            actual = admin.execute('SELECT base_offset, pg_column_compression(data) FROM broker.batches WHERE topic=%s ORDER BY base_offset', (topic,)).fetchall()
            assert actual == [(0, 'pglz'), (1, 'pglz'), (2, 'lz4'), (3, 'lz4')]
            with psycopg.connect(DSN) as rollback:
                data = make_batch(b'not-committed-' * 16384)
                assert rollback.execute('SELECT public.broker_append_bytes(%s,0,%s)', (topic, data)).fetchone()[0]['offset'] == 4
                rollback.rollback()
            assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 4
        finally:
            admin.execute(sql.SQL('ALTER TABLE broker.batches ALTER data SET COMPRESSION {}').format(sql.Identifier(restore)))
    tp = TopicPartition(topic, 0)
    c = consumer(group)
    try:
        c.assign([tp])
        got = records(c, 4)
        assert [(r.key, r.value, r.headers, r.timestamp) for r in got] == expected
        c.commit({tp: OffsetAndMetadata(1, 'mixed-compression')})
    finally:
        c.close()
    gateway.stop()
    pgctl = os.environ['PG_BROKER_TEST_PG_CTL']
    try:
        subprocess.run([pgctl, '-D', pgdata, '-m', 'immediate', '-w', 'stop'], check=True, capture_output=True)
    finally:
        subprocess.run([pgctl, '-D', pgdata, '-l', str(ROOT / '.dev/postgres.log'), '-o', '-p 55432 -h 127.0.0.1 -k /tmp', '-w', 'start'], check=True, capture_output=True)
        gateway.start()
    c = consumer(group)
    try:
        c.assign([tp])
        assert c.committed(tp) == 1
        got = records(c, 3)
        assert [r.offset for r in got] == [1, 2, 3]
        assert [(r.key, r.value, r.headers, r.timestamp) for r in got] == expected[1:]
    finally:
        c.close()

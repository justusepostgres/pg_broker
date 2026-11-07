"""Binary reads must preserve the established SQL read and byte-budget contract."""
import psycopg
import pytest
from psycopg.types.json import Jsonb
from kafka.record.default_records import DefaultRecordBatchBuilder
from conftest import DSN
from test_broker import create, db, make_batch


def test_binary_fetch_matches_json_bounds_and_budgets(gateway):
    topic = create(3)
    builder = DefaultRecordBatchBuilder(2, 0, False, -1, -1, -1, 1048576)
    for offset in range(3):
        builder.append(offset, 1700000000000 + offset, None, b'inside-batch', [])
    batch = bytes(builder.build())
    assert db(dict(op='append', topic=topic, partition=0, data=batch.hex()))['offset'] == 0
    assert db(dict(op='append', topic=topic, partition=2, data=make_batch(b'x' * 3000).hex()))['offset'] == 0
    requests = [dict(topic=topic, partition=p, offset=offset, max=1048576)
                for p, offset in [(1, 0), (99, 0), (0, -1), (0, 1), (2, 0), (0, 3), (0, 0)]]
    for budget in (1, len(batch), len(batch) + 3500, 4194304):
        query = dict(requests=requests, max=budget)
        expected = db(dict(op='fetch_many', **query))
        with psycopg.connect(DSN) as conn:
            rows = conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(query),)).fetchall()
        actual = []
        for index, error, high, earliest, offset, data in rows:
            if data is None:
                assert index == len(actual) and offset is None
                actual.append(dict(error=error, high=high, batches=[]))
            else:
                assert index == len(actual) - 1
                actual[index]['batches'].append(dict(offset=offset, data=bytes(data).hex()))
        normalized = [dict(error=r.get('error', 0), high=r.get('high', 0), batches=r.get('batches', [])) for r in expected]
        assert actual == normalized


def test_binary_fetch_rejects_unbounded_input_and_cannot_write(gateway):
    topic = create()
    batch = make_batch()
    assert db(dict(op='append', topic=topic, partition=0, data=batch.hex()))['offset'] == 0
    with psycopg.connect(DSN, autocommit=True) as conn:
        for query in ({}, dict(requests={}), dict(requests=[None]), dict(requests=[{}] * 4097)):
            with pytest.raises(psycopg.errors.InvalidParameterValue):
                conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(query),))
        assert conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(dict(requests=[])),)).fetchall() == []
        query = dict(requests=[dict(op='append', topic=topic, partition=0, offset=0, data=batch.hex())])
        rows = conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(query),)).fetchall()
        assert rows[0][:4] == (0, 0, 1, 0)
        assert rows[1][4] == 0 and bytes(rows[1][5]) == batch
    assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 1


def test_binary_tail_snapshot_survives_concurrent_append_and_retention(gateway):
    topic = create()
    batch = make_batch(b'before-snapshot')
    assert db(dict(op='append', topic=topic, partition=0, data=batch.hex()))['offset'] == 0

    def fetch(conn, offset):
        query = dict(requests=[dict(topic=topic, partition=0, offset=offset)])
        return conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(query),)).fetchall()

    with psycopg.connect(DSN) as reader:
        reader.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        assert fetch(reader, 1) == [(0, 0, 1, 0, None, None)]
        assert db(dict(op='append', topic=topic, partition=0, data=make_batch(b'later').hex()))['offset'] == 1
        assert db(dict(op='retain', topic=topic, partition=0, retention_ms=0)) == dict(error=0, earliest=2, high=2)
        # The original high watermark stays empty, while its earlier data remains
        # visible despite the concurrent append and deletion in other sessions.
        assert fetch(reader, 1) == [(0, 0, 1, 0, None, None)]
        rows = fetch(reader, 0)
        assert rows[0] == (0, 0, 1, 0, None, None)
        assert len(rows) == 2 and rows[1][4] == 0 and bytes(rows[1][5]) == batch
        reader.commit()
        assert fetch(reader, 1) == [(0, 1, 2, 2, None, None)]
        assert fetch(reader, 2) == [(0, 0, 2, 2, None, None)]
        reader.commit()
        assert db(dict(op='append', topic=topic, partition=0, data=make_batch(b'resumed').hex()))['offset'] == 2
        rows = fetch(reader, 2)
        assert rows[0] == (0, 0, 3, 2, None, None)
        assert len(rows) == 2 and rows[1][4] == 2

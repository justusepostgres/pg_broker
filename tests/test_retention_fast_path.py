"""Retention must keep a contiguous suffix and avoid needless frontier rewrites."""
import psycopg

from conftest import DSN
from test_broker import create, db, make_batch, name


def frontier(topic):
    with psycopg.connect(DSN) as conn:
        return conn.execute('SELECT earliest,next_offset,xmin::text,ctid::text FROM broker.partitions WHERE topic=%s AND partition=0', (topic,)).fetchone()


def test_retention_noop_preserves_frontier_version_and_rejects_missing_duration(gateway):
    topic = create()
    request = dict(op='retain', topic=topic, partition=0, retention_ms=3600000)
    before = frontier(topic)
    assert db(request) == dict(error=0, earliest=0, high=0)
    assert frontier(topic) == before
    assert db(dict(op='append', topic=topic, partition=0, data=make_batch().hex()))['offset'] == 0
    before = frontier(topic)
    for _ in range(3):
        assert db(request) == dict(error=0, earliest=0, high=1)
        assert frontier(topic) == before
    for duration in (None, -1):
        assert db(dict(request, retention_ms=duration))['error'] == 42
    missing = dict(request); del missing['retention_ms']
    assert db(missing)['error'] == 42
    assert frontier(topic) == before
    assert db(dict(request, partition=99))['error'] == 3
    assert db(dict(op='fetch', topic=topic, partition=0, offset=0, max=1048576))['batches'][0]['offset'] == 0


def test_retention_clock_rollback_and_transaction_rollback_preserve_suffix(gateway):
    topic, group = create(), name()
    for offset in range(3):
        assert db(dict(op='append', topic=topic, partition=0, data=make_batch(str(offset).encode()).hex()))['offset'] == offset
    request = dict(op='retain', topic=topic, partition=0, retention_ms=3600000)
    with psycopg.connect(DSN) as conn:
        conn.execute("UPDATE broker.batches SET appended_at=clock_timestamp()-interval '2 hours' WHERE topic=%s AND base_offset=1", (topic,))
    before = frontier(topic)
    # A live first batch fences an older later batch after clock rollback.
    assert db(request) == dict(error=0, earliest=0, high=3)
    assert frontier(topic) == before
    with psycopg.connect(DSN) as conn:
        conn.execute("UPDATE broker.batches SET appended_at=clock_timestamp()-interval '2 hours' WHERE topic=%s AND base_offset=0", (topic,))
        assert db(request, conn) == dict(error=0, earliest=2, high=3)
        conn.rollback()
    assert frontier(topic)[:2] == (0, 3)
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT count(*) FROM broker.batches WHERE topic=%s', (topic,)).fetchone()[0] == 3
        conn.execute("UPDATE broker.batches SET appended_at=clock_timestamp()-interval '2 hours' WHERE topic=%s AND base_offset=0", (topic,))
    assert db(dict(op='commit', group=group, generation=-1, member='', topic=topic, partition=0, offset=1))['error'] == 0
    assert db(request) == dict(error=0, earliest=2, high=3)
    assert db(dict(op='offset_fetch', group=group, topic=topic, partition=0))['offset'] == 1
    assert db(dict(op='append', topic=topic, partition=0, data=make_batch().hex()))['offset'] == 3


def test_retention_expired_page_bound_and_nonmonotonic_suffix(gateway):
    topic = create()
    # Valid batch fixtures with explicit contiguous offsets make a >4096-batch
    # page without thousands of setup transactions. Only this topic is modified.
    with psycopg.connect(DSN) as conn:
        conn.execute('''INSERT INTO broker.batches(topic,partition,base_offset,record_count,data,max_timestamp,timestamps,appended_at)
          SELECT %s,0,i,1,%s,1700000000000,'[1700000000000]'::jsonb,
          CASE WHEN i=4100 THEN clock_timestamp() ELSE clock_timestamp()-interval '2 hours' END
          FROM generate_series(0,4101) i''', (topic, make_batch()))
        conn.execute('UPDATE broker.partitions SET next_offset=4102 WHERE topic=%s AND partition=0', (topic,))
    request = dict(op='retain', topic=topic, partition=0, retention_ms=3600000)
    assert db(request) == dict(error=0, earliest=4096, high=4102)
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT count(*) FROM broker.batches WHERE topic=%s', (topic,)).fetchone()[0] == 6
    assert db(request) == dict(error=0, earliest=4100, high=4102)
    before = frontier(topic)
    assert db(request) == dict(error=0, earliest=4100, high=4102)
    assert frontier(topic) == before
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT array_agg(base_offset ORDER BY base_offset) FROM broker.batches WHERE topic=%s', (topic,)).fetchone()[0] == [4100, 4101]
    assert db(dict(request, retention_ms=0)) == dict(error=0, earliest=4102, high=4102)
    before = frontier(topic)
    assert db(dict(request, retention_ms=0)) == dict(error=0, earliest=4102, high=4102)
    assert frontier(topic) == before
    assert db(dict(op='append', topic=topic, partition=0, data=make_batch().hex()))['offset'] == 4102

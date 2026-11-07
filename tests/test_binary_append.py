"""Binary writes must retain storage-boundary validation and transactional offsets."""
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from kafka.protocol.produce import ProduceRequest
from conftest import DSN, Gateway, batch_storage_relation
from test_broker import create, name, make_batch, db
from test_wire import wire


def test_binary_append_validates_whole_record_set_and_rolls_back(gateway):
    topic = create()
    first, second = make_batch(b'first'), make_batch(b'second')
    bad = bytearray(second); bad[-1] ^= 1
    with psycopg.connect(DSN, autocommit=True) as conn:
        for data in (b'', b'x' * 1048577, first + bytes(bad)):
            result = conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, data)).fetchone()[0]
            assert result['error'] == 2
            assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 0
        assert conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 99, first)).fetchone()[0]['error'] == 3
        assert conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, first + second)).fetchone()[0] == dict(error=0, offset=0)
    assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 2
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, first)).fetchone()[0]['offset'] == 2
        conn.rollback()
    assert db(dict(op='append', topic=topic, partition=0, data=second.hex()))['offset'] == 2


def test_binary_append_grant_and_gateway_fallback(gateway):
    topic, role = create(), name()
    actor = Gateway(make_conninfo(DSN, user=role), 19099)
    batch = make_batch(b'granted')
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        try:
            admin.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_call(jsonb) TO {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with psycopg.connect(make_conninfo(DSN, user=role)) as conn:
                    conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, batch))
            actor.start()
            request = ProduceRequest[3](None, -1, 10000, [(topic, [(0, batch)])])
            assert wire(request, 19099).topics[0][1][0][1:3] == (0, 0)
            admin.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_append_bytes(text,integer,bytea) TO {}').format(sql.Identifier(role)))
            actor.restart()
            assert wire(request, 19099).topics[0][1][0][1:3] == (0, 1)
            assert not admin.execute("SELECT has_function_privilege(%s, 'broker.append_bytes(text,integer,bytea,integer,bigint,jsonb)', 'EXECUTE')", (role,)).fetchone()[0]
            with psycopg.connect(make_conninfo(DSN, user=role)) as conn:
                assert conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, b'')).fetchone()[0]['error'] == 2
        finally:
            actor.stop(); actor.log.close()
            admin.execute(sql.SQL('REVOKE EXECUTE ON FUNCTION public.broker_call(jsonb),public.broker_append_bytes(text,integer,bytea) FROM {}').format(sql.Identifier(role)))
            admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def test_binary_multibatch_database_failure_is_atomic(gateway):
    topic, constraint = create(), name()
    data = make_batch(b'first', 1000) + make_batch(b'rejected', 2000)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL('ALTER TABLE {storage} ADD CONSTRAINT {} CHECK (topic <> {} OR max_timestamp <> 2000) NOT VALID').format(sql.Identifier(constraint), sql.Literal(topic), storage=batch_storage_relation(conn)))
        try:
            with pytest.raises(psycopg.Error):
                conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, data))
            assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 0
            assert conn.execute('SELECT count(*) FROM broker.batches WHERE topic=%s', (topic,)).fetchone()[0] == 0
        finally:
            conn.execute(sql.SQL('ALTER TABLE {storage} DROP CONSTRAINT {}').format(sql.Identifier(constraint),storage=batch_storage_relation(conn)))
        assert conn.execute('SELECT public.broker_append_bytes(%s,%s,%s)', (topic, 0, data)).fetchone()[0]['offset'] == 0
    assert db(dict(op='lookup', topic=topic, partition=0, timestamp=-1))['offset'] == 2

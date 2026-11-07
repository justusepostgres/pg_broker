"""Opt-in 0.1.8 identities: real PostgreSQL races, rollback, and pgrx validation."""
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from conftest import DSN
from test_broker import make_batch


@pytest.fixture
def identity_database(request):
    database = 'identity_' + uuid.uuid4().hex
    version = getattr(request, 'param', '0.1.8')
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    dsn = make_conninfo(DSN, dbname=database)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL('CREATE EXTENSION pg_broker VERSION {}').format(sql.Literal(version)))
        yield dsn
    finally:
        # A crash test invalidates every pre-restart connection, including an
        # idle administrative one. Reconnect for cleanup rather than masking a
        # successful recovery assertion with a stale-connection teardown error.
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))


def call(conn, **request):
    return conn.execute('SELECT public.broker_call(%s)', (Jsonb(request),)).fetchone()[0]


def create(conn):
    assert call(conn, op='create', topic='same-name', partitions=2) == {'error': 0}


def identity(conn):
    return conn.execute("SELECT topic_id FROM broker.topics WHERE name='same-name'").fetchone()[0]


def append(conn, topic_id, partition=0, data=None):
    return conn.execute('SELECT public.broker_append_identified(%s,%s,%s,%s)',
        ('same-name', partition, topic_id, make_batch() if data is None else data)).fetchone()[0]


@pytest.mark.parametrize('identity_database', ['0.1.7', '0.1.8'], indirect=True)
def test_upgrade_and_fresh_install_preserve_records_and_identity(identity_database):
    with psycopg.connect(identity_database) as conn:
        create(conn)
        assert call(conn, op='append', topic='same-name', partition=0, data=make_batch().hex())['offset'] == 0
        original = conn.execute('SELECT base_offset,data FROM broker.batches').fetchall()
        conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.8'")
        topic_id = identity(conn)
        assert topic_id.version == 4
        assert conn.execute('SELECT base_offset,data FROM broker.batches').fetchall() == original
        assert append(conn, topic_id)['offset'] == 1
        conn.execute("UPDATE broker.topics SET retention_ms=1000 WHERE name='same-name'")
        assert identity(conn) == topic_id
    with psycopg.connect(identity_database) as conn:
        assert identity(conn) == topic_id
        assert conn.execute('SELECT next_offset FROM broker.partitions WHERE partition=0').fetchone()[0] == 2


def test_stale_identity_validation_and_immutable_updates(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as conn:
        create(conn)
        old = identity(conn)
        assert append(conn, old, data=b'broken record set') == {'error': 2}
        assert append(conn, None) == {'error': 42}
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            conn.execute('UPDATE broker.topics SET topic_id=%s', (uuid.uuid4(),))
        assert identity(conn) == old
        assert call(conn, op='delete', topic='same-name') == {'error': 0}
        create(conn)
        new = identity(conn)
        assert new != old
        assert append(conn, old) == {'error': 3}
        assert conn.execute('SELECT count(*) FROM broker.batches').fetchone()[0] == 0
        assert append(conn, new)['offset'] == 0


def test_delete_recreate_waits_for_identified_append_transaction(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as setup:
        create(setup)
        old = identity(setup)
    pids = Queue()
    def replace():
        with psycopg.connect(identity_database) as conn:
            conn.execute("SET LOCAL lock_timeout='5s'")
            pids.put(conn.info.backend_pid)
            assert call(conn, op='delete', topic='same-name') == {'error': 0}
            create(conn)
            return identity(conn)
    with psycopg.connect(identity_database) as writer, ThreadPoolExecutor(max_workers=1) as pool:
        assert append(writer, old)['offset'] == 0
        future = pool.submit(replace)
        pid = pids.get(timeout=2)
        try:
            with psycopg.connect(identity_database, autocommit=True) as observer:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    if writer.info.backend_pid in observer.execute('SELECT pg_blocking_pids(%s)', (pid,)).fetchone()[0]:
                        break
                    time.sleep(.01)
                else:
                    pytest.fail('delete did not wait on the identified append transaction')
            assert not future.done()
            writer.commit()
        finally:
            writer.rollback()
        new = future.result(timeout=3)
        assert new != old
        assert append(writer, old) == {'error': 3}
        assert append(writer, new)['offset'] == 0


def test_distinct_partitions_remain_concurrent_and_rollback_is_atomic(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as setup:
        create(setup)
        topic_id = identity(setup)
    with psycopg.connect(identity_database) as first, psycopg.connect(identity_database) as second:
        assert append(first, topic_id, partition=0)['offset'] == 0
        second.execute("SET LOCAL lock_timeout='500ms'")
        assert append(second, topic_id, partition=1)['offset'] == 0
        second.commit()
        first.rollback()
        assert first.execute('SELECT partition,next_offset FROM broker.partitions ORDER BY partition').fetchall() == [(0, 0), (1, 1)]
        assert first.execute('SELECT partition,base_offset FROM broker.batches').fetchall() == [(1, 0)]


def test_append_waiting_on_delete_cannot_write_recreated_topic(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as setup:
        create(setup)
        old = identity(setup)
    pids = Queue()

    def stale_append():
        with psycopg.connect(identity_database) as conn:
            conn.execute("SET LOCAL lock_timeout='5s'")
            pids.put(conn.info.backend_pid)
            return append(conn, old)

    with psycopg.connect(identity_database) as remover, ThreadPoolExecutor(max_workers=1) as pool:
        assert call(remover, op='delete', topic='same-name') == {'error': 0}
        create(remover)
        new = identity(remover)
        future = pool.submit(stale_append)
        try:
            pid = pids.get(timeout=2)
            with psycopg.connect(identity_database, autocommit=True) as observer:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    if remover.info.backend_pid in observer.execute('SELECT pg_blocking_pids(%s)', (pid,)).fetchone()[0]:
                        break
                    time.sleep(.01)
                else:
                    pytest.fail('append did not wait on the deleting transaction')
            remover.commit()
        finally:
            remover.rollback()
        assert future.result(timeout=3) == {'error': 3}
        assert remover.execute('SELECT count(*) FROM broker.batches').fetchone()[0] == 0
        assert append(remover, new)['offset'] == 0


def test_identified_entrypoint_is_private_until_explicitly_granted(identity_database):
    role = 'identity_reader_' + uuid.uuid4().hex
    with psycopg.connect(identity_database, autocommit=True) as owner:
        create(owner)
        topic_id = identity(owner)
        owner.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            with psycopg.connect(identity_database, autocommit=True) as caller:
                caller.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    append(caller, topic_id)
                owner.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_append_identified(text,integer,uuid,bytea) TO {}').format(sql.Identifier(role)))
                assert append(caller, topic_id)['offset'] == 0
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    caller.execute('SELECT * FROM broker.topics')
        finally:
            owner.execute(sql.SQL('REVOKE ALL ON FUNCTION public.broker_append_identified(text,integer,uuid,bytea) FROM {}').format(sql.Identifier(role)))
            owner.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))

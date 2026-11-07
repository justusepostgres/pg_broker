"""Opt-in local journal primitives; not a consensus or snapshot implementation."""
from concurrent.futures import ThreadPoolExecutor
import time
import uuid
import json
import os
import subprocess
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from test_raft_votes import claim


def log_id(index, term=1):
    return dict(leader_id=dict(term=term, node_id=1), index=index)


def entry(index, term=1):
    return dict(log_id=log_id(index, term), payload={'Normal': [index % 256]})


def call(conn, epoch, op='state', value=None):
    return conn.execute('SELECT public.broker_raft_log(%s,%s,%s,%s)',
                        ('journal', epoch, op, Jsonb({} if value is None else value))).fetchone()[0]


def read(conn, epoch, start=0, end=2**64-1, limit=256):
    return call(conn, epoch, 'read', dict(start=log_id(start), end=log_id(end), limit=limit))


@pytest.mark.parametrize('identity_database', ['0.1.9', '0.1.10'], indirect=True)
def test_journal_upgrade_append_reopen_and_fencing(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as conn:
        before = claim(conn, 'journal')
        conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.10'")
        epoch = before['epoch']
        assert call(conn, epoch) == dict(purged=None, committed=None, last=None)
        assert call(conn, epoch, 'append', [entry(i) for i in range(3)])['last'] == log_id(2)
        call(conn, epoch, 'append', [entry(i) for i in range(3)])  # Lost-ack retry.
        assert read(conn, epoch, limit=2) == [entry(0), entry(1)]
    with psycopg.connect(identity_database, autocommit=True) as conn:
        newer = claim(conn, 'journal')['epoch']
        assert read(conn, newer) == [entry(i) for i in range(3)]
        for op, value in [('state', {}), ('append', []), ('truncate', log_id(2)),
                          ('commit', log_id(1)), ('purge', log_id(1))]:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                call(conn, epoch, op, value)


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_journal_atomic_append_no_holes_and_committed_prefix(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as conn:
        e = claim(conn, 'journal')['epoch']
        for batch in ([entry(1)], [entry(0), entry(2)], [entry(0), entry(0)]):
            with pytest.raises(psycopg.errors.InvalidParameterValue):
                call(conn, e, 'append', batch)
            assert read(conn, e) == []
        call(conn, e, 'append', [entry(i) for i in range(4)])
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            call(conn, e, 'append', [entry(2, 2)])
        call(conn, e, 'commit', log_id(1))
        for op, value in [('commit', log_id(0)), ('truncate', log_id(1)),
                          ('commit', log_id(2, 9)), ('purge', log_id(2))]:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                call(conn, e, op, value)
        call(conn, e, 'truncate', log_id(2))
        call(conn, e, 'append', [entry(2, 2), entry(3, 2)])
        call(conn, e, 'purge', log_id(1))
        assert read(conn, e) == [entry(2, 2), entry(3, 2)]
        with conn.transaction(force_rollback=True):
            call(conn, e, 'commit', log_id(3, 2))
            call(conn, e, 'purge', log_id(3, 2))
            assert call(conn, e)['last'] == log_id(3, 2)
        assert call(conn, e) == dict(last=log_id(3, 2), purged=log_id(1), committed=log_id(1))
        call(conn, e, 'commit', log_id(3, 2))
        call(conn, e, 'purge', log_id(3, 2))
        assert read(conn, e) == []
        assert call(conn, e)['last'] == log_id(3, 2)
        call(conn, e, 'append', [entry(4, 2)])
        assert read(conn, e) == [entry(4, 2)]


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_journal_serializes_owner_claim_with_append(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as setup:
        epoch = claim(setup, 'journal')['epoch']
    def successor():
        with psycopg.connect(identity_database, autocommit=True) as conn:
            return claim(conn, 'journal')['epoch']
    with ThreadPoolExecutor(max_workers=1) as pool:
        with psycopg.connect(identity_database) as conn:
            call(conn, epoch, 'append', [entry(0)])
            future = pool.submit(successor)
            deadline = time.monotonic() + 3
            with psycopg.connect(identity_database, autocommit=True) as observer:
                while time.monotonic() < deadline:
                    blocked = observer.execute('SELECT count(*) FROM pg_stat_activity '
                        'WHERE cardinality(pg_blocking_pids(pid))>0 AND datname=current_database()').fetchone()[0]
                    if blocked:
                        break
                    time.sleep(.01)
                else:
                    pytest.fail('successor did not block on the journal owner row')
                assert not future.done()
        newer = future.result(timeout=5)
    with psycopg.connect(identity_database, autocommit=True) as conn:
        assert read(conn, newer) == [entry(0)]
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            call(conn, epoch, 'append', [entry(1)])


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_journal_bounds_and_permissions(identity_database):
    role = 'journal_' + uuid.uuid4().hex
    with psycopg.connect(identity_database, autocommit=True) as conn:
        e = claim(conn, 'journal')['epoch']
        for invalid in (-1, 1.5, 2**64, '1', None):
            with pytest.raises(psycopg.errors.InvalidParameterValue):
                conn.execute('SELECT broker.raft_log_index(%s)', (Jsonb(log_id(invalid)),))
        assert conn.execute('SELECT broker.raft_log_index(%s)', (Jsonb(log_id(2**64-1)),)).fetchone()[0] == 2**64-1
        for value in ([entry(i) for i in range(257)], [dict(entry(0), payload='x' * 1048576)]):
            with pytest.raises(psycopg.errors.InvalidParameterValue):
                call(conn, e, 'append', value)
        assert read(conn, e) == []
        conn.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            conn.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                call(conn, e)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute('SELECT * FROM broker.raft_log_entries')
        finally:
            conn.execute('RESET ROLE')
            conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_journal_read_budget_and_pagination(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as conn:
        e = claim(conn, 'journal')['epoch']
        entries = [dict(entry(i), payload='x' * 1048400) for i in range(9)]
        for start in range(0, 9, 3):
            call(conn, e, 'append', entries[start:start+3])
        page = read(conn, e)
        assert 0 < len(page) < len(entries)
        assert len(json.dumps(page).encode()) <= 8388608
        assert page + read(conn, e, start=len(page)) == entries
        for start, end, limit in [(0, 8, 0), (0, 8, 257), (8, 0, 1)]:
            with pytest.raises(psycopg.errors.InvalidParameterValue):
                read(conn, e, start, end, limit)


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_journal_confirmed_prefix_survives_process_crash(identity_database):
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH') != '1':
        pytest.skip('requires an explicitly disposable PostgreSQL cluster')
    data = Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    assert data.name == 'raft-log-native-pg'
    pg_ctl = os.environ['PG_BROKER_TEST_PG_CTL']
    with psycopg.connect(identity_database, autocommit=True) as conn:
        e = claim(conn, 'journal')['epoch']
        conn.execute('SET synchronous_commit=on')
        call(conn, e, 'append', [entry(i) for i in range(4)])
        call(conn, e, 'commit', log_id(2))
        call(conn, e, 'purge', log_id(1))
    subprocess.run([pg_ctl, '-D', str(data), '-m', 'immediate', '-w', 'stop'],
                   check=True, capture_output=True)
    subprocess.run([pg_ctl, '-D', str(data), '-l', str(data.parent/'raft-log-restart.log'),
                    '-o', '-p 55432 -h 127.0.0.1 -k /tmp', '-w', 'start'],
                   check=True, capture_output=True)
    with psycopg.connect(identity_database, autocommit=True) as conn:
        successor = claim(conn, 'journal')['epoch']
        assert read(conn, successor) == [entry(2), entry(3)]
        assert call(conn, successor) == dict(last=log_id(3), committed=log_id(2), purged=log_id(1))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            call(conn, e, 'append', [entry(4)])

"""Experimental vote persistence only: no consensus or replicated-record claim."""
import os
from pathlib import Path
import subprocess
import uuid

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest

from test_topic_identity import identity_database


def claim(conn, key):
    return conn.execute('SELECT public.broker_raft_vote_claim(%s)', (key,)).fetchone()[0]


def read(conn, key, epoch):
    return conn.execute('SELECT public.broker_raft_vote_read(%s,%s)', (key, epoch)).fetchone()[0]


def save(conn, key, state, vote):
    return conn.execute('SELECT public.broker_raft_vote_cas(%s,%s,%s,%s)',
        (key, state['epoch'], state['revision'], Jsonb(vote))).fetchone()[0]


@pytest.mark.parametrize('identity_database', ['0.1.8'], indirect=True)
def test_vote_upgrade_preserves_topic_identity(identity_database):
    with psycopg.connect(identity_database) as conn:
        conn.execute("SELECT broker_call('{\"op\":\"create\",\"topic\":\"vote-upgrade\",\"partitions\":1}')")
        before = conn.execute('SELECT name,topic_id FROM broker.topics').fetchall()
        conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.9'")
        assert conn.execute('SELECT name,topic_id FROM broker.topics').fetchall() == before
        assert claim(conn, 'group')['vote'] is None


@pytest.mark.parametrize('identity_database', ['0.1.9'], indirect=True)
def test_claim_and_vote_changes_rollback_atomically(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as conn:
        state = claim(conn, 'group')
        with conn.transaction(force_rollback=True):
            newer = claim(conn, 'group')
            assert newer['epoch'] > state['epoch']
            save(conn, 'group', newer, {'example': 'uncommitted'})
        assert read(conn, 'group', state['epoch']) == state
        saved = save(conn, 'group', state, {'example': 'durable'})
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            save(conn, 'group', state, {'example': 'stale revision'})
        successor = claim(conn, 'group')
        assert successor['vote'] == saved['vote']
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            read(conn, 'group', state['epoch'])


@pytest.mark.parametrize('identity_database', ['0.1.9'], indirect=True)
def test_vote_permissions_and_storage_bounds(identity_database):
    role = 'vote_reader_' + uuid.uuid4().hex
    functions = ['broker_raft_vote_claim(text)', 'broker_raft_vote_read(text,bigint)',
                 'broker_raft_vote_cas(text,bigint,bigint,jsonb)']
    with psycopg.connect(identity_database, autocommit=True) as owner:
        owner.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            with psycopg.connect(identity_database, autocommit=True) as caller:
                caller.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    claim(caller, 'group')
                for function in functions:
                    owner.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.' + function + ' TO {}').format(sql.Identifier(role)))
                state = claim(caller, 'group')
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    caller.execute('SELECT * FROM broker.raft_votes')
                for vote in ([], {'oversized': 'x' * 1024}):
                    with pytest.raises(psycopg.errors.CheckViolation):
                        save(caller, 'group', state, vote)
                for key in ('', 'x' * 250):
                    with pytest.raises(psycopg.errors.CheckViolation):
                        claim(caller, key)
                assert read(caller, 'group', state['epoch']) == state
        finally:
            for function in functions:
                owner.execute(sql.SQL('REVOKE ALL ON FUNCTION public.' + function + ' FROM {}').format(sql.Identifier(role)))
            owner.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('identity_database', ['0.1.9'], indirect=True)
def test_confirmed_vote_survives_postgres_process_crash(identity_database):
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH') != '1':
        pytest.skip('requires an explicitly disposable PostgreSQL cluster')
    data = Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    pg_ctl = os.environ['PG_BROKER_TEST_PG_CTL']
    assert data.name == 'raft-vote-native-pg'
    with psycopg.connect(identity_database, autocommit=True) as conn:
        state = claim(conn, 'crash')
        state = save(conn, 'crash', state, {'leader_id': {'term': 7, 'node_id': 2}, 'committed': True})
    subprocess.run([pg_ctl, '-D', str(data), '-m', 'immediate', '-w', 'stop'], check=True, capture_output=True)
    subprocess.run([pg_ctl, '-D', str(data), '-l', str(data.parent/'raft-vote-restart.log'),
                    '-o', '-p 55432 -h 127.0.0.1 -k /tmp', '-w', 'start'], check=True, capture_output=True)
    with psycopg.connect(identity_database, autocommit=True) as conn:
        assert read(conn, 'crash', state['epoch']) == state
        assert claim(conn, 'crash')['vote'] == state['vote']

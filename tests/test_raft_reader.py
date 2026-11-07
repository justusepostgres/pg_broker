"""Read-only replica handles; no network-replication claim."""
import uuid
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from test_raft_votes import claim
from test_raft_log import call, entry, log_id


def read(conn, epoch):
    return conn.execute('SELECT public.broker_raft_log_read(%s,%s,%s,%s)',
        ('journal', epoch, Jsonb(dict(start=log_id(0), end=log_id(2))), True)).fetchone()[0]


@pytest.mark.parametrize('identity_database', ['0.1.10'], indirect=True)
def test_reader_upgrade_and_execute_grant_remain_read_only(identity_database):
    role = 'reader_' + uuid.uuid4().hex
    with psycopg.connect(identity_database, autocommit=True) as conn:
        e = claim(conn, 'journal')['epoch']
        call(conn, e, 'append', [entry(0)])
        conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.11'")
        assert read(conn, e) == [entry(0)]
        conn.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            conn.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                read(conn, e)
            conn.execute('RESET ROLE')
            conn.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_raft_log_read(text,bigint,jsonb,boolean) TO {}').format(sql.Identifier(role)))
            conn.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            assert read(conn, e) == [entry(0)]
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                call(conn, e, 'append', [entry(1)])
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute('SELECT * FROM broker.raft_log_entries')
        finally:
            conn.execute('RESET ROLE')
            conn.execute(sql.SQL('REVOKE ALL ON FUNCTION public.broker_raft_log_read(text,bigint,jsonb,boolean) FROM {}').format(sql.Identifier(role)))
            conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('identity_database', ['0.1.11'], indirect=True)
def test_readers_share_owner_lock_without_claiming_new_epoch(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as setup:
        e = claim(setup, 'journal')['epoch']
        call(setup, e, 'append', [entry(0)])
    with psycopg.connect(identity_database) as held:
        held.execute("SELECT 1 FROM broker.raft_votes WHERE group_key='journal' FOR SHARE")
        with psycopg.connect(identity_database, autocommit=True) as other:
            other.execute("SET statement_timeout='500ms'")
            assert read(other, e) == [entry(0)]
            assert other.execute("SELECT writer_epoch FROM broker.raft_votes WHERE group_key='journal'").fetchone()[0] == e

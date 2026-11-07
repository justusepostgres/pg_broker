"""Committed journal application into real broker partitions; no network Raft claim."""
import uuid
import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_topic_identity import identity_database
from test_raft_votes import claim
from test_raft_log import call, log_id
from test_broker import make_batch


def bind(c, epoch, topic, tid, partition=0):
    return c.execute('SELECT public.broker_raft_partition_bind(%s,%s,%s,%s,%s)',
                     ('journal', epoch, topic, tid, partition)).fetchone()[0]


def apply(c, epoch, index):
    return c.execute('SELECT public.broker_raft_partition_apply(%s,%s,%s)',
                     ('journal', epoch, Jsonb(log_id(index)))).fetchone()[0]


def prepare(c):
    epoch = claim(c, 'journal')['epoch']
    call(c, epoch)
    tid = uuid.uuid4()
    c.execute('INSERT INTO broker.topics(name,partitions,topic_id) VALUES(%s,2,%s)', ('replica', tid))
    c.execute("INSERT INTO broker.partitions(topic,partition) VALUES('replica',0),('replica',1)")
    return epoch, tid


def entry(index, offset=0, value=b'payload', append_ms=1700000000123):
    return dict(log_id=log_id(index), payload=dict(Normal=dict(
        op='append', expected_offset=offset, appended_at_ms=append_ms, data=make_batch(value).hex())))


def offset(c):
    return c.execute("SELECT next_offset FROM broker.partitions WHERE topic='replica' AND partition=0").fetchone()[0]


@pytest.mark.parametrize('identity_database', ['0.1.11', '0.1.12'], indirect=True)
def test_apply_upgrade_atomic_replay_and_reopen(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as c:
        epoch, tid = prepare(c)
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.12'")
        bind(c, epoch, 'replica', tid)
        membership = dict(configs=[[1, 2, 3]], nodes={'1': {'addr': 'node1'}})
        entries = [dict(log_id=log_id(0), payload='Blank'),
                   dict(log_id=log_id(1), payload=dict(Membership=membership)), entry(2)]
        call(c, epoch, 'append', entries)
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            apply(c, epoch, 0)  # Present does not mean committed.
        call(c, epoch, 'commit', log_id(2))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            apply(c, epoch, 1)  # Cannot skip a prefix.
        apply(c, epoch, 0)
        result = apply(c, epoch, 1)
        assert result['membership'] == dict(log_id=log_id(1), membership=membership)
        with c.transaction(force_rollback=True):
            assert apply(c, epoch, 2)['result']['offset'] == 0
            assert offset(c) == 1
        assert offset(c) == 0
        result = apply(c, epoch, 2)
        assert result['applied'] == log_id(2) and result['result']['offset'] == 0
        assert apply(c, epoch, 2) == result  # Lost confirmation retry does not append twice.
        assert offset(c) == 1
        assert c.execute("SELECT (extract(epoch FROM appended_at)*1000)::bigint FROM broker.batches WHERE topic='replica'").fetchone()[0] == 1700000000123
    with psycopg.connect(identity_database, autocommit=True) as c:
        newer = claim(c, 'journal')['epoch']
        assert bind(c, newer, 'replica', tid) == result
        assert apply(c, newer, 2) == result
        assert offset(c) == 1
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            apply(c, epoch, 2)  # Even cached replies fence old local owners.
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            bind(c, newer, 'replica', tid, 1)


@pytest.mark.parametrize('identity_database', ['0.1.12'], indirect=True)
@pytest.mark.parametrize('fault', ['offset', 'crc', 'identity'])
def test_apply_divergence_does_not_advance_marker(identity_database, fault):
    with psycopg.connect(identity_database, autocommit=True) as c:
        epoch, tid = prepare(c)
        bind(c, epoch, 'replica', tid)
        record = entry(0, offset=1 if fault=='offset' else 0)
        if fault=='crc':
            data = bytearray.fromhex(record['payload']['Normal']['data']); data[-1] ^= 1
            record['payload']['Normal']['data'] = data.hex()
        call(c, epoch, 'append', [record]); call(c, epoch, 'commit', log_id(0))
        if fault=='identity':
            c.execute("DELETE FROM broker.topics WHERE name='replica'")
            c.execute("INSERT INTO broker.topics(name,partitions) VALUES('replica',2)")
            c.execute("INSERT INTO broker.partitions(topic,partition) VALUES('replica',0)")
        with pytest.raises((psycopg.errors.ObjectNotInPrerequisiteState, psycopg.errors.InvalidParameterValue)):
            apply(c, epoch, 0)
        assert c.execute("SELECT applied FROM broker.raft_partition_state WHERE group_key='journal'").fetchone()[0] is None
        assert offset(c) == 0


@pytest.mark.parametrize('identity_database', ['0.1.12'], indirect=True)
def test_apply_permissions_and_unique_binding(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as c:
        epoch, tid = prepare(c)
        bind(c, epoch, 'replica', tid)
        second = claim(c, 'other')['epoch']
        c.execute('SELECT public.broker_raft_log(%s,%s,%s,%s)', ('other',second,'state',Jsonb({})))
        with pytest.raises(psycopg.errors.UniqueViolation):
            c.execute('SELECT public.broker_raft_partition_bind(%s,%s,%s,%s,0)',('other',second,'replica',tid))
        role = 'apply_reader_' + uuid.uuid4().hex[:10]
        c.execute('CREATE ROLE '+role)
        try:
            c.execute('SET ROLE '+role)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                apply(c, epoch, 0)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                bind(c, epoch, 'replica', tid)
        finally:
            c.execute('RESET ROLE')
            c.execute('DROP ROLE '+role)


@pytest.mark.parametrize('identity_database', ['0.1.12'], indirect=True)
def test_application_confirmed_data_and_marker_survive_restart(identity_database):
    import os
    import subprocess
    from pathlib import Path
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH') != '1':
        pytest.skip('requires an explicitly disposable PostgreSQL cluster')
    data = Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    assert data.name == 'raft-apply-crash-native-pg'
    pg_ctl = os.environ['PG_BROKER_TEST_PG_CTL']
    with psycopg.connect(identity_database, autocommit=True) as c:
        c.execute('SET synchronous_commit=on')
        epoch, tid = prepare(c)
        bind(c, epoch, 'replica', tid)
        call(c, epoch, 'append', [entry(0)])
        call(c, epoch, 'commit', log_id(0))
        saved = apply(c, epoch, 0)
        stored = c.execute("SELECT data,appended_at FROM broker.batches WHERE topic='replica'").fetchone()
    subprocess.run([pg_ctl,'-D',str(data),'-m','immediate','-w','stop'],check=True,capture_output=True)
    subprocess.run([pg_ctl,'-D',str(data),'-l',str(data.parent/'raft-apply-crash-restart.log'),
                    '-o','-p 55432 -h 127.0.0.1 -k /tmp','-w','start'],check=True,capture_output=True)
    with psycopg.connect(identity_database, autocommit=True) as c:
        newer = claim(c, 'journal')['epoch']
        assert bind(c, newer, 'replica', tid) == saved
        assert apply(c, newer, 0) == saved
        assert offset(c) == 1
        assert c.execute("SELECT data,appended_at FROM broker.batches WHERE topic='replica'").fetchone() == stored
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            apply(c, epoch, 0)


@pytest.mark.parametrize('identity_database', ['0.1.12'], indirect=True)
def test_bound_replica_cannot_purge_unapplied_committed_entry(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as c:
        epoch, tid = prepare(c)
        bind(c, epoch, 'replica', tid)
        call(c, epoch, 'append', [entry(0)])
        call(c, epoch, 'commit', log_id(0))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            call(c, epoch, 'purge', log_id(0))
        assert c.execute("SELECT count(*) FROM broker.raft_log_entries WHERE group_key='journal'").fetchone()[0] == 1
        assert call(c, epoch)['purged'] is None
        saved = apply(c, epoch, 0)
        call(c, epoch, 'purge', log_id(0))
        assert apply(c, epoch, 0) == saved
        assert offset(c) == 1

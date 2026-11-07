"""Durable paged snapshot capture, not network snapshot installation."""
import uuid
import psycopg
import pytest
from test_topic_identity import identity_database
from test_raft_apply import prepare, bind, apply, entry, offset
from test_raft_log import call, log_id
from test_raft_votes import claim


def snapshot(c, epoch, operation='begin', sid=None):
    if operation=='begin':
        return c.execute('SELECT public.broker_raft_snapshot_begin(%s,%s)', ('journal',epoch)).fetchone()[0]
    assert operation in ('copy','release')
    return c.execute('SELECT public.broker_raft_snapshot_'+operation+'(%s,%s,%s)', ('journal',epoch,sid)).fetchone()[0]


def seed(c, count=3, size=20):
    epoch, tid = prepare(c)
    bind(c,epoch,'replica',tid)
    for start in range(0,count,8):
        entries=[entry(i,offset=i,value=bytes([i%256])*size) for i in range(start,min(start+8,count))]
        call(c,epoch,'append',entries)
        call(c,epoch,'commit',log_id(start+len(entries)-1))
        for i in range(start,start+len(entries)): apply(c,epoch,i)
    return epoch,tid


@pytest.mark.parametrize('identity_database',['0.1.12','0.1.13'],indirect=True)
def test_capture_upgrade_pins_range_and_preserves_point_in_time(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=seed(c)
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.13'")
        original=c.execute("SELECT base_offset,data,appended_at FROM broker.batches WHERE topic='replica' ORDER BY base_offset").fetchall()
        state=snapshot(c,epoch); sid=state['snapshot_id']
        assert state['end_offset']==3 and state['cursor_offset']==0 and state['state']=='capturing'
        assert state['manifest']['application']['applied']==log_id(2)
        assert snapshot(c,epoch)==state
        for command in ["DELETE FROM broker.batches WHERE topic='replica' AND base_offset=0",
                        "UPDATE broker.batches SET appended_at=now() WHERE topic='replica' AND base_offset=0",
                        "DELETE FROM broker.topics WHERE name='replica'"]:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): c.execute(command)
        # Application proceeds beyond the captured boundary while copying waits.
        call(c,epoch,'append',[entry(3,offset=3)])
        call(c,epoch,'commit',log_id(3)); apply(c,epoch,3)
        newer=claim(c,'journal')['epoch']
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): snapshot(c,epoch,'copy',sid)
        completed=snapshot(c,newer,'copy',sid)
        assert completed['state']=='complete' and completed['cursor_offset']==3
        assert completed['manifest']==state['manifest']
        assert snapshot(c,newer,'copy',sid)==completed
        captured=c.execute('SELECT base_offset,data,appended_at FROM broker.raft_snapshot_batches WHERE snapshot_id=%s ORDER BY base_offset',(sid,)).fetchall()
        assert captured==original
        assert offset(c)==4
        c.execute("DELETE FROM broker.batches WHERE topic='replica' AND base_offset=0") # Pin released.
        assert c.execute('SELECT count(*) FROM broker.raft_snapshot_batches WHERE snapshot_id=%s',(sid,)).fetchone()[0]==3


@pytest.mark.parametrize('identity_database',['0.1.13'],indirect=True)
@pytest.mark.parametrize('count,size',[(130,20),(20,450000)])
def test_capture_bounded_copy_release_and_rollback(identity_database,count,size):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c,count,size)
        initial=snapshot(c,epoch); sid=initial['snapshot_id']
        with c.transaction(force_rollback=True):
            first=snapshot(c,epoch,'copy',sid)
            assert 0<first['cursor_offset']<count and first['state']=='capturing'
        assert snapshot(c,epoch)==initial
        assert c.execute('SELECT count(*) FROM broker.raft_snapshot_batches WHERE snapshot_id=%s',(sid,)).fetchone()[0]==0
        first=snapshot(c,epoch,'copy',sid)
        assert 0<first['cursor_offset']<count
        assert snapshot(c,epoch,'copy',sid)['state']=='complete'
        second=snapshot(c,epoch)
        assert second['snapshot_id']!=sid
        while snapshot(c,epoch,'copy',second['snapshot_id'])['state']!='complete': pass
        with pytest.raises(psycopg.errors.ProgramLimitExceeded): snapshot(c,epoch)
        results=[]
        while True:
            released=snapshot(c,epoch,'release',sid)['released']; results.append(released)
            if released: break
        assert results==([False,True] if count==130 else [True])
        assert snapshot(c,epoch,'release',sid)=={'released':True}
        assert snapshot(c,epoch)['state']=='capturing'


@pytest.mark.parametrize('identity_database',['0.1.13'],indirect=True)
def test_snapshot_private_api_and_abort_release_pin(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c)
        snap=snapshot(c,epoch); sid=snap['snapshot_id']
        role='snapshot_reader_'+uuid.uuid4().hex[:10]
        c.execute('CREATE ROLE '+role)
        try:
            c.execute('SET ROLE '+role)
            for op in ('begin','copy','release'):
                with pytest.raises(psycopg.errors.InsufficientPrivilege): snapshot(c,epoch,op,sid)
        finally:
            c.execute('RESET ROLE'); c.execute('DROP ROLE '+role)
        assert snapshot(c,epoch,'release',sid)=={'released':True}
        c.execute("DELETE FROM broker.batches WHERE topic='replica' AND base_offset=0")


@pytest.mark.parametrize('identity_database',['0.1.13'],indirect=True)
def test_old_repeatable_read_writer_cannot_miss_snapshot_pin(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c)
        with psycopg.connect(identity_database) as old:
            old.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            old.execute("SELECT count(*) FROM broker.batches WHERE topic='replica'")
            snap=snapshot(c,epoch)
            with pytest.raises(psycopg.errors.SerializationFailure):
                old.execute("DELETE FROM broker.batches WHERE topic='replica' AND base_offset=0")
            old.rollback()
        assert snapshot(c,epoch,'copy',snap['snapshot_id'])['state']=='complete'


@pytest.mark.parametrize('identity_database',['0.1.13'],indirect=True)
def test_snapshot_capture_resumes_after_process_crash(identity_database):
    import os
    import subprocess
    from pathlib import Path
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH')!='1':
        pytest.skip('requires an explicitly disposable PostgreSQL cluster')
    data=Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    assert data.name=='raft-snapshot-crash-native-pg'
    pg_ctl=os.environ['PG_BROKER_TEST_PG_CTL']
    with psycopg.connect(identity_database,autocommit=True) as c:
        c.execute('SET synchronous_commit=on')
        epoch,_=seed(c,130)
        snap=snapshot(c,epoch); sid=snap['snapshot_id']
        first=snapshot(c,epoch,'copy',sid)
        assert first['cursor_offset']==128 and first['state']=='capturing'
    subprocess.run([pg_ctl,'-D',str(data),'-m','immediate','-w','stop'],check=True,capture_output=True)
    subprocess.run([pg_ctl,'-D',str(data),'-l',str(data.parent/'raft-snapshot-crash-restart.log'),
                    '-o','-p 55432 -h 127.0.0.1 -k /tmp','-w','start'],check=True,capture_output=True)
    with psycopg.connect(identity_database,autocommit=True) as c:
        newer=claim(c,'journal')['epoch']
        assert snapshot(c,newer)==first
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): snapshot(c,epoch,'copy',sid)
        assert snapshot(c,newer,'copy',sid)['state']=='complete'
        source=c.execute("SELECT base_offset,data,appended_at FROM broker.batches WHERE topic='replica' ORDER BY base_offset").fetchall()
        saved=c.execute('SELECT base_offset,data,appended_at FROM broker.raft_snapshot_batches WHERE snapshot_id=%s ORDER BY base_offset',(sid,)).fetchall()
        assert saved==source and len(saved)==130

"""Committed retention boundaries, not replica-local clock decisions."""
import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_topic_identity import identity_database
from test_raft_apply import prepare,bind,apply,entry
from test_raft_log import call,log_id
from test_raft_snapshot import seed,snapshot
from test_raft_votes import claim
from test_broker import make_batch


def retention(index,expected,new):
    return dict(log_id=log_id(index),payload=dict(Normal=dict(op='retain',expected_earliest=expected,new_earliest=new)))


def bounds(c):
    return c.execute("SELECT earliest,next_offset FROM broker.partitions WHERE topic='replica' AND partition=0").fetchone()


@pytest.mark.parametrize('identity_database',['0.1.15','0.1.16'],indirect=True)
def test_upgrade_retention_commit_rollback_retry_and_reopen(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c)
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.16'")
        call(c,epoch,'append',[retention(3,0,2)])
        with pytest.raises(psycopg.Error,match='uncommitted'):apply(c,epoch,3)
        assert bounds(c)==(0,3)
        call(c,epoch,'commit',log_id(3))
        with c.transaction(force_rollback=True):
            assert apply(c,epoch,3)['result']==dict(error=0,offset=2)
            assert bounds(c)==(2,3)
        assert bounds(c)==(0,3)
        assert apply(c,epoch,3)['result']['offset']==2
        assert apply(c,epoch,3)['result']['offset']==2
        assert c.execute("SELECT base_offset FROM broker.batches WHERE topic='replica' AND partition=0").fetchall()==[(2,)]
    with psycopg.connect(identity_database,autocommit=True) as c:
        newer=claim(c,'journal')['epoch']
        assert apply(c,newer,3)['result']['offset']==2
        with pytest.raises(psycopg.Error,match='fenced'):apply(c,epoch,3)
        call(c,newer,'append',[retention(4,2,3)]);call(c,newer,'commit',log_id(4));apply(c,newer,4)
        assert bounds(c)==(3,3)
        assert c.execute('SELECT count(*) FROM broker.raft_apply_context').fetchone()[0]==0


@pytest.mark.parametrize('identity_database',['0.1.16'],indirect=True)
@pytest.mark.parametrize('expected,new',[(1,2),(0,4),(0,-1)])
def test_divergent_or_invalid_retention_never_advances_applied(identity_database,expected,new):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c)
        call(c,epoch,'append',[retention(3,expected,new)]);call(c,epoch,'commit',log_id(3))
        with pytest.raises(psycopg.Error):apply(c,epoch,3)
        assert bounds(c)==(0,3)
        assert c.execute('SELECT applied FROM broker.raft_partition_state').fetchone()[0]==log_id(2)
        assert c.execute('SELECT count(*) FROM broker.raft_apply_context').fetchone()[0]==0


@pytest.mark.parametrize('identity_database',['0.1.16'],indirect=True)
def test_retention_waits_for_snapshot_pin_then_retries_same_entry(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c);snap=snapshot(c,epoch)
        call(c,epoch,'append',[retention(3,0,3)]);call(c,epoch,'commit',log_id(3))
        with pytest.raises(psycopg.Error,match='pinned'):apply(c,epoch,3)
        assert bounds(c)==(0,3)
        assert snapshot(c,epoch,'copy',snap['snapshot_id'])['state']=='complete'
        assert apply(c,epoch,3)['result']['offset']==3
        assert c.execute('SELECT count(*) FROM broker.raft_snapshot_batches').fetchone()[0]==3


@pytest.mark.parametrize('identity_database',['0.1.16'],indirect=True)
def test_rejects_cut_inside_batch(identity_database):
    from kafka.record.default_records import DefaultRecordBatchBuilder
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        builder=DefaultRecordBatchBuilder(2,0,False,-1,-1,-1,1024*1024)
        builder.append(0,1700000000000,None,b'a',[]);builder.append(1,1700000000000,None,b'b',[])
        item=entry(0);item['payload']['Normal']['data']=bytes(builder.build()).hex()
        call(c,epoch,'append',[item]);call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        assert bounds(c)==(0,2)
        call(c,epoch,'append',[retention(1,0,1)]);call(c,epoch,'commit',log_id(1))
        with pytest.raises(psycopg.Error,match='batch boundary'):apply(c,epoch,1)
        assert bounds(c)==(0,2)


@pytest.mark.parametrize('identity_database',['0.1.16'],indirect=True)
@pytest.mark.parametrize('cutoff',[4096,4097])
def test_delete_work_bound(identity_database,cutoff):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        item=entry(0);item['payload']['Normal']['data']=(make_batch()*4097).hex()
        call(c,epoch,'append',[item]);call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        call(c,epoch,'append',[retention(1,0,cutoff)]);call(c,epoch,'commit',log_id(1))
        if cutoff==4097:
            with pytest.raises(psycopg.errors.ProgramLimitExceeded):apply(c,epoch,1)
            assert bounds(c)==(0,4097)
        else:
            assert apply(c,epoch,1)['result']['offset']==4096
            assert bounds(c)==(4096,4097)


@pytest.mark.parametrize('identity_database',['0.1.16'],indirect=True)
@pytest.mark.parametrize('confirmed',[False,True])
def test_retention_process_crash_recovers_data_and_marker_together(identity_database,confirmed):
    import os
    import subprocess
    from pathlib import Path
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH')!='1':
        pytest.skip('requires an explicitly disposable PostgreSQL cluster')
    data=Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    assert data.name=='raft-retention-crash-native-pg'
    pg_ctl=os.environ['PG_BROKER_TEST_PG_CTL']
    c=psycopg.connect(identity_database,autocommit=True)
    try:
        c.execute('SET synchronous_commit=on')
        epoch,_=seed(c)
        original=c.execute("SELECT base_offset,data FROM broker.batches WHERE topic='replica' ORDER BY base_offset").fetchall()
        call(c,epoch,'append',[retention(3,0,2)]);call(c,epoch,'commit',log_id(3))
        if not confirmed:c.execute('BEGIN')
        saved=apply(c,epoch,3)
        assert bounds(c)==(2,3)
        subprocess.run([pg_ctl,'-D',str(data),'-m','immediate','-w','stop'],check=True,capture_output=True)
    finally:
        c.close()
    subprocess.run([pg_ctl,'-D',str(data),'-l',str(data.parent/'raft-retention-crash-restart.log'),
                    '-o','-p 55432 -h 127.0.0.1 -k /tmp','-w','start'],check=True,capture_output=True)
    with psycopg.connect(identity_database,autocommit=True) as recovered:
        assert bounds(recovered)==((2,3) if confirmed else (0,3))
        assert recovered.execute('SELECT applied FROM broker.raft_partition_state').fetchone()[0]==log_id(3 if confirmed else 2)
        assert recovered.execute("SELECT base_offset,data FROM broker.batches WHERE topic='replica' ORDER BY base_offset").fetchall()==(original[2:] if confirmed else original)
        assert recovered.execute('SELECT count(*) FROM broker.raft_apply_context').fetchone()[0]==0
        newer=claim(recovered,'journal')['epoch']
        assert apply(recovered,newer,3)==saved
        with pytest.raises(psycopg.Error,match='fenced'):apply(recovered,epoch,3)
        assert bounds(recovered)==(2,3)

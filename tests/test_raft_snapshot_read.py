"""Bounded reads from completed snapshot images, not network installation."""
import uuid
import psycopg
import pytest
from test_topic_identity import identity_database
from test_raft_snapshot import seed, snapshot
from test_raft_votes import claim


def read(c,epoch,sid,start):
    return c.execute('SELECT public.broker_raft_snapshot_read(%s,%s,%s,%s)',('journal',epoch,sid,start)).fetchone()[0]


@pytest.mark.parametrize('identity_database',['0.1.13','0.1.14'],indirect=True)
def test_read_upgrade_stable_pages_and_released_owner_fencing(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c,3)
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.14'")
        capture=snapshot(c,epoch); sid=capture['snapshot_id']
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): read(c,epoch,sid,0)
        snapshot(c,epoch,'copy',sid)
        page=read(c,epoch,sid,0)
        assert page['eof'] and page['next_offset']==3
        assert [b['base_offset'] for b in page['batches']]==[0,1,2]
        source=c.execute("SELECT data,(extract(epoch FROM appended_at)*1000000)::bigint FROM broker.batches WHERE topic='replica' ORDER BY base_offset").fetchall()
        assert [(bytes.fromhex(b['data']),b['appended_at_micros']) for b in page['batches']]==source
        # Source retention after capture cannot change the captured image.
        c.execute("DELETE FROM broker.batches WHERE topic='replica'")
        assert read(c,epoch,sid,0)==page
        assert read(c,epoch,sid,3)['batches']==[]
        for cursor in (-1,4):
            with pytest.raises(psycopg.errors.InvalidParameterValue): read(c,epoch,sid,cursor)
        newer=claim(c,'journal')['epoch']
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): read(c,epoch,sid,3)
        assert read(c,newer,sid,0)==page
        snapshot(c,newer,'release',sid)
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): read(c,newer,sid,0)


@pytest.mark.parametrize('identity_database',['0.1.14'],indirect=True)
@pytest.mark.parametrize('count,size',[(130,20),(20,450000)])
def test_read_count_byte_bounds_continuation_and_no_repeated_records(identity_database,count,size):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,_=seed(c,count,size)
        capture=snapshot(c,epoch); sid=capture['snapshot_id']
        while snapshot(c,epoch,'copy',sid)['state']!='complete': pass
        start=0; collected=[]; pages=0
        while True:
            page=read(c,epoch,sid,start); pages+=1
            assert len(page['batches'])<=128
            assert sum(len(b['data'])//2 for b in page['batches'])<=2097152
            assert page['next_offset']>start
            collected.extend(b['base_offset'] for b in page['batches'])
            start=page['next_offset']
            if page['eof']: break
        assert collected==list(range(count)) and pages>1 and start==count


@pytest.mark.parametrize('identity_database',['0.1.14'],indirect=True)
def test_read_permissions_and_nonboundary_cursor(identity_database):
    from kafka.record.default_records import DefaultRecordBatchBuilder
    from test_raft_apply import prepare,bind,apply,entry
    from test_raft_log import call,log_id
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        builder=DefaultRecordBatchBuilder(2,0,False,-1,-1,-1,1048576)
        builder.append(0,1700000000000,b'key',b'value',[('header',b'one')])
        builder.append(1,1700000000001,None,None,[('header',None)])
        item=entry(0);item['payload']['Normal']['data']=bytes(builder.build()).hex()
        call(c,epoch,'append',[item]);call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        sid=snapshot(c,epoch)['snapshot_id'];snapshot(c,epoch,'copy',sid)
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): read(c,epoch,sid,1)
        page=read(c,epoch,sid,0)
        assert page['batches'][0]['data']==item['payload']['Normal']['data']
        assert page['batches'][0]['record_count']==2
        role='snapshot_page_reader_'+uuid.uuid4().hex[:10];c.execute('CREATE ROLE '+role)
        try:
            c.execute('SET ROLE '+role)
            with pytest.raises(psycopg.errors.InsufficientPrivilege): read(c,epoch,sid,0)
        finally:
            c.execute('RESET ROLE');c.execute('DROP ROLE '+role)


@pytest.mark.parametrize('identity_database',['0.1.14'],indirect=True)
@pytest.mark.parametrize('empty',[False,True])
def test_read_retained_interval_and_empty_snapshot(identity_database,empty):
    from test_raft_apply import prepare,bind,apply
    from test_raft_log import call,log_id
    with psycopg.connect(identity_database,autocommit=True) as c:
        if empty:
            epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
            call(c,epoch,'append',[dict(log_id=log_id(0),payload='Blank')])
            call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        else:
            epoch,_=seed(c,3)
            # Represent an already-retained prefix, before capturing the image.
            with c.transaction():
                c.execute("DELETE FROM broker.batches WHERE topic='replica' AND base_offset=0")
                c.execute("UPDATE broker.partitions SET earliest=1 WHERE topic='replica' AND partition=0")
        captured=snapshot(c,epoch);sid=captured['snapshot_id']
        snapshot(c,epoch,'copy',sid)
        start=0 if empty else 1
        page=read(c,epoch,sid,start)
        assert page['eof'] and page['snapshot']['earliest']==start
        assert [b['base_offset'] for b in page['batches']]==([] if empty else [1,2])
        assert page['next_offset']==(0 if empty else 3)
        if not empty:
            with pytest.raises(psycopg.errors.InvalidParameterValue): read(c,epoch,sid,0)

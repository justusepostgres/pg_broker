"""Opt-in generation visibility foundation; this is not a snapshot installer."""
import uuid
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from test_broker import make_batch
from test_raft_apply import prepare, bind, apply, entry
from test_raft_log import call, log_id


def rpc(c, **q):
    return c.execute('SELECT public.broker_call(%s)',(Jsonb(q),),prepare=True).fetchone()[0]


def fetch(c):
    return rpc(c,op='fetch',topic='generations',partition=0,offset=0,max=1048576)


@pytest.mark.parametrize('identity_database',['0.1.20','0.1.21'],indirect=True)
def test_upgrade_cached_fetch_active_visibility_append_and_retention(identity_database):
    old=make_batch(b'old',1000);new=make_batch(b'new',1000)
    generation=uuid.uuid4()
    with psycopg.connect(identity_database,autocommit=True) as c:
        assert rpc(c,op='create',topic='generations',partitions=1)['error']==0
        assert c.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',old)).fetchone()[0]['offset']==0
        assert fetch(c)['batches'][0]['data']==old.hex()
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.21'")
        assert fetch(c)['batches'][0]['data']==old.hex()
        c.execute('''INSERT INTO broker.batch_storage(topic,partition,base_offset,record_count,data,max_timestamp,timestamps,appended_at,storage_generation)
            SELECT topic,partition,base_offset,record_count,%s,max_timestamp,timestamps,appended_at,%s
            FROM broker.batches WHERE topic='generations' ''',(new,generation))
        assert fetch(c)['batches'][0]['data']==old.hex(), 'inactive data leaked'
        with c.transaction(force_rollback=True):
            c.execute("UPDATE broker.partitions SET storage_generation=%s WHERE topic='generations'",(generation,))
            assert fetch(c)['batches'][0]['data']==new.hex()
        assert fetch(c)['batches'][0]['data']==old.hex()
        c.execute("UPDATE broker.partitions SET storage_generation=%s WHERE topic='generations'",(generation,))
        assert fetch(c)['batches'][0]['data']==new.hex()
        assert c.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',new)).fetchone()[0]['offset']==1
        assert [r['offset'] for r in fetch(c)['batches']]==[0,1]
        assert c.execute("SELECT count(*) FROM broker.batch_storage WHERE storage_generation=%s",(generation,)).fetchone()[0]==2
        assert rpc(c,op='retain',topic='generations',partition=0,retention_ms=0)['error']==0
        assert c.execute('SELECT count(*) FROM broker.batches').fetchone()[0]==0
        assert c.execute('SELECT count(*) FROM broker.batch_storage').fetchone()[0]==1
        assert rpc(c,op='delete',topic='generations')['error']==0
        assert c.execute('SELECT count(*) FROM broker.batch_storage').fetchone()[0]==0


@pytest.mark.parametrize('identity_database',['0.1.21'],indirect=True)
def test_repeatable_read_observes_one_generation(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        rpc(c,op='create',topic='generations',partitions=1)
        c.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',make_batch()))
        with psycopg.connect(identity_database) as reader:
            reader.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            assert len(fetch(reader)['batches'])==1
            c.execute("UPDATE broker.partitions SET storage_generation=%s,next_offset=0 WHERE topic='generations'",(uuid.uuid4(),))
            assert len(fetch(reader)['batches'])==1
        assert fetch(c)['batches']==[]


@pytest.mark.parametrize('identity_database',['0.1.21'],indirect=True)
def test_committed_application_guards_and_snapshot_copy_survive_view(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        call(c,epoch,'append',[entry(0)]);call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        for statement in ["DELETE FROM broker.batches WHERE topic='replica' AND partition=0",
                          "UPDATE broker.partitions SET storage_generation=gen_random_uuid() WHERE topic='replica' AND partition=0"]:
            with pytest.raises(psycopg.Error,match='committed Raft application'):c.execute(statement)
        capture=c.execute('SELECT public.broker_raft_snapshot_begin(%s,%s)',('journal',epoch)).fetchone()[0]
        capture=c.execute('SELECT public.broker_raft_snapshot_copy(%s,%s,%s)',('journal',epoch,capture['snapshot_id'])).fetchone()[0]
        assert capture['state']=='complete' and capture['cursor_offset']==1
        assert c.execute('SELECT data FROM broker.raft_snapshot_batches').fetchone()[0]==c.execute("SELECT data FROM broker.batches WHERE topic='replica'").fetchone()[0]


@pytest.mark.parametrize('identity_database',['0.1.20'],indirect=True)
def test_upgrade_resets_internal_grants_and_keeps_dump_registration(identity_database):
    role='generation_'+uuid.uuid4().hex
    with psycopg.connect(identity_database,autocommit=True) as c:
        c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            c.execute(sql.SQL('GRANT SELECT ON broker.batches TO {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {}').format(sql.Identifier(role)))
            c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.21'")
            for relation in ('broker.batch_storage','broker.batches'):
                assert not c.execute('SELECT has_table_privilege(%s,%s,\'SELECT\')',(role,relation)).fetchone()[0]
            assert c.execute("SELECT 'broker.batch_storage'::regclass::oid=ANY(extconfig) FROM pg_extension WHERE extname='pg_broker'").fetchone()[0]
        finally:
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('identity_database',['0.1.21'],indirect=True)
def test_logical_archive_preserves_active_and_inactive_generations(identity_database,tmp_path):
    import os
    import subprocess
    from pathlib import Path
    from psycopg.conninfo import make_conninfo
    bindir=Path(subprocess.check_output([os.getenv('PG_CONFIG','pg_config'),'--bindir'],text=True).strip())
    restored='generation_restore_'+uuid.uuid4().hex
    target=make_conninfo(identity_database,dbname=restored)
    with psycopg.connect(identity_database,autocommit=True) as c:
        rpc(c,op='create',topic='generations',partitions=1)
        c.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',make_batch(b'old',1000)))
        gen=uuid.uuid4()
        c.execute('''INSERT INTO broker.batch_storage(topic,partition,base_offset,record_count,data,max_timestamp,timestamps,appended_at,storage_generation)
            SELECT topic,partition,base_offset,record_count,data,max_timestamp,timestamps,appended_at,%s FROM broker.batches''',(gen,))
        c.execute("UPDATE broker.partitions SET storage_generation=%s WHERE topic='generations'",(gen,))
        c.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',make_batch(b'new',2000)))
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        call(c,epoch,'append',[entry(0)]);call(c,epoch,'commit',log_id(0));apply(c,epoch,0)
        before=c.execute('SELECT * FROM broker.batch_storage ORDER BY topic,partition,storage_generation,base_offset').fetchall()
        archive=tmp_path/'generations.dump'
        subprocess.run([str(bindir/'pg_dump'),'--format=custom','--file',str(archive),'--dbname',identity_database],check=True,capture_output=True)
        c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(restored)))
        try:
            # Opt-in restores must preinstall the captured extension version:
            # pg_dump's CREATE EXTENSION does not select an experimental version.
            with psycopg.connect(target,autocommit=True) as dest:
                dest.execute("CREATE EXTENSION pg_broker VERSION '0.1.21'")
            subprocess.run([str(bindir/'pg_restore'),'--exit-on-error','--single-transaction','--dbname',target,str(archive)],check=True,capture_output=True)
            with psycopg.connect(target,autocommit=True) as dest:
                assert dest.execute('SELECT * FROM broker.batch_storage ORDER BY topic,partition,storage_generation,base_offset').fetchall()==before
                assert fetch(dest)==fetch(c)
                assert dest.execute("SELECT applied FROM broker.raft_partition_state WHERE group_key='journal'").fetchone()[0]==log_id(0)
                assert dest.execute('SELECT public.broker_append_bytes(%s,0,%s)',('generations',make_batch(b'after-restore',3000))).fetchone()[0]['offset']==2
        finally:
            c.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(restored)))

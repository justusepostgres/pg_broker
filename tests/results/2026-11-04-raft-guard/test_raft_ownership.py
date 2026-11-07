"""Opt-in ownership fencing on actual PostgreSQL; no network consensus claim."""
import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_topic_identity import identity_database
from test_raft_apply import prepare, bind, apply, entry, offset
from test_raft_log import call, log_id
from test_broker import make_batch


@pytest.mark.parametrize('identity_database', ['0.1.14', '0.1.15', '0.1.17', '0.1.18'], indirect=True)
def test_ownership_upgrade_legacy_paths_and_committed_apply(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as c:
        epoch, tid = prepare(c)
        original_oid=c.execute("SELECT 'public.broker_raft_partition_apply(text,bigint,jsonb)'::regprocedure::oid").fetchone()[0]
        version=c.execute("SELECT extversion FROM pg_extension WHERE extname='pg_broker'").fetchone()[0]
        target='0.1.18' if version in ('0.1.17','0.1.18') else '0.1.15'
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '"+target+"'")
        assert c.execute("SELECT 'public.broker_raft_partition_apply(text,bigint,jsonb)'::regprocedure::oid").fetchone()[0]==original_oid
        bind(c, epoch, 'replica', tid)
        for sql, args in [
            ('SELECT public.broker_append_bytes(%s,0,%s)', ('replica', make_batch())),
            ('SELECT public.broker_append_identified(%s,0,%s,%s)', ('replica', tid, make_batch())),
            ('SELECT public.broker_call(%s)', (Jsonb(dict(op='append',topic='replica',partition=0,data=make_batch().hex())),)),
        ]:
            with pytest.raises(psycopg.Error, match='requires committed Raft application'):
                c.execute(sql,args)
            assert offset(c)==0
        assert c.execute('SELECT public.broker_append_bytes(%s,1,%s)',('replica',make_batch())).fetchone()[0]['offset']==0
        call(c,epoch,'append',[entry(0)])
        with pytest.raises(psycopg.Error,match='uncommitted'):
            apply(c,epoch,0)
        assert c.execute('SELECT count(*) FROM broker.raft_apply_context').fetchone()[0]==0
        call(c,epoch,'commit',log_id(0))
        with c.transaction(force_rollback=True):
            assert apply(c,epoch,0)['result']['offset']==0
            assert offset(c)==1
        assert offset(c)==0
        assert apply(c,epoch,0)['result']['offset']==0
        assert apply(c,epoch,0)['result']['offset']==0
        assert offset(c)==1
        assert c.execute('SELECT count(*) FROM broker.raft_apply_context').fetchone()[0]==0
        for sql in ["DELETE FROM broker.topics WHERE name='replica'",
                    "DELETE FROM broker.batches WHERE topic='replica' AND partition=0",
                    "UPDATE broker.partitions SET earliest=1 WHERE topic='replica' AND partition=0"]:
            with pytest.raises(psycopg.Error,match='bound partition|committed Raft application'):
                c.execute(sql)
        assert offset(c)==1
        assert c.execute('SELECT public.broker_raft_snapshot_begin(%s,%s)',('journal',epoch)).fetchone()[0]['end_offset']==1


@pytest.mark.parametrize('identity_database', ['0.1.15', '0.1.18'], indirect=True)
def test_old_repeatable_read_writer_cannot_miss_binding(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as admin:
        epoch,tid=prepare(admin)
        with psycopg.connect(identity_database) as old:
            old.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            assert offset(old)==0
            bind(admin,epoch,'replica',tid)
            with pytest.raises(psycopg.Error,match='concurrent update'):
                old.execute('SELECT public.broker_append_bytes(%s,0,%s)',('replica',make_batch()))
        assert offset(admin)==0


@pytest.mark.parametrize('identity_database', ['0.1.15', '0.1.18'], indirect=True)
def test_gateway_role_cannot_forge_apply_context(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch,tid=prepare(c);bind(c,epoch,'replica',tid)
        # Use PUBLIC ACL inspection rather than cluster-wide role creation.
        assert c.execute("SELECT has_table_privilege('public','broker.raft_apply_context','INSERT')").fetchone()[0] is False
        assert c.execute("SELECT has_function_privilege('public','broker.raft_partition_apply_inner(text,bigint,jsonb)','EXECUTE')").fetchone()[0] is False


@pytest.mark.parametrize('identity_database', ['0.1.14'], indirect=True)
def test_default_grants_cannot_expose_authorization_helpers(identity_database):
    import uuid
    from psycopg import sql
    role='ownership_'+uuid.uuid4().hex
    with psycopg.connect(identity_database,autocommit=True) as c:
        with c.transaction(force_rollback=True):
            c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('GRANT USAGE ON SCHEMA broker TO {}').format(sql.Identifier(role)))
            for privilege,kind in [('ALL','TABLES'),('EXECUTE','FUNCTIONS')]:
                c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES IN SCHEMA broker GRANT {} ON {} TO {}').format(
                    sql.SQL(privilege),sql.SQL(kind),sql.Identifier(role)))
            c.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_raft_partition_apply(text,bigint,jsonb) TO {}').format(sql.Identifier(role)))
            c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.15'")
            assert c.execute("SELECT has_function_privilege(%s,'public.broker_raft_partition_apply(text,bigint,jsonb)','EXECUTE')",(role,)).fetchone()[0]
            assert not c.execute("SELECT has_table_privilege(%s,'broker.raft_apply_context','INSERT')",(role,)).fetchone()[0]
            for signature in ['broker.raft_partition_apply_inner(text,bigint,jsonb)',
                              'broker.raft_partition_bind_unfenced(text,bigint,text,uuid,integer)']:
                assert not c.execute('SELECT has_function_privilege(%s,%s,\'EXECUTE\')',(role,signature)).fetchone()[0]
            c.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege),c.transaction():
                c.execute("INSERT INTO broker.raft_apply_context VALUES('forged',pg_current_xact_id())")


@pytest.mark.parametrize('identity_database', ['0.1.15', '0.1.18'], indirect=True)
def test_binding_waits_for_inflight_writer_then_rejects_populated_partition(identity_database):
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue
    import time
    with psycopg.connect(identity_database,autocommit=True) as admin:
        epoch,tid=prepare(admin)
        with psycopg.connect(identity_database) as writer:
            writer.execute('SELECT public.broker_append_bytes(%s,0,%s)',('replica',make_batch()))
            writer_pid=writer.info.backend_pid
            pid=Queue()
            def binding():
                with psycopg.connect(identity_database,autocommit=True) as other:
                    other.execute("SET statement_timeout='10s'")
                    pid.put(other.info.backend_pid)
                    return bind(other,epoch,'replica',tid)
            with ThreadPoolExecutor(max_workers=1) as pool:
                result=pool.submit(binding)
                try:
                    binding_pid=pid.get(timeout=5)
                    deadline=time.monotonic()+5
                    while not admin.execute('SELECT %s = ANY(pg_blocking_pids(%s))',(writer_pid,binding_pid)).fetchone()[0]:
                        assert time.monotonic()<deadline, 'binding never waited for the writer'
                        time.sleep(0.01)
                    writer.commit()
                    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState,match='empty'):
                        result.result(timeout=5)
                finally:
                    writer.rollback()
            assert offset(admin)==1
            assert admin.execute('SELECT count(*) FROM broker.raft_partition_state').fetchone()[0]==0


@pytest.mark.parametrize('identity_database', ['0.1.17', '0.1.18'], indirect=True)
def test_guard_avoids_redundant_partition_lock(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        prepare(c)
        version=c.execute("SELECT extversion FROM pg_extension WHERE extname='pg_broker'").fetchone()[0]
        definition=c.execute("SELECT pg_get_functiondef('broker.raft_require_application(text,integer)'::regprocedure)").fetchone()[0]
        c.execute(definition.replace('FUNCTION broker.raft_require_application(', 'FUNCTION broker.test_require_original('))
        c.execute('CREATE TABLE broker.test_require_calls(n integer)')
        c.execute("CREATE OR REPLACE FUNCTION broker.raft_require_application(t text,p integer) RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO broker.test_require_calls VALUES(1); PERFORM broker.test_require_original(t,p); END $$")
        assert c.execute('SELECT public.broker_append_bytes(%s,1,%s)',('replica',make_batch())).fetchone()[0]['offset']==0
        assert c.execute('SELECT count(*) FROM broker.test_require_calls').fetchone()[0]==(1 if version=='0.1.18' else 2)


@pytest.mark.parametrize('identity_database', ['0.1.18'], indirect=True)
@pytest.mark.parametrize('isolation,code', [('READ COMMITTED','55000'),('REPEATABLE READ','40001')])
def test_direct_update_waits_for_binding_and_is_fenced(identity_database,isolation,code):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from queue import Queue
    import time
    ready=Queue();release=Event()
    def writer():
        with psycopg.connect(identity_database) as c:
            c.execute('SET TRANSACTION ISOLATION LEVEL '+isolation)
            c.execute('SELECT next_offset FROM broker.partitions WHERE topic=%s AND partition=0',('replica',))
            ready.put(c.info.backend_pid)
            assert release.wait(5)
            try:
                c.execute("UPDATE broker.partitions SET earliest=1 WHERE topic='replica' AND partition=0")
            except psycopg.Error as error:
                return error.sqlstate
            return 'unexpected success'
    with psycopg.connect(identity_database,autocommit=True) as admin:
        epoch,tid=prepare(admin)
        with ThreadPoolExecutor(max_workers=1) as pool:
            task=pool.submit(writer)
            try:
                pid=ready.get(timeout=5)
                with admin.transaction():
                    bind(admin,epoch,'replica',tid)
                    release.set()
                    deadline=time.monotonic()+5
                    while time.monotonic()<deadline:
                        blockers=admin.execute('SELECT pg_blocking_pids(%s)',(pid,)).fetchone()[0]
                        if admin.info.backend_pid in blockers:break
                        time.sleep(.01)
                    else:pytest.fail('writer did not wait for binding transaction')
                assert task.result(timeout=5)==code
            finally:release.set()
        assert admin.execute("SELECT earliest,next_offset FROM broker.partitions WHERE topic='replica' AND partition=0").fetchone()==(0,0)

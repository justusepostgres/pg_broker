"""Opt-in bulk offset commits retain durable fencing and atomicity."""
import psycopg
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from conftest import Gateway
from test_wire import wire
from kafka.protocol.commit import OffsetCommitRequest


def request(group='bulk', partition=0, offset=1, **extra):
    return dict(op='commit',group=group,member='',generation=-1,topic='events',
                partition=partition,offset=offset,metadata='',**extra)


def bulk(c, requests):
    return c.execute('SELECT public.broker_commit_many(%s)',(Jsonb(dict(requests=requests)),)).fetchone()[0]


def setup(c):
    c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.17'")
    c.execute("SELECT public.broker_call('{\"op\":\"create\",\"topic\":\"events\",\"partitions\":3}')")


@pytest.mark.parametrize('identity_database',['0.1.16','0.1.17'],indirect=True)
def test_bulk_upgrade_order_errors_and_one_transaction(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        setup(c)
        entries=[request(partition=0,offset=2),request(partition=1,offset=3),
                 request(partition=99),request(partition=2,offset=-1),request(partition=0,offset=7)]
        assert bulk(c,entries)==[dict(error=n) for n in (0,0,3,1,0)]
        rows=c.execute('SELECT partition,committed,xmin::text FROM broker.offsets ORDER BY partition').fetchall()
        assert [(p,o) for p,o,_ in rows]==[(0,7),(1,3)]
        assert len({x for _,_,x in rows})==1
        with c.transaction():
            assert bulk(c,[request(partition=0,offset=99)])[0]['error']==0
            c.execute('ROLLBACK')
        assert c.execute('SELECT committed FROM broker.offsets WHERE partition=0').fetchone()[0]==7


@pytest.mark.parametrize('identity_database',['0.1.17'],indirect=True)
def test_bulk_database_failure_rolls_back_all_entries(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        setup(c)
        c.execute('ALTER TABLE broker.offsets ADD CONSTRAINT reject_thirteen CHECK (committed<>13)')
        with pytest.raises(psycopg.errors.CheckViolation):bulk(c,[request(offset=2),request(partition=1,offset=13)])
        assert c.execute('SELECT count(*) FROM broker.offsets').fetchone()[0]==0
        assert c.execute('SELECT count(*) FROM broker.groups').fetchone()[0]==0


@pytest.mark.parametrize('identity_database',['0.1.17'],indirect=True)
def test_bulk_validates_group_and_count_before_state_changes(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        setup(c)
        assert bulk(c,[])==[]
        for entries in ([request(),request(group='different')],[request()]*4097,None,[request(),None]):
            with pytest.raises(psycopg.Error):bulk(c,entries)
            assert c.execute('SELECT count(*) FROM broker.groups').fetchone()[0]==0


@pytest.mark.parametrize('identity_database',['0.1.17'],indirect=True)
def test_bulk_checks_expiration_once_and_preserves_private_acl(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        setup(c)
        # Instrument the private expiration helper only in this disposable DB.
        definition=c.execute("SELECT pg_get_functiondef('broker.expire_members(text)'::regprocedure)").fetchone()[0]
        c.execute(definition.replace('FUNCTION broker.expire_members(', 'FUNCTION broker.test_expire_original('))
        c.execute('CREATE TABLE broker.test_expiration_calls(n integer)')
        c.execute("CREATE OR REPLACE FUNCTION broker.expire_members(g text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO broker.test_expiration_calls VALUES(1); PERFORM broker.test_expire_original(g); END $$")
        assert bulk(c,[request(partition=p) for p in range(3)])==[dict(error=0)]*3
        assert c.execute('SELECT count(*) FROM broker.test_expiration_calls').fetchone()[0]==1
        c.execute('CREATE ROLE bulk_unprivileged')
        try:
            assert not c.execute("SELECT has_function_privilege('bulk_unprivileged','broker.commit_many(jsonb)','EXECUTE')").fetchone()[0]
            assert not c.execute("SELECT has_function_privilege('bulk_unprivileged','public.broker_commit_many(jsonb)','EXECUTE')").fetchone()[0]
        finally:
            c.execute('DROP ROLE bulk_unprivileged')


@pytest.mark.parametrize('identity_database',['0.1.7','0.1.17'],indirect=True)
def test_gateway_selects_bulk_capability_or_legacy_fallback(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        version=c.execute("SELECT extversion FROM pg_extension WHERE extname='pg_broker'").fetchone()[0]
        c.execute("SELECT public.broker_call('{\"op\":\"create\",\"topic\":\"events\",\"partitions\":3}')")
        definition=c.execute("SELECT pg_get_functiondef('broker.expire_members(text)'::regprocedure)").fetchone()[0]
        c.execute(definition.replace('FUNCTION broker.expire_members(', 'FUNCTION broker.test_expire_original('))
        c.execute('CREATE TABLE broker.test_expiration_calls(n integer)')
        c.execute("CREATE OR REPLACE FUNCTION broker.expire_members(g text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO broker.test_expiration_calls VALUES(1); PERFORM broker.test_expire_original(g); END $$")
        gateway=Gateway(dsn=identity_database,port=19093)
        try:
            gateway.start()
            result=wire(OffsetCommitRequest[2]('bulk',-1,'',-1,[('events',[(p,5,'') for p in range(3)])]),port=19093)
            assert result.topics==[('events',[(p,0) for p in range(3)])]
            assert c.execute('SELECT count(*) FROM broker.test_expiration_calls').fetchone()[0]==(1 if version=='0.1.17' else 3)
        finally:
            gateway.stop()
            gateway.log.close()

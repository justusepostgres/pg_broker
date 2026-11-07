"""Opt-in committed read pages; real PostgreSQL owner/visibility boundaries."""
import json
import uuid
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from test_raft_votes import claim
from test_raft_log import call, entry, log_id


def page(c, epoch, start, end):
    return c.execute('SELECT public.broker_raft_committed_page(%s,%s,%s)',
                     ('journal',epoch,Jsonb(dict(start=log_id(start),end=end)))).fetchone()[0]


@pytest.mark.parametrize('identity_database',['0.1.18','0.1.19'],indirect=True)
def test_upgrade_and_large_committed_jump(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.19'")
        epoch=claim(c,'journal')['epoch']
        for start in range(0,4097,256):call(c,epoch,'append',[entry(i) for i in range(start,min(start+256,4097))])
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(4096))
        call(c,epoch,'commit',log_id(4096))
        seen=[]
        while len(seen)<4097:
            rows=page(c,epoch,len(seen),log_id(4096))
            assert 0<len(rows)<=256
            seen.extend(rows)
        assert seen==[entry(i) for i in range(4097)]
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(4096,term=2))
        call(c,epoch,'purge',log_id(255))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(4096))
        assert page(c,epoch,256,log_id(4096))[0]==entry(256)
        claim(c,'journal')
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,256,log_id(4096))


@pytest.mark.parametrize('identity_database',['0.1.19'],indirect=True)
def test_page_byte_bound_and_committed_prefix_only(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch=claim(c,'journal')['epoch']
        entries=[dict(log_id=log_id(i),payload={'Normal':'x'*900000}) for i in range(12)]
        for row in entries:call(c,epoch,'append',[row])
        call(c,epoch,'commit',log_id(10))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(11))
        first=page(c,epoch,0,log_id(10))
        assert 0<len(first)<11 and len(json.dumps(first).encode())<=8*1024*1024
        assert first+page(c,epoch,len(first),log_id(10))==entries[:11]
        with pytest.raises(psycopg.errors.InvalidParameterValue):page(c,epoch,11,log_id(10))


@pytest.mark.parametrize('identity_database',['0.1.19'],indirect=True)
def test_owner_change_waits_for_page_transaction(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch=claim(c,'journal')['epoch'];call(c,epoch,'append',[entry(0)]);call(c,epoch,'commit',log_id(0))
        with psycopg.connect(identity_database) as held:
            assert page(held,epoch,0,log_id(0))==[entry(0)]
            c.execute("SET statement_timeout='200ms'")
            with pytest.raises(psycopg.errors.QueryCanceled):claim(c,'journal')
        claim(c,'journal')
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(0))


@pytest.mark.parametrize('identity_database',['0.1.18'],indirect=True)
def test_default_grants_are_removed_and_explicit_reader_cannot_write(identity_database):
    role='page_'+uuid.uuid4().hex
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch=claim(c,'journal')['epoch'];call(c,epoch,'append',[entry(0)]);call(c,epoch,'commit',log_id(0))
        c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {}').format(sql.Identifier(role)))
            c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.19'")
            c.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):page(c,epoch,0,log_id(0))
            c.execute('RESET ROLE')
            c.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_raft_committed_page(text,bigint,jsonb) TO {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            assert page(c,epoch,0,log_id(0))==[entry(0)]
            with pytest.raises(psycopg.errors.InsufficientPrivilege):call(c,epoch,'append',[entry(1)])
            with pytest.raises(psycopg.errors.InsufficientPrivilege):c.execute('SELECT * FROM broker.raft_log_entries')
        finally:
            c.execute('RESET ROLE')
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('identity_database',['0.1.19'],indirect=True)
@pytest.mark.parametrize('confirmed',[False,True])
def test_committed_page_boundary_survives_process_crash(identity_database,confirmed):
    import os
    import subprocess
    from pathlib import Path
    if os.getenv('PG_BROKER_TEST_ALLOW_CRASH')!='1':
        pytest.skip('requires explicitly disposable PostgreSQL')
    data=Path(os.environ['PG_BROKER_TEST_PGDATA']).resolve()
    assert data.name=='committed-pages-crash-native-pg'
    pg_ctl=os.environ['PG_BROKER_TEST_PG_CTL']
    with psycopg.connect(identity_database,autocommit=True) as c:
        epoch=claim(c,'journal')['epoch']
        for start in range(0,513,256):call(c,epoch,'append',[entry(i) for i in range(start,min(start+256,513))])
        call(c,epoch,'commit',log_id(255))
    pending=psycopg.connect(identity_database,autocommit=confirmed)
    try:
        call(pending,epoch,'commit',log_id(512))
        subprocess.run([pg_ctl,'-D',str(data),'-m','immediate','-w','stop'],check=True,capture_output=True)
    finally:pending.close()
    subprocess.run([pg_ctl,'-D',str(data),'-l',str(data.parent/'committed-pages-crash-restart.log'),
                    '-o','-p 55432 -h 127.0.0.1 -k /tmp','-w','start'],check=True,capture_output=True)
    with psycopg.connect(identity_database,autocommit=True) as c:
        successor=claim(c,'journal')['epoch']
        end=512 if confirmed else 255
        assert call(c,successor)['committed']==log_id(end)
        seen=[]
        while len(seen)<=end:seen.extend(page(c,successor,len(seen),log_id(end)))
        assert seen==[entry(i) for i in range(end+1)]
        if not confirmed:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,successor,256,log_id(512))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(end))

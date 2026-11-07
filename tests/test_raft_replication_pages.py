"""Atomic vote/entry reads for future bounded replication, not a live network."""
import json
import uuid
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest
from test_topic_identity import identity_database
from test_raft_votes import claim, save
from test_raft_log import call, entry, log_id


def vote(term=1, committed=True):
    return {'leader_id': {'term': term, 'node_id': 1}, 'committed': committed}


def page(c, epoch, start, end, expected=None):
    return c.execute('SELECT public.broker_raft_replication_page(%s,%s,%s)',
        ('journal', epoch, Jsonb(dict(start=log_id(start), end=end,
                                    vote=vote() if expected is None else expected)))).fetchone()[0]


@pytest.mark.parametrize('identity_database', ['0.1.19','0.1.20'], indirect=True)
def test_uncommitted_log_pages_and_vote_change(identity_database):
    with psycopg.connect(identity_database, autocommit=True) as c:
        c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.20'")
        owner=claim(c,'journal');epoch=owner['epoch']
        assert page(c,epoch,0,log_id(4096))=={'vote':None,'entries':[]}
        owner=save(c,'journal',owner,vote())
        for start in range(0,4097,256):
            call(c,epoch,'append',[entry(i) for i in range(start,min(start+256,4097))])
        seen=[]
        while len(seen)<4097:
            result=page(c,epoch,len(seen),log_id(4096))
            assert result['vote']==vote() and 0<len(result['entries'])<=256
            seen.extend(result['entries'])
        assert seen==[entry(i) for i in range(4097)]
        save(c,'journal',owner,vote(2,False))
        assert page(c,epoch,256,log_id(4096))=={'vote':vote(2,False),'entries':[]}
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            page(c,epoch,0,log_id(4096),vote(2,False))
        claim(c,'journal')
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            page(c,epoch,0,log_id(4096))


@pytest.mark.parametrize('identity_database',['0.1.20'],indirect=True)
def test_page_locks_vote_and_rejects_replaced_endpoint(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        owner=claim(c,'journal');owner=save(c,'journal',owner,vote());epoch=owner['epoch']
        call(c,epoch,'append',[entry(i) for i in range(4)])
        with psycopg.connect(identity_database) as held:
            assert len(page(held,epoch,0,log_id(3))['entries'])==4
            c.execute("SET statement_timeout='150ms'")
            with pytest.raises(psycopg.errors.QueryCanceled):
                save(c,'journal',owner,vote(2))
        call(c,epoch,'truncate',log_id(2))
        call(c,epoch,'append',[entry(2,term=2),entry(3,term=2)])
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(3))
        assert len(page(c,epoch,0,log_id(3,term=2))['entries'])==4


@pytest.mark.parametrize('identity_database',['0.1.20'],indirect=True)
def test_page_byte_bounds_and_purged_prefix(identity_database):
    with psycopg.connect(identity_database,autocommit=True) as c:
        owner=claim(c,'journal');save(c,'journal',owner,vote());epoch=owner['epoch']
        rows=[dict(log_id=log_id(i),payload={'Normal':'x'*900000}) for i in range(12)]
        for row in rows:call(c,epoch,'append',[row])
        first=page(c,epoch,0,log_id(11))['entries']
        assert 0<len(first)<12 and len(json.dumps(first).encode())<=8*1024*1024
        assert first+page(c,epoch,len(first),log_id(11))['entries']==rows
        call(c,epoch,'commit',log_id(11));call(c,epoch,'purge',log_id(0))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):page(c,epoch,0,log_id(11))
        with pytest.raises(psycopg.errors.InvalidParameterValue):page(c,epoch,11,log_id(10))


@pytest.mark.parametrize('identity_database',['0.1.19'],indirect=True)
def test_upgrade_removes_inherited_privileges(identity_database):
    role='replication_'+uuid.uuid4().hex
    with psycopg.connect(identity_database,autocommit=True) as c:
        c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {}').format(sql.Identifier(role)))
            c.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.20'")
            assert not c.execute("SELECT has_function_privilege(%s,'public.broker_raft_replication_page(text,bigint,jsonb)','EXECUTE')",(role,)).fetchone()[0]
        finally:
            c.execute(sql.SQL('ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))

"""Use the target client's own protocol encoders for fault injection and version coverage."""
import os
import socket
import struct
import subprocess
import time
from pathlib import Path
from kafka.protocol.api import RequestHeader
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.group import JoinGroupRequest, SyncGroupRequest, HeartbeatRequest, LeaveGroupRequest, ProtocolMetadata, MemberAssignment
from kafka.protocol.commit import OffsetCommitRequest, OffsetFetchRequest
from kafka.protocol.produce import ProduceRequest
import pytest
from test_broker import create, name, make_batch, db
from conftest import ROOT, DSN, batch_storage_relation


def encoded(obj):
    return obj.encode()

def wire(request, port=19092):
    header = RequestHeader(request, correlation_id=19)
    payload = header.encode() + request.encode()
    with socket.create_connection(('127.0.0.1', port), timeout=20) as sock:
        sock.sendall(struct.pack('>i', len(payload)) + payload)
        def read(n):
            data = b''
            while len(data) < n:
                block = sock.recv(n-len(data))
                assert block, 'unexpected EOF'
                data += block
            return data
        response = read(struct.unpack('>i', read(4))[0])
    assert struct.unpack('>i', response[:4])[0] == 19
    return request.RESPONSE_TYPE.decode(response[4:])

@pytest.mark.parametrize('version', range(5))
def test_metadata_versions(gateway, version):
    request = MetadataRequest[version](None, False) if version == 4 else MetadataRequest[version](None)
    assert wire(request).brokers[0][0] == 1

@pytest.mark.parametrize('version', range(3))
def test_group_versions_and_fencing(gateway, version):
    t, g = create(), name()
    meta = encoded(ProtocolMetadata(0, [t], b''))
    args = [g, 6000] + ([15000] if version else []) + ['', 'consumer', [('range', meta)]]
    joined = wire(JoinGroupRequest[version](*args))
    assert joined.error_code == 0
    gen, member = joined.generation_id, joined.member_id
    sv = int(version == 2)
    assignment = encoded(MemberAssignment(0, [(t, [0])], b''))
    sync = SyncGroupRequest[sv](g, gen, member, [(member, assignment)])
    assert wire(sync).error_code == 0
    assert wire(sync).error_code == 0  # retry after lost response
    assert wire(HeartbeatRequest[sv](g, gen, member)).error_code == 0
    assert wire(HeartbeatRequest[sv](g, gen-1, member)).error_code == 22
    stale = wire(OffsetCommitRequest[2](g, gen-1, member, -1, [(t, [(0, 2, '')])]))
    assert stale.topics[0][1][0][1] == 22
    good = wire(OffsetCommitRequest[2](g, gen, member, -1, [(t, [(0, 2, 'saved')])]))
    assert good.topics[0][1][0][1] == 0
    assert wire(LeaveGroupRequest[sv](g, member)).error_code == 0
    assert wire(HeartbeatRequest[sv](g, gen, member)).error_code == 25


def test_second_gateway_shared_coordinator(gateway):
    t, g = create(), name()
    meta = encoded(ProtocolMetadata(0, [t], b''))
    joined = wire(JoinGroupRequest[2](g, 6000, 15000, '', 'consumer', [('range', meta)]))
    member, gen = joined.member_id, joined.generation_id
    assignment = encoded(MemberAssignment(0, [(t, [0])], b''))
    assert wire(SyncGroupRequest[1](g, gen, member, [(member, assignment)])).error_code == 0
    with open(ROOT / '.dev/second-gateway.log', 'a') as log:
        env = dict(os.environ, DATABASE_URL=DSN, LISTEN_ADDR='127.0.0.1:19093', ADVERTISED_PORT='19092')
        proc = subprocess.Popen([str(ROOT / 'target/debug/pg-broker-gateway')], env=env, stdout=log, stderr=log)
        try:
            for _ in range(100):
                try:
                    with socket.create_connection(('127.0.0.1', 19093), .1): break
                except OSError: time.sleep(.05)
            assert wire(HeartbeatRequest[1](g, gen, member), 19093).error_code == 0
            assert wire(OffsetCommitRequest[2](g, gen, member, -1, [(t, [(0, 7, 'other gateway')])]), 19093).topics[0][1][0][1] == 0
            assert wire(OffsetFetchRequest[1](g, [(t, [0])])).topics[0][1][0][1] == 7
        finally:
            proc.kill(); proc.wait(5)


def test_offset_commit_batch_order_errors_and_transaction(gateway):
    import psycopg
    t, other, g = create(3), create(), name()
    response = wire(OffsetCommitRequest[2](g, -1, '', -1, [
        (t, [(0, 2, 'first'), (1, 3, 'second'), (99, 4, 'unknown'),
             (2, -1, 'invalid')]),
        (other, [(0, 5, 'other')]), (t, [(0, 7, 'last')]),
    ]))
    assert response.topics == [
        (t, [(0, 0), (1, 0), (99, 3), (2, 1)]),
        (other, [(0, 0)]), (t, [(0, 0)])]
    with psycopg.connect(DSN) as conn:
        rows = conn.execute('SELECT topic, partition, committed, metadata, xmin::text '
                            'FROM broker.offsets WHERE group_id=%s', (g,)).fetchall()
    assert {(r[0], r[1], r[2], r[3]) for r in rows} == {
        (t, 0, 7, 'last'), (t, 1, 3, 'second'), (other, 0, 5, 'other')}
    assert len({r[4] for r in rows}) == 1, 'one transaction per Kafka request'


def test_offset_commit_batch_enforces_each_partition_ownership_and_generation(gateway):
    import psycopg
    t, g = create(3), name()
    meta = encoded(ProtocolMetadata(0, [t], b''))
    joined = wire(JoinGroupRequest[2](g, 6000, 15000, '', 'consumer', [('range', meta)]))
    assert joined.error_code == 0
    member, generation = joined.member_id, joined.generation_id
    assignment = encoded(MemberAssignment(0, [(t, [0, 1])], b''))
    assert wire(SyncGroupRequest[1](g, generation, member, [(member, assignment)])).error_code == 0
    entries = [(0, 5, 'owned'), (2, 6, 'not-owned'), (1, 7, 'owned')]
    response = wire(OffsetCommitRequest[2](g, generation, member, -1, [(t, entries)]))
    assert response.topics == [(t, [(0, 0), (2, 27), (1, 0)])]
    replacement = [(p, 99, 'must-not-replace') for p in (0, 1, 2)]
    stale = wire(OffsetCommitRequest[2](g, generation - 1, member, -1, [(t, replacement)]))
    assert stale.topics == [(t, [(0, 22), (1, 22), (2, 22)])]
    assert wire(LeaveGroupRequest[1](g, member)).error_code == 0
    expired = wire(OffsetCommitRequest[2](g, generation, member, -1, [(t, replacement)]))
    assert expired.topics == [(t, [(0, 25), (1, 25), (2, 25)])]
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT partition, committed, metadata FROM broker.offsets '
                            'WHERE group_id=%s ORDER BY partition', (g,)).fetchall() == [
                                (0, 5, 'owned'), (1, 7, 'owned')]


@pytest.mark.parametrize('fault', ['limit', 'truncated', 'trailing', 'expanded'])
def test_invalid_offset_commit_has_no_committed_prefix(gateway, fault):
    import psycopg
    t, g = create(), name()
    if fault == 'expanded':
        g = name() + 'g' * 32000
    count = 4097 if fault == 'limit' else 300 if fault == 'expanded' else 2
    parts = [(0, 4, '')] * count
    request = OffsetCommitRequest[2](g, -1, '', -1, [(t, parts)])
    header = RequestHeader(request, correlation_id=19)
    payload = header.encode() + request.encode()
    if fault == 'truncated':
        payload = payload[:-1]
    elif fault == 'trailing':
        payload += b'x'
    with socket.create_connection(('127.0.0.1', 19092), timeout=20) as sock:
        sock.sendall(struct.pack('>i', len(payload)) + payload)
        assert sock.recv(1) == b''
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT count(*) FROM broker.offsets WHERE group_id=%s',
                            (g,)).fetchone()[0] == 0


def test_offset_commit_database_failure_rolls_back_whole_request(gateway):
    import psycopg
    from psycopg import sql
    t, g = create(2), name()
    function = sql.Identifier('broker', 'test_commit_' + g)
    trigger = sql.Identifier('test_commit_' + g)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS "
                             "$$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$").format(function))
        try:
            conn.execute(sql.SQL('CREATE TRIGGER {} BEFORE INSERT ON broker.offsets '
                                 'FOR EACH ROW WHEN (NEW.group_id={} AND NEW.partition=1) '
                                 'EXECUTE FUNCTION {}()').format(trigger, sql.Literal(g), function))
            with pytest.raises(AssertionError, match='unexpected EOF'):
                wire(OffsetCommitRequest[2](g, -1, '', -1, [(t, [(0, 2, ''), (1, 3, '')])]))
            assert conn.execute('SELECT count(*) FROM broker.offsets WHERE group_id=%s',
                                (g,)).fetchone()[0] == 0
        finally:
            conn.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON broker.offsets').format(trigger))
            conn.execute(sql.SQL('DROP FUNCTION {}()').format(function))


def test_wire_multibatch_and_bad_crc(gateway):
    t = create()
    request = ProduceRequest[3](None, -1, 10000, [(t, [(0, make_batch(b'a') + make_batch(b'b'))])])
    assert wire(request).topics[0][1][0][1:3] == (0, 0)
    bad = bytearray(make_batch()); bad[-1] ^= 1
    request = ProduceRequest[3](None, 1, 10000, [(t, [(0, bytes(bad))])])
    assert wire(request).topics[0][1][0][1] == 2
    assert db(dict(op='lookup', topic=t, partition=0, timestamp=-1))['offset'] == 2


def test_batched_produce_preserves_order_errors_and_other_request_progress(gateway):
    import concurrent.futures
    import psycopg
    from kafka import KafkaConsumer, TopicPartition
    from test_broker import records

    t = create(3)
    bad = bytearray(make_batch(b'bad')); bad[-1] ^= 1
    request = ProduceRequest[3](None, -1, 10000, [
        (t, [(0, make_batch(b'first')), (1, make_batch(b'independent')),
             (2, bytes(bad)), (99, make_batch(b'unknown'))]),
        (t, [(0, make_batch(b'second'))]),
    ])
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with psycopg.connect(DSN) as locked:
            locked.execute('SELECT 1 FROM broker.partitions WHERE topic=%s AND partition=0 FOR UPDATE', (t,))
            future = executor.submit(wire, request)
            # One blocked partition delays this request, while another request
            # targeting an unlocked partition can still make progress.
            other = wire(ProduceRequest[3](None, -1, 10000,
                         [(t, [(1, make_batch(b'other-request'))])]))
            assert other.topics[0][1][0][:3] == (1, 0, 0)
            assert not future.done(), 'response must await every partition commit'
        response = future.result(timeout=10)
    assert [row[:3] for row in response.topics[0][1]] == [
        (0, 0, 0), (1, 0, 1), (2, 2, -1), (99, 3, -1)]
    assert response.topics[1][1][0][:3] == (0, 0, 1)
    consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:19092',
                             enable_auto_commit=False, auto_offset_reset='earliest')
    try:
        consumer.assign([TopicPartition(t, 0)])
        assert [(r.offset, r.value) for r in records(consumer, 2)] == [(0, b'first'), (1, b'second')]
    finally:
        consumer.close()


def test_produce_partition_limit_rejects_before_writing(gateway):
    t = create()
    request = ProduceRequest[3](None, -1, 10000,
        [(t, [(0, make_batch(b'must-not-commit'))] + [(0, b'')] * 4096)])
    with pytest.raises(AssertionError, match='unexpected EOF'):
        wire(request)
    assert db(dict(op='lookup', topic=t, partition=0, timestamp=-1))['offset'] == 0


@pytest.mark.parametrize('acks', [-1, 0, 1])
@pytest.mark.parametrize('fault', ['trailing', 'truncated'])
def test_malformed_produce_frame_rejects_all_partitions_before_writing(gateway, acks, fault):
    t = create(2)
    request = ProduceRequest[3](None, acks, 10000, [(t, [
        (0, make_batch(b'first-must-not-commit')),
        (1, make_batch(b'second-must-not-commit'))])])
    header = RequestHeader(request, correlation_id=29)
    payload = header.encode() + request.encode()
    payload = payload + b'\x00' if fault == 'trailing' else payload[:-1]
    with socket.create_connection(('127.0.0.1', 19092), timeout=5) as sock:
        sock.sendall(struct.pack('>i', len(payload)) + payload)
        assert sock.recv(1) == b'', 'malformed frame must close connection'
    for partition in (0, 1):
        assert db(dict(op='lookup', topic=t, partition=partition, timestamp=-1))['offset'] == 0


def test_frame_bounds(gateway):
    for length in (-1, 8*1024*1024+1):
        with socket.create_connection(('127.0.0.1',19092), 2) as sock:
            sock.sendall(struct.pack('>i',length))
            assert sock.recv(1) == b''

@pytest.mark.parametrize('version', range(3))
def test_api_versions_classic(gateway, version):
    from kafka.protocol.admin import ApiVersionRequest
    response = wire(ApiVersionRequest[version]())
    assert response.error_code == 0
    assert (18, 0, 3) in response.api_versions
    assert (0, 3, 3) in response.api_versions


def test_multi_partition_fetch_shares_byte_budget(gateway):
    from kafka.protocol.fetch import FetchRequest
    t = create(4)
    batch = make_batch(b'x' * 1000)
    for partition in (1, 2, 3):
        assert db(dict(op='append', topic=t, partition=partition,
                       data=batch.hex()))['error'] == 0

    def fetch(limit):
        # Empty first partition and an unknown partition must not consume budget.
        request = FetchRequest[4](-1, 0, 0, limit, 0,
                                 [(t, [(p, 0, 1048576) for p in (0, 99, 1, 2, 3)])])
        return wire(request).topics[0][1]

    rows = fetch(len(batch) * 2)
    assert [row[0] for row in rows] == [0, 99, 1, 2, 3]
    assert [row[1] for row in rows] == [0, 3, 0, 0, 0]
    assert [len(row[-1]) for row in rows] == [0, 0, len(batch), len(batch), 0]
    assert [row[2] for row in rows if row[1] == 0] == [0, 1, 1, 1]
    # Kafka permits the first nonempty batch even when it exceeds the budget,
    # but must not let every subsequent partition use the same exception.
    rows = fetch(1)
    assert [len(row[-1]) for row in rows] == [0, 0, len(batch), 0, 0]
    assert wire(FetchRequest[4](-1, 0, 0, 100, 0, [])).topics == []


def test_sql_fetch_permissions_and_spi_fallback(gateway):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from psycopg.types.json import Jsonb
    from kafka.protocol.fetch import FetchRequest
    from conftest import Gateway
    t, role = create(), name()
    batch = make_batch(b'permission-safe')
    assert db(dict(op='append', topic=t, partition=0, data=batch.hex()))['offset'] == 0
    query = dict(op='fetch_many', requests=[dict(topic=t, partition=0, offset=0, max=1048576)], max=1048576)
    actor = Gateway(make_conninfo(DSN, user=role), 19098)
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        try:
            admin.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_call(jsonb) TO {}').format(sql.Identifier(role)))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with psycopg.connect(make_conninfo(DSN, user=role)) as conn:
                    conn.execute('SELECT public.broker_fetch_many(%s)', (Jsonb(query),))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with psycopg.connect(make_conninfo(DSN, user=role)) as conn:
                    conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (Jsonb(query),))
            actor.start()
            request = FetchRequest[4](-1, 0, 0, 1048576, 0, [(t, [(0, 0, 1048576)])])
            assert wire(request, 19098).topics[0][1][0][-1] == batch
            admin.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_fetch_many(jsonb) TO {}').format(sql.Identifier(role)))
            actor.restart()
            assert wire(request, 19098).topics[0][1][0][-1] == batch
            admin.execute(sql.SQL('GRANT EXECUTE ON FUNCTION public.broker_fetch_binary(jsonb) TO {}').format(sql.Identifier(role)))
            actor.restart()
            assert wire(request, 19098).topics[0][1][0][-1] == batch
            assert not admin.execute("SELECT has_function_privilege(%s, 'broker.fetch_many(jsonb)', 'EXECUTE')", (role,)).fetchone()[0]
            assert not admin.execute("SELECT has_function_privilege(%s, 'broker.dispatch(jsonb)', 'EXECUTE')", (role,)).fetchone()[0]
            # The SQL-only entry point must also ignore a supplied write opcode.
            query['requests'][0]['op'] = 'append'
            query['requests'][0]['data'] = batch.hex()
            with psycopg.connect(make_conninfo(DSN, user=role)) as conn:
                assert conn.execute('SELECT public.broker_fetch_many(%s)', (Jsonb(query),)).fetchone()[0][0]['high'] == 1
            assert db(dict(op='lookup', topic=t, partition=0, timestamp=-1))['offset'] == 1
        finally:
            actor.stop(); actor.log.close()
            admin.execute(sql.SQL('REVOKE EXECUTE ON FUNCTION public.broker_call(jsonb), public.broker_fetch_many(jsonb), public.broker_fetch_binary(jsonb) FROM {}').format(sql.Identifier(role)))
            admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def test_produce_batch_database_failure_rolls_back_every_partition(gateway):
    import psycopg
    from psycopg import sql
    t=create(2)
    function=sql.Identifier('broker','test_produce_'+t)
    trigger=sql.Identifier('test_produce_'+t)
    with psycopg.connect(DSN,autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected append failure'; END $$").format(function))
        try:
            conn.execute(sql.SQL('CREATE TRIGGER {} BEFORE INSERT ON {storage} FOR EACH ROW WHEN (NEW.topic={} AND NEW.partition=1) EXECUTE FUNCTION {}()').format(trigger,sql.Literal(t),function,storage=batch_storage_relation(conn)))
            with pytest.raises(AssertionError,match='unexpected EOF'):
                wire(ProduceRequest[3](None,-1,10000,[(t,[(0,make_batch(b'a')),(1,make_batch(b'b'))])]))
            assert conn.execute('SELECT next_offset FROM broker.partitions WHERE topic=%s ORDER BY partition',(t,)).fetchall()==[(0,),(0,)]
            assert conn.execute('SELECT count(*) FROM broker.batches WHERE topic=%s',(t,)).fetchone()[0]==0
        finally:
            conn.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON {storage}').format(trigger,storage=batch_storage_relation(conn)))
            conn.execute(sql.SQL('DROP FUNCTION {}()').format(function))


def test_produce_batch_uses_one_transaction_and_canonical_lock_order(gateway):
    import concurrent.futures
    import threading
    import psycopg
    from psycopg import sql
    t=create(3)
    table=sql.Identifier('broker','test_xids_'+t)
    function=sql.Identifier('broker','test_xids_'+t)
    trigger=sql.Identifier('test_xids_'+t)
    with psycopg.connect(DSN,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE TABLE {}(xid bigint,partition integer)').format(table))
        conn.execute(sql.SQL('CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO {} VALUES(txid_current(),NEW.partition); RETURN NEW; END $$').format(function,table))
        try:
            conn.execute(sql.SQL('CREATE TRIGGER {} AFTER INSERT ON {storage} FOR EACH ROW WHEN (NEW.topic={}) EXECUTE FUNCTION {}()').format(trigger,sql.Literal(t),function,storage=batch_storage_relation(conn)))
            barrier=threading.Barrier(2)
            def send(parts):
                barrier.wait(timeout=5)
                return wire(ProduceRequest[3](None,-1,10000,[(t,[(p,make_batch(b'parallel')) for p in parts])]))
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures=[executor.submit(send,parts) for parts in ([0,1,2],[2,1,0])]
                responses=[f.result(timeout=10) for f in futures]
            assert sorted({row[2] for row in response.topics[0][1]}.pop() for response in responses)==[0,1]
            for response in responses:
                assert len({row[2] for row in response.topics[0][1]})==1
                assert all(row[1]==0 for row in response.topics[0][1])
            assert conn.execute(sql.SQL('SELECT count(*),count(DISTINCT partition) FROM {} GROUP BY xid ORDER BY xid').format(table)).fetchall()==[(3,3),(3,3)]
        finally:
            conn.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON {storage}').format(trigger,storage=batch_storage_relation(conn)))
            conn.execute(sql.SQL('DROP FUNCTION {}()').format(function))
            conn.execute(sql.SQL('DROP TABLE {}').format(table))


def test_produce_expanded_topic_names_rejected_before_any_append(gateway):
    t=create()
    request=ProduceRequest[3](None,-1,10000,[(t,[(0,make_batch())]),('x'*32000,[(0,b'')]*300)])
    with pytest.raises(AssertionError,match='unexpected EOF'):
        wire(request)
    assert db(dict(op='lookup',topic=t,partition=0,timestamp=-1))['offset']==0

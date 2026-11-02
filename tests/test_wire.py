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
from conftest import ROOT, DSN


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


def test_wire_multibatch_and_bad_crc(gateway):
    t = create()
    request = ProduceRequest[3](None, -1, 10000, [(t, [(0, make_batch(b'a') + make_batch(b'b'))])])
    assert wire(request).topics[0][1][0][1:3] == (0, 0)
    bad = bytearray(make_batch()); bad[-1] ^= 1
    request = ProduceRequest[3](None, 1, 10000, [(t, [(0, bytes(bad))])])
    assert wire(request).topics[0][1][0][1] == 2
    assert db(dict(op='lookup', topic=t, partition=0, timestamp=-1))['offset'] == 2


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

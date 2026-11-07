"""Fault-inject the primary check, preserving real PostgreSQL transaction behavior."""
import select
import socket
import socketserver
import struct
import threading
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from conftest import DSN, Gateway
from test_broker import create, name
from test_wire import wire
from kafka.protocol.produce import ProduceRequest
from test_broker import make_batch


@pytest.mark.parametrize("failure", ["primary", "timeout_reset"])
def test_pipeline_failure_rolls_back_and_discards_failed_connection(gateway, failure):
    config=conninfo_to_dict(DSN)
    upstream=(config.get('host','127.0.0.1'),int(config.get('port',5432)))
    arm=threading.Event();injected=threading.Event();stopped=threading.Event()
    checked_connections=[];queries=[]
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            with socket.create_connection(upstream,3) as database:
                pending=b'';startup=True
                try:
                    while not stopped.is_set():
                        ready,_,_=select.select([self.request,database],[],[],.1)
                        for source in ready:
                            data=source.recv(65536)
                            if not data:return
                            if source is database:
                                self.request.sendall(data);continue
                            pending+=data
                            while True:
                                header=4 if startup else 5
                                if len(pending)<header:break
                                size=struct.unpack('>i',pending[0:4] if startup else pending[1:5])[0]+(0 if startup else 1)
                                assert header<=size<=16*1024*1024
                                if len(pending)<size:break
                                message,pending=pending[:size],pending[size:]
                                if startup:startup=False
                                elif message[:1]==b'Q':
                                    query=message[5:];queries.append(query)
                                    if b'pg_catalog.pg_is_in_recovery()' in query:
                                        assert query.startswith(b'SET LOCAL synchronous_commit=on;')
                                        checked_connections.append(self.client_address)
                                    target = (b'NOT pg_catalog.pg_is_in_recovery()' if failure == 'primary'
                                              else b'SET LOCAL statement_timeout=0')
                                    if arm.is_set() and target in query:
                                        arm.clear();injected.set()
                                        replacement = b'false' if failure == 'primary' else b'SELECT 1/0'
                                        query=query.replace(target,replacement)
                                        message=b'Q'+struct.pack('>i',len(query)+4)+query
                                database.sendall(message)
                except (OSError,ValueError):pass
    class Proxy(socketserver.ThreadingTCPServer):daemon_threads=True
    proxy=Proxy(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=proxy.serve_forever,daemon=True);thread.start()
    actor=Gateway(make_conninfo(DSN,host='127.0.0.1',port=proxy.server_address[1],sslmode='disable'),19096)
    topic=create()
    def produce():
        return wire(ProduceRequest[3](None,-1,5000,[(topic,[(0,make_batch())])]),port=19096)
    try:
        actor.start()
        assert produce().topics[0][1][0][1:3]==(0,0)
        previous=checked_connections[-1]
        arm.set()
        with pytest.raises(AssertionError,match='unexpected EOF'):produce()
        assert injected.is_set()
        with psycopg.connect(DSN) as c:
            assert c.execute('SELECT next_offset FROM broker.partitions WHERE topic=%s AND partition=0',(topic,)).fetchone()[0]==1
        assert produce().topics[0][1][0][1:3]==(0,1)
        assert checked_connections[-1]!=previous
        assert not any(q.startswith(b'SELECT 1 / (NOT pg_is_in_recovery())') for q in queries)
    finally:
        actor.stop();actor.log.close();stopped.set()
        proxy.shutdown();proxy.server_close();thread.join(3)

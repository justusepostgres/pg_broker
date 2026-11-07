"""Drop TCP traffic after BEGIN: server-side timeouts cannot help this case."""
import select
import socket
import socketserver
import struct
import threading
import time
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from conftest import DSN, Gateway


def test_database_blackhole_is_bounded_and_cancelled_connections_are_discarded(monkeypatch):
    config=conninfo_to_dict(DSN)
    upstream=(config.get('host','127.0.0.1'),int(config.get('port',5432)))
    arm=threading.Event(); blackhole=threading.Event(); stopped=threading.Event()
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            with socket.create_connection(upstream,3) as database:
                peers={self.request:database,database:self.request}
                tail=b''
                try:
                    while not stopped.is_set():
                        ready,_,_=select.select(list(peers),[],[],.1)
                        for source in ready:
                            data=source.recv(65536)
                            if not data: return
                            if source is self.request:
                                tail=(tail+data)[-65536:] if arm.is_set() else b''
                                if arm.is_set() and b'START TRANSACTION' in tail:
                                    arm.clear();blackhole.set()
                            if not blackhole.is_set(): peers[source].sendall(data)
                except OSError:
                    pass
    class Proxy(socketserver.ThreadingTCPServer):
        daemon_threads=True
    proxy=Proxy(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=proxy.serve_forever,daemon=True);thread.start()
    monkeypatch.setenv('HEALTH_ADDR','127.0.0.1:19097')
    gateway=Gateway(make_conninfo(DSN,host='127.0.0.1',port=proxy.server_address[1],sslmode='disable'),19096)
    def metadata(expect_closed=False):
        body=struct.pack('>hhihi',3,0,1,-1,0)
        with socket.create_connection(('127.0.0.1',19096),3) as client:
            client.settimeout(14)
            client.sendall(struct.pack('>i',len(body))+body)
            reply=client.recv(4)
            assert (reply==b'') if expect_closed else len(reply)==4
    def ready():
        with socket.create_connection(('127.0.0.1',19097),3) as client:
            client.settimeout(6)
            client.sendall(b'GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n')
            return client.recv(1024)
    try:
        gateway.start()
        arm.set();start=time.monotonic()
        metadata(expect_closed=True)
        assert blackhole.is_set()
        assert 9 <= time.monotonic()-start < 14
        blackhole.clear();metadata()
        # /ready cancels State.db at four seconds, before its own ten-second
        # deadline. The abandoned connection must still be physically discarded.
        arm.set()
        assert b'503 Service Unavailable' in ready()
        assert blackhole.is_set()
        blackhole.clear()
        assert b'200 OK' in ready()
        metadata()
    finally:
        gateway.stop();gateway.log.close();stopped.set()
        proxy.shutdown();proxy.server_close();thread.join(3)

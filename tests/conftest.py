import os
import socket
import subprocess
import time
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
DSN = os.getenv('DATABASE_URL', 'host=127.0.0.1 port=55432 user=postgres dbname=postgres')

class Gateway:
    def __init__(self):
        self.process = None
        self.log = open(ROOT / '.dev/test-gateway.log', 'a')
    def start(self):
        env = dict(os.environ, DATABASE_URL=DSN, LISTEN_ADDR='127.0.0.1:19092', ADVERTISED_PORT='19092')
        self.process = subprocess.Popen([str(ROOT / 'target/debug/pg-broker-gateway')], env=env, stdout=self.log, stderr=self.log)
        for _ in range(100):
            if self.process.poll() is not None:
                raise RuntimeError('gateway failed; see .dev/test-gateway.log')
            try:
                with socket.create_connection(('127.0.0.1', 19092), .1):
                    return
            except OSError:
                time.sleep(.05)
        raise RuntimeError('gateway start timeout')
    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
    def restart(self):
        self.stop()
        self.start()

@pytest.fixture(scope='session')
def gateway():
    g = Gateway()
    g.start()
    yield g
    g.stop()
    g.log.close()

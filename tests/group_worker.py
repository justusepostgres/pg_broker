"""Unmodified KafkaConsumer in a separate process, suitable for SIGKILL tests."""
import json
import os
import sys
from pathlib import Path
from kafka import KafkaConsumer

consumer = KafkaConsumer(sys.argv[1], bootstrap_servers='127.0.0.1:19092', group_id=sys.argv[2],
                         enable_auto_commit=False, auto_offset_reset='earliest',
                         session_timeout_ms=6000, heartbeat_interval_ms=1000,
                         max_poll_interval_ms=15000)
path = Path(sys.argv[3])
while True:
    consumer.poll(timeout_ms=200)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(sorted(p.partition for p in consumer.assignment())))
    os.replace(tmp, path)

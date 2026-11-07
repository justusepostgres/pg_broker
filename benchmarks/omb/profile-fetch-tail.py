#!/usr/bin/env python3
"""Local SQL diagnostic; requires a fresh disposable DB with extension 0.1.6.

Uses valid Kafka batch fixtures and compares the same store before/after upgrade.
This measures warm SQL calls, not Kafka latency, WAL durability or capacity.
"""
import argparse
import json
import statistics
import sys
import time
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tests'))
from test_broker import make_batch

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dsn', required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
if args.output.exists():
    parser.error('output exists; preserve earlier evidence')
result = {'scope': 'native warm SQL diagnostic, sequential before/after, not OMB', 'versions': {}}
with psycopg.connect(args.dsn, autocommit=True) as conn:
    assert conn.execute("SELECT extversion FROM pg_extension WHERE extname='pg_broker'").fetchone()[0] == '0.1.6'
    assert conn.execute('SELECT count(*) FROM broker.topics').fetchone()[0] == 0, 'requires empty disposable broker DB'
    result['postgres'] = conn.execute('SELECT version()').fetchone()[0]
    assert conn.execute("SELECT public.broker_call(%s)", (Jsonb(dict(op='create', topic='tail-profile', partitions=16)),)).fetchone()[0]['error'] == 0
    # Fixture rows are contiguous and contain valid one-record batches. Direct
    # inserts avoid measuring thousands of unrelated setup append transactions.
    with conn.transaction():
        conn.execute('''INSERT INTO broker.batches(topic,partition,base_offset,record_count,data,max_timestamp,timestamps)
          SELECT 'tail-profile',p,i,1,%s,1700000000000,'[1700000000000]'::jsonb
          FROM generate_series(0,15) p CROSS JOIN generate_series(0,999) i''', (make_batch(b'x' * 1024),))
        conn.execute("UPDATE broker.partitions SET next_offset=1000 WHERE topic='tail-profile'")
    conn.execute('ANALYZE broker.batches')
    conn.execute('ANALYZE broker.partitions')
    for version in ('0.1.6', '0.1.7'):
        if version == '0.1.7':
            conn.execute("ALTER EXTENSION pg_broker UPDATE TO '0.1.7'")
        measurements = {}
        for mode in ('all_caught_up', 'half_caught_up', 'all_have_data'):
            requests = []
            for partition in range(16):
                caught_up = mode == 'all_caught_up' or (mode == 'half_caught_up' and partition % 2 == 0)
                requests.append(dict(topic='tail-profile', partition=partition, offset=1000 if caught_up else 999))
            q = Jsonb(dict(requests=requests))
            times = []
            with conn.transaction():
                conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                for sample in range(110):
                    start = time.perf_counter_ns()
                    rows = conn.execute('SELECT * FROM public.broker_fetch_binary(%s)', (q,)).fetchall()
                    elapsed = (time.perf_counter_ns() - start) / 1e6
                    assert sum(row[5] is None for row in rows) == 16
                    assert all(row[1:4] == (0, 1000, 0) for row in rows)
                    expected_batches = {'all_caught_up': 0, 'half_caught_up': 8, 'all_have_data': 16}[mode]
                    assert len(rows) == 16 + expected_batches
                    if sample >= 10:
                        times.append(elapsed)
                plan = [row[0] for row in conn.execute('EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) SELECT * FROM public.broker_fetch_binary(%s)', (q,))]
            measurements[mode] = dict(samples_ms=times, median_ms=statistics.median(times), plan=plan)
        result['versions'][version] = measurements
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({v: {mode: m['median_ms'] for mode, m in modes.items()} for v, modes in result['versions'].items()}, indent=2))

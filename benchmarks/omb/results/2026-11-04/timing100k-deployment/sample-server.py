#!/usr/bin/env python3
"""Bounded diagnostic sampler; never starts/stops database or gateway containers."""
import argparse
import datetime
import json
from pathlib import Path
import re
import signal
import subprocess
import threading
import time

WAITS = """SELECT coalesce(json_agg(s),'[]'::json) FROM (
 SELECT backend_type,state,wait_event_type,wait_event,count(*) AS backends
 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
 GROUP BY backend_type,state,wait_event_type,wait_event) s"""
STATS = """SELECT json_build_object(
 'captured_at',clock_timestamp(),
 'io_timing',current_setting('track_io_timing'),
 'wal_io_timing',current_setting('track_wal_io_timing'),
 'track_functions',current_setting('track_functions'),
 'functions',(SELECT coalesce(json_agg(s),'[]'::json) FROM pg_stat_user_functions s
              WHERE schemaname IN ('public','broker')),
 'database',(SELECT row_to_json(s) FROM pg_stat_database s WHERE datname=current_database()),
 'wal',(SELECT row_to_json(s) FROM pg_stat_wal s),
 'io',(SELECT json_agg(s) FROM pg_stat_io s))"""


def command(args):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=8).stdout


def cgroup_path(text):
    rows = [line[3:] for line in text.splitlines() if line.startswith('0::/')]
    if len(rows) != 1 or '..' in Path(rows[0]).parts:
        raise ValueError('expected one unified absolute cgroup path')
    return Path('/sys/fs/cgroup') / rows[0].lstrip('/')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('container_prefix')
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'omb-pair-[a-z0-9-]{1,39}', args.container_prefix):
        parser.error('expected benchmark container prefix')
    out = args.output
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    db = args.container_prefix + '-db'
    def sql(query):
        return json.loads(command(['sudo', 'docker', 'exec', '-e',
            'PGOPTIONS=-c statement_timeout=1000 -c lock_timeout=500', db,
            'psql', '-X', '-U', 'postgres', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query]))
    groups = {}
    for suffix in ('db', 'gateway'):
        pid = int(command(['sudo','docker','inspect','--format','{{.State.Pid}}',
                           args.container_prefix + '-' + suffix]).strip())
        if pid <= 0:
            raise RuntimeError('container is not running')
        groups[suffix] = cgroup_path(Path(f'/proc/{pid}/cgroup').read_text())
    (out/'diagnostic-stats-before.json').write_text(json.dumps(sql(STATS), indent=2)+'\n')
    count = 0
    errors = 0
    with (out/'pidstat.txt').open('x') as log, (out/'samples.jsonl').open('x') as samples:
        proc = subprocess.Popen(['pidstat','-u','-r','-d','-p','ALL','2','550'],
                                stdout=log,stderr=subprocess.STDOUT)
        try:
            for _ in range(550):
                begin = time.monotonic()
                sample = {'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
                try:
                    sample['waits'] = sql(WAITS)
                    sample['host_proc_stat'] = Path('/proc/stat').read_text()
                    sample['cgroups'] = {name: {file: (path/file).read_text()
                        for file in ('cpu.stat','memory.current','io.stat')}
                        for name,path in groups.items()}
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    # Preserve gaps; never interpret failed samples as zero load.
                    sample['error'] = type(error).__name__ + ': ' + str(error)
                    errors += 1
                sample['elapsed_seconds'] = time.monotonic()-begin
                samples.write(json.dumps(sample)+'\n');samples.flush()
                count += 1
                if stop.wait(max(0, 2-(time.monotonic()-begin))):
                    break
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill();proc.wait(timeout=5)
    (out/'diagnostic-stats-after.json').write_text(json.dumps(sql(STATS), indent=2)+'\n')
    (out/'sampling-complete.json').write_text(json.dumps({
        'samples':count,'sample_errors':errors,'pidstat_exit':proc.returncode,
        'acceptance_run':False,'scope':'Instrumented diagnosis; timing disabled values are not latency evidence.'
    },indent=2)+'\n')


if __name__ == '__main__':
    main()

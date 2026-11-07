#!/usr/bin/env python3
"""Compare completed diagnostic WAL on/off runs, including crash evidence."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

from compare import driver_config, workload_number
from summarize import summarize


def require(value, message):
    if not value:
        raise ValueError(message)


def inspect(directory, mode):
    p = Path(directory)
    require(bool((p / 'complete.txt').read_text().strip()), 'missing completion marker')
    require(json.loads((p / 'storage-experiment.json').read_text()) ==
            {'wal_init_zero': mode, 'restart_mode': 'crash'}, 'unexpected experiment mode')
    instrumentation = json.loads((p / 'instrumentation.json').read_text())
    require(instrumentation.get('acceptance_run') is False, 'missing diagnostic label')
    require((p / 'wal-settings.txt').read_text().strip() ==
            f'{mode}|on|16MB|fdatasync|on|on|on', 'WAL/durability settings differ')
    require((p / 'storage.txt').read_text().splitlines() ==
            ['0.1.7|on|on|on|t|t', '16|t'], 'storage/restart checks differ')
    before, after = ((p / f'offsets-{stage}.txt').read_text() for stage in ('before', 'after'))
    require(before == after, 'partition endpoints changed after crash')
    rows = [line.split('|') for line in before.splitlines()]
    require(len(rows) == 16 and all(len(row) == 4 for row in rows), 'missing partition endpoints')
    topics = {row[0] for row in rows}
    require(len(topics) == 1 and {int(row[1]) for row in rows} == set(range(16)) and
            all(int(row[2]) == 0 and int(row[3]) > 0 for row in rows), 'invalid endpoints')
    n, valid = (p / 'compression.txt').read_text().strip().split('|')
    require(int(n) > 0 and valid == 't', 'LZ4 check failed')
    recovery = (p / 'postgres-recovery.log').read_text()
    require(all(message in recovery for message in
                ('database system was interrupted', 'redo starts at', 'redo done at',
                 'database system is ready to accept connections')), 'missing crash redo evidence')
    limits = [line.split() for line in (p / 'broker-limits.txt').read_text().splitlines()]
    require(len(limits) == 2 and all(len(row) == 3 for row in limits), 'missing resource limits')
    for suffix, memory in (('-db', 8 * 1024**3), ('-gateway', 1024**3)):
        selected = [row for row in limits if row[0].endswith(suffix)]
        require(len(selected) == 1 and selected[0][1:] == ['0,1', str(memory)], 'resource limits differ')
    checked_sql = set()
    for line in (p / 'sql-checksums.txt').read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        name = Path(name).name
        require(name not in checked_sql, 'duplicate SQL provenance')
        checked_sql.add(name)
        require(hashlib.sha256((p / name).read_bytes()).hexdigest() == digest,
                'SQL provenance mismatch')
    require(checked_sql == {'server-profile.sql', 'server-profile-stats.sql', 'verify-storage.sql',
                            'pg_broker--0.1.5--0.1.6.sql', 'pg_broker--0.1.6--0.1.7.sql'},
            'missing SQL provenance')
    result = summarize(p / 'broker.json')
    workload = (p / 'broker.json.workload.yaml').read_text()
    require(result['samples'] == workload_number(workload, 'testDurationMinutes') * 6,
            'incomplete result')
    log = (p / 'broker.json.log').read_text()
    marker = list(re.finditer(r'(?m)^(\d{2}:\d{2}:\d{2}\.\d{3}) .*Starting benchmark traffic .*$', log))
    require(len(marker) == 1, 'missing unique measurement start')
    ends = re.findall(r'(?m)^(\d{2}:\d{2}:\d{2}\.\d{3}) .*WorkloadGenerator - Pub rate ',
                      log[marker[0].end():])
    require(len(ends) == result['samples'], 'measurement log/result sample counts differ')
    # OMB log lines omit dates; anchor their UTC clocks to the client JVM logs,
    # rather than the completion marker (cleanup can cross midnight).
    observed = []
    for segment in p.glob('client-*.log*'):
        first = re.search(r'^\[([^]]+)\]', segment.read_text())
        require(first is not None, 'missing client JVM timestamp')
        stamp = datetime.fromisoformat(first[1])
        require(stamp.tzinfo is not None, 'missing client JVM timezone')
        observed.append(stamp.astimezone(timezone.utc))
    require(observed, 'missing client JVM logs for measurement date')
    first_observed = min(observed)
    day = first_observed.date().isoformat()
    start = datetime.fromisoformat(f'{day}T{marker[0][1]}+00:00')
    if start < first_observed:
        start += timedelta(days=1)
    end = start
    for clock in ends:
        current = datetime.fromisoformat(f'{end.date().isoformat()}T{clock}+00:00')
        if current < end:
            current += timedelta(days=1)
        require(current > end and (current - start).total_seconds() < 1500,
                'invalid or out-of-order measurement timestamps')
        end = current
    completed = datetime.fromisoformat((p / 'complete.txt').read_text().strip())
    require(completed.tzinfo is not None, 'missing completion timezone')
    require(start < end <= completed and (end - first_observed).total_seconds() < 1500,
            'measurement dates disagree with JVM/completion evidence')
    slow = []
    for line in (p / 'postgres.log').read_text().splitlines():
        match = re.match(r'^(.*?) UTC .*duration: ([0-9.]+) ms\s+(.*)', line)
        if match:
            timestamp = datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc)
            if start <= timestamp <= end:
                slow.append(dict(completed_at=timestamp.isoformat(), duration_ms=float(match[2]),
                                 statement=match[3]))
    result.update(measured_slow_statements=slow,
                  measured_slow_commit_count=sum(row['statement'] == 'statement: COMMIT' for row in slow),
                  client_warning_error_matches=len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b', log)))
    same_files = ('runner.sh', 'source-hashes.json', 'image.txt', 'binaries.txt', 'settings.txt',
                  'host.txt', 'java.txt', 'volume-profile.json', 'server-profile.sql',
                  'server-profile-stats.sql', 'verify-storage.sql',
                  'pg_broker--0.1.5--0.1.6.sql', 'pg_broker--0.1.6--0.1.7.sql',
                  'broker.json.revision', 'instrumentation.json')
    contract = {name: (p / name).read_text() for name in same_files}
    contract.update(workload=workload, driver=driver_config(p / 'broker.json.driver.yaml'))
    return result, contract, topics, {row[0] for row in limits}


def compare(on_directory, off_directory):
    require(Path(on_directory).resolve() != Path(off_directory).resolve(), 'duplicate directory')
    on, a, topics_a, stores_a = inspect(on_directory, 'on')
    off, b, topics_b, stores_b = inspect(off_directory, 'off')
    require(a == b, 'configuration/provenance differs beyond WAL initialization')
    require(not topics_a & topics_b and not stores_a & stores_b, 'fresh stores/topics required')
    return dict(on=on, off=off, scope='Single instrumented fresh-store diagnostic pair. '
                'Captured configuration and PostgreSQL process-crash redo/endpoints verified. '
                'No maximum-capacity, host-loss, RF=3, repeatability or production-default acceptance.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('on_directory')
    parser.add_argument('off_directory')
    args = parser.parse_args()
    print(json.dumps(compare(args.on_directory, args.off_directory), indent=2))

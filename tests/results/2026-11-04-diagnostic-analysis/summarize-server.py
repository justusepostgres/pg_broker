#!/usr/bin/env python3
"""Summarize diagnostic samples within an explicit UTC measurement window."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path


def timestamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('timezone is required')
    return result


def database_delta(before, after):
    """Whole diagnostic lifetime counters, including warmup and sampler reads."""
    seconds = (timestamp(after['captured_at'])-timestamp(before['captured_at'])).total_seconds()
    if seconds <= 0:
        raise ValueError('invalid database snapshot interval')
    def delta(section, key):
        a, b = before[section], after[section]
        if a['stats_reset'] != b['stats_reset']:
            raise ValueError('database statistics reset')
        result = b[key]-a[key]
        if result < 0:
            raise ValueError('database counter decreased')
        return result
    wal = {key:delta('wal',key) for key in (
        'wal_records','wal_fpi','wal_bytes','wal_buffers_full','wal_write','wal_sync',
        'wal_write_time','wal_sync_time')}
    database = {key:delta('database',key) for key in (
        'xact_commit','xact_rollback','blks_read','blks_hit','tup_inserted','tup_updated','tup_deleted')}
    timing = before['wal_io_timing'] == after['wal_io_timing'] == 'on'
    return dict(seconds=seconds,wal=wal,database=database,wal_timing_enabled=timing,
        wal_sync_mean_ms=wal['wal_sync_time']/wal['wal_sync'] if timing and wal['wal_sync'] else None,
        wal_write_mean_ms=wal['wal_write_time']/wal['wal_write'] if timing and wal['wal_write'] else None,
        wal_bytes_per_second=wal['wal_bytes']/seconds,
        scope='Whole diagnostic lifetime, including warmup and sampler transactions. '
              'Database commits include read-only work; they are not Kafka Produce request counts. '
              'Disabled WAL timing is missing latency evidence, not zero latency.')


def summarize(path, start, end):
    start, end = timestamp(start), timestamp(end)
    if end <= start:
        raise ValueError('invalid measurement window')
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    selected = [(timestamp(row['at']), row) for row in rows
                if start <= timestamp(row['at']) <= end]
    if any(b[0] <= a[0] for a,b in zip(selected, selected[1:])):
        raise ValueError('sample times must increase')
    errors = sum('error' in row for _,row in selected)
    waits = Counter()
    memory = Counter()
    cpu = Counter()
    covered = 0.0
    intervals = 0
    for _,row in selected:
        if 'error' in row:
            continue
        for wait in row['waits']:
            if wait['backend_type'] == 'client backend' and wait['state'] == 'active':
                label = (wait['wait_event_type'] or 'no_reported_wait') + ':' + (wait['wait_event'] or '')
                waits[label] += wait['backends']
        for name,group in row['cgroups'].items():
            value = int(group['memory.current'])
            if value < 0:
                raise ValueError('negative memory counter')
            memory[name] = max(memory[name],value)
    for (a,first),(b,last) in zip(selected,selected[1:]):
        # Do not bridge missing samples or silently treat a reset as zero usage.
        if 'error' in first or 'error' in last:
            continue
        if first['cgroups'].keys() != last['cgroups'].keys():
            raise ValueError('container set changed')
        for name in first['cgroups']:
            def counters(row):
                return {key:int(value) for key,value in
                        (line.split() for line in row['cgroups'][name]['cpu.stat'].splitlines())}
            before,after = counters(first),counters(last)
            for key in ('usage_usec','user_usec','system_usec','throttled_usec'):
                delta = after[key]-before[key]
                if delta < 0:
                    raise ValueError('CPU counter reset')
                cpu[name+':'+key] += delta
        covered += (b-a).total_seconds()
        intervals += 1
    return dict(samples=len(selected),sample_errors=errors,valid_intervals=intervals,
                covered_seconds=covered,requested_seconds=(end-start).total_seconds(),
                mean_cpu_cores={key:value/1e6/covered for key,value in cpu.items()
                                if key.endswith('usage_usec')} if covered else {},
                cpu_counter_deltas=dict(cpu),max_memory_bytes=dict(memory),
                active_backend_observations=dict(waits),
                scope='Instrumented samples, not acceptance. Wait counts are observations, not elapsed wait time; '
                      'no_reported_wait does not prove CPU execution. CPU cores are usage-seconds per covered wall-second.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('samples')
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--stats-before', type=Path)
    parser.add_argument('--stats-after', type=Path)
    args = parser.parse_args()
    if bool(args.stats_before) != bool(args.stats_after):
        parser.error('both statistics snapshots are required')
    result = summarize(args.samples,args.start,args.end)
    if args.stats_before:
        result['database_lifetime'] = database_delta(json.loads(args.stats_before.read_text()),
                                                     json.loads(args.stats_after.read_text()))
    print(json.dumps(result,indent=2))

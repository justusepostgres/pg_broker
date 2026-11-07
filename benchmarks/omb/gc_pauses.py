#!/usr/bin/env python3
"""Extract Java 17 GC/safepoint pauses; temporal overlap is not causation."""
import argparse
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import re

LINE = re.compile(r'^\[([^]]+)\]\[[^]]+\]\[[^]]+\]\[([^]]+)\] (.*)$')


def instant(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('timestamps must include a timezone')
    return result


def analyze(paths, since, until):
    start, end = instant(since), instant(until)
    if start >= end:
        raise ValueError('measurement bounds must be increasing')
    seen, events, observed, sources = set(), [], [], []
    for name in paths:
        path = Path(name).resolve()
        if path in seen:
            raise ValueError('duplicate log path')
        seen.add(path)
        text = path.read_text()
        if not text or not text.endswith('\n'):
            raise ValueError(f'{path}: empty or incomplete log segment')
        sources.append(str(path))
        for number, line in enumerate(text.splitlines(), 1):
            match = LINE.fullmatch(line)
            if not match:
                raise ValueError(f'{path}:{number}: unexpected log format')
            stamp, tags, message = match.groups()
            timestamp = instant(stamp)
            observed.append(timestamp)
            kind, duration = None, None
            if tags.strip() == 'gc' and ' Pause ' in message:
                value = re.search(r' ([0-9.]+)ms$', message)
                if value:
                    kind, duration = 'gc', float(value[1])
                else:
                    raise ValueError(f'{path}:{number}: missing GC pause duration')
            elif tags.strip() == 'safepoint' and message.startswith('Safepoint '):
                value = re.search(r' Total: ([0-9]+) ns$', message)
                if not value:
                    raise ValueError(f'{path}:{number}: missing safepoint duration')
                kind, duration = 'safepoint', int(value[1]) / 1_000_000
            if kind:
                if not math.isfinite(duration) or duration < 0:
                    raise ValueError('invalid pause duration')
                # Java reports these at completion. An event crossing a boundary
                # is included if its completion timestamp falls in this interval.
                if start <= timestamp < end:
                    events.append(dict(completed_at=timestamp.isoformat(), kind=kind,
                                       duration_ms=duration, source=str(path), line=number))
    if not observed:
        raise ValueError('no log observations')
    events.sort(key=lambda event: instant(event['completed_at']))
    return dict(
        scope='Diagnostic only. GC and safepoint events overlap; do not add their durations. '
              'Observed timestamps do not prove complete coverage; retain all rotated segments. '
              'No causal attribution or performance acceptance.',
        since=start.isoformat(), until=end.isoformat(), sources=sources,
        first_observed=min(observed).isoformat(), last_observed=max(observed).isoformat(),
        counts={kind: sum(e['kind'] == kind for e in events) for kind in ('gc', 'safepoint')},
        max_ms={kind: max((e['duration_ms'] for e in events if e['kind'] == kind), default=None)
                for kind in ('gc', 'safepoint')}, events=events)


def with_windows(report, result_path, log_path):
    """Coarse log-window alignment, not exact message/GC causal attribution."""
    data = json.loads(Path(result_path).read_text())
    log = Path(log_path).read_text()
    markers = list(re.finditer(
        r'(?m)^(\d{2}:\d{2}:\d{2}\.\d{3}) .*Starting benchmark traffic .*$', log))
    start, end = instant(report['since']), instant(report['until'])
    if len(markers) != 1 or markers[0][1] != start.strftime('%H:%M:%S.%f')[:-3]:
        raise ValueError('measurement start must match the unique OMB start marker')
    times = re.findall(r'(?m)^(\d{2}:\d{2}:\d{2}\.\d{3}) .*WorkloadGenerator - Pub rate ',
                       log[markers[0].end():])
    fields = ('publishLatency99pct', 'publishLatencyMax', 'endToEndLatency99pct',
              'endToEndLatencyMax', 'backlog')
    if not times or any(len(data[field]) != len(times) for field in fields):
        raise ValueError('OMB sample and measured window counts differ')
    windows, previous = [], start
    for index, clock in enumerate(times):
        current = datetime.combine(previous.date(), datetime.strptime(clock, '%H:%M:%S.%f').time(),
                                   previous.tzinfo)
        if current < previous:
            current += timedelta(days=1)
        if not previous < current <= end:
            raise ValueError('invalid or out-of-order measured window')
        selected = [event for event in report['events']
                    if previous <= instant(event['completed_at']) < current]
        windows.append(dict(sample=index + 1, since=previous.isoformat(), until=current.isoformat(),
                            omb={field: data[field][index] for field in fields},
                            pause_count={kind: sum(e['kind'] == kind for e in selected)
                                         for kind in ('gc', 'safepoint')},
                            pause_max_ms={kind: max((e['duration_ms'] for e in selected
                                                   if e['kind'] == kind), default=None)
                                          for kind in ('gc', 'safepoint')}))
        previous = current
    if previous != end:
        raise ValueError('measurement end must match the final OMB window timestamp')
    return dict(report, windows=windows,
                alignment='Approximate OMB log-time windows. Completion-time matching only; '
                          'not per-record timing, pause attribution or coverage proof.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--since', required=True, help='Inclusive ISO timestamp with timezone')
    parser.add_argument('--until', required=True, help='Exclusive ISO timestamp with timezone')
    parser.add_argument('--omb-json', help='Completed OMB result, paired with --omb-log')
    parser.add_argument('--omb-log', help='Matching OMB client log, paired with --omb-json')
    parser.add_argument('logs', nargs='+', help='Every retained segment for one client JVM')
    args = parser.parse_args()
    if bool(args.omb_json) != bool(args.omb_log):
        parser.error('--omb-json and --omb-log must be supplied together')
    report = analyze(args.logs, args.since, args.until)
    if args.omb_json:
        report = with_windows(report, args.omb_json, args.omb_log)
    print(json.dumps(report, indent=2))

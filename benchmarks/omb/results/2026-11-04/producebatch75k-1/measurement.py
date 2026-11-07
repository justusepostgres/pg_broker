"""Validate timed OMB completion without assuming exactly six samples/minute."""
import re


def verify_measurement(data, log, minutes, require_timing=False):
    starts = re.findall(r'Starting benchmark traffic \((\d+)m\)', log)
    count = len(data['publishRate'])
    if not starts and not require_timing:
        if count != minutes * 6:
            raise ValueError('incomplete measurement samples without timing evidence')
        return {'timing_verified': False, 'samples': count}
    if starts != [str(minutes)]:
        raise ValueError('missing or ambiguous measurement start')
    start = None
    samples = []
    aggregate = None
    written = False
    day = 0
    previous = None
    for line in log.splitlines():
        relevant = any(x in line for x in ('Starting benchmark traffic (',
                       'Pub rate ', '----- Aggregated Pub Latency', 'Writing test result into'))
        if not relevant:
            continue
        match = re.match(r'^(\d{2}):(\d{2}):(\d{2})\.(\d{3})\s', line)
        if not match:
            raise ValueError('measurement event missing timestamp')
        h, m, s, ms = map(int, match.groups())
        if h > 23 or m > 59 or s > 59:
            raise ValueError('invalid measurement timestamp')
        seconds = h * 3600 + m * 60 + s + ms / 1000
        if previous is not None and seconds < previous:
            if previous - seconds < 12 * 3600 or day:
                raise ValueError('measurement clock moved backwards')
            day = 86400
        previous = seconds
        timestamp = day + seconds
        if 'Starting benchmark traffic (' in line:
            start = timestamp
        elif start is not None and 'Pub rate ' in line:
            if aggregate is not None:
                raise ValueError('measurement samples after aggregate')
            samples.append(timestamp)
        elif start is not None and '----- Aggregated Pub Latency' in line:
            if aggregate is not None:
                raise ValueError('duplicate measurement aggregate')
            aggregate = timestamp
        elif start is not None and 'Writing test result into' in line:
            written = aggregate is not None and timestamp >= aggregate
    if len(samples) != count or count < 2 or aggregate is None or not written:
        raise ValueError('incomplete measurement events or sample count mismatch')
    duration = samples[-1] - start
    if duration < minutes * 60 - .01 or aggregate < samples[-1]:
        raise ValueError('measurement ended before requested duration')
    intervals = [b - a for a, b in zip([start] + samples[:-1], samples)]
    if min(intervals) <= 0:
        raise ValueError('non-increasing measurement samples')
    return {'timing_verified': True, 'samples': count,
            'requested_seconds': minutes * 60, 'measured_seconds': duration,
            'shortest_interval_seconds': min(intervals), 'longest_interval_seconds': max(intervals)}

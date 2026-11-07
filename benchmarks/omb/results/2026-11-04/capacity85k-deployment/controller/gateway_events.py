"""Classify retained gateway diagnostics without equating silence with success."""
import re
from pathlib import Path

CONNECTION = re.compile(r'^(?:unix_ms=([0-9]+) )?connection: (.*)$')


def gateway_events(path):
    path = Path(path)
    if not path.is_file():
        return {'recorded': False}
    counts = dict(connection_events=0, frame_header_deadlines=0,
                  frame_payload_deadlines=0, request_deadlines=0,
                  response_write_deadlines=0, database_deadlines=0,
                  unclassified_deadlines=0, other_connection_events=0,
                  other_diagnostic_lines=0)
    categories = (
        ('frame_header_deadlines', r'^frame-length read exceeded 60s \(idle or incomplete header\): deadline has elapsed$'),
        ('frame_payload_deadlines', r'^frame-payload read exceeded 10s: deadline has elapsed$'),
        ('request_deadlines', r'^request exceeded 310s: api=-?\d+ version=-?\d+ correlation=-?\d+: deadline has elapsed$'),
        ('response_write_deadlines', r'^response write exceeded 10s: api=-?\d+ version=-?\d+ correlation=-?\d+: deadline has elapsed$'),
        ('database_deadlines', r'^(?:database transaction timed out; outcome unknown|commit confirmation timed out; outcome unknown): deadline has elapsed$'),
    )
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith('pg-broker listening on '):
            continue
        event = CONNECTION.fullmatch(line)
        if not event:
            counts['other_diagnostic_lines'] += 1
            continue
        counts['connection_events'] += 1
        message = event.group(2)
        # The connection loop wraps handler failures with Kafka request context.
        # Strip only this known wrapper, never arbitrary nested context.
        message = re.sub(r'^request failed: api=-?\d+ version=-?\d+ correlation=-?\d+: ', '', message, count=1)
        for category, pattern in categories:
            if re.fullmatch(pattern, message):
                counts[category] += 1
                break
        else:
            counts['unclassified_deadlines' if 'deadline has elapsed' in message
                   else 'other_connection_events'] += 1
    return dict(recorded=True, **counts,
                scope='Entire retained gateway log, including warmup/shutdown. '
                      'Header deadlines may be idle or incomplete requests; their cause is unknown. '
                      'Counts do not prove loss, durability, or absence of unlogged failures.')

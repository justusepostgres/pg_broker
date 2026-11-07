"""Count client log events without treating stack-trace lines as new events.

Counts cover the entire retained client log, including warmup and shutdown.
They are diagnostics, not distinct lost records or a measurement-only error rate.
"""
import re

EVENT = re.compile(r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+\[[^\r\n]*?\]\s+(INFO|WARN|ERROR)\s+([^\r\n]*?)\s+-\s+(.*)$')


def client_events(log):
    counts = dict(warning_events=0, error_events=0, offset_commit_failure_events=0,
                  invalid_fetch_response_events=0)
    for line in log.splitlines():
        match = EVENT.match(line)
        if not match:
            continue
        level, logger, message = match.groups()
        if level in ('WARN', 'ERROR'):
            counts['warning_events' if level == 'WARN' else 'error_events'] += 1
        if (logger.strip().endswith('FetchSessionHandler')
                and re.search(r'\bsent an invalid (?:full|incremental) fetch response\b', message)):
            counts['invalid_fetch_response_events'] += 1
        if (level == 'ERROR' and logger.strip().endswith('ConsumerCoordinator')
                and 'Offset commit with offsets ' in message and message.endswith(' failed')):
            counts['offset_commit_failure_events'] += 1
    return dict(counts, scope='Entire retained client log, including warmup/shutdown; '
                'event counts are not lost-record counts or measurement-only rates.')

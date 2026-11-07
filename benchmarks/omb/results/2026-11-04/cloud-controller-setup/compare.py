#!/usr/bin/env python3
"""Compare complete paired OMB evidence without declaring capacity or parity."""
import argparse
import json
import math
import re
import statistics
from pathlib import Path

from summarize import summarize
from environment import instrumentation, wal_settings, kafka_flush_policy
from placement import placement
from measurement import verify_measurement
from client_events import client_events
from gateway_events import gateway_events


def driver_config(path):
    # Only endpoints may differ; all producer/consumer properties must match.
    text = path.read_text()
    pattern = r'(?m)^(\s*bootstrap\.servers=)[^\r\n]+$'
    text, count = re.subn(pattern, r'\1<endpoint>', text)
    if count != 1:
        raise ValueError(f'{path}: expected exactly one bootstrap endpoint')
    return text


def workload_number(text, field):
    matches = re.findall(rf'(?m)^{re.escape(field)}:\s*([0-9]+)\s*$', text)
    if len(matches) != 1 or int(matches[0]) <= 0:
        raise ValueError(f'workload requires a positive fixed {field}')
    return int(matches[0])


def compare_pair(directory, reference_system, candidate_system):
    directory = Path(directory)
    a, b = (directory / f'{system}.json' for system in (reference_system, candidate_system))
    workload = Path(f'{a}.workload.yaml').read_text()
    if workload != Path(f'{b}.workload.yaml').read_text():
        raise ValueError(f'{directory}: workload configurations differ')
    if driver_config(Path(f'{a}.driver.yaml')) != driver_config(Path(f'{b}.driver.yaml')):
        raise ValueError(f'{directory}: client configurations differ beyond endpoints')
    revision = Path(f'{a}.revision').read_text().strip()
    if not revision or revision != Path(f'{b}.revision').read_text().strip():
        raise ValueError(f'{directory}: upstream revisions differ or are missing')
    rate = workload_number(workload, 'producerRate')
    expected_samples = workload_number(workload, 'testDurationMinutes') * 6
    results = {}
    for system, path in ((reference_system, a), (candidate_system, b)):
        result = summarize(path)
        result['measurement'] = verify_measurement(json.loads(path.read_text()),
            Path(f'{path}.log').read_text(), expected_samples // 6)
        for key in ('publish_p99_ms', 'end_to_end_p99_ms', 'publish_delay_p99_us'):
            value = result[key]
            if not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
                raise ValueError(f'{path}: invalid {key}')
        log = Path(f'{path}.log').read_text()
        result['client_events'] = client_events(log)
        result['client_warning_error_matches'] = len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b', log))
        result['offered_rate_fraction_published'] = result['publish_msg_s_mean'] / rate
        result['offered_rate_fraction_consumed'] = result['consume_msg_s_mean'] / rate
        # A trend is diagnostic: even a flat backlog does not establish drain,
        # durability, repeatability, or sustainable capacity beyond this run.
        backlog = json.loads(path.read_text())['backlog']
        x = list(range(len(backlog)))
        slope, _ = statistics.linear_regression(x, backlog)
        result['backlog_linear_trend_messages_per_second'] = slope / 10
        results[system] = result
    if results[reference_system]['workload'] != results[candidate_system]['workload']:
        raise ValueError(f'{directory}: result workloads differ')
    reference = results[reference_system]['end_to_end_p99_ms']
    if reference <= 0:
        raise ValueError(f'{a}: reference p99 must be positive')
    return {
        'directory': str(directory), 'offered_messages_per_second': rate,
        'instrumentation': instrumentation(directory),
        'wal': wal_settings(directory),
        'placement': placement(directory),
        'kafka_flush_policy': kafka_flush_policy(directory),
        'gateway_events': gateway_events(directory / 'gateway.log'),
        'results': results,
        'candidate_to_reference_end_to_end_p99_ratio': results[candidate_system]['end_to_end_p99_ms'] / reference,
        'conclusion': 'Observed fixed-load trial only; no capacity, repeatability, RF=3 or scaling acceptance.',
    }, (workload, driver_config(Path(f'{a}.driver.yaml')), revision,
        json.dumps(instrumentation(directory), sort_keys=True),
        json.dumps(wal_settings(directory), sort_keys=True),
        json.dumps({k: v for k, v in placement(directory).items() if k != 'units'}, sort_keys=True),
        json.dumps(kafka_flush_policy(directory), sort_keys=True))


def compare(directories, reference_system='kafka', candidate_system='broker', verify_environment=False):
    for system in (reference_system, candidate_system):
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', system):
            raise ValueError('system names must be simple lowercase file stems')
    if reference_system == candidate_system:
        raise ValueError('reference and candidate must differ')
    if verify_environment and (reference_system, candidate_system) != ('kafka', 'broker'):
        raise ValueError('environment verification requires Kafka/broker comparison')
    trials = []
    environments = set()
    stores = set()
    configurations = set()
    seen = set()
    for directory in directories:
        resolved = Path(directory).resolve()
        if resolved in seen:
            raise ValueError(f'duplicate trial: {directory}')
        seen.add(resolved)
        result, config = compare_pair(directory, reference_system, candidate_system)
        configurations.add(config)
        if verify_environment:
            from environment import verify
            result['environment'] = verify(directory)
            identifiers = set(result['environment']['stores'])
            if stores & identifiers:
                raise ValueError('trials reuse captured store or container identities')
            stores.update(identifiers)
            environments.add(json.dumps(result['environment']['contract'], sort_keys=True))
        trials.append(result)
    if not trials:
        raise ValueError('at least one pair is required')
    if len(environments) > 1:
        raise ValueError('trials have different captured environments or deployment artifacts')
    if len(configurations) != 1:
        raise ValueError('trials have different workloads, clients, upstream revisions, instrumentation, WAL settings, placement or Kafka flush policy')
    return {'reference_system': reference_system, 'candidate_system': candidate_system,
            'trials': trials, 'trial_count': len(trials),
            'at_least_three_trials': len(trials) >= 3,
            'end_to_end_p99_ratios': [t['candidate_to_reference_end_to_end_p99_ratio'] for t in trials]}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    parser.add_argument('--verify-environment', action='store_true')
    parser.add_argument('--reference', default='kafka')
    parser.add_argument('--candidate', default='broker')
    args = parser.parse_args()
    print(json.dumps(compare(args.directories, args.reference, args.candidate, args.verify_environment), indent=2, allow_nan=False))

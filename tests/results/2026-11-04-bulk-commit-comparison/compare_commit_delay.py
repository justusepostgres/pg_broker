#!/usr/bin/env python3
"""Compare two completed instrumented runs; never declare Kafka parity."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

from compare import driver_config
from measurement import verify_measurement
from summarize import summarize
from client_events import client_events

spec = importlib.util.spec_from_file_location('server_summary', Path(__file__).with_name('summarize-server.py'))
analysis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)

# This experiment has two reviewed, frozen harness revisions. Updating this
# allowlist requires a new source review, not merely a new successful result.
SOURCES = {
    0: {'runner.sh':'2d7a6e7c68e43297f7f29c3a88bda85656ee19a9780c24865acab13d829795ac',
        'sample-server.py':'520c8f88b41570bb77f84660f0ff89b41437a58510af7d03f56ae859234525c5'},
    500: {'runner.sh':'7f1ff0eb85e40d96fc70e3ff59b9899493e1dc820bcda6c98634a70402b65b51',
          'sample-server.py':'02d82439e10b94711d055b773f738c8ed0d20fd3db2926f4c2ac6c43f1a7a3a0'},
}


def require(value, message):
    if not value:
        raise ValueError(message)


def inspect(directory, delay, observed_baseline_settings=None, *, sources=None, sql_version='0.1.7'):
    p = Path(directory)
    for name,digest in (SOURCES[delay] if sources is None else sources).items():
        require(hashlib.sha256((p/name).read_bytes()).hexdigest() == digest, 'unreviewed diagnostic source')
    require((p/'complete.txt').read_text().strip(), 'missing completion marker')
    instrument = json.loads((p/'instrumentation.json').read_text())
    require(instrument.get('acceptance_run') is False and instrument.get('io_and_wal_timing') is True
            and instrument.get('track_functions') == 'all', 'timed diagnostic required')
    require((p/'wal-settings.txt').read_text().strip() == 'off|on|16MB|fdatasync|on|on|on', 'durability settings differ')
    require((p/'storage.txt').read_text().splitlines() == [sql_version+'|on|on|on|t|t','16|t'], 'restart/storage check failed')
    require((p/'timing-settings.txt').read_text().strip() == 'on|on|all', 'timing settings differ')
    sampling = json.loads((p/'sampling-complete.json').read_text())
    require(sampling['samples'] > 400 and sampling['sample_errors'] == 0, 'incomplete sampling')
    snapshots = [json.loads((p/f'diagnostic-stats-{name}.json').read_text()) for name in ('before','after')]
    settings_provenance = 'both statistics snapshots'
    for snapshot in snapshots:
        require(snapshot['io_timing'] == snapshot['wal_io_timing'] == 'on'
                and snapshot['track_functions'] == 'all', 'snapshot timing differs')
        if 'commit_delay' in snapshot and 'commit_siblings' in snapshot:
            require(snapshot['commit_delay'] == str(delay) and snapshot['commit_siblings'] == ('1' if delay else '5'),
                    'group-commit settings differ')
        else:
            require(delay == 0 and observed_baseline_settings is not None, 'missing group-commit settings evidence')
            require(Path(observed_baseline_settings).read_text().strip() == '0|5|on|on|on', 'baseline settings differ')
            settings_provenance = 'separate read-only baseline observation; fields absent from original snapshots'
    rows = [line.split() for line in (p/'broker-limits.txt').read_text().splitlines()]
    require(len(rows) == 2 and all(len(r) == 3 for r in rows), 'missing resource limits')
    for suffix, memory in (('-db',8*1024**3),('-gateway',1024**3)):
        selected = [r for r in rows if r[0].endswith(suffix)]
        require(len(selected) == 1 and selected[0][1:] == ['0,1',str(memory)], 'resource limits differ')
    result = summarize(p/'broker.json')
    log = (p/'broker.json.log').read_text()
    result['measurement'] = verify_measurement(json.loads((p/'broker.json').read_text()), log,15,require_timing=True)
    result['client_events'] = client_events(log)
    result['database_lifetime'] = analysis.database_delta(*snapshots)
    result['commit_settings_provenance'] = settings_provenance
    # Labels/paths in resource and invocation files intentionally differ.
    same_files = ('gateway-sha256.txt','image.txt','extension-binary.txt','wal-settings.txt',
                  'timing-settings.txt','volume-profile.json','host.txt','java.txt',
                  'broker.json.workload.yaml','broker.json.revision','verify-storage.sql',
                  'pg_broker--0.1.5--0.1.6.sql','pg_broker--0.1.6--0.1.7.sql',
                  'client-broker/limits.txt','client-broker/java.txt','client-broker/heap.txt',
                  'client-broker/runner.sh','client-broker/run.sh','client-broker/execute.py')
    contract = {name:(p/name).read_text() for name in same_files}
    contract['driver'] = driver_config(p/'broker.json.driver.yaml')
    return result,contract,{r[0] for r in rows}


def compare(reference, candidate, observed_baseline_settings=None):
    require(Path(reference).resolve() != Path(candidate).resolve(), 'duplicate run')
    old,a,stores_a = inspect(reference,0,observed_baseline_settings)
    new,b,stores_b = inspect(candidate,500)
    require(a == b, 'captured configuration differs beyond the diagnostic variant')
    require(not stores_a & stores_b, 'stores reused')
    return dict(reference=old,candidate=new,
                candidate_publish_rate_ratio=new['publish_msg_s_mean']/old['publish_msg_s_mean'] if old['publish_msg_s_mean'] else None,
                candidate_end_to_end_p99_ratio=new['end_to_end_p99_ms']/old['end_to_end_p99_ms'] if old['end_to_end_p99_ms'] else None,
                scope='One instrumented fresh-store comparison. Report delay censoring, errors and tails alongside ratios. '
                      'Sampler schemas differ only in added settings capture; reviewed runner changes enable the variant. '
                      'No repeatability, maximum-capacity, RF3, Kafka-parity or production-default acceptance.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference')
    parser.add_argument('candidate')
    parser.add_argument('--observed-baseline-settings',type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.reference,args.candidate,args.observed_baseline_settings),indent=2))

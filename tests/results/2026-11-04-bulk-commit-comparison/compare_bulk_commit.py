#!/usr/bin/env python3
"""Inspect the frozen bulk-commit candidate; compare the whole SQL/binary revision."""
import argparse
import json
from pathlib import Path
from compare_commit_delay import inspect, require

# Reviewed deployment revisions. Do not derive these from untrusted result files.
SOURCES = {'runner.sh': '39bc188b90533e3bf071e9f7684fb48f334e356fa78b73de3bebbaee6f69602d', 'sample-server.py': '02d82439e10b94711d055b773f738c8ed0d20fd3db2926f4c2ac6c43f1a7a3a0', 'pg_broker--0.1.7--0.1.8.sql': '484fa1ee78c2f7474310d751c3462ad3ec6918693305df587645d669166f0481', 'pg_broker--0.1.8--0.1.9.sql': 'f42d889fa1ba8058c2fe435a8fc6af6a8a3a8a12feae0713c9ffb75565b10465', 'pg_broker--0.1.9--0.1.10.sql': '7f9d214de9ee0fb78f236e0d983632bfaa67ac8994d1fe478ab9c910df8c7722', 'pg_broker--0.1.10--0.1.11.sql': 'ecc32af8a8dbe84d3315be4cc3d3289102027acfd8b2e19831dc23772524bc8d', 'pg_broker--0.1.11--0.1.12.sql': '7a97b9b559b916458880be1813146c193ea5a4a0404f90eca9ef476cdff2acd2', 'pg_broker--0.1.12--0.1.13.sql': '18ffa3f8d27cdc55ed5ad12dd07ff28311517c80664b4720ce4b69bdf4a66a17', 'pg_broker--0.1.13--0.1.14.sql': '699341742bd314db50f1d759f7cbf376c2d98b36c8ecd4e4b02aefc32fd4cc86', 'pg_broker--0.1.14--0.1.15.sql': 'e16c34b2b3ce72ff573bb34a105e1fb7b7a697abc924849fc2b8efa432c97216', 'pg_broker--0.1.15--0.1.16.sql': 'cf2e7c05e4cc15415504541ea65895b4df51afce7881a4ba435768bb7664c175', 'pg_broker--0.1.16--0.1.17.sql': '6bd69bb1214a08718432998f8bdca2682d2afbc9346da0d026aaae80527c4dcd'}
BASELINE_BINARY = '0e0b5024eb643859f18b23a111870a45f28a2e65e522505ddeac8592bddb41fc'
CANDIDATE_BINARY = '81ceb16a9475cc5e35cd7f34e67c9132e8efe7418a4e3e9e3489f4abf4802103'


def compare(reference, candidate, observed_baseline_settings=None):
    require(Path(reference).resolve()!=Path(candidate).resolve(), 'duplicate run')
    old,a,stores_a=inspect(reference,0,observed_baseline_settings)
    new,b,stores_b=inspect(candidate,0,sources=SOURCES,sql_version='0.1.17')
    for contract,expected in ((a,BASELINE_BINARY),(b,CANDIDATE_BINARY)):
        require(contract.pop('gateway-sha256.txt').split()[0]==expected, 'unexpected gateway binary')
    require(a==b, 'captured workload or resource configuration differs')
    require(not stores_a & stores_b, 'stores reused')
    instrument=json.loads((Path(candidate)/'instrumentation.json').read_text())
    require(instrument.get('sql_version')=='0.1.17' and instrument.get('gateway_changed') is True,
            'candidate revision is not explicitly recorded')
    functions=new['database_lifetime']['functions']
    bulk=[f for f in functions if f['function']=='broker.commit_many']
    require(len(bulk)==1 and bulk[0]['calls']>0, 'bulk path was not exercised')
    return dict(reference=old,candidate=new,
                candidate_publish_rate_ratio=new['publish_msg_s_mean']/old['publish_msg_s_mean'] if old['publish_msg_s_mean'] else None,
                candidate_end_to_end_p99_ratio=new['end_to_end_p99_ms']/old['end_to_end_p99_ms'] if old['end_to_end_p99_ms'] else None,
                scope='One instrumented fresh-store comparison of complete gateway/SQL revisions, including intervening ownership checks. '
                      'Not an isolated causal estimate of bulk commits. Preserve errors, delay censoring and tails. '
                      'No repeatability, sustainable-capacity, RF3 or Kafka-parity acceptance.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference')
    parser.add_argument('candidate')
    parser.add_argument('--observed-baseline-settings',type=Path)
    args=parser.parse_args()
    print(json.dumps(compare(args.reference,args.candidate,args.observed_baseline_settings),indent=2,allow_nan=False))

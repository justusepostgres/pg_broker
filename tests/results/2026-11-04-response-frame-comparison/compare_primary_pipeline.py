#!/usr/bin/env python3
"""Compare a reviewed gateway revision on identical SQL and harness sources."""
import argparse
import json
import re
from pathlib import Path
from compare_commit_delay import inspect, require
from compare_partition_guard import SOURCES
from compare_bulk_commit import CANDIDATE_BINARY as BASELINE_BINARY


def compare(reference,candidate,candidate_sha256,*,baseline_sha256=BASELINE_BINARY):
    require(re.fullmatch('[a-f0-9]{64}',candidate_sha256) is not None,'invalid candidate SHA-256')
    require(re.fullmatch('[a-f0-9]{64}',baseline_sha256) is not None,'invalid baseline SHA-256')
    require(candidate_sha256!=baseline_sha256,'candidate must differ from baseline')
    require(Path(reference).resolve()!=Path(candidate).resolve(),'duplicate run')
    old,a,stores_a=inspect(reference,0,sources=SOURCES,sql_version='0.1.18')
    new,b,stores_b=inspect(candidate,0,sources=SOURCES,sql_version='0.1.18')
    for contract,expected in ((a,baseline_sha256),(b,candidate_sha256)):
        require(contract.pop('gateway-sha256.txt').split()[0]==expected,'unexpected gateway binary')
    require(a==b,'captured SQL/workload/resource configuration differs')
    require(not stores_a & stores_b,'stores reused')
    for result,directory in ((old,reference),(new,candidate)):
        instrument=json.loads((Path(directory)/'instrumentation.json').read_text())
        require(instrument.get('sql_version')=='0.1.18','SQL revision not explicitly recorded')
        functions={f['function']:f for f in result['database_lifetime']['functions']}
        for name in ('broker.commit_many','public.broker_append_bytes','broker.raft_require_application'):
            require(functions.get(name,{}).get('calls',0)>0,'required storage path was not exercised')
    return dict(reference=old,candidate=new,baseline_sha256=baseline_sha256,candidate_sha256=candidate_sha256,
                candidate_publish_rate_ratio=new['publish_msg_s_mean']/old['publish_msg_s_mean'] if old['publish_msg_s_mean'] else None,
                candidate_end_to_end_p99_ratio=new['end_to_end_p99_ms']/old['end_to_end_p99_ms'] if old['end_to_end_p99_ms'] else None,
                scope='One instrumented fresh-store gateway comparison with identical SQL, harness, workload and resource limits. '
                      'Candidate digest must come from the reviewed build evidence. '
                      'No repeatability, sustainable-capacity, RF3 or Kafka-parity acceptance.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference');parser.add_argument('candidate')
    parser.add_argument('--candidate-sha256',required=True)
    parser.add_argument('--baseline-sha256',default=BASELINE_BINARY)
    args=parser.parse_args()
    print(json.dumps(compare(args.reference,args.candidate,args.candidate_sha256,baseline_sha256=args.baseline_sha256),indent=2,allow_nan=False))

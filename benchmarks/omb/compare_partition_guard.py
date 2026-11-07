#!/usr/bin/env python3
"""Compare the reviewed SQL-only guard change on the same gateway binary."""
import argparse
import json
from pathlib import Path
from compare_commit_delay import inspect, require
from compare_bulk_commit import SOURCES as BASELINE_SOURCES, CANDIDATE_BINARY

SOURCES = {'pg_broker--0.1.12--0.1.13.sql': '18ffa3f8d27cdc55ed5ad12dd07ff28311517c80664b4720ce4b69bdf4a66a17', 'runner.sh': 'fdb3ec16614e4fe8c16d790cbf789f0dc40b4d24ace2ff47295b336a72cdcb75', 'sample-server.py': '02d82439e10b94711d055b773f738c8ed0d20fd3db2926f4c2ac6c43f1a7a3a0', 'pg_broker--0.1.14--0.1.15.sql': 'e16c34b2b3ce72ff573bb34a105e1fb7b7a697abc924849fc2b8efa432c97216', 'pg_broker--0.1.11--0.1.12.sql': '7a97b9b559b916458880be1813146c193ea5a4a0404f90eca9ef476cdff2acd2', 'pg_broker--0.1.8--0.1.9.sql': 'f42d889fa1ba8058c2fe435a8fc6af6a8a3a8a12feae0713c9ffb75565b10465', 'pg_broker--0.1.13--0.1.14.sql': '699341742bd314db50f1d759f7cbf376c2d98b36c8ecd4e4b02aefc32fd4cc86', 'pg_broker--0.1.9--0.1.10.sql': '7f9d214de9ee0fb78f236e0d983632bfaa67ac8994d1fe478ab9c910df8c7722', 'pg_broker--0.1.16--0.1.17.sql': '6bd69bb1214a08718432998f8bdca2682d2afbc9346da0d026aaae80527c4dcd', 'pg_broker--0.1.7--0.1.8.sql': '484fa1ee78c2f7474310d751c3462ad3ec6918693305df587645d669166f0481', 'pg_broker--0.1.15--0.1.16.sql': 'cf2e7c05e4cc15415504541ea65895b4df51afce7881a4ba435768bb7664c175', 'pg_broker--0.1.10--0.1.11.sql': 'ecc32af8a8dbe84d3315be4cc3d3289102027acfd8b2e19831dc23772524bc8d', 'pg_broker--0.1.17--0.1.18.sql': 'cbc78d3be2d4f296863f7d4f05320a66c2d66e886ec71ed2cd6e2c9c038c44d6'}


def compare(reference,candidate):
    require(Path(reference).resolve()!=Path(candidate).resolve(),'duplicate run')
    old,a,stores_a=inspect(reference,0,sources=BASELINE_SOURCES,sql_version='0.1.17')
    new,b,stores_b=inspect(candidate,0,sources=SOURCES,sql_version='0.1.18')
    require(a==b,'captured binary/workload/resource configuration differs')
    require(a['gateway-sha256.txt'].split()[0]==CANDIDATE_BINARY,'unexpected gateway binary')
    require(not stores_a & stores_b,'stores reused')
    for result,version,directory in ((old,'0.1.17',reference),(new,'0.1.18',candidate)):
        instrument=json.loads((Path(directory)/'instrumentation.json').read_text())
        require(instrument.get('sql_version')==version,'SQL revision not explicitly recorded')
        functions={f['function']:f for f in result['database_lifetime']['functions']}
        require(functions.get('broker.commit_many',{}).get('calls',0)>0,'bulk path was not exercised')
        require(functions.get('public.broker_append_bytes',{}).get('calls',0)>0,'append path was not exercised')
        require(functions.get('broker.raft_require_application',{}).get('calls',0)>0,'ownership checks were not exercised')
        result['ownership_helper_calls_per_append_entry']=functions['broker.raft_require_application']['calls']/functions['public.broker_append_bytes']['calls']
    return dict(reference=old,candidate=new,
                candidate_publish_rate_ratio=new['publish_msg_s_mean']/old['publish_msg_s_mean'] if old['publish_msg_s_mean'] else None,
                candidate_end_to_end_p99_ratio=new['end_to_end_p99_ms']/old['end_to_end_p99_ms'] if old['end_to_end_p99_ms'] else None,
                scope='One instrumented fresh-store SQL-only comparison; same gateway, workload and resource limits. '
                      'Function counts cover the whole sampling lifetime, including warmup. '
                      'No repeatability, sustainable-capacity, RF3 or Kafka-parity acceptance.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference');parser.add_argument('candidate')
    args=parser.parse_args()
    print(json.dumps(compare(args.reference,args.candidate),indent=2,allow_nan=False))

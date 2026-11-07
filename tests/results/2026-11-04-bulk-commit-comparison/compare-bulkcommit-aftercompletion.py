from pathlib import Path
import sys,json,time,hashlib
sys.path.insert(0,str(Path('benchmarks/omb').resolve()))
from compare_bulk_commit import compare
root=Path.cwd();base=root/'benchmarks/omb/results/2026-11-04'
marker=root/'.dev/omb-cloud-bulkcommit2.json'
deadline=time.monotonic()+3600
while True:
    state=json.loads(marker.read_text())
    assert state['pair_invocation']=='470977121aa440c7932e1abae7c69bf0'
    if state['pair_phase']=='complete':break
    if state['pair_phase']=='failed':raise RuntimeError('Original run failed; preserve evidence, do not replay')
    if time.monotonic()>deadline:raise TimeoutError('Collection observation expired; no workload action taken')
    time.sleep(30)
report=compare(base/'timing100k-1',base/'bulkcommit-2',base/'timing100k-1-interim/commit-settings-observed.txt')
out=base/'bulkcommit-2-versus-timing100k-1.json'
with out.open('x') as f:f.write(json.dumps(report,indent=2,allow_nan=False)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('reference','candidate')},indent=2),flush=True)

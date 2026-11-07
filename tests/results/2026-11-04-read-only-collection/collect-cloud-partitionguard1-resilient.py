from pathlib import Path
import sys,subprocess,json,time,tarfile,hashlib,datetime
sys.path.insert(0,str(Path('benchmarks/omb').resolve()))
from remote_client import ssh
from read_only_collection import service, download_completed
import uuid
from summarize import summarize
from measurement import verify_measurement

def compare(directories, verify_environment=True):
 assert len(directories)==1
 p=directories[0]
 assert json.loads((p/'instrumentation.json').read_text())['acceptance_run'] is False
 assert (p/'storage.txt').read_text().splitlines()==['0.1.18|on|on|on|t|t','16|t']
 assert (p/'timing-settings.txt').read_text().strip()=='on|on|all'
 assert (p/'commit-delay-settings.txt').read_text().strip()=='0|5'
 for name in ('before','after'):
  snapshot=json.loads((p/f'diagnostic-stats-{name}.json').read_text())
  assert snapshot['io_timing']==snapshot['wal_io_timing']=='on' and snapshot['track_functions']=='all'
  assert snapshot['commit_delay']=='0' and snapshot['commit_siblings']=='5'
 after=json.loads((p/'diagnostic-stats-after.json').read_text())
 assert any(f['schemaname']=='broker' and f['funcname']=='commit_many' and f['calls']>0 for f in after['functions'])
 assert (p/'gateway-sha256.txt').read_text().split()[0]=='81ceb16a9475cc5e35cd7f34e67c9132e8efe7418a4e3e9e3489f4abf4802103'
 assert hashlib.sha256((p/'runner.sh').read_bytes()).hexdigest()=='fdb3ec16614e4fe8c16d790cbf789f0dc40b4d24ace2ff47295b336a72cdcb75'
 sampling=json.loads((p/'sampling-complete.json').read_text())
 assert sampling['samples']>0 and sampling['sample_errors']==0,sampling
 measurement=verify_measurement(json.loads((p/'broker.json').read_text()),(p/'broker.json.log').read_text(),15,require_timing=True)
 return dict(scope='Instrumented broker-only diagnosis; no matched comparison or acceptance',result=summarize(p/'broker.json'),sampling=sampling,measurement=measurement)

root=Path.cwd()
state=dict(host='170.9.24.220',key=str(root/'.dev/oci-cnpg-key'),known_hosts=str(root/'.dev/oci-omb-known-hosts'))
unit='omb-cloud-pair-018-partitionguard-1.service'
invocation=sys.argv[1]
assert len(invocation)==32 and all(c in '0123456789abcdef' for c in invocation)
remote='/home/ubuntu/results/omb-pair-018-partitionguard-1'
base=root/'benchmarks/omb/results/2026-11-04'
dest=base/'partitionguard-1'
staging=base/'partitionguard-1.incoming'
assert not dest.exists() and not staging.exists(), 'refusing to overwrite prior evidence'
deadline=time.monotonic()+3600
while True:
 status=service(state,unit,invocation)
 observed_text='\n'.join(k+'='+v for k,v in status.items())+'\n'
 assert status.get('InvocationID')==invocation, status
 print(datetime.datetime.now(datetime.timezone.utc).isoformat(),status,flush=True)
 if status.get('MainPID')=='0' and status.get('SubState') in ('exited','dead','failed'):
  assert status.get('Result')=='success' and status.get('ExecMainStatus')=='0',status
  break
 if time.monotonic()>=deadline:
  raise TimeoutError('observation budget expired; original server job was not stopped')
 time.sleep(30)
archive=root/('.dev/partitionguard018-1-'+uuid.uuid4().hex+'.tar.gz')
download_completed(state,unit,invocation,remote,archive)
with tarfile.open(archive,'r:gz') as bundle:
 members=bundle.getmembers()
 assert len(members)<=1000 and sum(m.size for m in members)<=256*1024*1024
 for member in members:
  assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
  assert member.isfile() or member.isdir()
 bundle.extractall(staging,filter='data')
(staging/'pair-service-status.txt').write_text(observed_text)
# Full verification runs before publishing the completed local evidence.
compare([staging],verify_environment=True)
staging.rename(dest)
report=compare([dest],verify_environment=True)
from compare_partition_guard import compare as compare_guard
matched=compare_guard(base/'bulkcommit-2',dest)
(base/'partitionguard-1-versus-bulkcommit-2.json').write_text(json.dumps(matched,indent=2,allow_nan=False)+'\n')
(base/'partitionguard-1-comparison.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
(dest/'SHA256SUMS.local').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(dest))+'\n' for p in sorted(dest.rglob('*')) if p.is_file() and p.name!='SHA256SUMS.local'))
p=root/'.dev/omb-cloud-partitionguard1.json';d=json.loads(p.read_text());d.update(pair_phase='complete',completed_observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),local_results=str(dest),comparison=str(base/'partitionguard-1-comparison.json'),pair_status=status)
p.write_text(json.dumps(d,indent=2)+'\n')
print(json.dumps(report,indent=2),flush=True)

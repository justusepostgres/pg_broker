from pathlib import Path
import sys,json,time,datetime,hashlib,subprocess,importlib.util
sys.path.insert(0,'benchmarks/omb')
from remote_client import ssh
from measurement import verify_measurement
root=Path.cwd();marker=root/'.dev/groupcommit500-after-timing-state.json'
assert not marker.exists(),'one-shot observation already exists; inspect its handle'
server=dict(host='170.9.24.220',key=str(root/'.dev/oci-cnpg-key'),known_hosts=str(root/'.dev/oci-omb-known-hosts'))
handles={'omb-cloud-pair-017-timing100k-1.service':'147112804c714f5eaa800b0b46ce4074','omb-cloud-controller-timing100k-1.service':'5038ae8e573c4bdcb57dbfbf71d39250'}
source_files=[root/'tests/results/2026-11-04-group-commit-preparation'/n for n in ('oci-pair.sh','cloud-controller.py','sample-server.py')]+[root/'.dev/deploy-groupcommit5001.py']
hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
state=dict(phase='waiting',handles=handles,candidate_hashes=hashes,scope='One-shot fresh group-commit diagnostic only after baseline completion and validation')
def save():marker.write_text(json.dumps(state,indent=2)+'\n')
save();deadline=time.monotonic()+1800
try:
 while True:
  statuses={}
  for unit,invocation in handles.items():
   r=ssh(server,['systemctl','show',unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],capture_output=True,text=True,check=True)
   status=dict(line.split('=',1) for line in r.stdout.splitlines());assert status['InvocationID']==invocation,status
   if status['MainPID']=='0':assert status['SubState']=='exited' and status['Result']=='success' and status['ExecMainStatus']=='0',status
   statuses[unit]=status
  state.update(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),statuses=statuses);save()
  print(state['observed_at'],statuses,flush=True)
  collected=json.loads((root/'.dev/omb-cloud-timing100k1.json').read_text())
  if all(s['MainPID']=='0' for s in statuses.values()) and collected.get('pair_phase')=='complete':break
  if time.monotonic()>deadline:raise TimeoutError('Observation expired; original workload was not stopped and candidate was not launched')
  time.sleep(30)
 p=root/'benchmarks/omb/results/2026-11-04/timing100k-1'
 result=json.loads((p/'broker.json').read_text())
 verify_measurement(result,(p/'broker.json.log').read_text(),15,require_timing=True)
 assert (p/'storage.txt').read_text().splitlines()==['0.1.7|on|on|on|t|t','16|t']
 sampling=json.loads((p/'sampling-complete.json').read_text());assert sampling['sample_errors']==0 and sampling['samples']>400
 assert (p/'timing-settings.txt').read_text().strip()=='on|on|all'
 before=json.loads((p/'diagnostic-stats-before.json').read_text());after=json.loads((p/'diagnostic-stats-after.json').read_text())
 assert before['wal_io_timing']==after['wal_io_timing']=='on'
 assert (root/'benchmarks/omb/results/2026-11-04/timing100k-1-interim/commit-settings-observed.txt').read_text().strip()=='0|5|on|on|on'
 syncs=after['wal']['wal_sync']-before['wal']['wal_sync'];ms=after['wal']['wal_sync_time']-before['wal']['wal_sync_time']
 assert before['wal']['stats_reset']==after['wal']['stats_reset'] and syncs>0
 mean=ms/syncs
 assert mean>=0.5,'Final sync cost does not support this queued experiment'
 assert result['aggregatedPublishDelayLatency99pct']>=60000000,'Final run did not reproduce the expected overload'
 state.update(phase='baseline-verified',baseline_wal_sync_mean_ms=mean,baseline_syncs=syncs);save()
 for file,expected in hashes.items():assert hashlib.sha256(Path(file).read_bytes()).hexdigest()==expected,'prepared source changed'
 state['phase']='deployment-requested';save()
 subprocess.run([sys.executable,str(root/'.dev/deploy-groupcommit5001.py')],check=True,timeout=180)
 state.update(phase='candidate-launched',candidate=json.loads((root/'.dev/omb-cloud-groupcommit5001.json').read_text()));save()
except BaseException as error:
 state.update(phase='failed',error=str(error),note='Inspect original service and deployment markers; never blindly replay');save();raise

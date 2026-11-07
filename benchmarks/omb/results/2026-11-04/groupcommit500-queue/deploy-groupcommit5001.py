from pathlib import Path
import sys,json,hashlib,io,tarfile
sys.path.insert(0,'benchmarks/omb')
from remote_client import ssh
from compare import compare
root=Path.cwd();label='017-groupcommit500-1';unit='omb-cloud-controller-groupcommit500-1.service'
marker=root/'.dev/omb-cloud-groupcommit5001.json';assert not marker.exists()
previous=json.loads((root/'.dev/omb-cloud-timing100k1.json').read_text());assert previous['pair_phase']=='complete'
assert json.loads((root/'benchmarks/omb/results/2026-11-04/timing100k-1-comparison.json').read_text())['sampling']['sample_errors']==0
server=dict(host='170.9.24.220',key=str(root/'.dev/oci-cnpg-key'),known_hosts=str(root/'.dev/oci-omb-known-hosts'))
def status(unit):
 r=ssh(server,['systemctl','show',unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],capture_output=True,text=True,check=True)
 return dict(line.split('=',1) for line in r.stdout.splitlines())
old=status(previous['unit']);assert old['InvocationID']==previous['controller_invocation'] and old['MainPID']=='0' and old['SubState']=='exited' and old['Result']=='success',old
files={n:(root/'tests/results/2026-11-04-group-commit-preparation'/n).read_bytes() for n in ('oci-pair.sh','cloud-controller.py','sample-server.py')}
hashes={n:hashlib.sha256(v).hexdigest() for n,v in files.items()}
manifest=dict(label=label,hashes=hashes,scope='Instrumented broker-only diagnosis; never acceptance')
files['manifest.json']=json.dumps(manifest,indent=2).encode()
buffer=io.BytesIO()
with tarfile.open(fileobj=buffer,mode='w:gz') as archive:
 for n,v in files.items():
  info=tarfile.TarInfo(n);info.size=len(v);archive.addfile(info,io.BytesIO(v))
bundle=buffer.getvalue();digest=hashlib.sha256(bundle).hexdigest()
evidence=root/'benchmarks/omb/results/2026-11-04/groupcommit500-deployment';evidence.mkdir(exist_ok=False)
for n,v in files.items():(evidence/n).write_bytes(v)
(evidence/'harness-tests.log').write_bytes((root/'.dev/group-commit-experiment-tests.log').read_bytes())
info=dict(phase='upload-requested',label=label,unit=unit,bundle_sha256=digest,manifest=manifest)
def save():marker.write_text(json.dumps(info,indent=2)+'\n')
save()
program=r'''import sys,io,tarfile,pathlib,hashlib,json,fcntl,subprocess,os
with open('/home/ubuntu/omb-pair.lock','w') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 assert not subprocess.check_output(['sudo','docker','ps','-q'],text=True).strip()
 assert subprocess.run(['pgrep','-x','java'],capture_output=True).returncode==1
 profile=json.loads(pathlib.Path('/home/ubuntu/omb-volume-profile.json').read_text());assert profile['size_in_gbs']==1024 and profile['vpus_per_gb']==10
 subprocess.run(['pidstat','-V'],check=True,capture_output=True)
 data=sys.stdin.buffer.read(1024*1024+1);assert len(data)<=1024*1024 and hashlib.sha256(data).hexdigest()==sys.argv[1]
 stage=pathlib.Path('/home/ubuntu/groupcommit500-deploy-1');stage.mkdir(mode=0o700,exist_ok=False)
 with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
  ms=archive.getmembers();assert len(ms)==4 and sum(m.size for m in ms)<1024*1024
  assert all(m.isfile() and pathlib.Path(m.name).name==m.name for m in ms)
  archive.extractall(stage,filter='data')
 manifest=json.loads((stage/'manifest.json').read_text())
 installed={}
 for name,expected in manifest['hashes'].items():
  assert name in ('oci-pair.sh','sample-server.py','cloud-controller.py')
  content=(stage/name).read_bytes();assert hashlib.sha256(content).hexdigest()==expected
  directory=pathlib.Path('/home/ubuntu/omb-cloud-controller' if name=='cloud-controller.py' else '/home/ubuntu/pg-broker/benchmarks/omb')
  target=directory/name;temporary=directory/(name+'.incoming-groupcommit1')
  with temporary.open('xb') as f:f.write(content)
  os.chmod(temporary,0o700 if name.endswith('.sh') else 0o600);temporary.replace(target)
  installed[str(target)]=hashlib.sha256(target.read_bytes()).hexdigest()
 subprocess.run(['bash','-n','/home/ubuntu/pg-broker/benchmarks/omb/oci-pair.sh'],check=True)
 print(json.dumps(installed,indent=2))
'''
r=ssh(server,['python3','-c',program,digest],input=bundle,capture_output=True,check=True)
(evidence/'installation.json').write_bytes(r.stdout)
info['phase']='launch-requested';save()
ssh(server,['sudo','systemd-run','--unit='+unit,'--uid=ubuntu','--property=RuntimeMaxSec=4000','--property=RemainAfterExit=yes','--property=AllowedCPUs=2,3','--property=MemoryMax=256M','/usr/bin/python3','-u','/home/ubuntu/omb-cloud-controller/cloud-controller.py',label,'--order','broker-first','--rate','100k','--runner-sha256',hashes['oci-pair.sh'],'--volume-size-gib','1024','--diagnostic-broker-only','--diagnostic-timing','--diagnostic-commit-delay-us','500'],check=True)
observed=status(unit);assert observed['InvocationID']
info.update(phase='observing',controller_invocation=observed['InvocationID'],observed=observed);save()
(evidence/'launch.json').write_text(json.dumps(info,indent=2)+'\n')
(evidence/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in sorted(evidence.iterdir()) if p.is_file() and p.name!='SHA256SUMS'))
print(json.dumps(info,indent=2))

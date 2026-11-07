from pathlib import Path
import sys, subprocess, time, json, hashlib, fcntl, shlex
sys.path.insert(0,str(Path('benchmarks/omb').resolve()))
from remote_client import ssh
root=Path.cwd(); state=dict(host='170.9.24.220',key=str(root/'.dev/oci-cnpg-key'),known_hosts=str(root/'.dev/oci-omb-known-hosts'))
unit='omb-cloud-controller-primarypipeline-1.service'; invocation='2592713f9d8e4c1d855f0dd0ef9890c6'
archive=root/'.dev/omb-responseframe018-source.tar.gz'; digest=hashlib.sha256(archive.read_bytes()).hexdigest()
assert json.loads((root/'tests/results/2026-11-04-response-frame/verification.json').read_text())['native_tests']==51
marker=root/'.dev/omb-active-responseframe018-build.json'
build_unit='omb-build-responseframe018.service'
with open(root/'.dev/responseframe018-build-controller.lock','w') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 assert not marker.exists(), 'Existing build handle: inspect it rather than relaunch'
 deadline=time.monotonic()+1800
 while True:
  r=ssh(state,['systemctl','show',unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],text=True,capture_output=True,check=True)
  status=dict(line.split('=',1) for line in r.stdout.splitlines())
  assert status['InvocationID']==invocation,status
  print('Waiting for original comparison:',status,flush=True)
  if status['MainPID']=='0':
   assert status['SubState']=='exited' and status['Result']=='success' and status['ExecMainStatus']=='0',status
   collected=json.loads((root/'.dev/omb-cloud-primarypipeline1.json').read_text())
   verified=root/'.dev/response-frame-native-result.json'
   if collected.get('pair_phase')=='complete' and verified.exists():
    assert json.loads(verified.read_text())['exit_code']==0
    break
  if time.monotonic()>deadline:raise TimeoutError('Observation expired; no build launched or benchmark stopped')
  time.sleep(30)
 for path,expected in json.loads((root/'.dev/responseframe018-source-hashes.json').read_text()).items():
  assert hashlib.sha256((root/path).read_bytes()).hexdigest()==expected, 'Source changed since archive: '+path
 remote='/home/ubuntu/omb-responseframe018-source.tar.gz'
 with archive.open('rb') as stream:
  ssh(state,['bash','-c','set -e; set -o noclobber; cat > '+shlex.quote(remote)],stdin=stream,check=True)
 command=f'''set -euo pipefail
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ $(hostname) = pg-broker-omb-verification-1 ]]
[[ -z $(sudo docker ps -q) ]]
! pgrep -x java >/dev/null
printf '%s  %s\\n' {digest} {remote} | sha256sum -c -
mkdir /home/ubuntu/candidate-responseframe018
sudo docker run --name omb-build-responseframe018 --memory=6g --cpus=2 --network=none \\
 -v {remote}:/input.tar.gz:ro -v /home/ubuntu/candidate-responseframe018:/out \\
 sha256:dccffb5d7a3ac4a4ae357dfcf851613e38a1d8b0ee25d7834f75185aee827c28 \\
 bash -c 'set -e; tar -xzf /input.tar.gz -C /src; rustc --version > /out/rust-version.txt; cargo build --release --locked --offline -p pg-broker-gateway; cp target/release/pg-broker-gateway /out/pg-broker-gateway; sha256sum /out/pg-broker-gateway > /out/SHA256SUMS'
'''
 script='/home/ubuntu/build-responseframe018.sh'
 ssh(state,['bash','-c','set -e; set -o noclobber; cat > '+shlex.quote(script)],input=command,text=True,check=True)
 info=dict(phase='launch-requested',unit=build_unit,host=state['host'],source_sha256=digest,source_manifest='.dev/responseframe018-source-hashes.json',output='/home/ubuntu/candidate-responseframe018',builder='sha256:dccffb5d7a3ac4a4ae357dfcf851613e38a1d8b0ee25d7834f75185aee827c28',previous_pair_invocation=invocation)
 marker.write_text(json.dumps(info,indent=2)+'\n')
 ssh(state,['sudo','systemd-run','--unit='+build_unit,'--uid=ubuntu','--property=RuntimeMaxSec=900','--property=RemainAfterExit=yes','/bin/bash',script],check=True)
 r=ssh(state,['systemctl','show',build_unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],text=True,capture_output=True,check=True)
 status=dict(line.split('=',1) for line in r.stdout.splitlines());assert status['InvocationID']
 info.update(phase='started',invocation=status['InvocationID'],observed=status);marker.write_text(json.dumps(info,indent=2)+'\n');print(json.dumps(info,indent=2),flush=True)

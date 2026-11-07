from pathlib import Path
import sys,json,time,subprocess,hashlib,fcntl
sys.path.insert(0,str(Path('benchmarks/omb').resolve()))
from remote_client import ssh
root=Path.cwd();base=root/'benchmarks/omb/results/2026-11-04'
server=dict(host='170.9.24.220',key=str(root/'.dev/oci-cnpg-key'),known_hosts=str(root/'.dev/oci-omb-known-hosts'))
marker=root/'.dev/partitionguard-after-bulkcommit-state.json'
prepared=root/'tests/results/2026-11-04-partition-guard-preparation'
def status(unit):
    r=ssh(server,['systemctl','show',unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],capture_output=True,text=True,check=True)
    return dict(line.split('=',1) for line in r.stdout.splitlines())
with (root/'.dev/partitionguard-after-bulkcommit.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert not marker.exists(), 'Inspect existing one-shot controller; do not replay'
    state=dict(phase='waiting',previous_pair_invocation='470977121aa440c7932e1abae7c69bf0',previous_controller_invocation='a3bcfa7640c54515a345255658c8f858')
    def save():marker.write_text(json.dumps(state,indent=2)+'\n')
    save();deadline=time.monotonic()+3600
    while True:
        pair=status('omb-cloud-pair-017-bulkcommit-2.service')
        outer=status('omb-cloud-controller-bulkcommit-2.service')
        assert pair['InvocationID']==state['previous_pair_invocation'] and outer['InvocationID']==state['previous_controller_invocation']
        print(pair,outer,flush=True)
        for row in (pair,outer):
            if row['MainPID']=='0':assert row['SubState']=='exited' and row['Result']=='success' and row['ExecMainStatus']=='0',row
        report_path=base/'bulkcommit-2-versus-timing100k-1.json'
        collected=json.loads((root/'.dev/omb-cloud-bulkcommit2.json').read_text())
        if pair['MainPID']==outer['MainPID']=='0' and collected['pair_phase']=='complete' and report_path.exists():break
        if time.monotonic()>deadline:raise TimeoutError('Observation expired without changing the workload')
        time.sleep(30)
    report=json.loads(report_path.read_text())
    assert report['candidate']['measurement']['measured_seconds']>=900
    for line in (prepared/'SHA256SUMS').read_text().splitlines():
        digest,name=line.split('  ',1)
        assert hashlib.sha256((prepared/name).read_bytes()).hexdigest()==digest,name
    state.update(phase='launch-requested',comparison=str(report_path));save()
    subprocess.run([sys.executable,str(root/'.dev/deploy-partitionguard1.py')],check=True,timeout=240)
    state.update(phase='candidate-launched');save()

"""Continue only a successful completed first pair; never replay a launch."""
from pathlib import Path
import sys, subprocess, json, time, hashlib, shlex, fcntl, datetime
ROOT=Path.cwd()
sys.path.insert(0,str(ROOT/'benchmarks/omb'))
from remote_client import ssh
from compare import compare
STATE=dict(host='170.9.24.220',key=str(ROOT/'.dev/oci-cnpg-key'),known_hosts=str(ROOT/'.dev/oci-omb-known-hosts'))
SHA='0e0b5024eb643859f18b23a111870a45f28a2e65e522505ddeac8592bddb41fc'
RUNNER_SHA='a0ef0ceb0c2674d0e863a0c4c8a95d620e9df0e0f66b903fa23f535f4dab8647'
FIRST_INV='142e323f16bd471e9083a4c1712d5d95'
FIRST_UNIT='omb-producebatch017-75k-1.service'
BINARY='/home/ubuntu/candidate-producebatch017/pg-broker-gateway'
CONTROL=ROOT/'.dev/omb-producebatch75k017-repetition.json'
BASE=ROOT/'benchmarks/omb/results/2026-11-04'

def save(**fields):
    state.update(fields,updated_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    tmp=CONTROL.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(CONTROL)

def status(unit):
    r=ssh(STATE,['systemctl','show',unit,'-p','InvocationID','-p','MainPID','-p','SubState','-p','Result','-p','ExecMainStatus'],capture_output=True,text=True,check=True)
    return dict(line.split('=',1) for line in r.stdout.splitlines())

def eligible(report):
    t=report['trials'][-1]
    reasons=[]
    if t['candidate_to_reference_end_to_end_p99_ratio']>1.25:reasons.append('p99 exceeds 1.25 times Kafka')
    for system,r in t['results'].items():
        if r['publish_error_msg_s_max']!=0:reasons.append(system+' publish errors')
        if r['client_events']['offset_commit_failure_events']!=0:reasons.append(system+' offset commit failures')
        if r['offered_rate_fraction_published']<.99 or r['offered_rate_fraction_consumed']<.99:reasons.append(system+' missed offered rate')
    return reasons

with (ROOT/'.dev/producebatch75k017-repetition.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert not CONTROL.exists(),'Inspect existing repetition state; never replay launches'
    state={'phase':'waiting-first-pair','binary_sha256':SHA,'runner_sha256':RUNNER_SHA,
           'scope':'Two additional fixed-load RF1 trials, not capacity or distributed acceptance',
           'plan':[{'trial':2,'order':'kafka-first'},{'trial':3,'order':'broker-first'}]}
    save()
    deadline=time.monotonic()+3600
    while True:
        observed=status(FIRST_UNIT)
        assert observed['InvocationID']==FIRST_INV,observed
        first=json.loads((ROOT/'.dev/omb-active-producebatch75k017.json').read_text())
        print('Original pair:',observed,'collection:',first['phase'],flush=True)
        if first['phase']=='complete':
            assert observed['MainPID']=='0' and observed['SubState']=='exited' and observed['Result']=='success' and observed['ExecMainStatus']=='0',observed
            break
        assert observed['SubState'] in ('running','exited'),observed
        if time.monotonic()>deadline:
            save(phase='observation-expired',note='Original workload not stopped; inspect exact unit before resuming')
            sys.exit(1)
        time.sleep(30)
    directories=[BASE/'producebatch75k-1']
    first_report=compare(directories,verify_environment=True)
    reasons=eligible(first_report)
    if reasons:
        save(phase='stopped-after-first',reasons=reasons)
        print('No repeats launched:',reasons,flush=True)
        sys.exit(0)
    runner=(ROOT/'benchmarks/omb/oci-pair.sh').read_bytes()
    assert hashlib.sha256(runner).hexdigest()==RUNNER_SHA
    advance_template=(ROOT/'.dev/advance-producebatch75k017.py').read_text()
    collect_template=(ROOT/'.dev/collect-producebatch75k017.py').read_text()
    for trial,order in ((2,'kafka-first'),(3,'broker-first')):
        label=f'017-producebatch75k-{trial}'
        unit=f'omb-producebatch017-75k-{trial}.service'
        per_control=ROOT/f'.dev/omb-active-producebatch75k017-{trial}.json'
        assert not per_control.exists(),'Existing trial must be inspected, not replayed'
        command=f'''set -euo pipefail
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -x java >/dev/null
[[ $(sudo blockdev --getsize64 /dev/sda) = 687194767360 ]]
python3 -c 'import json; d=json.load(open("/home/ubuntu/omb-volume-profile.json")); assert d["size_in_gbs"]==640 and d["vpus_per_gb"]==10'
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 95 ]]
printf '%s  %s\\n' {RUNNER_SHA} /home/ubuntu/pg-broker/benchmarks/omb/oci-pair.sh | sha256sum -c -
printf '%s  %s\\n' {SHA} {BINARY} | sha256sum -c -
[[ ! -e /home/ubuntu/results/omb-pair-{label} ]]
'''
        # No data cleanup, provisioning, or artifact upload occurs here.
        try:ssh(STATE,['bash','-c',command],check=True)
        except subprocess.CalledProcessError:
            save(phase='preflight-failed',trial=trial,note='No new launch; inspect disk, artifacts and exact prior workload')
            raise
        info=dict(phase='launch-requested',unit=unit,pair_label=label,binary_sha256=SHA,runner_sha256=RUNNER_SHA,order=order)
        per_control.write_text(json.dumps(info,indent=2)+'\n')
        save(phase='launch-requested',trial=trial,unit=unit)
        ssh(STATE,['sudo','systemd-run','--unit='+unit,'--uid=ubuntu','--property=RuntimeMaxSec=3600','--property=RemainAfterExit=yes','--setenv=OMB_EXTERNAL_CLIENT=1','--setenv=OMB_SERVER_HOST=10.84.0.247','--setenv=OMB_KAFKA_FLUSH_POLICY=background','--setenv=OMB_GATEWAY_PATH='+BINARY,'--setenv=OMB_GATEWAY_SHA256='+SHA,'/bin/bash','/home/ubuntu/pg-broker/benchmarks/omb/oci-pair.sh',label,'75k',order,'off'],check=True)
        observed=status(unit);assert observed['InvocationID'] and observed['SubState']=='running',observed
        info.update(phase='running',invocation=observed['InvocationID']);per_control.write_text(json.dumps(info,indent=2)+'\n')
        save(phase='running',trial=trial,unit=unit,invocation=observed['InvocationID'])
        for kind,template in [('advance',advance_template),('collect',collect_template)]:
            script=template.replace(FIRST_INV,observed['InvocationID']).replace(FIRST_UNIT,unit).replace('017-producebatch75k-1',label).replace('producebatch75k-1',f'producebatch75k-{trial}').replace('producebatch75k017',f'producebatch75k017-{trial}').replace('producebatch75k-comparison',f'producebatch75k-{trial}-comparison')
            if order=='kafka-first':script=script.replace("('broker','kafka')","('kafka','broker')")
            target=ROOT/f'.dev/{kind}-producebatch75k017-{trial}.py'
            assert not target.exists();target.write_text(script)
            subprocess.run([sys.executable,'-u',str(target)],check=True,timeout=4000)
        directories.append(BASE/f'producebatch75k-{trial}')
        report=compare(directories,verify_environment=True)
        (BASE/f'producebatch75k-through-trial-{trial}.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
        # Retain every trial, including regressions. Finish the planned repetitions
        # unless infrastructure validation fails; no favorable-run selection.
        save(phase='trial-complete',trial=trial,diagnostic_reasons=eligible(report))
    save(phase='complete',report=str(BASE/'producebatch75k-through-trial-3.json'),note='Three fixed-load trials collected; inspect all results, capacity and RF3/scaling remain pending')

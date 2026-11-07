#!/usr/bin/env python3
"""Hard-stop a tagged disposable OCI primary host, verify Kafka recovery, restart it.
Never use this on a shared/production cluster. Requires an observer VM that survives.
"""
import argparse
import json
import shlex
import subprocess
import time


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--observer',required=True)
    parser.add_argument('--victim-instance-id',required=True)
    parser.add_argument('--victim-node',required=True)
    parser.add_argument('--ssh-key',required=True)
    parser.add_argument('--known-hosts',required=True)
    args=parser.parse_args()
    ssh=['ssh','-i',args.ssh_key,'-o','UserKnownHostsFile='+args.known_hosts,
         '-o','ConnectTimeout=10','ubuntu@'+args.observer]
    def remote(command,timeout=60):
        result=subprocess.run([*ssh,command],capture_output=True,text=True,timeout=timeout)
        if result.returncode:
            print(result.stdout, end='', flush=True)
            print(result.stderr, end='', flush=True)
            result.check_returncode()
        return result.stdout
    def k(*parts):
        return remote(shlex.join(['sudo','k3s','kubectl','-n','pg-broker',*parts]))
    def get(kind,name):return json.loads(k('get',kind,name,'-o','json'))
    def oci(*parts):
        return json.loads(subprocess.check_output(['oci',*parts],text=True))
    instance=oci('compute','instance','get','--instance-id',args.victim_instance_id)['data']
    assert instance['display-name']==args.victim_node
    assert instance['freeform-tags'].get('purpose')=='pg-broker-ha-verification', 'only disposable tagged test hosts'
    assert instance['lifecycle-state']=='RUNNING'
    assert remote('hostname').strip()!=args.victim_node
    cluster=get('cluster','broker-db');old_primary=cluster['status']['currentPrimary']
    assert get('pod',old_primary)['spec']['nodeName']==args.victim_node
    assert get('pod','kafka-client')['spec']['nodeName']!=args.victim_node
    k('exec','kafka-client','--','python','-c',
      "import json; p='/tmp/pg-broker-smoke.json'; json.load(open(p)); open(p,'a').close()")
    pods=json.loads(k('get','pods','-l','cnpg.io/cluster=broker-db','-o','json'))['items']
    database_pods=[p for p in pods if p['metadata']['name'] in cluster['status']['instanceNames']]
    assert len({p['spec']['nodeName'] for p in database_pods})==3
    assert cluster['status']['readyInstances']==3
    start=time.monotonic()
    stopped=False
    try:
        # STOP is an abrupt power-off, unlike SOFTSTOP's guest shutdown signal.
        stopped=True
        oci('compute','instance','action','--instance-id',args.victim_instance_id,'--action','STOP')
        deadline=time.monotonic()+300
        new_primary=old_primary
        while time.monotonic()<deadline:
            try:
                state=get('cluster','broker-db')['status']
                new_primary=state.get('currentPrimary',old_primary)
                if new_primary!=old_primary and new_primary==state.get('targetPrimary'):
                    recovery=k('exec',new_primary,'-c','postgres','--','psql','-U','postgres','-d','broker','-Atc','SELECT pg_is_in_recovery()').strip()
                    if recovery=='f': break
            except (subprocess.SubprocessError,KeyError):
                pass
            time.sleep(3)
        else: raise AssertionError('CNPG did not promote a writable survivor within 300 seconds')
        print(remote('cd pg-broker && scripts/cnpg-vm.sh resume',timeout=120),end='',flush=True)
        recovery_seconds=round(time.monotonic()-start,2)
        print(remote('cd pg-broker && scripts/cnpg-vm.sh seed',timeout=120),end='',flush=True)
        print(json.dumps({'result':'PASS','old_primary':old_primary,'new_primary':new_primary,
                          'stopped_host':args.victim_node,'observed_resume_seconds':recovery_seconds,
                          'verified':'acknowledged records and group checkpoints survive hard host loss; new writes work'}),flush=True)
    finally:
        if stopped:
            # OCI may still be finishing STOP if an early validation failed.
            for _ in range(90):
                state=oci('compute','instance','get','--instance-id',args.victim_instance_id)['data']['lifecycle-state']
                if state=='STOPPED':break
                time.sleep(2)
            assert state=='STOPPED', 'test host has not stopped; inspect before restarting'
            oci('compute','instance','action','--instance-id',args.victim_instance_id,'--action','START')
            print('Restart requested for disposable test host',flush=True)
    remote('sudo k3s kubectl wait node '+shlex.quote(args.victim_node)+' --for=condition=Ready --timeout=300s',timeout=320)
    remote('sudo k3s kubectl -n pg-broker wait cluster/broker-db --for=condition=Ready --timeout=300s',timeout=320)
    assert k('exec',old_primary,'-c','postgres','--','psql','-U','postgres','-d','broker','-Atc','SELECT pg_is_in_recovery()').strip()=='t'
    print('PASS: original primary returned as a standby; three instances healthy',flush=True)


if __name__=='__main__':main()

#!/usr/bin/env python3
"""Restore a consistent logical backup into a NEW CNPG cluster; disposable K3s only.
Run after cnpg-vm.sh seed. Archives are kept locally with mode 0600.
"""
import json
import os
from pathlib import Path
import subprocess

K = ['sudo','k3s','kubectl','-n','pg-broker']
TARGET = 'broker-restore'

def k(*args, **kwargs):
    return subprocess.run([*K,*args],check=True,**kwargs)

def get(kind,name):
    return json.loads(k('get',kind,name,'-o','json',capture_output=True,text=True).stdout)

def apply(value):
    k('apply','-f','-',input=json.dumps(value),text=True)


def main():
    assert subprocess.run([*K,'get','cluster',TARGET],capture_output=True).returncode != 0, 'restore target already exists'
    source=get('cluster','broker-db')
    primary=source['status']['currentPrimary']
    os.umask(0o077)
    directory=Path('.dev/backups');directory.mkdir(parents=True,exist_ok=True)
    archive=directory/'broker.dump'
    # pg_dump uses one consistent snapshot while the source remains available.
    with archive.open('wb') as output:
        os.fchmod(output.fileno(), 0o600)
        k('exec',primary,'-c','postgres','--','pg_dump','-U','postgres','-Fc','broker',stdout=output)
    spec=source['spec']
    spec['bootstrap']={'initdb':{'database':'broker_restore','owner':'broker_app','dataChecksums':True}}
    # Certificates from the source cluster must not be reused by name.
    spec.pop('certificates',None)
    apply({'apiVersion':source['apiVersion'],'kind':'Cluster','metadata':{'name':TARGET},'spec':spec})
    k('wait','cluster/'+TARGET,'--for=condition=Ready','--timeout=600s')
    restored_primary=get('cluster',TARGET)['status']['currentPrimary']
    with archive.open('rb') as source_file:
        k('exec','-i',restored_primary,'-c','postgres','--','pg_restore','-U','postgres',
          '--exit-on-error','--single-transaction','--dbname','broker_restore',stdin=source_file)
    deployment=get('deployment','gateway')
    spec=deployment['spec']
    spec['replicas']=1
    spec['selector']['matchLabels']={'app':'pg-broker-gateway-restore'}
    spec['template']['metadata']={'labels':{'app':'pg-broker-gateway-restore'}}
    spec['template']['spec'].pop('affinity',None)
    container=spec['template']['spec']['containers'][0]
    for env in container['env']:
        if env['name']=='PGHOST':env['value']='broker-restore-rw'
        if env['name']=='PGDATABASE':env['value']='broker_restore'
        if env['name']=='ADVERTISED_HOST':env['value']='broker-restore.pg-broker.svc.cluster.local'
        if 'valueFrom' in env:
            ref=env['valueFrom'].get('secretKeyRef',{})
            if ref.get('name')=='broker-db-app':ref['name']='broker-restore-app'
    for volume in spec['template']['spec']['volumes']:
        if volume.get('secret',{}).get('secretName')=='broker-db-ca':volume['secret']['secretName']='broker-restore-ca'
    apply({'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'gateway-restore'},'spec':spec})
    apply({'apiVersion':'v1','kind':'Service','metadata':{'name':TARGET},
           'spec':{'selector':{'app':'pg-broker-gateway-restore'},'ports':[{'port':9092,'targetPort':'kafka'}]}})
    k('rollout','status','deployment/gateway-restore','--timeout=180s')
    k('exec','kafka-client','--','env','BOOTSTRAP_SERVERS=broker-restore.pg-broker.svc.cluster.local:9092',
      'python','/tmp/cnpg_smoke.py','resume')
    print(json.dumps({'result':'PASS','archive':str(archive),'bytes':archive.stat().st_size,
                      'source_cluster':'broker-db','restored_cluster':TARGET}),flush=True)
    # Keep the restored cluster for inspection; delete only this named test clone
    # after inspecting it, or destroy the entire disposable test environment.


if __name__=='__main__':main()

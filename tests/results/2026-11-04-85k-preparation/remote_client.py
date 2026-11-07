#!/usr/bin/env python3
"""Start or collect a bounded workload on a dedicated client; never replay a run."""
import argparse
import ipaddress
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile


def ssh(state, command, **kwargs):
    return subprocess.run([
        'ssh', '-i', state['key'], '-o', f"UserKnownHostsFile={state['known_hosts']}",
        '-o', 'StrictHostKeyChecking=yes', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        f"ubuntu@{state['host']}", shlex.join(command),
    ], timeout=120, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'collect'))
    parser.add_argument('directory', type=Path)
    parser.add_argument('--host')
    parser.add_argument('--key', type=Path)
    parser.add_argument('--known-hosts', type=Path)
    parser.add_argument('--label')
    parser.add_argument('--rate', choices=('smoke', '32k', '50k', '75k', '85k', '100k'))
    parser.add_argument('--bootstrap')
    parser.add_argument('--preflight', action='store_true')
    args = parser.parse_args()
    directory = args.directory.resolve()
    path = directory / 'control.json'
    if args.action == 'start':
        if not all((args.host, args.key, args.known_hosts, args.label, args.rate, args.bootstrap)):
            parser.error('start requires host, key, known-hosts, label, rate and bootstrap')
        if not re.fullmatch('[a-z0-9][a-z0-9-]{0,55}', args.label):
            parser.error('invalid label')
        ipaddress.ip_address(args.host)
        host, port = args.bootstrap.split(':')
        address = ipaddress.IPv4Address(host)
        if port not in ('9092', '19092') or not any(address in ipaddress.ip_network(cidr)
                for cidr in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')):
            parser.error('bootstrap must be RFC1918 IPv4 on port 9092 or 19092')
        if not args.key.is_file() or not args.known_hosts.is_file():
            parser.error('key and verified known-hosts files must exist')
        directory.mkdir(parents=True, exist_ok=False)
        state = dict(host=args.host, key=str(args.key.resolve()),
                     known_hosts=str(args.known_hosts.resolve()), label=args.label,
                     unit=f'omb-client-{args.label}.service', rate=args.rate,
                     bootstrap=args.bootstrap, preflight=args.preflight, phase='launch-requested')
        # Persist before the network request: an observation failure must never
        # cause a second launch. Use collect against this exact handle instead.
        path.write_text(json.dumps(state, indent=2) + '\n')
        command = ['sudo', 'systemd-run', f"--unit={state['unit']}", '--uid=ubuntu',
                   '--property=RuntimeMaxSec=1600', '--property=MemoryMax=2G',
                   '--property=AllowedCPUs=0,1', '--property=RemainAfterExit=yes', '/bin/bash',
                   '/home/ubuntu/pg-broker/benchmarks/omb/client-workload.sh',
                   args.label, args.rate, args.bootstrap]
        if args.preflight:
            command.append('--preflight')
        with (directory / 'launch.log').open('w') as output:
            ssh(state, command, stdout=output, stderr=subprocess.STDOUT, check=True)
    else:
        state = json.loads(path.read_text())
    result = ssh(state, ['systemctl', 'show', state['unit'], '-p', 'LoadState',
                        '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result',
                        '-p', 'ExecMainStatus', '-p', 'InvocationID'],
                 capture_output=True, text=True, check=True)
    status = dict(line.split('=', 1) for line in result.stdout.splitlines())
    (directory / 'service-status.txt').write_text(result.stdout)
    previous = state.get('invocation')
    invocation = status.get('InvocationID')
    if previous and invocation != previous:
        raise RuntimeError('service invocation changed; refusing unrelated evidence')
    if invocation:
        state['invocation'] = invocation
    state['phase'] = status.get('ActiveState', 'unknown')
    path.write_text(json.dumps(state, indent=2) + '\n')
    print(json.dumps(status, indent=2))
    if status.get('LoadState') == 'not-found':
        raise RuntimeError('service handle missing; inspect launch.log before any new launch')
    if status.get('ActiveState') not in ('inactive', 'failed') and status.get('SubState') != 'exited':
        return
    if not (directory / 'artifacts').exists():
        with (directory / 'service.log').open('w') as output:
            ssh(state, ['sudo', 'journalctl', '-u', state['unit'], '--no-pager'],
                stdout=output, stderr=subprocess.STDOUT, check=True)
        archive = directory / 'artifacts.tar.gz'
        with archive.open('wb') as output:
            ssh(state, ['tar', '-C', f"/home/ubuntu/results/omb-client-{state['label']}",
                        '-czf', '-', '.'], stdout=output, check=True)
        with tarfile.open(archive) as bundle:
            if sum(member.size for member in bundle.getmembers()) > 128 * 1024**2:
                raise RuntimeError('client evidence exceeds 128 MiB extraction bound')
            incoming = directory / 'artifacts.incoming'
            if incoming.exists():
                shutil.rmtree(incoming)
            bundle.extractall(incoming, filter='data')
            incoming.replace(directory / 'artifacts')
    marker = 'preflight-complete.txt' if state['preflight'] else 'complete.txt'
    if status.get('Result') != 'success' or status.get('ExecMainStatus') != '0' or not (
            directory / 'artifacts' / marker).is_file():
        raise RuntimeError('client run failed; retained service and workload evidence')
    state['phase'] = 'complete'
    path.write_text(json.dumps(state, indent=2) + '\n')
    print('Collected completed client evidence; this does not declare performance acceptance.')


if __name__ == '__main__':
    main()

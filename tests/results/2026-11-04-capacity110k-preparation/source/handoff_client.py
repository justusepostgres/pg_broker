#!/usr/bin/env python3
"""Advance one requested external-client phase; repeat safely to collect its result."""
import argparse
import fcntl
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import time

from remote_client import ssh


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='Local control directory for this phase')
    parser.add_argument('--pair', required=True)
    parser.add_argument('--system', required=True, choices=('kafka', 'broker'))
    parser.add_argument('--server-host', required=True, help='Server SSH IPv4 address')
    parser.add_argument('--server-private', required=True)
    parser.add_argument('--client-host', required=True, help='Client SSH IPv4 address')
    parser.add_argument('--key', type=Path, required=True)
    parser.add_argument('--server-known-hosts', type=Path, required=True)
    parser.add_argument('--client-known-hosts', type=Path, required=True)
    parser.add_argument('--wait', action='store_true',
                        help='Poll the same phase every 30 seconds for up to 30 minutes')
    args = parser.parse_args()
    if not re.fullmatch('[a-z0-9][a-z0-9-]{0,39}', args.pair):
        parser.error('invalid pair label')
    for host in (args.server_host, args.server_private, args.client_host):
        ipaddress.IPv4Address(host)
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'handoff.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.wait:
            wait_for_delivery(args, directory)
        else:
            advance(args, directory)


def wait_for_delivery(args, directory):
    """Keep the same durable handle; observation failures never trigger a launch."""
    deadline = time.monotonic() + 1800
    while True:
        # Exceptions propagate. The caller can resume with the same directory;
        # neither an SSH failure nor our timeout is evidence the remote job stopped.
        if advance(args, directory):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Observation deadline reached; remote run was not stopped. '
                               'Resume with the same directory to collect its original invocation.')
        time.sleep(min(30, remaining))


def advance(args, directory):
    server = dict(host=args.server_host, key=str(args.key.resolve()),
                  known_hosts=str(args.server_known_hosts.resolve()))
    out = f'/home/ubuntu/results/omb-pair-{args.pair}'
    requested = ssh(server, ['cat', f'{out}/{args.system}-request.json'],
                    capture_output=True, text=True, check=True)
    request = json.loads(requested.stdout)
    port = 19092 if args.system == 'kafka' else 9092
    expected = dict(label=f'{args.pair}-{args.system}', rate=request.get('rate'),
                    bootstrap=f'{args.server_private}:{port}', system=args.system)
    if request != expected or request['rate'] not in ('32k', '50k', '75k', '85k', '100k', '110k'):
        raise ValueError('request does not match the selected pair, system or private endpoint')
    saved = directory / 'request.json'
    if saved.exists() and json.loads(saved.read_text()) != request:
        raise ValueError('request changed; refusing to replay a different workload')
    saved.write_text(json.dumps(request, indent=2) + '\n')
    control = directory / 'client'
    tool = Path(__file__).with_name('remote_client.py')
    if not control.exists():
        if (directory / 'delivered.json').exists():
            raise ValueError('delivery recorded but client handle missing; refusing a new launch')
        command = [sys.executable, str(tool), 'start', str(control), '--host', args.client_host,
                   '--key', str(args.key), '--known-hosts', str(args.client_known_hosts),
                   '--label', request['label'], '--rate', request['rate'],
                   '--bootstrap', request['bootstrap']]
    else:
        state = json.loads((control / 'control.json').read_text())
        if (state['host'], state['label'], state['rate'], state['bootstrap'], state['preflight']) != (
                args.client_host, request['label'], request['rate'], request['bootstrap'], False):
            raise ValueError('existing client handle differs from request')
        if (directory / 'delivered.json').exists():
            delivery = json.loads((directory / 'delivered.json').read_text())
            if delivery['invocation'] != state.get('invocation') or state['phase'] != 'complete':
                raise ValueError('delivery disagrees with completed client invocation')
            print('Completed client phase already delivered; no launch or transfer repeated.')
            return True
        command = [sys.executable, str(tool), 'collect', str(control)]
    subprocess.run(command, check=True)
    state = json.loads((control / 'control.json').read_text())
    if state['phase'] != 'complete':
        print('Client still running; repeat this command against the same directory to collect.')
        return False
    archive = directory / 'handoff.tar.gz'
    if not archive.exists():
        temporary = directory / 'handoff.tar.gz.incoming'
        with tarfile.open(temporary, 'w:gz') as bundle:
            for file in sorted((control / 'artifacts').iterdir()):
                if not file.is_file() or file.is_symlink():
                    raise ValueError('unexpected client artifact type')
                bundle.add(file, arcname=file.name)
            bundle.add(control / 'service-status.txt', arcname='service-status.txt')
        temporary.replace(archive)
    with archive.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    remote_archive = f'{out}/{args.system}-handoff.tar.gz'
    # Stream through SSH: only the controller holds the SSH private key.
    with archive.open('rb') as source:
        ssh(server, ['sh', '-c', f'cat > {shlex.quote(remote_archive)}'], stdin=source, check=True)
    incoming, final = f'{out}/client-{args.system}.incoming', f'{out}/client-{args.system}'
    manifest = f'{out}/{args.system}-handoff.sha256'
    # A previously successful rename may have outlived a lost SSH response.
    # Verify the archive identity and reuse that delivery, never overwrite it.
    program = f'''set -eu
echo {shlex.quote(digest + '  ' + remote_archive)} | sha256sum -c -
if test -d {shlex.quote(final)}; then
 test "$(cat {shlex.quote(manifest)})" = {shlex.quote(digest)}
else
 mkdir -p {shlex.quote(incoming)}
 tar -xzf {shlex.quote(remote_archive)} -C {shlex.quote(incoming)}
 test -s {shlex.quote(incoming + '/complete.txt')}
 echo {shlex.quote(digest)} > {shlex.quote(manifest)}
 mv {shlex.quote(incoming)} {shlex.quote(final)}
fi'''
    ssh(server, ['sh', '-c', program], check=True)
    (directory / 'delivered.json').write_text(json.dumps(
        dict(archive_sha256=digest, invocation=state['invocation'], destination=final), indent=2) + '\n')
    print('Completed client evidence atomically delivered to the waiting server runner.')
    return True


if __name__ == '__main__':
    main()

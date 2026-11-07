"""Launch one fresh, opposite-order repeat only after exact trial-1 collection."""
from pathlib import Path
import json, sys, hashlib
sys.path.insert(0, 'benchmarks/omb')
from remote_client import ssh
from read_only_collection import service
from compare import compare
root = Path.cwd()
marker = root / '.dev/omb-cloud-capacity85k2.json'
assert not marker.exists(), 'prior launch intent exists; inspect its exact invocation, never replay'
previous = json.loads((root / '.dev/omb-cloud-capacity85k1.json').read_text())
assert previous['pair_phase'] == 'complete', 'first trial must be collected and verified'
evidence = root / 'benchmarks/omb/results/2026-11-04/capacity85k-deployment'
manifest = json.loads((evidence / 'manifest.json').read_text())
binary = manifest['binary_sha256']
report = compare([root / 'benchmarks/omb/results/2026-11-04/capacity85k-1'],
                 verify_environment=True, expected_sql_version='0.1.18', expected_gateway_sha256=binary)
for system in ('kafka', 'broker'):
    assert report['trials'][0]['results'][system]['measurement']['timing_verified']
assert report['trials'][0]['offered_messages_per_second'] == 85000
server = dict(host='170.9.24.220', key=str(root / '.dev/oci-cnpg-key'), known_hosts=str(root / '.dev/oci-omb-known-hosts'))
for unit, invocation in [(previous['unit'], previous['controller_invocation']),
                         ('omb-cloud-pair-018-capacity85k-1.service', 'e3e4046642094bc19b7be1ccfe9f093e')]:
    observed = service(server, unit, invocation)
    assert observed['MainPID'] == '0' and observed['SubState'] == 'exited' and observed['Result'] == 'success' and observed['ExecMainStatus'] == '0', observed
# Preserve exactly the deployed artifacts; local report-only changes are not deployed.
paths = {'controller': '/home/ubuntu/omb-cloud-controller', 'server': '/home/ubuntu/pg-broker/benchmarks/omb', 'sql': '/home/ubuntu/pg-broker/crates/extension/sql'}
checks = []
for name, digest in manifest['hashes'].items():
    assert hashlib.sha256((evidence / name).read_bytes()).hexdigest() == digest
    prefix, filename = name.split('/')
    if prefix in paths:
        checks.append(f'{digest}  {paths[prefix]}/{filename}\n')
ssh(server, ['sha256sum', '-c', '-'], input=''.join(checks), text=True, capture_output=True, check=True)
client_checks = ''.join(f'{digest}  /home/ubuntu/pg-broker/benchmarks/omb/{name.split("/")[1]}\n' for name,digest in manifest['hashes'].items() if name.startswith('client/'))
ssh(server, ['ssh', '-i', '/home/ubuntu/omb-cloud-controller/client-key', '-o', 'UserKnownHostsFile=/home/ubuntu/omb-cloud-controller/known-hosts', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'ubuntu@10.84.0.134', 'sha256sum -c -'], input=client_checks, text=True, capture_output=True, check=True)
label = '018-capacity85k-2'
unit = 'omb-cloud-controller-capacity85k-2.service'
info = dict(phase='launch-requested', label=label, unit=unit, order='broker-first', manifest=manifest,
            predecessor_invocation='e3e4046642094bc19b7be1ccfe9f093e')
with marker.open('x') as f:
    json.dump(info, f, indent=2)
# No retry around mutation. If SSH fails after acceptance, inspect this unique unit.
ssh(server, ['sudo', 'systemd-run', '--unit='+unit, '--uid=ubuntu', '--property=RuntimeMaxSec=4300', '--property=RemainAfterExit=yes', '--property=AllowedCPUs=2,3', '--property=MemoryMax=256M', '/usr/bin/python3', '-u', '/home/ubuntu/omb-cloud-controller/cloud-controller.py', label, '--order', 'broker-first', '--rate', '85k', '--runner-sha256', manifest['hashes']['server/oci-pair.sh'], '--volume-size-gib', '1024', '--candidate-pair', '--partition-guard-candidate', '--bulk-commit-binary', '/home/ubuntu/candidate-queryreset018/pg-broker-gateway', '--bulk-commit-sha256', binary], check=True)
r = ssh(server, ['systemctl', 'show', unit, '-p', 'InvocationID', '-p', 'MainPID', '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus'], capture_output=True, text=True, check=True)
observed = dict(line.split('=',1) for line in r.stdout.splitlines())
assert observed['InvocationID']
info.update(phase='observing', controller_invocation=observed['InvocationID'], observed=observed)
marker.write_text(json.dumps(info, indent=2)+'\n')
print(json.dumps(info, indent=2))

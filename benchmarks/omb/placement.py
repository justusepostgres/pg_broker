"""Validate captured client placement; do not infer it from bootstrap addresses."""
import hashlib
import ipaddress
import json
from pathlib import Path

PAYLOAD_SHA256 = 'cda43e4dbb40bd54370afdd28c063e85c25b57de0defd9be7493750fd7c14217'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def placement(directory):
    root = Path(directory)
    path = root / 'placement.json'
    if not path.exists():
        require(not any((root / f'client-{s}').exists() for s in ('kafka', 'broker')),
                'missing client placement marker')
        return {'recorded': False}
    value = json.loads(path.read_text())
    require(set(value) == {'kind', 'server_host'}, 'invalid placement fields')
    address = ipaddress.IPv4Address(value['server_host'])
    if value['kind'] == 'loopback':
        require(str(address) == '127.0.0.1', 'invalid loopback placement')
        require(not any((root / f'client-{s}').exists() for s in ('kafka', 'broker')),
                'external client artifacts contradict loopback placement')
        return {'recorded': True, 'settings': value}
    require(value['kind'] == 'separate-client' and any(
        address in ipaddress.ip_network(cidr) for cidr in
        ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')), 'invalid separate-client placement')
    contracts, units = [], []
    for system, port in (('kafka', 19092), ('broker', 9092)):
        p = root / f'client-{system}'
        require(bool((p / 'complete.txt').read_text().strip()) and
                not (p / 'preflight-complete.txt').exists(), 'client workload not complete')
        require((p / 'bootstrap.txt').read_text().strip() == f'{address}:{port}',
                'client bootstrap disagrees with placement')
        require((p / 'heap.txt').read_text().strip() == '-Xms1G -Xmx1G', 'client heap differs')
        require((p / 'cpu-count.txt').read_text().strip() == '2', 'client CPU count differs')
        require((p / 'java.txt').read_bytes() == (root / 'java.txt').read_bytes(), 'client Java differs')
        require((p / 'host.txt').read_text().strip() != (root / 'host.txt').read_text().strip(),
                'client host is not independent')
        require(not (p / 'tracked-changes.txt').read_text().strip(), 'modified upstream client')
        require((p / 'revision.txt').read_bytes() == (root / f'{system}.json.revision').read_bytes(),
                'client revision differs')
        limits = dict(line.split('=', 1) for line in (p / 'limits.txt').read_text().splitlines())
        require(limits == {'RuntimeMaxUSec': '26min 40s', 'AllowedCPUs': '0-1',
                           'MemoryMax': '2147483648'}, 'client resource limits differ')
        unit = (p / 'unit-name.txt').read_text().strip()
        require(unit.startswith('omb-client-') and unit.endswith(f'-{system}.service'),
                'invalid client unit identity')
        units.append(unit)
        status = dict(line.split('=', 1) for line in (p / 'service-status.txt').read_text().splitlines())
        require(status.get('Result') == 'success' and status.get('ExecMainStatus') == '0' and
                status.get('SubState') == 'exited' and bool(status.get('InvocationID')),
                'client service completion not verified')
        payload, payload_name = (p / 'payload-checksum.txt').read_text().split()
        require(payload == PAYLOAD_SHA256 and Path(payload_name).name == 'payload-1Kb.data',
                'client payload differs from pinned upstream fixture')
        scripts = {}
        for line in (p / 'script-checksums.txt').read_text().splitlines():
            digest, name = line.split(maxsplit=1)
            name = Path(name).name
            require(name not in scripts and hashlib.sha256((p / name).read_bytes()).hexdigest() == digest,
                    'client script provenance differs')
            scripts[name] = digest
        require(set(scripts) == {'runner.sh', 'run.sh', 'execute.py', 'summarize.py'},
                'missing client script provenance')
        for suffix in ('', '.log', '.driver.yaml', '.workload.yaml', '.revision', '.process.txt'):
            require((p / f'result.json{suffix}').read_bytes() ==
                    (root / f'{system}.json{suffix}').read_bytes(), 'client result handoff differs')
        contracts.append(dict(host=(p / 'host.txt').read_text().strip(), limits=limits,
                              java=(p / 'java.txt').read_text().strip(), scripts=scripts,
                              payload_sha256=payload))
    require(contracts[0] == contracts[1], 'client host/configuration differs across systems')
    return {'recorded': True, 'settings': value, 'client_contract': contracts[0], 'units': units}

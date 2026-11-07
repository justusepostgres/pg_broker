"""Validate captured RF=1 comparison evidence, not live-host attestation."""
import hashlib
import json
import re
from pathlib import Path
from placement import placement

GIB = 1024 ** 3


def require(condition, message):
    if not condition:
        raise ValueError(message)


def instrumentation(directory):
    path = Path(directory) / 'instrumentation.json'
    if not path.exists():
        # Historical captures did not record this field. Absence is not proof
        # that a run had no instrumentation.
        return {'recorded': False}
    value = json.loads(path.read_text())
    require(isinstance(value, dict) and isinstance(value.get('kind'), str)
            and bool(value['kind']) and type(value.get('acceptance_run')) is bool,
            'invalid instrumentation provenance')
    return {'recorded': True, 'settings': value}


def wal_settings(directory):
    p = Path(directory)
    actual, requested = p / 'wal-settings.txt', p / 'wal-init-zero.txt'
    if not actual.exists() and not requested.exists():
        return {'recorded': False}
    mode = requested.read_text().strip()
    require(mode in ('on', 'off'), 'invalid requested WAL initialization')
    value = actual.read_text().strip()
    require(value == f'{mode}|on|16MB|fdatasync|on|on|on', 'WAL/durability settings differ')
    return {'recorded': True, 'settings': value}


def properties(text):
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, value = line.split('=', 1)
        require(key.strip() not in result, f'duplicate property: {key}')
        result[key.strip()] = value.strip()
    return result


def kafka_flush_policy(directory, verify_effective=False):
    p = Path(directory)
    marker, config_path = p / 'kafka-flush-policy.txt', p / 'kafka.properties'
    if not marker.exists() and not config_path.exists():
        return {'recorded': False}
    config = properties(config_path.read_text())
    mode = marker.read_text().strip() if marker.exists() else 'per-append'
    require(mode in ('per-append', 'background'), 'invalid Kafka flush policy')
    messages = '1' if mode == 'per-append' else '9223372036854775807'
    require(config.get('log.flush.interval.messages') == messages, 'Kafka flush policy contradicts configuration')
    require('log.flush.interval.ms' not in config and 'log.flush.scheduler.interval.ms' not in config,
            'unexpected Kafka flush timer override')
    if verify_effective and marker.exists():
        expected = {'log.flush.interval.messages': messages, 'log.flush.interval.ms': 'null',
                    'log.flush.scheduler.interval.ms': '9223372036854775807'}
        for name in ('kafka-server.log', 'kafka-recovery.log'):
            log = (p / name).read_text()
            for field, value in expected.items():
                observed = re.findall(rf'(?m)\b{re.escape(field)} = (\S+)\s*$', log)
                require(bool(observed) and set(observed) == {value},
                        f'Kafka effective flush setting differs: {name} {field}')
    return {'recorded': True, 'mode': mode, 'flush_messages': messages,
            'scope': ('Explicit Kafka local-flush reference.' if mode == 'per-append' else
                      'Kafka default-flush reference; local acknowledgment durability differs from PostgreSQL.')}


def cpus(text):
    result = set()
    for term in text.split(','):
        bounds = term.split('-')
        if len(bounds) == 1:
            result.add(int(bounds[0]))
        else:
            require(len(bounds) == 2, 'invalid CPU range')
            low, high = map(int, bounds)
            require(0 <= low <= high <= 4096, 'invalid CPU range')
            result.update(range(low, high + 1))
    return result


def offsets(text):
    rows = [line.split(':') for line in text.splitlines()]
    require(len(rows) == 16 and all(len(r) == 3 for r in rows), 'expected 16 Kafka offsets')
    require(len({r[0] for r in rows}) == 1 and rows[0][0].startswith('test-topic-'), 'unexpected Kafka topic')
    require({int(r[1]) for r in rows} == set(range(16)), 'Kafka partition set differs')
    require(all(int(r[2]) > 0 for r in rows), 'Kafka partition has no data')
    return sorted((r[0], int(r[1]), int(r[2])) for r in rows)


def verify(directory, *, expected_sql_version="0.1.7", expected_gateway_sha256=None):
    require(expected_sql_version in ("0.1.7", "0.1.18"), "unsupported expected SQL version")
    if expected_sql_version != "0.1.7" or expected_gateway_sha256 is not None:
        require(isinstance(expected_gateway_sha256, str) and re.fullmatch("[a-f0-9]{64}", expected_gateway_sha256),
                "explicit reviewed gateway digest required")
    p = Path(directory)
    require(bool((p / 'complete.txt').read_text().strip()), 'missing completion marker')
    order = (p / 'order.txt').read_text().strip()
    require(order in ('kafka-first', 'broker-first'), 'invalid run order')
    limits = properties((p / 'kafka-limits.txt').read_text())
    require(cpus(limits['AllowedCPUs']) == {0, 1} and int(limits['MemoryMax']) == 9 * GIB,
            'Kafka resource limits differ')
    broker = [line.split() for line in (p / 'broker-limits.txt').read_text().splitlines()]
    require(len(broker) == 2 and all(len(r) == 3 for r in broker), 'missing broker limits')
    expected = {'-db': 8 * GIB, '-gateway': GIB}
    for suffix, memory in expected.items():
        rows = [r for r in broker if r[0].endswith(suffix)]
        require(len(rows) == 1 and cpus(rows[0][1]) == {0, 1} and int(rows[0][2]) == memory,
                f'broker {suffix} resource limits differ')
    require((p / 'storage.txt').read_text().splitlines() == [expected_sql_version+'|on|on|on|t|t', '16|t'],
            'broker version, durability, permissions or ranges differ')
    n, valid = (p / 'compression.txt').read_text().strip().split('|')
    require(int(n) > 0 and valid == 't', 'LZ4 storage check failed')
    require(offsets((p / 'kafka-offsets-before.txt').read_text()) ==
            offsets((p / 'kafka-offsets-after.txt').read_text()), 'Kafka restart offsets differ')
    config = properties((p / 'kafka.properties').read_text())
    flush_policy = kafka_flush_policy(p, verify_effective=True)
    for key, value in {'default.replication.factor': '1',
                       'min.insync.replicas': '1', 'num.partitions': '16',
                       'auto.create.topics.enable': 'false'}.items():
        require(config.get(key) == value, f'Kafka {key} differs')
    require(config.get('log.dirs', '').startswith('/home/ubuntu/omb-pair-') and
            config['log.dirs'].endswith('-kafka-data'), 'unexpected Kafka data directory')
    stores = [config['log.dirs']] + [r[0] for r in broker]
    config['log.dirs'] = '<fresh-trial-store>'
    volume = json.loads((p / 'volume-profile.json').read_text())
    size, vpus = volume['size_in_gbs'], volume['vpus_per_gb']
    require(isinstance(size, int) and size > 0 and isinstance(vpus, int) and vpus >= 0, 'invalid volume profile')
    disks = [r.split() for r in (p / 'disks.txt').read_text().splitlines()]
    disk = [r for r in disks if r and r[0] == 'sda']
    require(len(disk) == 1 and int(disk[0][1]) == size * GIB, 'guest disk size disagrees with volume profile')
    sql = {}
    for line in (p / 'sql-checksums.txt').read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        name = Path(name).name
        require(name not in sql, 'duplicate SQL checksum')
        require(hashlib.sha256((p / name).read_bytes()).hexdigest() == digest, f'SQL checksum differs: {name}')
        sql[name] = digest
    expected_sql = {'pg_broker--0.1.5--0.1.6.sql', 'pg_broker--0.1.6--0.1.7.sql', 'verify-storage.sql'}
    if expected_sql_version == '0.1.18':
        from sql_contract import SQL18_UPGRADE_SHA256
        expected_sql.update(SQL18_UPGRADE_SHA256)
        require(all(sql.get(name) == digest for name,digest in SQL18_UPGRADE_SHA256.items()),
                'unreviewed SQL migration')
    if expected_gateway_sha256 is not None:
        require((p/'gateway-sha256.txt').read_text().split()[0] == expected_gateway_sha256,
                'gateway digest differs from reviewed candidate')
    require(set(sql) == expected_sql,
            'missing SQL provenance')
    # These fields must match between trials. They are captured evidence, not
    # independent proof of host identity, binary contents or deployment safety.
    client_placement = placement(p)
    stores.extend(client_placement.pop('units', []))
    gateway_digest = None
    if (p / 'gateway-sha256.txt').exists():
        marker = (p / 'gateway-sha256.txt').read_text().strip()
        match = re.fullmatch(r'([a-f0-9]{64})  (/home/ubuntu/candidate-[a-z0-9-]+/pg-broker-gateway)', marker)
        require(match is not None, 'invalid gateway checksum provenance')
        require((p / 'binaries.txt').read_text().strip() == match[2] + ': OK',
                'gateway checksum check differs')
        gateway_digest = match[1]
    contract = dict(instrumentation=instrumentation(p), wal=wal_settings(p), placement=client_placement,
                    gateway_sha256=gateway_digest,
                    kafka_flush_policy=flush_policy,
                    volume_size_gib=size, volume_vpus_per_gb=vpus, kafka_config=config, sql=sql,
                    image=(p / 'image.txt').read_text().strip(),
                    extension_binary=(p / 'extension-binary.txt').read_text().strip(),
                    gateway_check=(p / 'binaries.txt').read_text().strip(),
                    runner_sha256=hashlib.sha256((p / 'runner.sh').read_bytes()).hexdigest(),
                    host=(p / 'host.txt').read_text().strip(), java=(p / 'java.txt').read_text().strip())
    require(all(contract[k] for k in ('image', 'extension_binary', 'gateway_check', 'host', 'java')),
            'missing runtime provenance')
    return dict(order=order, stores=stores, contract=contract,
                scope='Captured RF=1 environment and restart evidence only; no live-host attestation or performance acceptance.')

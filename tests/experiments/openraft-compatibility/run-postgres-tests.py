#!/usr/bin/env python3
"""Run isolated Raft probes on three disposable PostgreSQL 17 servers.

Requires the locally installed pg_broker SQL0.1.20 extension. Never targets an
existing database. Results and server logs survive cleanup of disposable data.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pg-bindir', type=Path, help='directory containing PostgreSQL 17 binaries')
    parser.add_argument('--output', type=Path, help='new evidence directory; existing paths are refused')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    bindir = args.pg_bindir or Path(subprocess.check_output(['pg_config', '--bindir'], text=True).strip())
    bindir = bindir.resolve()
    version = subprocess.check_output([str(bindir / 'postgres'), '--version'], text=True)
    if 'PostgreSQL) 17.' not in version:
        raise ValueError('This probe requires PostgreSQL 17')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H%M%SZ')
    out = (args.output or root / 'tests/results' / (stamp + '-alpha-quorum')).resolve()
    data = root / '.dev/alpha-quorum-crash-native-pg'
    if data.exists():
        raise FileExistsError(f'{data}: inspect the prior run; this runner will not reuse or remove it')
    out.mkdir(parents=True, exist_ok=False)
    data.mkdir(parents=True, exist_ok=False)

    def run(*command):
        return subprocess.run([str(bindir / command[0]), *map(str, command[1:])],
                              check=True, text=True, capture_output=True, timeout=30)

    # Record a node before starting it, so cleanup also covers an ambiguous start.
    created = []
    try:
        dsns = []
        identities = []
        for index in range(3):
            node = data / f'node{index}'
            port = 55433 + index
            run('initdb', '-D', node, '-U', 'postgres', '--auth=trust')
            created.append(node)
            options = f'-p {port} -h 127.0.0.1 -k /tmp -c shared_buffers=16MB -c max_connections=20'
            run('pg_ctl', '-D', node, '-l', out / f'server-{index}.log', '-o', options, '-w', 'start')
            run('psql', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-v', 'ON_ERROR_STOP=1',
                '-c', "CREATE EXTENSION pg_broker VERSION '0.1.20'")
            identity = run('psql', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-At', '-c',
                "SELECT json_build_object('system_identifier',system_identifier::text,"
                "'port',current_setting('port'),'data_directory',current_setting('data_directory'),"
                "'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'),"
                "'synchronous_commit',current_setting('synchronous_commit')) FROM pg_control_system()")
            captured = json.loads(identity.stdout)
            assert Path(captured['data_directory']).resolve() == node.resolve()
            assert captured['port'] == str(port)
            identities.append(captured)
            dsns.append(f'host=127.0.0.1 port={port} user=postgres dbname=postgres')
        assert len({row['system_identifier'] for row in identities}) == 3
        assert all(row[k] == 'on' for row in identities for k in ('fsync','full_page_writes','synchronous_commit'))
        (out / 'storage-identities.json').write_text(json.dumps(identities, indent=2) + '\n')
        env = dict(os.environ, DATABASE_URL=dsns[0], PG_BROKER_TEST_QUORUM_DSNS=json.dumps(dsns),
                   PG_BROKER_TEST_QUORUM_ROOT=str(data), PG_BROKER_TEST_PG_CTL=str(bindir / 'pg_ctl'),
                   PG_BROKER_TEST_ALLOW_CRASH='1', CARGO_INCREMENTAL='0',
                   CARGO_PROFILE_DEV_DEBUG='0', CARGO_BUILD_JOBS='1')
        command = ['cargo', 'test', '--locked', '--manifest-path',
                   'tests/experiments/openraft-compatibility/Cargo.toml',
                   '--target-dir', 'target/openraft-format-probe']
        results = []
        for name, argv in [('unit', command), ('postgres', command + ['--', '--ignored', '--test-threads=1'])]:
            started = time.monotonic()
            with (out / f'{name}.log').open('w') as log:
                result = subprocess.run(argv, cwd=root, env=env, stdout=log,
                                        stderr=subprocess.STDOUT, timeout=300)
            results.append(dict(suite=name, exit_code=result.returncode, seconds=time.monotonic()-started))
            (out / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
            print((out / f'{name}.log').read_text(), flush=True)
            if result.returncode:
                raise RuntimeError(f'{name} failed; evidence: {out}')
    finally:
        for node in created:
            status = subprocess.run([str(bindir / 'pg_ctl'), '-D', str(node), 'status'],
                                    capture_output=True, timeout=15)
            if status.returncode == 0:
                run('pg_ctl', '-D', node, '-m', 'fast', '-w', 'stop')
            elif status.returncode != 3:
                raise RuntimeError(f'Cannot verify {node} stopped; retaining data')
        for index in range(3):
            restart = data / f'node{index}/restart.log'
            if restart.exists():
                shutil.copyfile(restart, out / f'restart-{index}.log')
        shutil.rmtree(data)
    print(f'Verified evidence: {out}')


if __name__ == '__main__':
    main()

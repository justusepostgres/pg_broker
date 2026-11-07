#!/usr/bin/env python3
"""Run a fresh fixed-load pair and both handoffs on the benchmark server.

Run as ubuntu in a bounded systemd unit on CPUs 2-3. No desktop process is
required after launch. Refuses existing control directories; never replays a
launch. A failed pair remains failed and must be collected separately.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

import handoff_client
from compare import compare

RUNNER_SHA = 'a0ef0ceb0c2674d0e863a0c4c8a95d620e9df0e0f66b903fa23f535f4dab8647'
BINARY_SHA = '0e0b5024eb643859f18b23a111870a45f28a2e65e522505ddeac8592bddb41fc'
BINARY = '/home/ubuntu/candidate-producebatch017/pg-broker-gateway'
RUNNER = '/home/ubuntu/pg-broker/benchmarks/omb/oci-pair.sh'


def command(args, **kwargs):
    return subprocess.run(args, check=True, timeout=120, **kwargs)


def service(unit):
    result = command(['systemctl', 'show', unit, '-p', 'InvocationID', '-p', 'MainPID',
                      '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus'],
                     text=True, capture_output=True)
    return dict(line.split('=', 1) for line in result.stdout.splitlines())


def local_server_ssh(state, args, **kwargs):
    # Handoff's server commands execute locally; only client collection uses SSH.
    if state['host'] != '127.0.0.1':
        raise ValueError('server handoff must be local')
    return subprocess.run(args, timeout=120, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('label')
    parser.add_argument('--order', choices=('broker-first', 'kafka-first'), default='broker-first')
    parser.add_argument('--rate', choices=('75k', '100k'), default='75k')
    parser.add_argument('--runner-sha256', default=RUNNER_SHA)
    parser.add_argument('--volume-size-gib', type=int, default=640)
    parser.add_argument('--diagnostic-broker-only', action='store_true')
    parser.add_argument('--diagnostic-timing', action='store_true')
    parser.add_argument('--diagnostic-commit-delay-us', type=int, choices=(0,500), default=0)
    parser.add_argument('--bulk-commit-binary')
    parser.add_argument('--bulk-commit-sha256')
    parser.add_argument('--partition-guard-candidate',action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,32}', args.label):
        parser.error('invalid unique label')
    if not re.fullmatch(r'[a-f0-9]{64}', args.runner_sha256):
        parser.error('runner SHA-256 must be a lowercase 64-character digest')
    if args.volume_size_gib < 640:
        parser.error('volume must be at least 640 GiB for these workloads')
    if args.rate == '100k' and args.runner_sha256 == RUNNER_SHA:
        parser.error('100k requires the reviewed runner that supports 100k, with its explicit digest')
    if args.diagnostic_broker_only and (args.rate != '100k' or args.order != 'broker-first'):
        parser.error('broker-only diagnosis requires 100k and broker-first')
    if args.diagnostic_timing and not args.diagnostic_broker_only:
        parser.error('timing requires broker-only diagnostic mode')
    if args.diagnostic_commit_delay_us and not args.diagnostic_timing:
        parser.error('commit-delay experiment requires diagnostic timing')
    candidate = args.bulk_commit_binary is not None or args.bulk_commit_sha256 is not None
    if args.partition_guard_candidate and not candidate:
        parser.error('partition guard candidate requires the explicit bulk candidate binary')
    if candidate:
        if not args.diagnostic_timing or args.diagnostic_commit_delay_us:
            parser.error('bulk commit candidate requires timed diagnosis with default commit delay')
        if not re.fullmatch(r'/home/ubuntu/candidate-[a-z0-9-]+/pg-broker-gateway', args.bulk_commit_binary or ''):
            parser.error('invalid candidate binary path')
        if not re.fullmatch(r'[a-f0-9]{64}', args.bulk_commit_sha256 or ''):
            parser.error('candidate requires explicit SHA-256')
    binary = args.bulk_commit_binary if candidate else BINARY
    binary_sha = args.bulk_commit_sha256 if candidate else BINARY_SHA
    sql_version = ('0.1.18' if args.partition_guard_candidate else '0.1.17') if candidate else '0.1.7'
    root = Path(__file__).resolve().parent
    out = root / ('control-' + args.label)
    out.mkdir(exist_ok=False)
    unit = 'omb-cloud-pair-' + args.label + '.service'
    pair = Path('/home/ubuntu/results') / ('omb-pair-' + args.label)
    state = dict(binary=binary,binary_sha256=binary_sha,sql_version=sql_version,phase='preflight', unit=unit, label=args.label, order=args.order,
                 rate=args.rate, runner_sha256=args.runner_sha256,
                 volume_size_gib=args.volume_size_gib, vpus_per_gb=10,
                 diagnostic_broker_only=args.diagnostic_broker_only,
                 diagnostic_timing=args.diagnostic_timing,
                 diagnostic_commit_delay_us=args.diagnostic_commit_delay_us,
                 scope=('Instrumented broker-only diagnosis; not acceptance' if args.diagnostic_broker_only else
                        'Cloud-hosted handoff; fixed RF1 measurement, not capacity or distributed acceptance'))
    def save(**fields):
        state.update(fields)
        temporary = out / 'state.incoming'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(out / 'state.json')
    save()
    try:
        assert not pair.exists(), 'pair directory already exists'
        assert not command(['sudo','docker','ps','-q'], capture_output=True,text=True).stdout.strip()
        assert subprocess.run(['pgrep','-x','java'],capture_output=True).returncode == 1
        minimum_free = 125 if args.rate == '100k' else 95
        command(['bash','-c', 'exec 9>/home/ubuntu/omb-pair.lock; flock -n 9; '
                 'test "$(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9)" '
                 f'-gt {minimum_free}'])
        command(['sha256sum','-c','-'],input=f'{args.runner_sha256}  {RUNNER}\n{binary_sha}  {binary}\n',text=True)
        profile=json.loads(Path('/home/ubuntu/omb-volume-profile.json').read_text())
        assert profile['size_in_gbs']==args.volume_size_gib and profile['vpus_per_gb']==10
        save(phase='launch-requested')
        command(['sudo','systemd-run','--unit='+unit,'--uid=ubuntu',
                 '--property=RuntimeMaxSec=3600','--property=RemainAfterExit=yes',
                 '--setenv=OMB_EXTERNAL_CLIENT=1','--setenv=OMB_SERVER_HOST=10.84.0.247',
                 '--setenv=OMB_DIAGNOSTIC_BROKER_ONLY='+str(int(args.diagnostic_broker_only)),
                 '--setenv=OMB_DIAGNOSTIC_TIMING='+str(int(args.diagnostic_timing)),
                 '--setenv=OMB_DIAGNOSTIC_COMMIT_DELAY_US='+str(args.diagnostic_commit_delay_us),
                 '--setenv=OMB_SQL_VERSION='+sql_version,
                 '--setenv=OMB_KAFKA_FLUSH_POLICY=background','--setenv=OMB_GATEWAY_PATH='+binary,
                 '--setenv=OMB_GATEWAY_SHA256='+binary_sha,'/bin/bash',RUNNER,args.label,args.rate,args.order,'off'])
        observed=service(unit)
        assert observed['InvocationID'] and observed['SubState']=='running', observed
        invocation=observed['InvocationID']
        save(phase='running',invocation=invocation)
        handoff_client.ssh=local_server_ssh
        systems=('broker','kafka') if args.order=='broker-first' else ('kafka','broker')
        if args.diagnostic_broker_only:systems=('broker',)
        for system in systems:
            phase=out/system;phase.mkdir()
            handoff_args=argparse.Namespace(server_host='127.0.0.1',server_private='10.84.0.247',
                client_host='10.84.0.134',key=root/'client-key',server_known_hosts=root/'known-hosts',
                client_known_hosts=root/'known-hosts',pair=args.label,system=system)
            deadline=time.monotonic()+1800
            while True:
                observed=service(unit)
                assert observed['InvocationID']==invocation and observed['SubState']=='running',observed
                if (pair/(system+'-request.json')).exists():
                    try:
                        if handoff_client.advance(handoff_args,phase):break
                    except subprocess.CalledProcessError as error:
                        # Durable handles in advance prevent a duplicate client launch.
                        save(phase='handoff-retry',system=system,last_error=str(error))
                        print('Same-handle handoff retry:',error,flush=True)
                if time.monotonic()>deadline:raise TimeoutError('Handoff observation expired; do not replay pair')
                time.sleep(10)
            save(phase='delivered',system=system)
        deadline=time.monotonic()+300
        while True:
            observed=service(unit)
            assert observed['InvocationID']==invocation,observed
            if observed['MainPID']=='0':
                assert observed['SubState']=='exited' and observed['Result']=='success' and observed['ExecMainStatus']=='0',observed
                break
            if time.monotonic()>deadline:raise TimeoutError('Recovery observation expired; inspect original pair')
            time.sleep(10)
        assert (pair/'complete.txt').is_file()
        if args.diagnostic_broker_only:
            from summarize import summarize
            from measurement import verify_measurement
            result=json.loads((pair/'broker.json').read_text())
            timing=verify_measurement(result,(pair/'broker.json.log').read_text(),15,require_timing=True)
            assert json.loads((pair/'instrumentation.json').read_text())['acceptance_run'] is False
            sampling=json.loads((pair/'sampling-complete.json').read_text())
            assert sampling['samples'] > 0 and sampling['sample_errors'] == 0, sampling
            if args.diagnostic_timing:
                for name in ('before','after'):
                    snapshot=json.loads((pair/f'diagnostic-stats-{name}.json').read_text())
                    assert snapshot['io_timing']==snapshot['wal_io_timing']=='on'
                    assert snapshot['track_functions']=='all'
                    assert snapshot['commit_delay']==str(args.diagnostic_commit_delay_us)
                    assert snapshot['commit_siblings']==('1' if args.diagnostic_commit_delay_us else '5')
            if candidate:
                assert (pair/'storage.txt').read_text().splitlines()==[sql_version+'|on|on|on|t|t','16|t']
                snapshot=json.loads((pair/'diagnostic-stats-after.json').read_text())
                assert any(f['schemaname']=='broker' and f['funcname']=='commit_many' and f['calls']>0 for f in snapshot['functions']), 'bulk path was not exercised'
            report=dict(result=summarize(pair/'broker.json'),measurement=timing,sampling=sampling,
                        scope='Instrumented broker-only diagnosis; not a matched acceptance comparison')
        else:
            report=compare([pair],verify_environment=True)
        (out/'comparison.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
        save(phase='complete',terminal_status=observed,results=str(pair))
    except BaseException as error:
        save(phase='failed',error=str(error),note='Inspect recorded service invocation; never blindly replay launch')
        raise


if __name__=='__main__':main()

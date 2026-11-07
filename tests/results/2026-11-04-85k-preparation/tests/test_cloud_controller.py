import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import test_compare

spec = importlib.util.spec_from_file_location('cloud_controller', Path(__file__).resolve().parents[1]/'cloud-controller.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class CloudControllerTests(unittest.TestCase):
    def exercise(self, retry=False, failed=False, rate='75k', size=640, diagnostic=False, timing=False, delay=0, bulk=False, guard=False):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            sql_version=('0.1.18' if guard else '0.1.17') if bulk else '0.1.7'
            def path(value):
                value=str(value)
                return root/'home'/value[len('/home/ubuntu/'):] if value.startswith('/home/ubuntu/') else Path(value)
            profile=path('/home/ubuntu/omb-volume-profile.json');profile.parent.mkdir()
            profile.write_text(json.dumps(dict(size_in_gbs=size,vpus_per_gb=10)))
            pair=path('/home/ubuntu/results/omb-pair-fixture')
            def command(args, **kwargs):
                if args[:2]==['sudo','systemd-run']:
                    pair.mkdir(parents=True)
                    for name in ('broker-request.json','kafka-request.json','complete.txt'):(pair/name).write_text('fixture')
                    if diagnostic:
                        (pair/'broker.json').write_text('{}')
                        (pair/'broker.json.log').write_text('fixture')
                        (pair/'instrumentation.json').write_text('{"acceptance_run":false}')
                        (pair/'sampling-complete.json').write_text('{"samples":450,"sample_errors":0}')
                        if bulk:
                            (pair/'storage.txt').write_text(sql_version+'|on|on|on|t|t\n16|t\n')
                        if timing:
                            for name in ('before','after'):
                                (pair/f'diagnostic-stats-{name}.json').write_text(json.dumps(dict(io_timing='on',wal_io_timing='on',track_functions='all',commit_delay=str(delay),commit_siblings='1' if delay else '5',functions=[dict(schemaname='broker',funcname='commit_many',calls=42)] if bulk else [])))
                return subprocess.CompletedProcess(args,0,stdout='')
            running=dict(InvocationID='original',SubState='running',MainPID='42',Result='success',ExecMainStatus='0')
            done=dict(running,SubState='exited',MainPID='0')
            statuses=[running,running,running,done]
            deliveries=[True,True]
            if diagnostic:
                statuses=[running,running,done]
                deliveries=[True]
            if retry:
                statuses.insert(2,running)
                deliveries.insert(0,subprocess.CalledProcessError(255,'ssh'))
            if failed:statuses=[running,dict(done,SubState='failed',Result='exit-code',ExecMainStatus='1')]
            with (patch.object(module,'__file__',str(root/'cloud-controller.py')),patch.object(module,'Path',side_effect=path),
                 patch.object(module,'command',side_effect=command) as commands,patch.object(module,'service',side_effect=statuses),
                 patch.object(module.subprocess,'run',return_value=subprocess.CompletedProcess([],1)),
                 patch.object(module.handoff_client,'advance',side_effect=deliveries) as advance,
                 patch.object(module,'compare',return_value={'verified':'fixture'}) as compare,
                 patch('summarize.summarize',return_value={}),
                 patch('measurement.verify_measurement',return_value={}),patch.object(module.time,'sleep'),
                 patch('sys.argv',['controller','fixture','--rate',rate,'--volume-size-gib',str(size),
                                   '--runner-sha256','a'*64]+(['--diagnostic-broker-only'] if diagnostic else [])+(['--diagnostic-timing'] if timing else [])+(['--diagnostic-commit-delay-us',str(delay)] if delay else [])+(['--bulk-commit-binary','/home/ubuntu/candidate-bulkcommit017/pg-broker-gateway','--bulk-commit-sha256','b'*64] if bulk else [])+(['--partition-guard-candidate'] if guard else []) if rate in ('85k','100k') else ['controller','fixture'])):
                original_ssh=module.handoff_client.ssh
                try:
                    if failed:
                        with self.assertRaises(AssertionError):module.main()
                    else:module.main()
                    state=json.loads((root/'control-fixture/state.json').read_text())
                    self.assertEqual(state['phase'],'failed' if failed else 'complete')
                    self.assertEqual(state['invocation'],'original')
                    self.assertEqual(state['rate'],rate)
                    self.assertEqual(state['volume_size_gib'],size)
                    self.assertEqual(sum(c.args[0][:2]==['sudo','systemd-run'] for c in commands.call_args_list),1)
                    launch=next(c.args[0] for c in commands.call_args_list if c.args[0][:2]==['sudo','systemd-run'])
                    self.assertEqual(launch[-3],rate)
                    self.assertIn('--setenv=OMB_SQL_VERSION='+sql_version,launch)
                    self.assertEqual(state['sql_version'],sql_version)
                    if bulk:
                        self.assertIn('--setenv=OMB_GATEWAY_SHA256='+'b'*64,launch)
                        self.assertIn('--setenv=OMB_GATEWAY_PATH=/home/ubuntu/candidate-bulkcommit017/pg-broker-gateway',launch)
                    self.assertIn('--setenv=OMB_DIAGNOSTIC_BROKER_ONLY='+str(int(diagnostic)),launch)
                    self.assertIn('--setenv=OMB_DIAGNOSTIC_TIMING='+str(int(timing)),launch)
                    self.assertIn('--setenv=OMB_DIAGNOSTIC_COMMIT_DELAY_US='+str(delay),launch)
                    if diagnostic:
                        compare.assert_not_called()
                        self.assertEqual(advance.call_count,1)
                        self.assertEqual(advance.call_args.args[0].system,'broker')
                        self.assertIn('not acceptance',state['scope'])
                    free_check=next(c.args[0][-1] for c in commands.call_args_list if c.args[0][:2]==['bash','-c'])
                    self.assertTrue(free_check.endswith('-gt 125' if rate in ('85k','100k') else '-gt 95'))
                    count=commands.call_count
                    with self.assertRaises(FileExistsError):module.main()
                    self.assertEqual(commands.call_count,count,'existing durable control cannot relaunch')
                    if retry:self.assertEqual(advance.call_args_list[0].args[1],advance.call_args_list[1].args[1])
                finally:module.handoff_client.ssh=original_ssh
    def test_success_and_duplicate_launch_guard(self):self.exercise()
    def test_transient_handoff_failure_reuses_same_phase(self):self.exercise(retry=True)
    def test_terminal_server_failure_is_not_replayed(self):self.exercise(failed=True)
    def test_85k_forwards_rate_and_requires_full_storage_headroom(self):self.exercise(rate='85k',size=1024)
    def test_85k_rejects_old_runner_before_creating_state(self):
        with patch('sys.argv',['controller','fixture','--rate','85k']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

    def test_100k_records_profile_and_enforces_larger_preflight(self):self.exercise(rate='100k',size=1024)
    def test_100k_rejects_old_runner_before_creating_state(self):
        with patch('sys.argv',['controller','fixture','--rate','100k']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

    def test_diagnostic_runs_only_broker_and_never_compares(self):
        self.exercise(rate='100k',size=1024,diagnostic=True)

    def test_diagnostic_rejects_kafka_first(self):
        with patch('sys.argv',['controller','fixture','--rate','100k','--runner-sha256','a'*64,
                              '--diagnostic-broker-only','--order','kafka-first']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

    def test_timing_diagnostic_validates_settings(self):
        self.exercise(rate='100k',size=1024,diagnostic=True,timing=True)

    def test_timing_rejected_for_acceptance_mode(self):
        with patch('sys.argv',['controller','fixture','--diagnostic-timing']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

    def test_commit_delay_requires_timing(self):
        with patch('sys.argv',['controller','fixture','--diagnostic-commit-delay-us','500']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

    def test_commit_delay_setting_forwarded_and_verified(self):
        self.exercise(rate='100k',size=1024,diagnostic=True,timing=True,delay=500)

    def test_bulk_candidate_records_artifacts_and_selects_sql(self):
        self.exercise(rate='100k',size=1024,diagnostic=True,timing=True,bulk=True)

    def test_bulk_candidate_rejects_ambiguous_or_unsafe_configuration(self):
        base=['controller','fixture','--rate','100k','--runner-sha256','a'*64]
        binary=['--bulk-commit-binary','/home/ubuntu/candidate-bulkcommit017/pg-broker-gateway']
        digest=['--bulk-commit-sha256','b'*64]
        timed=['--diagnostic-broker-only','--diagnostic-timing']
        for flags in (binary+digest, timed+binary, timed+digest,
                      timed+binary+digest+['--diagnostic-commit-delay-us','500'],
                      timed+['--bulk-commit-binary','/tmp/bad']+digest):
            with self.subTest(flags=flags), patch('sys.argv',base+flags),patch.object(module,'Path') as path:
                with self.assertRaises(SystemExit):module.main()
                path.assert_not_called()

    def test_partition_guard_candidate_selects_sql18(self):
        self.exercise(rate='100k',size=1024,diagnostic=True,timing=True,bulk=True,guard=True)

    def test_partition_guard_requires_explicit_candidate(self):
        with patch('sys.argv',['controller','fixture','--partition-guard-candidate']),patch.object(module,'Path') as path:
            with self.assertRaises(SystemExit):module.main()
            path.assert_not_called()

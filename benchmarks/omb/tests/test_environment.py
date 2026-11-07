import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import test_compare as fixture
from environment import verify


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        fixture.CompareTests.setUp(self)
        files = {
            'complete.txt': '2026-11-03T20:00:00Z\n', 'order.txt': 'kafka-first\n',
            'kafka-limits.txt': 'AllowedCPUs=0-1\nMemoryMax=9663676416\n',
            'broker-limits.txt': '/demo-db 0,1 8589934592\n/demo-gateway 0,1 1073741824\n',
            'storage.txt': '0.1.7|on|on|on|t|t\n16|t\n', 'compression.txt': '100|t\n',
            'kafka.properties': 'log.dirs=/home/ubuntu/omb-pair-demo-kafka-data\nlog.flush.interval.messages=1\ndefault.replication.factor=1\nmin.insync.replicas=1\nnum.partitions=16\nauto.create.topics.enable=false\n',
            'volume-profile.json': json.dumps(dict(size_in_gbs=160, vpus_per_gb=10)),
            'disks.txt': 'NAME SIZE FSTYPE\nsda 171798691840\n',
            'image.txt': 'sha256:fixture-image\n', 'extension-binary.txt': 'fixture-extension\n',
            'binaries.txt': '/candidate/gateway: OK\n', 'runner.sh': '# fixed runner\n',
            'host.txt': 'Linux fixed-host aarch64\n', 'java.txt': 'openjdk fixture-version\n',
        }
        ends = ''.join(f'test-topic-fixture:{p}:1000\n' for p in range(16))
        files.update({'kafka-offsets-before.txt': ends, 'kafka-offsets-after.txt': ends})
        checksums = []
        for name in ('pg_broker--0.1.5--0.1.6.sql', 'pg_broker--0.1.6--0.1.7.sql', 'verify-storage.sql'):
            files[name] = f'-- fixture {name}\n'
            checksums.append(f'{hashlib.sha256(files[name].encode()).hexdigest()}  /remote/{name}\n')
        files['sql-checksums.txt'] = ''.join(checksums)
        for name, content in files.items():
            (self.root / name).write_text(content)

    def candidate_evidence(self):
        from sql_contract import SQL18_UPGRADE_SHA256
        sql_root = Path(__file__).resolve().parents[3]/'crates/extension/sql'
        checksums = (self.root/'sql-checksums.txt').read_text()
        for name,digest in SQL18_UPGRADE_SHA256.items():
            (self.root/name).write_bytes((sql_root/name).read_bytes())
            checksums += f'{digest}  /remote/{name}\n'
        (self.root/'sql-checksums.txt').write_text(checksums)
        (self.root/'storage.txt').write_text('0.1.18|on|on|on|t|t\n16|t\n')
        (self.root/'gateway-sha256.txt').write_text('a'*64+'  /home/ubuntu/candidate-fixture/pg-broker-gateway\n')

        (self.root/'binaries.txt').write_text('/home/ubuntu/candidate-fixture/pg-broker-gateway: OK\n')

    def test_candidate_requires_explicit_version_and_reviewed_binary(self):
        self.candidate_evidence()
        with self.assertRaises(ValueError):verify(self.root)
        with self.assertRaises(ValueError):verify(self.root,expected_sql_version='0.1.18')
        with self.assertRaises(ValueError):verify(self.root,expected_sql_version='0.1.18',expected_gateway_sha256='b'*64)
        report=fixture.module.compare([self.root],verify_environment=True,
            expected_sql_version='0.1.18',expected_gateway_sha256='a'*64)
        self.assertFalse(report['at_least_three_trials'])
        with self.assertRaises(ValueError):
            fixture.module.compare([self.root],expected_sql_version='0.1.18',expected_gateway_sha256='a'*64)

    def test_candidate_rejects_self_consistent_unreviewed_sql(self):
        self.candidate_evidence()
        name='pg_broker--0.1.17--0.1.18.sql'
        old=hashlib.sha256((self.root/name).read_bytes()).hexdigest()
        (self.root/name).write_text('-- tampered migration\n')
        new=hashlib.sha256((self.root/name).read_bytes()).hexdigest()
        p=self.root/'sql-checksums.txt';p.write_text(p.read_text().replace(old,new))
        with self.assertRaisesRegex(ValueError,'unreviewed SQL'):
            verify(self.root,expected_sql_version='0.1.18',expected_gateway_sha256='a'*64)

    def test_valid_evidence_is_not_performance_acceptance(self):
        report = fixture.module.compare([self.root], verify_environment=True)
        self.assertEqual(report['end_to_end_p99_ratios'], [1.5])
        self.assertIn('no live-host attestation', report['trials'][0]['environment']['scope'])
        self.assertFalse(report['at_least_three_trials'])

    def test_rejects_missing_and_contradictory_evidence(self):
        for name, content in {
            'kafka-limits.txt': 'AllowedCPUs=0-3\nMemoryMax=9663676416\n',
            'broker-limits.txt': '/demo-db 0,1 9663676416\n/demo-gateway 0,1 1073741824\n',
            'storage.txt': '0.1.7|on|on|off|t|t\n16|t\n',
            'compression.txt': '0|t\n',
            'kafka-offsets-after.txt': 'test-topic-fixture:0:999\n',
            'disks.txt': 'NAME SIZE\nsda 85899345920\n',
            'pg_broker--0.1.6--0.1.7.sql': '-- changed artifact\n',
        }.items():
            with self.subTest(name=name):
                p = self.root / name
                original = p.read_text()
                p.write_text(content)
                with self.assertRaises(ValueError):
                    verify(self.root)
                p.write_text(original)
        (self.root / 'complete.txt').unlink()
        with self.assertRaises(FileNotFoundError):
            verify(self.root)

    def test_gateway_digest_is_captured_and_checked(self):
        self.assertIsNone(verify(self.root)['contract']['gateway_sha256'])
        marker = self.root / 'gateway-sha256.txt'
        binary = '/home/ubuntu/candidate-demo/pg-broker-gateway'
        marker.write_text('a' * 64 + '  ' + binary + '\n')
        with self.assertRaisesRegex(ValueError, 'gateway checksum check differs'):
            verify(self.root)
        (self.root / 'binaries.txt').write_text(binary + ': OK\n')
        self.assertEqual(verify(self.root)['contract']['gateway_sha256'], 'a' * 64)
        marker.write_text('not-a-digest  ' + binary + '\n')
        with self.assertRaisesRegex(ValueError, 'invalid gateway checksum'):
            verify(self.root)

    def test_rejects_reused_stores_and_mixed_storage_profiles(self):
        with tempfile.TemporaryDirectory() as other:
            second = Path(other)
            shutil.copytree(self.root, second, dirs_exist_ok=True)
            with self.assertRaisesRegex(ValueError, 'reuse captured'):
                fixture.module.compare([self.root, second], verify_environment=True)
            for name in ('kafka.properties', 'broker-limits.txt'):
                p = second / name
                p.write_text(p.read_text().replace('demo', 'second'))
            report = fixture.module.compare([self.root, second], verify_environment=True)
            self.assertEqual(report['trial_count'], 2)
            (second / 'volume-profile.json').write_text(json.dumps(dict(size_in_gbs=80, vpus_per_gb=10)))
            (second / 'disks.txt').write_text('NAME SIZE\nsda 85899345920\n')
            with self.assertRaisesRegex(ValueError, 'different captured environments'):
                fixture.module.compare([self.root, second], verify_environment=True)

    def test_instrumentation_cannot_be_mixed_even_without_environment_check(self):
        self.assertEqual(verify(self.root)['contract']['instrumentation'], {'recorded': False})
        with tempfile.TemporaryDirectory() as other:
            second = Path(other)
            shutil.copytree(self.root, second, dirs_exist_ok=True)
            marker = second / 'instrumentation.json'
            marker.write_text(json.dumps({'kind': 'client-jvm-gc-safepoint', 'acceptance_run': False}))
            with self.assertRaisesRegex(ValueError, 'instrumentation'):
                fixture.module.compare([self.root, second])
            report = fixture.module.compare([second])
            self.assertFalse(report['trials'][0]['instrumentation']['settings']['acceptance_run'])
            for invalid in (None, [], {}, {'kind': 'gc', 'acceptance_run': 'false'}):
                marker.write_text(json.dumps(invalid))
                with self.assertRaisesRegex(ValueError, 'instrumentation provenance'):
                    fixture.module.compare([second])

    def test_wal_settings_reject_weakened_durability_and_mixed_modes(self):
        self.assertEqual(fixture.module.compare([self.root])['trials'][0]['wal'], {'recorded': False})
        (self.root / 'wal-init-zero.txt').write_text('on\n')
        actual = self.root / 'wal-settings.txt'
        for invalid in ('off|on|16MB|fdatasync|on|on|on', 'on|on|16MB|fdatasync|off|on|on'):
            actual.write_text(invalid + '\n')
            with self.assertRaisesRegex(ValueError, 'WAL/durability settings differ'):
                fixture.module.compare([self.root])
        actual.write_text('on|on|16MB|fdatasync|on|on|on\n')
        with tempfile.TemporaryDirectory() as other:
            second = Path(other)
            shutil.copytree(self.root, second, dirs_exist_ok=True)
            (second / 'wal-init-zero.txt').write_text('off\n')
            (second / 'wal-settings.txt').write_text('off|on|16MB|fdatasync|on|on|on\n')
            self.assertTrue(fixture.module.compare([second])['trials'][0]['wal']['recorded'])
            with self.assertRaisesRegex(ValueError, 'WAL settings'):
                fixture.module.compare([self.root, second])

    def test_background_kafka_reference_is_labeled_and_cannot_mix_with_local_flush(self):
        with tempfile.TemporaryDirectory() as other:
            second = Path(other)
            shutil.copytree(self.root, second, dirs_exist_ok=True)
            (second / 'kafka-flush-policy.txt').write_text('background\n')
            p = second / 'kafka.properties'
            p.write_text(p.read_text().replace('log.flush.interval.messages=1',
                'log.flush.interval.messages=9223372036854775807'))
            config_log = ('log.flush.interval.messages = 9223372036854775807\n'
                          'log.flush.interval.ms = null\n'
                          'log.flush.scheduler.interval.ms = 9223372036854775807\n')
            for name in ('kafka-server.log', 'kafka-recovery.log'):
                (second / name).write_text(config_log)
            result = fixture.module.compare([second], verify_environment=True)
            policy = result['trials'][0]['kafka_flush_policy']
            self.assertEqual(policy['mode'], 'background')
            self.assertIn('durability differs', policy['scope'])
            with self.assertRaisesRegex(ValueError, 'Kafka flush policy'):
                fixture.module.compare([self.root, second])
            (second / 'kafka-flush-policy.txt').unlink()
            with self.assertRaisesRegex(ValueError, 'contradicts'):
                fixture.module.compare([second])

    def test_explicit_flush_policy_requires_matching_effective_start_and_restart_settings(self):
        (self.root / 'kafka-flush-policy.txt').write_text('per-append\n')
        config_log = ('log.flush.interval.messages = 1\nlog.flush.interval.ms = null\n'
                      'log.flush.scheduler.interval.ms = 9223372036854775807\n')
        for name in ('kafka-server.log', 'kafka-recovery.log'):
            (self.root / name).write_text(config_log)
        verify(self.root)
        for name in ('kafka-server.log', 'kafka-recovery.log'):
            with self.subTest(name=name):
                (self.root / name).write_text(config_log.replace('messages = 1', 'messages = 100'))
                with self.assertRaisesRegex(ValueError, 'effective flush setting differs'):
                    verify(self.root)
                (self.root / name).write_text(config_log)
        p = self.root / 'kafka.properties'
        p.write_text(p.read_text() + 'log.flush.interval.ms=1000\n')
        with self.assertRaisesRegex(ValueError, 'timer override'):
            verify(self.root)


if __name__ == '__main__':
    unittest.main()

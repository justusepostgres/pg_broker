import hashlib
from datetime import datetime, timedelta
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import test_environment as fixture
from compare_wal_init import compare


class WalDiagnosticTests(unittest.TestCase):
    def setUp(self):
        fixture.EnvironmentTests.setUp(self)
        self.on = self.root
        files = {
            'storage-experiment.json': json.dumps({'wal_init_zero': 'on', 'restart_mode': 'crash'}),
            'instrumentation.json': json.dumps({'kind': 'server-profile', 'acceptance_run': False}),
            'wal-settings.txt': 'on|on|16MB|fdatasync|on|on|on\n',
            'complete.txt': '2026-11-04T00:04:10Z\n',
            'client-1.log': '[2026-11-03T23:58:00.000+0000][0.001s][info][gc] Using G1\n',
            'broker.json.log': '23:59:00.000 INFO Starting benchmark traffic now\n' + ''.join(
                (datetime(2026, 11, 3, 23, 59) + timedelta(seconds=10*i)).strftime('%H:%M:%S.000')
                + ' INFO WorkloadGenerator - Pub rate 10000\n' for i in range(1, 31)),
            'postgres-recovery.log': 'database system was interrupted\nredo starts at 0/100\n'
                                     'redo done at 0/200\ndatabase system is ready to accept connections\n',
            'postgres.log': '2026-11-03 23:58:59.999 UTC [1] duration: 100.000 ms statement: COMMIT\n'
                            '2026-11-04 00:00:30.000 UTC [1] duration: 42.000 ms statement: COMMIT\n'
                            '2026-11-04 00:04:00.001 UTC [1] duration: 100.000 ms statement: COMMIT\n',
            'source-hashes.json': '{}\n', 'settings.txt': 'fixture settings\n',
            'server-profile.sql': '-- profile\n', 'server-profile-stats.sql': '-- stats\n',
        }
        for stage in ('before', 'after'):
            files[f'offsets-{stage}.txt'] = ''.join(f'fixture-on|{i}|0|1000\n' for i in range(16))
        for name, content in files.items():
            (self.on / name).write_text(content)
        with (self.on / 'sql-checksums.txt').open('a') as checksums:
            for name in ('server-profile.sql', 'server-profile-stats.sql'):
                checksums.write(f'{hashlib.sha256((self.on / name).read_bytes()).hexdigest()}  {name}\n')
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        self.off = Path(other.name)
        shutil.copytree(self.on, self.off, dirs_exist_ok=True)
        (self.off / 'storage-experiment.json').write_text(json.dumps(
            {'wal_init_zero': 'off', 'restart_mode': 'crash'}))
        (self.off / 'wal-settings.txt').write_text('off|on|16MB|fdatasync|on|on|on\n')
        for name in ('offsets-before.txt', 'offsets-after.txt'):
            p = self.off / name
            p.write_text(p.read_text().replace('fixture-on', 'fixture-off'))
        p = self.off / 'broker-limits.txt'
        p.write_text(p.read_text().replace('demo', 'other'))

    def test_midnight_measurement_excludes_warmup_and_cleanup(self):
        report = compare(self.on, self.off)
        for mode in ('on', 'off'):
            self.assertEqual(report[mode]['measured_slow_commit_count'], 1)
            self.assertEqual(report[mode]['measured_slow_statements'][0]['duration_ms'], 42)
        self.assertIn('No maximum-capacity', report['scope'])

    def test_rejects_missing_redo_lost_offsets_weaker_durability_and_bad_provenance(self):
        mutations = {
            'postgres-recovery.log': 'database system is ready to accept connections\n',
            'offsets-after.txt': 'fixture-off|0|0|999\n',
            'wal-settings.txt': 'off|on|16MB|fdatasync|off|on|on\n',
            'server-profile.sql': '-- altered\n',
            'sql-checksums.txt': '',
            'complete.txt': '2026-11-03T23:59:30Z\n',
            'storage-experiment.json': '{"wal_init_zero":"off","restart_mode":"clean"}',
        }
        for name, content in mutations.items():
            with self.subTest(name=name):
                p = self.off / name
                original = p.read_text()
                p.write_text(content)
                with self.assertRaises(ValueError):
                    compare(self.on, self.off)
                p.write_text(original)

    def test_rejects_reused_topic_and_changed_client(self):
        for name in ('offsets-before.txt', 'offsets-after.txt'):
            (self.off / name).write_text((self.on / name).read_text())
        with self.assertRaisesRegex(ValueError, 'fresh stores/topics'):
            compare(self.on, self.off)
        p = self.off / 'broker.json.driver.yaml'
        p.write_text(p.read_text().replace('acks=all', 'acks=0'))
        with self.assertRaisesRegex(ValueError, 'configuration/provenance'):
            compare(self.on, self.off)

    def test_rejects_truncated_or_out_of_order_measurement_logs(self):
        p = self.off / 'broker.json.log'
        original = p.read_text()
        for altered in (original.rsplit('\n', 2)[0] + '\n',
                        original.replace('00:01:00.000', '00:00:30.000')):
            p.write_text(altered)
            with self.assertRaisesRegex(ValueError, 'sample counts|out-of-order'):
                compare(self.on, self.off)


if __name__ == '__main__':
    unittest.main()

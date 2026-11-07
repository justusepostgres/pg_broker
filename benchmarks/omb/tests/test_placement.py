import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import test_environment as fixture
from placement import placement, PAYLOAD_SHA256
from compare import compare


class PlacementTests(unittest.TestCase):
    def setUp(self):
        fixture.EnvironmentTests.setUp(self)
        (self.root / 'placement.json').write_text(json.dumps(
            {'kind': 'separate-client', 'server_host': '10.84.0.247'}))
        for system, port in (('kafka', 19092), ('broker', 9092)):
            p = self.root / f'client-{system}'
            p.mkdir()
            files = {
                'complete.txt': '2026-11-03T20:00:00Z\n',
                'bootstrap.txt': f'10.84.0.247:{port}\n',
                'heap.txt': '-Xms1G -Xmx1G\n', 'cpu-count.txt': '2\n',
                'java.txt': (self.root / 'java.txt').read_text(),
                'host.txt': 'Linux independent-client aarch64\n',
                'tracked-changes.txt': '', 'revision.txt': 'pinned-revision\n',
                'limits.txt': 'RuntimeMaxUSec=26min 40s\nAllowedCPUs=0-1\nMemoryMax=2147483648\n',
                'unit-name.txt': f'omb-client-fixture-{system}.service\n',
                'payload-checksum.txt': f'{PAYLOAD_SHA256}  /upstream/payload/payload-1Kb.data\n',
                'service-status.txt': 'Result=success\nExecMainStatus=0\nSubState=exited\nInvocationID=fixture-invocation\n',
            }
            (self.root / f'{system}.json.process.txt').write_text('exit_code=-15\npost_result_cleanup=True\n')
            for suffix in ('', '.log', '.driver.yaml', '.workload.yaml', '.revision', '.process.txt'):
                files[f'result.json{suffix}'] = (self.root / f'{system}.json{suffix}').read_text()
            checksums = []
            for name in ('runner.sh', 'run.sh', 'execute.py', 'summarize.py'):
                files[name] = f'# fixture {name}\n'
                checksums.append(f'{hashlib.sha256(files[name].encode()).hexdigest()}  /remote/{name}\n')
            files['script-checksums.txt'] = ''.join(checksums)
            for name, content in files.items():
                (p / name).write_text(content)

    def test_valid_separate_client_is_recorded_without_capacity_claim(self):
        report = compare([self.root], verify_environment=True)
        self.assertEqual(report['trials'][0]['placement']['settings']['kind'], 'separate-client')
        self.assertIn('omb-client-fixture-broker.service', report['trials'][0]['environment']['stores'])

    def test_rejects_changed_client_resources_or_handoff(self):
        p = self.root / 'client-broker'
        for name, content in {
            'limits.txt': 'RuntimeMaxUSec=26min 40s\nAllowedCPUs=0-3\nMemoryMax=2147483648\n',
            'host.txt': (self.root / 'host.txt').read_text(),
            'java.txt': 'different runtime\n',
            'bootstrap.txt': '127.0.0.1:9092\n',
            'tracked-changes.txt': ' M client.java\n',
            'result.json': '{}\n',
            'runner.sh': '# changed\n',
            'revision.txt': 'unmatched-revision\n',
            'payload-checksum.txt': 'incorrect-checksum  /upstream/payload/payload-1Kb.data\n',
            'service-status.txt': 'Result=success\nExecMainStatus=0\nSubState=running\nInvocationID=fixture\n',
        }.items():
            with self.subTest(name=name):
                file = p / name
                original = file.read_text()
                file.write_text(content)
                with self.assertRaises(ValueError):
                    placement(self.root)
                file.write_text(original)
        (p / 'preflight-complete.txt').write_text('not a workload\n')
        with self.assertRaisesRegex(ValueError, 'not complete'):
            placement(self.root)

    def test_rejects_absent_or_contradictory_placement(self):
        marker = self.root / 'placement.json'
        marker.unlink()
        with self.assertRaisesRegex(ValueError, 'missing client placement'):
            placement(self.root)
        marker.write_text(json.dumps({'kind': 'loopback', 'server_host': '127.0.0.1'}))
        with self.assertRaisesRegex(ValueError, 'contradict'):
            placement(self.root)

    def test_rejects_mixed_placement_even_without_environment_verification(self):
        with tempfile.TemporaryDirectory() as other:
            p = Path(other)
            shutil.copytree(self.root, p, dirs_exist_ok=True)
            for system in ('kafka', 'broker'):
                shutil.rmtree(p / f'client-{system}')
            (p / 'placement.json').write_text(json.dumps(
                {'kind': 'loopback', 'server_host': '127.0.0.1'}))
            with self.assertRaisesRegex(ValueError, 'placement'):
                compare([self.root, p])


if __name__ == '__main__':
    unittest.main()

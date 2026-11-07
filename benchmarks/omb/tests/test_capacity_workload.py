"""The higher offered load changes only workload name/rate, not client behavior."""
import os
from pathlib import Path
import subprocess
import unittest

ROOT=Path(__file__).resolve().parents[1]


class CapacityWorkloadTests(unittest.TestCase):
    def test_110k_preserves_full_duration_and_all_other_workload_fields(self):
        source=(ROOT/'100k.yaml').read_text()
        self.assertEqual((ROOT/'110k.yaml').read_text(), source.replace('name: 100k-', 'name: 110k-').replace('producerRate: 100000', 'producerRate: 110000'))
        self.assertIn('testDurationMinutes: 15\n', source)

    def test_real_runner_prefix_accepts_110k_only_with_candidate_contract(self):
        source=(ROOT/'oci-pair.sh').read_text()
        prefix=source[:source.index('python3 - "$external_client"')]
        env={k:v for k,v in os.environ.items() if not k.startswith('OMB_')}
        env.update(OMB_SQL_VERSION='0.1.18',OMB_CANDIDATE_PAIR='1',OMB_EXTERNAL_CLIENT='1',
                   OMB_GATEWAY_PATH='/home/ubuntu/candidate-fixture/pg-broker-gateway',OMB_GATEWAY_SHA256='a'*64)
        for change,expected in (({},0),({'OMB_DIAGNOSTIC_BROKER_ONLY':'1'},2),({'OMB_GATEWAY_SHA256':''},2)):
            with self.subTest(change=change):
                result=subprocess.run(['bash','-c',prefix,'runner','fixture','110k','kafka-first','off'],env=env|change,capture_output=True,text=True)
                self.assertEqual(result.returncode,expected,result.stderr)

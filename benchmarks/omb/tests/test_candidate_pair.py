"""Run the actual runner's validation prefix without any cloud operations."""
import os
from pathlib import Path
import subprocess
import unittest


class CandidatePairRunnerTests(unittest.TestCase):
    def test_explicit_candidate_pair_preserves_mode_and_durability_constraints(self):
        source = (Path(__file__).resolve().parents[1]/'oci-pair.sh').read_text()
        prefix = source[:source.index('python3 - "$external_client"')]
        base = {k:v for k,v in os.environ.items() if not k.startswith('OMB_')}
        valid = dict(OMB_SQL_VERSION='0.1.18', OMB_CANDIDATE_PAIR='1',
                     OMB_EXTERNAL_CLIENT='1', OMB_GATEWAY_PATH='/home/ubuntu/candidate-fixture/pg-broker-gateway',
                     OMB_GATEWAY_SHA256='a'*64)
        for change, expected in (({},0), ({'OMB_CANDIDATE_PAIR':'0'},2),
                                 ({'OMB_DIAGNOSTIC_BROKER_ONLY':'1'},2),
                                 ({'OMB_DIAGNOSTIC_TIMING':'1'},2),
                                 ({'OMB_DIAGNOSTIC_COMMIT_DELAY_US':'500'},2),
                                 ({'OMB_SQL_VERSION':'0.1.7'},2),
                                 ({'OMB_GATEWAY_SHA256':''},2)):
            with self.subTest(change=change):
                result=subprocess.run(['bash','-c',prefix,'runner','fixture','85k','kafka-first','off'],
                                      env=base|valid|change,capture_output=True,text=True)
                self.assertEqual(result.returncode,expected,result.stderr)

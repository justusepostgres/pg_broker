import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import test_compare
from test_measurement import log
import test_summarize_server
import compare_commit_delay as module


class CommitDelayTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        digest=hashlib.sha256(b'fixture').hexdigest()
        guard=patch.object(module,'SOURCES',{n:{'runner.sh':digest,'sample-server.py':digest} for n in (0,500)})
        guard.start();self.addCleanup(guard.stop)
        self.old=self.fixture('baseline',0);self.new=self.fixture('candidate',500)

    def fixture(self,label,delay):
        p=self.root/label;p.mkdir();(p/'client-broker').mkdir()
        # Populate the explicit comparison contract; these are synthetic files.
        for name in ('runner.sh','sample-server.py','complete.txt','gateway-sha256.txt','image.txt',
                     'extension-binary.txt','volume-profile.json','host.txt','java.txt',
                     'broker.json.workload.yaml','broker.json.revision','verify-storage.sql',
                     'pg_broker--0.1.5--0.1.6.sql','pg_broker--0.1.6--0.1.7.sql',
                     'client-broker/limits.txt','client-broker/java.txt','client-broker/heap.txt',
                     'client-broker/runner.sh','client-broker/run.sh','client-broker/execute.py'):
            (p/name).write_text('fixture')
        (p/'broker.json.driver.yaml').write_text('  bootstrap.servers=localhost:9092\n  acks=all\n')
        (p/'wal-settings.txt').write_text('off|on|16MB|fdatasync|on|on|on')
        (p/'timing-settings.txt').write_text('on|on|all')
        (p/'storage.txt').write_text('0.1.7|on|on|on|t|t\n16|t\n')
        (p/'broker-limits.txt').write_text(f'/{label}-db 0,1 {8*1024**3}\n/{label}-gateway 0,1 {1024**3}\n')
        (p/'instrumentation.json').write_text(json.dumps(dict(acceptance_run=False,io_and_wal_timing=True,track_functions='all')))
        (p/'sampling-complete.json').write_text('{"samples":450,"sample_errors":0}')
        before,after=test_summarize_server.SummaryTests().snapshots('on')
        for name,row in (('before',before),('after',after)):
            row.update(io_timing='on',track_functions='all',functions=[],commit_delay=str(delay),commit_siblings='1' if delay else '5')
            (p/f'diagnostic-stats-{name}.json').write_text(json.dumps(row))
        result=dict(workload='fixture',publishRate=[90000]*89,consumeRate=[90000]*89,publishErrorRate=[0]*89,
                    backlog=[0]*89,aggregatedPublishLatency99pct=10,aggregatedPublishDelayLatency99pct=60000255,
                    aggregatedEndToEndLatency99pct=11)
        (p/'broker.json').write_text(json.dumps(result));(p/'broker.json.log').write_text(log())
        return p

    def test_keeps_overload_visible_without_acceptance_claim(self):
        result=module.compare(self.old,self.new)
        self.assertEqual(result['candidate_publish_rate_ratio'],1)
        self.assertEqual(result['candidate']['publish_delay_p99_us'],60000255)
        self.assertIn('No repeatability',result['scope'])

    def test_rejects_unreviewed_source_and_mismatched_contract(self):
        (self.new/'runner.sh').write_text('unreviewed')
        with self.assertRaisesRegex(ValueError,'unreviewed'):module.compare(self.old,self.new)
        (self.new/'runner.sh').write_text('fixture')
        (self.new/'gateway-sha256.txt').write_text('different binary')
        with self.assertRaisesRegex(ValueError,'configuration differs'):module.compare(self.old,self.new)

    def test_missing_baseline_settings_require_explicit_observation(self):
        for name in ('before','after'):
            p=self.old/f'diagnostic-stats-{name}.json';row=json.loads(p.read_text())
            del row['commit_delay'];del row['commit_siblings'];p.write_text(json.dumps(row))
        with self.assertRaisesRegex(ValueError,'missing group-commit'):module.compare(self.old,self.new)
        observed=self.root/'observed.txt';observed.write_text('0|5|on|on|on')
        self.assertIn('separate read-only',module.compare(self.old,self.new,observed)['reference']['commit_settings_provenance'])

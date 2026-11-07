import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('omb_compare', ROOT / 'compare.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CompareTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for system, port, latency in [('kafka', 19092, 10), ('broker', 9092, 15)]:
            p = self.root / f'{system}.json'
            data = dict(workload='test', publishRate=[10000] * 30,
                        consumeRate=[9999] * 30, publishErrorRate=[0] * 30,
                        backlog=list(range(30)), aggregatedPublishLatency99pct=9,
                        aggregatedPublishDelayLatency99pct=100,
                        aggregatedEndToEndLatency99pct=latency)
            p.write_text(json.dumps(data))
            Path(f'{p}.workload.yaml').write_text('producerRate: 10000\ntestDurationMinutes: 5\n')
            Path(f'{p}.driver.yaml').write_text(f'  bootstrap.servers=localhost:{port}\n  acks=all\n')
            Path(f'{p}.revision').write_text('pinned-revision\n')
            Path(f'{p}.log').write_text('INFO complete\n')

    def test_reports_ratio_trend_and_each_trial_without_parity_claim(self):
        result = module.compare([self.root])
        self.assertEqual(result['end_to_end_p99_ratios'], [1.5])
        self.assertFalse(result['at_least_three_trials'])
        broker = result['trials'][0]['results']['broker']
        self.assertAlmostEqual(broker['backlog_linear_trend_messages_per_second'], .1)
        self.assertEqual(broker['backlog_last'], 29)

    def test_reports_gateway_evidence_without_inventing_missing_logs(self):
        trial = module.compare([self.root])['trials'][0]
        self.assertEqual(trial['gateway_events'], {'recorded': False})
        (self.root / 'gateway.log').write_text('connection: deadline has elapsed\n')
        trial = module.compare([self.root])['trials'][0]
        self.assertTrue(trial['gateway_events']['recorded'])
        self.assertEqual(trial['gateway_events']['unclassified_deadlines'], 1)

    def test_preserves_delay_growth_even_when_consumer_backlog_is_empty(self):
        p = self.root / 'broker.json'
        data = json.loads(p.read_text())
        data['backlog'] = [0] * 30
        data['publishDelayLatencyAvg'] = [i * 1_000_000 for i in range(30)]
        p.write_text(json.dumps(data))
        summary = module.summarize(p)
        self.assertEqual(summary['backlog_max'], 0)
        self.assertEqual(summary['publish_delay_avg_windows'], dict(
            first_us=0, last_us=29_000_000, max_us=29_000_000,
            change_us=29_000_000, linear_trend_us_per_sample=1_000_000))
        for invalid in (None, [1], [True] * 30, [float('nan')] * 30, [-1] * 30):
            data['publishDelayLatencyAvg'] = invalid
            p.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'invalid publishDelayLatencyAvg'):
                module.summarize(p)
        del data['publishDelayLatencyAvg']
        p.write_text(json.dumps(data))
        self.assertIsNone(module.summarize(p)['publish_delay_avg_windows'])

    def test_pinned_producer_histogram_cap_is_visible_without_changing_raw_values(self):
        p = self.root / 'broker.json'
        data = json.loads(p.read_text())
        data['aggregatedPublishDelayLatency99pct'] = 60_000_255
        data['aggregatedPublishLatencyMax'] = 60_000.255
        p.write_text(json.dumps(data))
        Path(f'{p}.revision').write_text('5b1fa70951a323da26bd587174b58bb2c65b0b5c\n')
        before = p.read_bytes()
        summary = module.summarize(p)
        limit = summary['producer_histogram_limit']
        self.assertTrue(limit['publish_at_cap'])
        self.assertTrue(limit['scheduling_delay_at_cap'])
        self.assertEqual(summary['publish_delay_p99_us'], 60_000_255)
        self.assertEqual(p.read_bytes(), before)
        data['aggregatedPublishDelayLatency99pct'] = 10
        data['aggregatedPublishLatencyMax'] = 20
        p.write_text(json.dumps(data))
        limit = module.summarize(p)['producer_histogram_limit']
        self.assertFalse(limit['publish_at_cap'])
        self.assertFalse(limit['scheduling_delay_at_cap'])
        Path(f'{p}.revision').write_text('unknown-revision\n')
        self.assertIsNone(module.summarize(p)['producer_histogram_limit'])

    def test_preserves_rare_stalls_and_missing_tail_metrics(self):
        result = module.compare([self.root])
        self.assertIsNone(result['trials'][0]['results']['broker']['end_to_end_p999_ms'])
        p = self.root / 'broker.json'
        data = json.loads(p.read_text())
        data.update(aggregatedEndToEndLatency999pct=60,
                    aggregatedEndToEndLatency9999pct=84,
                    aggregatedEndToEndLatencyMax=99,
                    aggregatedPublishDelayLatency999pct=2500)
        p.write_text(json.dumps(data))
        broker = module.compare([self.root])['trials'][0]['results']['broker']
        self.assertEqual(broker['end_to_end_p99_ms'], 15)
        self.assertEqual(broker['end_to_end_p999_ms'], 60)
        self.assertEqual(broker['end_to_end_p9999_ms'], 84)
        self.assertEqual(broker['end_to_end_max_ms'], 99)
        self.assertEqual(broker['publish_delay_p999_us'], 2500)
        for value in (None, float('inf'), -1, True):
            with self.subTest(value=value):
                data['aggregatedEndToEndLatency999pct'] = value
                p.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, 'invalid aggregatedEndToEndLatency999pct'):
                    module.compare([self.root])

    def test_explicit_compression_labels_preserve_reference_direction(self):
        for old, new in [('kafka', 'pglz'), ('broker', 'lz4')]:
            for p in self.root.glob(f'{old}.json*'):
                p.rename(p.with_name(p.name.replace(old, new, 1)))
        result = module.compare([self.root], 'pglz', 'lz4')
        self.assertEqual(result['reference_system'], 'pglz')
        self.assertEqual(result['candidate_system'], 'lz4')
        self.assertEqual(result['end_to_end_p99_ratios'], [1.5])
        reverse = module.compare([self.root], 'lz4', 'pglz')
        self.assertAlmostEqual(reverse['end_to_end_p99_ratios'][0], 2/3)
        for names in [('pglz', 'pglz'), ('../pglz', 'lz4')]:
            with self.assertRaises(ValueError):
                module.compare([self.root], *names)

    def test_rejects_config_mismatch_and_duplicate_evidence(self):
        with self.assertRaisesRegex(ValueError, 'duplicate trial'):
            module.compare([self.root, self.root])
        p = self.root / 'broker.json.driver.yaml'
        p.write_text(p.read_text().replace('acks=all', 'acks=0'))
        with self.assertRaisesRegex(ValueError, 'client configurations differ'):
            module.compare([self.root])

    def test_rejects_workload_and_revision_mismatch(self):
        for suffix in ('workload.yaml', 'revision'):
            with self.subTest(suffix=suffix):
                p = self.root / f'broker.json.{suffix}'
                original = p.read_text()
                p.write_text(original + 'changed')
                with self.assertRaises(ValueError):
                    module.compare([self.root])
                p.write_text(original)

    def test_rejects_incomplete_and_invalid_latency(self):
        p = self.root / 'broker.json'
        original = json.loads(p.read_text())
        data = dict(original, aggregatedEndToEndLatency99pct=float('nan'))
        p.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, 'invalid aggregatedEndToEnd'):
            module.compare([self.root])
        for key in ('publishRate', 'consumeRate', 'publishErrorRate', 'backlog'):
            original[key] = original[key][:-1]
        p.write_text(json.dumps(original))
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            module.compare([self.root])

    def test_rejects_booleans_and_nonfinite_metrics_in_standalone_summary(self):
        p = self.root / 'broker.json'
        original = json.loads(p.read_text())
        for key in ('publishRate', 'consumeRate', 'publishErrorRate', 'backlog',
                    'aggregatedPublishLatency99pct', 'aggregatedPublishDelayLatency99pct',
                    'aggregatedEndToEndLatency99pct'):
            for invalid in (True, False, None, -1, float('nan'), float('inf')):
                with self.subTest(key=key, invalid=invalid):
                    data = dict(original)
                    data[key] = [invalid] * 30 if isinstance(original[key], list) else invalid
                    p.write_text(json.dumps(data))
                    with self.assertRaisesRegex(ValueError, 'invalid'):
                        module.summarize(p)

    def test_preserves_unsustainable_rate_errors_and_client_failures(self):
        p = self.root / 'broker.json'
        data = json.loads(p.read_text())
        data['publishRate'] = [5000] * 30
        data['publishErrorRate'][1] = 2
        p.write_text(json.dumps(data))
        Path(f'{p}.log').write_text('WARN lagging\nERROR failed\n')
        broker = module.compare([self.root])['trials'][0]['results']['broker']
        self.assertEqual(broker['offered_rate_fraction_published'], .5)
        self.assertEqual(broker['publish_error_msg_s_max'], 2)
        self.assertEqual(broker['client_warning_error_matches'], 2)


if __name__ == '__main__':
    unittest.main()

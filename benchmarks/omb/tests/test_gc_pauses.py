from pathlib import Path
import json
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gc_pauses import analyze, with_windows


class PauseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'client.log'
        prefix = '[2026-11-03T20:10:57.395+0000][47.150s][info]'
        self.path.write_text(
            prefix + '[gc,start    ] GC(13) Pause Young (Normal) (G1 Evacuation Pause)\n' +
            prefix + '[gc          ] GC(13) Pause Young (Normal) (G1 Evacuation Pause) 950M->253M(1024M) 2.760ms\n' +
            prefix + '[safepoint   ] Safepoint "G1CollectForAllocation", Time since last: 35699204 ns, Reaching safepoint: 5560 ns, Cleanup: 4040 ns, At safepoint: 2801696 ns, Total: 2811296 ns\n')
        self.since, self.until = '2026-11-03T20:10:00+00:00', '2026-11-03T20:11:00+00:00'

    def test_keeps_overlapping_events_separate_and_normalizes_units(self):
        result = analyze([self.path], self.since, self.until)
        self.assertEqual(result['counts'], {'gc': 1, 'safepoint': 1})
        self.assertEqual(result['max_ms'], {'gc': 2.76, 'safepoint': 2.811296})
        self.assertEqual(len(result['events']), 2)

    def test_excludes_warmup_without_manufacturing_zero_pause(self):
        result = analyze([self.path], self.until, '2026-11-03T20:12:00+00:00')
        self.assertEqual(result['counts'], {'gc': 0, 'safepoint': 0})
        self.assertEqual(result['max_ms'], {'gc': None, 'safepoint': None})

    def test_rejects_ambiguous_bounds_duplicates_and_incomplete_logs(self):
        for paths, start, end in (([self.path] * 2, self.since, self.until),
                                  ([self.path], self.until, self.since),
                                  ([self.path], '2026-11-03T20:10:00', self.until)):
            with self.assertRaises(ValueError):
                analyze(paths, start, end)
        self.path.write_text(self.path.read_text().rstrip('\n'))
        with self.assertRaisesRegex(ValueError, 'incomplete log'):
            analyze([self.path], self.since, self.until)

    def test_window_alignment_requires_matching_bounds_and_sample_counts(self):
        result = self.path.with_name('result.json')
        log = self.path.with_name('omb.log')
        result.write_text(json.dumps({key: [1, 2] for key in (
            'publishLatency99pct', 'publishLatencyMax', 'endToEndLatency99pct',
            'endToEndLatencyMax', 'backlog')}))
        log.write_text('20:10:00.000 [main] INFO WorkloadGenerator - ----- Starting benchmark traffic (1m)------\n'
                       '20:10:55.000 [main] INFO WorkloadGenerator - Pub rate 1\n'
                       '20:11:00.000 [main] INFO WorkloadGenerator - Pub rate 1\n')
        report = analyze([self.path], self.since, self.until)
        windows = with_windows(report, result, log)['windows']
        self.assertIsNone(windows[0]['pause_max_ms']['gc'])
        self.assertEqual(windows[1]['pause_max_ms']['gc'], 2.76)
        self.assertEqual(windows[1]['omb']['backlog'], 2)
        with self.assertRaisesRegex(ValueError, 'measurement end'):
            with_windows(dict(report, until='2026-11-03T20:11:01+00:00'), result, log)
        log.write_text(log.read_text() + '20:11:10.000 [main] INFO WorkloadGenerator - Pub rate 1\n')
        with self.assertRaisesRegex(ValueError, 'counts differ'):
            with_windows(report, result, log)


if __name__ == '__main__':
    unittest.main()

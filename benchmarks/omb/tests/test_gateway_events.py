import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway_events import gateway_events


class GatewayEventsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'gateway.log'

    def test_missing_is_not_zero_and_empty_is_explicitly_recorded(self):
        self.assertEqual(gateway_events(self.path), {'recorded': False})
        self.path.write_text('')
        self.assertTrue(gateway_events(self.path)['recorded'])

    def test_deadline_phases_and_unclassified_legacy_messages(self):
        self.path.write_text('''pg-broker listening on 127.0.0.1:9092
unix_ms=1793750018606 connection: frame-length read exceeded 60s (idle or incomplete header): deadline has elapsed
unix_ms=1793750018607 connection: frame-payload read exceeded 10s: deadline has elapsed
unix_ms=1793750018608 connection: request exceeded 310s: api=1 version=4 correlation=42: deadline has elapsed
unix_ms=1793750018609 connection: response write exceeded 10s: api=0 version=3 correlation=-1: deadline has elapsed
unix_ms=1793750018610 connection: request failed: api=0 version=3 correlation=12: database transaction timed out; outcome unknown: deadline has elapsed
unix_ms=1793750018611 connection: request failed: api=8 version=2 correlation=13: commit confirmation timed out; outcome unknown: deadline has elapsed
connection: deadline has elapsed
unix_ms=1793750018613 connection: malformed request
notification listener: connection closed
unknown future diagnostic
''')
        result = gateway_events(self.path)
        self.assertEqual(result['connection_events'], 8)
        for name in ('frame_header_deadlines', 'frame_payload_deadlines', 'request_deadlines',
                     'response_write_deadlines', 'unclassified_deadlines', 'other_connection_events'):
            self.assertEqual(result[name], 1, name)
        self.assertEqual(result['database_deadlines'], 2)
        self.assertEqual(result['other_diagnostic_lines'], 2)
        self.assertIn('cause is unknown', result['scope'])

    def test_unknown_context_does_not_get_exonerated_as_idle(self):
        self.path.write_text('unix_ms=1 connection: nested: frame-length read exceeded 60s (idle or incomplete header): deadline has elapsed\n')
        result = gateway_events(self.path)
        self.assertEqual(result['frame_header_deadlines'], 0)
        self.assertEqual(result['unclassified_deadlines'], 1)

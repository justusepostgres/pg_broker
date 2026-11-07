import unittest
import test_compare
from measurement import verify_measurement


def timestamp(seconds):
    milliseconds = round(seconds * 1000) % 86400000
    return f'{milliseconds // 3600000:02}:{milliseconds // 60000 % 60:02}:{milliseconds // 1000 % 60:02}.{milliseconds % 1000:03}'


def log(samples=89, duration=900.13, start=0):
    lines = [f'{timestamp(start)} INFO Starting benchmark traffic (15m)']
    lines += [f'{timestamp(start + duration * i / samples)} INFO Pub rate 45000' for i in range(1, samples + 1)]
    lines += [f'{timestamp(start + duration + .1)} INFO ----- Aggregated Pub Latency',
              f'{timestamp(start + duration + 1)} INFO Writing test result into result.json']
    return '\n'.join(lines)


class MeasurementTests(unittest.TestCase):
    def test_slow_reporting_windows_and_midnight_still_complete_full_duration(self):
        for start in (0, 86300):
            result = verify_measurement({'publishRate': [45000] * 89}, log(start=start), 15, True)
            self.assertTrue(result['timing_verified'])
            self.assertAlmostEqual(result['measured_seconds'], 900.13)
            self.assertGreater(result['longest_interval_seconds'], 10)

    def test_missing_or_shortened_completion_is_rejected(self):
        for broken in (log(duration=899), log().replace('----- Aggregated Pub Latency', 'missing'),
                       log().replace('Writing test result into', 'missing'),
                       log().replace('Starting benchmark traffic (15m)', 'Starting benchmark traffic (5m)'),
                       log(samples=88)):
            with self.subTest(log=broken[-150:]), self.assertRaises(ValueError):
                verify_measurement({'publishRate': [45000] * 89}, broken, 15, True)

    def test_legacy_nominal_fallback_is_explicit_and_disallowed_by_runner(self):
        data = {'publishRate': [1] * 90}
        self.assertFalse(verify_measurement(data, 'legacy fixture', 15)['timing_verified'])
        with self.assertRaises(ValueError):
            verify_measurement(data, 'legacy fixture', 15, True)

    def test_duplicate_start_and_backward_clock_are_rejected(self):
        text = log()
        for broken in (text + '\n00:16:00.000 INFO Starting benchmark traffic (15m)',
                       text.replace('00:00:20.228', '00:00:01.000')):
            with self.assertRaises(ValueError):
                verify_measurement({'publishRate': [45000] * 89}, broken, 15, True)


class TimedRateTests(unittest.TestCase):
    def test_unequal_intervals_use_elapsed_time_including_midnight(self):
        from measurement import timed_rates
        text = '\n'.join([
            '23:59:50.000 INFO Starting benchmark traffic (1m)',
            '00:00:00.000 INFO Pub rate 100',
            '00:00:50.000 INFO Pub rate 200',
            '00:00:50.100 INFO ----- Aggregated Pub Latency',
            '00:00:51.000 INFO Writing test result into result.json',
        ])
        data = dict(publishRate=[100, 200], consumeRate=[90, 180],
                    publishErrorRate=[0, 6], backlog=[100, 200])
        measurement = verify_measurement(data, text, 1, True)
        result = timed_rates(data, measurement)
        self.assertEqual(measurement['interval_seconds'], [10, 50])
        self.assertAlmostEqual(result['publish_msg_s'], 11000 / 60)
        self.assertEqual(result['consume_msg_s'], 165)
        self.assertEqual(result['publish_error_msg_s'], 5)
        self.assertEqual(result['backlog_trend_messages_per_second'], 2)

    def test_legacy_timings_are_not_invented_and_invalid_samples_fail(self):
        from measurement import timed_rates
        self.assertIsNone(timed_rates({}, {'timing_verified': False}))
        measurement = dict(timing_verified=True, interval_seconds=[10, 50], measured_seconds=60)
        for values in ([1], [1, float('nan')], [-1, 2], [True, 1]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                timed_rates(dict(publishRate=values), measurement)
        with self.assertRaises(ValueError):
            timed_rates({}, dict(measurement, measured_seconds=59))

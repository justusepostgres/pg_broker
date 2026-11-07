#!/usr/bin/env python3
"""Summarize actual OMB output; never infer parity from publish rate alone."""
import argparse
import json
import math
import statistics
from pathlib import Path


def summarize(path):
    data = json.loads(Path(path).read_text())
    fields = ("publishRate", "consumeRate", "publishErrorRate", "backlog")
    samples = {key: data[key] for key in fields}
    lengths = {len(values) for values in samples.values()}
    if len(lengths) != 1 or not next(iter(lengths)):
        raise ValueError(f"{path}: missing or inconsistent measurement samples")
    for values in samples.values():
        if not all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in values):
            raise ValueError(f"{path}: invalid sample")
    for key in ('aggregatedPublishLatency99pct', 'aggregatedPublishDelayLatency99pct',
                'aggregatedEndToEndLatency99pct'):
        value = data[key]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{path}: invalid {key}")
    result = {
        "file": str(path),
        "workload": data["workload"],
        "samples": len(samples["publishRate"]),
        "publish_msg_s_mean": statistics.mean(samples["publishRate"]),
        "consume_msg_s_mean": statistics.mean(samples["consumeRate"]),
        "publish_error_msg_s_max": max(samples["publishErrorRate"]),
        "backlog_first": samples["backlog"][0],
        "backlog_last": samples["backlog"][-1],
        "backlog_max": max(samples["backlog"]),
        "publish_p99_ms": data["aggregatedPublishLatency99pct"],
        "publish_delay_p99_us": data["aggregatedPublishDelayLatency99pct"],
        "end_to_end_p99_ms": data["aggregatedEndToEndLatency99pct"],
    }
    # An empty consumer backlog can coexist with a producer falling behind its
    # offered schedule. Preserve delay growth rather than treating low backlog
    # or a single aggregate percentile as evidence of sustainable offered load.
    delays = data.get('publishDelayLatencyAvg')
    result['publish_delay_avg_windows'] = None
    if 'publishDelayLatencyAvg' in data:
        if not isinstance(delays, list) or len(delays) != result['samples'] or not all(
                type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in delays):
            raise ValueError(f'{path}: invalid publishDelayLatencyAvg samples')
        result['publish_delay_avg_windows'] = {
            'first_us': delays[0], 'last_us': delays[-1], 'max_us': max(delays),
            'change_us': delays[-1] - delays[0],
            # OMB windows are nominally 10 seconds, but actual lengths can vary.
            'linear_trend_us_per_sample': statistics.linear_regression(
                range(len(delays)), delays).slope if len(delays) > 1 else None,
        }
    # Keep rare stalls visible. Older fixtures/artifacts may omit a quantile;
    # report missing explicitly rather than manufacturing a zero latency.
    for label, source, unit in (
        ('publish', 'aggregatedPublishLatency', 'ms'),
        ('publish_delay', 'aggregatedPublishDelayLatency', 'us'),
        ('end_to_end', 'aggregatedEndToEndLatency', 'ms'),
    ):
        for percentile, suffix in (('p999', '999pct'), ('p9999', '9999pct'), ('max', 'Max')):
            key = source + suffix
            value = data.get(key)
            if key in data and (type(value) not in (int, float) or not math.isfinite(value) or value < 0):
                raise ValueError(f"{path}: invalid {key}")
            result[f'{label}_{percentile}_{unit}'] = value
    # WorkerStats.recordProducerSuccess in the pinned upstream revision clamps
    # both producer histograms at 60 seconds before recording. HdrHistogram can
    # report the containing bucket slightly above the cap. Do not mistake a
    # plateau there for stabilization, or infer the uncensored latency from it.
    revision = Path(f'{path}.revision')
    result['producer_histogram_limit'] = None
    if revision.is_file() and revision.read_text().strip() == '5b1fa70951a323da26bd587174b58bb2c65b0b5c':
        result['producer_histogram_limit'] = {
            'cap_us': 60_000_000,
            'publish_at_cap': any((result[f'publish_{q}_ms'] or 0) >= 60_000
                                  for q in ('p99', 'p999', 'p9999', 'max')),
            'scheduling_delay_at_cap': any((result[f'publish_delay_{q}_us'] or 0) >= 60_000_000
                                           for q in ('p99', 'p999', 'p9999', 'max')),
            'scope': 'Pinned upstream producer metrics clamp at 60s; values at the cap may be censored. '
                     'This cap does not apply to the end-to-end histogram.',
        }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="+")
    args = parser.parse_args()
    print(json.dumps([summarize(path) for path in args.results], indent=2))

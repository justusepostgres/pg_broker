#!/usr/bin/env python3
"""Bound an unchanged OMB JVM, including its post-result executor leak."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def run(driver, output, workload):
    output = Path(output)
    deadline = time.monotonic() + 25 * 60
    complete_at = None
    cleanup = False
    with Path(str(output) + '.log').open('x') as log:
        proc = subprocess.Popen(['bin/benchmark', '--drivers', driver,
                                 '--output', str(output), workload],
                                stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        try:
            while proc.poll() is None:
                if complete_at is None and output.exists():
                    try:
                        data = json.loads(output.read_text())
                        if data.get('publishRate') and data.get('consumeRate'):
                            complete_at = time.monotonic()
                    except json.JSONDecodeError:
                        pass  # The upstream writer is still writing the result.
                if complete_at is not None and time.monotonic() - complete_at >= 10:
                    # KafkaTopicCreator's executor can outlive all completed work.
                    # Never stop a live measurement just because its log is quiet.
                    cleanup = True
                    os.killpg(proc.pid, signal.SIGTERM)
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('OMB exceeded 25 minutes without exiting')
                time.sleep(1)
            proc.wait(timeout=15)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            Path(str(output) + '.process.txt').write_text(
                f'exit_code={proc.returncode}\npost_result_cleanup={cleanup}\n')
    if proc.returncode != 0 and not cleanup:
        raise RuntimeError(f'OMB exited with {proc.returncode}; inspect the log')
    if not output.is_file():
        raise RuntimeError('OMB exited without producing results')


if __name__ == '__main__':
    run(*sys.argv[1:])

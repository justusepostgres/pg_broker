#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
checkout=${OMB_CHECKOUT:-"$root/.dev/openmessaging-benchmark"}
workload=${1:?Usage: run.sh smoke|max-rate|10k|20k|32k|50k|75k|85k|100k bootstrap-server output.json}
bootstrap=${2:?bootstrap-server required}
output=${3:?absolute output.json path required}
case "$workload" in smoke|max-rate|10k|20k|32k|50k|75k|85k|100k) ;; *) exit 2 ;; esac
[[ "$output" = /* && "$bootstrap" != *$'\n'* && "$bootstrap" != *$'\r'* ]] || exit 2
[[ $(git -C "$checkout" rev-parse HEAD) = $(cat "$root/benchmarks/omb/revision") ]]
[[ -z $(git -C "$checkout" status --porcelain --untracked-files=no) ]]
[[ -f "$checkout/benchmark-framework/target/classpath.txt" ]]
mkdir -p "$(dirname "$output")"
[[ ! -e "$output" && ! -e "$output.driver.yaml" ]] || { echo 'Output exists; choose a new run name' >&2; exit 1; }
python3 - "$root/benchmarks/omb/driver-rf1.yaml" "$bootstrap" "$output.driver.yaml" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[3]).write_text(Path(sys.argv[1]).read_text().replace('bootstrap.servers=localhost:9092', 'bootstrap.servers='+sys.argv[2]))
PY
cp "$root/benchmarks/omb/$workload.yaml" "$output.workload.yaml"
cp "$root/benchmarks/omb/revision" "$output.revision"
cd "$checkout"
HEAP_OPTS=${HEAP_OPTS:--Xms1G -Xmx1G} python3 "$root/benchmarks/omb/execute.py" \
  "$output.driver.yaml" "$output" "$output.workload.yaml"
# Upstream can log a workload exception and exit zero without writing results.
[[ -s "$output" ]] || { echo "OMB did not produce results; inspect $output.log" >&2; exit 1; }
python3 "$root/benchmarks/omb/summarize.py" "$output" >"$output.summary.json"

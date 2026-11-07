#!/usr/bin/env bash
# Separate diagnostic pair. Logging adds overhead: never mix with acceptance runs.
set -euo pipefail
label=${1:?Usage: oci-gc-profile.sh unique-label 10k|20k|32k [kafka-first|broker-first]}
rate=${2:?offered-rate required}
order=${3:-kafka-first}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || exit 2
case "$rate" in 10k|20k|32k) ;; *) exit 2 ;; esac
case "$order" in kafka-first|broker-first) ;; *) exit 2 ;; esac
repo=/home/ubuntu/pg-broker
out="/home/ubuntu/results/omb-gc-$label"
pair="/home/ubuntu/results/omb-pair-gc-$label"
mkdir "$out"
cp "$repo/benchmarks/omb/oci-gc-profile.sh" "$out/runner.sh"
cat >"$out/instrumentation.json" <<'JSON'
{"kind":"client-jvm-gc-safepoint","heap_min":"1G","heap_max":"1G","logging":"gc*=info,safepoint=info","decorators":"time,uptime,level,tags","rotation_files":4,"rotation_size":"16M","acceptance_run":false}
JSON
# OMB's pinned launcher expands HEAP_OPTS and adds its existing UseG1GC flag.
# %t/%p give each sequential client JVM distinct files; rotation bounds logging.
export HEAP_OPTS="-Xms1G -Xmx1G -Xlog:gc*=info,safepoint=info:file=$out/client-%t-%p.log:time,uptime,level,tags:filecount=4,filesize=16M"
printf '%s\n' "$HEAP_OPTS" >"$out/heap-options.txt"
# Preserve the diagnostic marker even when the underlying pair fails. The
# pair still owns container cleanup and must be stopped before inspecting files.
mark_pair() {
 if [[ -d "$pair" ]]; then cp "$out/instrumentation.json" "$pair/instrumentation.json"; fi
}
trap mark_pair EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
bash "$repo/benchmarks/omb/oci-pair.sh" "gc-$label" "$rate" "$order"
mark_pair
python3 - "$out" "$order" <<'PY'
import json, sys
from pathlib import Path
p = Path(sys.argv[1])
logs = sorted(p.glob('client-*.log'))
assert len(logs) == 2, 'expected one base GC log per OMB client JVM'
systems = ['kafka', 'broker'] if sys.argv[2] == 'kafka-first' else ['broker', 'kafka']
# The filename timestamp orders sequential JVM starts. Keep rotated files too;
# missing early events due to rotation must be treated as incomplete coverage.
(p/'client-log-map.json').write_text(json.dumps(dict(zip(systems, [f.name for f in logs])), indent=2)+'\n')
PY
date -u +%FT%TZ >"$out/complete.txt"

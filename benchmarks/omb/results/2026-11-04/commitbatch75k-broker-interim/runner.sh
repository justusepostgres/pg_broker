#!/usr/bin/env bash
# Execute on the dedicated OCI load-generator host, inside a bounded systemd unit.
set -euo pipefail
label=${1:?Usage: client-workload.sh unique-label smoke|32k|50k|75k private-ip:port}
rate=${2:?workload required}
bootstrap=${3:?private broker endpoint required}
mode=${4:-run}
case "$mode" in run|--preflight) ;; *) exit 2 ;; esac
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,55}$ ]] || exit 2
case "$rate" in smoke|32k|50k|75k) ;; *) exit 2 ;; esac
python3 - "$bootstrap" <<'PY'
import ipaddress, sys
host, port = sys.argv[1].split(':')
address = ipaddress.IPv4Address(host)
assert any(address in ipaddress.ip_network(cidr) for cidr in
           ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')), 'RFC1918 endpoint required'
assert port in ('9092', '19092'), 'unexpected broker port'
PY
repo=/home/ubuntu/pg-broker
checkout="$repo/.dev/openmessaging-benchmark"
out="/home/ubuntu/results/omb-client-$label"
unit="omb-client-$label.service"
exec 9>/home/ubuntu/omb-client-workload.lock
flock -n 9
! pgrep -f '^java .*io.openmessaging.benchmark.Benchmark ' >/dev/null
[[ ! -e "$out" ]]
mkdir -p /home/ubuntu/results
mkdir "$out"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH="$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export HEAP_OPTS='-Xms1G -Xmx1G'
export OMB_CHECKOUT="$checkout"
java -version 2>"$out/java.txt"
grep -q 'openjdk version "17.0.20.1"' "$out/java.txt"
uname -a >"$out/host.txt"
nproc >"$out/cpu-count.txt"
free -b >"$out/memory.txt"
systemctl show "$unit" -p MemoryMax -p AllowedCPUs -p RuntimeMaxUSec >"$out/limits.txt"
grep -qx 'MemoryMax=2147483648' "$out/limits.txt"
grep -qx 'AllowedCPUs=0-1' "$out/limits.txt"
grep -qx 'RuntimeMaxUSec=26min 40s' "$out/limits.txt"
printf '%s\n' "$bootstrap" >"$out/bootstrap.txt"
printf '%s\n' "$unit" >"$out/unit-name.txt"
printf '%s\n' "$HEAP_OPTS" >"$out/heap.txt"
cp "$repo/benchmarks/omb/client-workload.sh" "$out/runner.sh"
for script in run.sh execute.py summarize.py; do
 cp "$repo/benchmarks/omb/$script" "$out/$script"
done
sha256sum "$out/runner.sh" "$out/run.sh" "$out/execute.py" "$out/summarize.py" >"$out/script-checksums.txt"
git -C "$checkout" rev-parse HEAD >"$out/revision.txt"
git -C "$checkout" status --porcelain --untracked-files=no >"$out/tracked-changes.txt"
[[ ! -s "$out/tracked-changes.txt" ]]
[[ $(cat "$out/revision.txt") = $(cat "$repo/benchmarks/omb/revision") ]]
sha256sum "$checkout/payload/payload-1Kb.data" >"$out/payload-checksum.txt"
date -u +%FT%TZ >"$out/started.txt"
if [[ "$mode" = --preflight ]]; then
 date -u +%FT%TZ >"$out/preflight-complete.txt"
 exit 0
fi
bash "$repo/benchmarks/omb/run.sh" "$rate" "$bootstrap" "$out/result.json"
date -u +%FT%TZ >"$out/complete.txt"

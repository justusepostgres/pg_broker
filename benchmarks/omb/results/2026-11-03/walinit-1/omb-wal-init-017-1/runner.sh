#!/usr/bin/env bash
# Fresh on/off stores with identical instrumentation and process-crash checks.
set -euo pipefail
label=${1:?Usage: oci-wal-init-pair.sh unique-label [on-first|off-first]}
order=${2:-on-first}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,23}$ ]] || exit 2
case "$order" in on-first|off-first) ;; *) exit 2 ;; esac
repo=/home/ubuntu/pg-broker
out="/home/ubuntu/results/omb-wal-init-$label"
mkdir "$out"
exec 8>/home/ubuntu/omb-wal-init.lock
flock -n 8
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 18 ]]
cp "$repo/benchmarks/omb/oci-wal-init-pair.sh" "$out/runner.sh"
cp "$repo/benchmarks/omb/oci-server-profile.sh" "$out/profile-runner.sh"
printf '%s\n' "$order" >"$out/order.txt"
if [[ "$order" = on-first ]]; then modes=(on off); else modes=(off on); fi
for mode in "${modes[@]}"; do
 cmp "$out/profile-runner.sh" "$repo/benchmarks/omb/oci-server-profile.sh"
 bash "$repo/benchmarks/omb/oci-server-profile.sh" "wal-$label-$mode" "$mode" crash
done
date -u +%FT%TZ >"$out/complete.txt"

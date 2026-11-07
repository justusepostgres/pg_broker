#!/usr/bin/env bash
# Three fresh matched pairs with alternating order; stop on infrastructure failure.
set -euo pipefail
label=${1:?Usage: oci-repeat.sh unique-label 10k|20k|32k}
rate=${2:?offered-rate required}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,35}$ ]] || exit 2
case "$rate" in 10k|20k|32k) ;; *) exit 2 ;; esac
repo=/home/ubuntu/pg-broker
out="/home/ubuntu/results/omb-repeat-$label"
mkdir "$out"
exec 8>/home/ubuntu/omb-repeat.lock
flock -n 8
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 55 ]]
cp "$repo/benchmarks/omb/oci-repeat.sh" "$out/runner.sh"
cp "$repo/benchmarks/omb/oci-pair.sh" "$out/pair-runner.sh"
printf 'trial\torder\tresults\n' >"$out/plan.tsv"
for trial in 1 2 3; do
 order=kafka-first
 [[ "$trial" != 2 ]] || order=broker-first
 printf '%s\t%s\t%s\n' "$trial" "$order" "/home/ubuntu/results/omb-pair-$label-$trial" >>"$out/plan.tsv"
done
for trial in 1 2 3; do
 order=kafka-first
 [[ "$trial" != 2 ]] || order=broker-first
 # The captured source must stay fixed throughout the experiment.
 cmp "$repo/benchmarks/omb/oci-pair.sh" "$out/pair-runner.sh"
 bash "$repo/benchmarks/omb/oci-pair.sh" "$label-$trial" "$rate" "$order"
 date -u +%FT%TZ >"$out/trial-$trial-complete.txt"
done
date -u +%FT%TZ >"$out/complete.txt"

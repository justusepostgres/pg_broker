#!/usr/bin/env bash
# Same image, fresh stores, one storage setting changed. No builds during traffic.
set -euo pipefail
label=${1:?Usage: oci-compression.sh unique-label 10k|20k|32k}
rate=${2:?offered-rate required}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,39}$ ]] || exit 2
case "$rate" in 10k|20k|32k) ;; *) exit 2 ;; esac
cd /home/ubuntu
repo=/home/ubuntu/pg-broker
name="omb-compression-$label"
out="/home/ubuntu/results/$name"
image=pg-broker:lz4-candidate-015
mkdir "$out"
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -f '^java .*io.openmessaging.benchmark.Benchmark ' >/dev/null
! pgrep -f ' kafka.Kafka ' >/dev/null
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 20 ]]
sudo docker image inspect "$image" --format '{{.Id}}' >"$out/image.txt"
cmp "$out/image.txt" /home/ubuntu/lz4-image-id.txt
image=$(cat "$out/image.txt")
sudo docker run --rm "$image" pg_config --configure >"$out/pg-config.txt"
grep -q -- --with-lz4 "$out/pg-config.txt"
cp "$repo/benchmarks/omb/oci-compression.sh" "$out/runner.sh"
cp "$repo/benchmarks/omb/verify-storage.sql" "$out/verify-storage.sql"
cleanup() {
 if [[ -n ${db:-} ]]; then
  sudo docker stop "$db-gateway" >/dev/null 2>&1 || true
  if [[ $(sudo docker inspect --format '{{.State.Running}}' "$db" 2>/dev/null) = true ]]; then
   sudo docker kill --signal=SIGINT "$db" >/dev/null
   sudo docker wait "$db" >/dev/null
  fi
 fi
}
trap cleanup EXIT
for algorithm in pglz lz4; do
 db="$name-$algorithm"
 ! sudo docker volume inspect "$db-data" >/dev/null 2>&1
 sudo docker run -d --name "$db" --cpuset-cpus=0,1 --memory=8g --network host \
  --env-file /home/ubuntu/broker-omb.env -v "$db-data:/data" "$image" container-db.sh
 for attempt in $(seq 1 60); do
  if sudo docker exec "$db" test -f /data/.broker-ready && \
   sudo docker exec "$db" psql -U postgres -Atc "SELECT extversion FROM pg_extension WHERE extname='pg_broker'" 2>/dev/null | grep -qx 0.1.5; then break; fi
  sleep 1
 done
 sudo docker exec "$db" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER TABLE broker.batches ALTER data SET COMPRESSION $algorithm"
 sudo docker run -d --name "$db-gateway" --cpuset-cpus=0,1 --memory=1g --network host \
  --env-file /home/ubuntu/broker-omb.env "$image"
 sudo docker inspect "$db" "$db-gateway" --format '{{.Name}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}' >"$out/$algorithm-limits.txt"
 cd "$repo"
 OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh "$rate" localhost:9092 "$out/$algorithm.json"
 sudo docker stop "$db-gateway" >/dev/null
 sudo docker logs "$db-gateway" >"$out/$algorithm-gateway.log" 2>&1
 sudo docker kill --signal=SIGINT "$db" >/dev/null
 sudo docker wait "$db" >/dev/null
 sudo docker start "$db" >/dev/null
 for attempt in $(seq 1 30); do
  if sudo docker exec "$db" pg_isready -U postgres >/dev/null; then break; fi
  sleep 1
 done
 sudo docker exec -i "$db" psql -U postgres -At < "$out/verify-storage.sql" >"$out/$algorithm-storage.txt"
 sudo docker exec "$db" psql -U postgres -Atc "SELECT count(*) FILTER (WHERE pg_column_compression(data)='$algorithm'), bool_and(coalesce(pg_column_compression(data),'$algorithm')='$algorithm'),pg_total_relation_size('broker.batches') FROM broker.batches" >"$out/$algorithm-compression.txt"
 python3 - "$out" "$algorithm" <<'CHECK'
import json, re, sys
from pathlib import Path
p, algorithm = Path(sys.argv[1]), sys.argv[2]
assert (p/f'{algorithm}-storage.txt').read_text().splitlines() == ['0.1.5|on|on|on|t|t', '16|t']
n, valid, size = (p/f'{algorithm}-compression.txt').read_text().strip().split('|')
assert int(n)>0 and valid=='t'
d=json.loads((p/f'{algorithm}.json').read_text())
assert len(d['publishRate'])==30
print(algorithm, 'max publish errors/s', max(d['publishErrorRate']),
      'client warnings/errors', len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b', (p/f'{algorithm}.json.log').read_text())),
      'relation bytes',size)
CHECK
 cleanup
 cd /home/ubuntu
 [[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 15 ]]
done
trap - EXIT
date -u +%FT%TZ >"$out/complete.txt"

#!/usr/bin/env bash
# Controlled RF=1 comparison on the dedicated four-core OCI benchmark host.
# Requires the provisioned /home/ubuntu artifacts; never builds during traffic.
set -euo pipefail
label=${1:?Usage: oci-pair.sh unique-label 10k|20k|32k}
rate=${2:?offered-rate required}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,39}$ ]] || exit 2
case "$rate" in 10k|20k|32k) ;; *) exit 2 ;; esac
cd /home/ubuntu
repo=/home/ubuntu/pg-broker
candidate=/home/ubuntu/candidate-cached015
name="omb-pair-$label"
out="/home/ubuntu/results/$name"
mkdir "$out" # Refuse reuse, including partially completed comparisons.
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -f '^java .*io.openmessaging.benchmark.Benchmark ' >/dev/null
! pgrep -f ' kafka.Kafka ' >/dev/null
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 25 ]]
[[ ! -e "/home/ubuntu/$name-kafka-data" ]]
! sudo docker volume inspect "$name-data" >/dev/null 2>&1
# Fixed candidate verified in the cached015 run. Fail closed on binary changes.
printf '%s  %s\n' \
 f7eb62b3a7953ab92a3df7f4f5e3be8b6bfe104b1f3fd513a4d101f5d2cb548a "$candidate/libpg_broker.so" \
 abfd3c64ec69ac00be19ae2dc7230ae16dc6d7faecd70b888e797ba0527a2435 "$candidate/pg-broker-gateway" \
 | sha256sum -c - >"$out/binaries.txt"
cleanup() {
 sudo systemctl stop "$name-kafka" >/dev/null 2>&1 || true
 sudo docker stop "$name-gateway" >/dev/null 2>&1 || true
 if [[ $(sudo docker inspect --format '{{.State.Running}}' "$name-db" 2>/dev/null) = true ]]; then
  sudo docker kill --signal=SIGINT "$name-db" >/dev/null
  sudo docker wait "$name-db" >/dev/null
 fi
}
trap cleanup EXIT
uname -a >"$out/host.txt"
java -version 2>"$out/java.txt"
sudo docker image inspect pg-broker:omb-release --format '{{.Id}}' >"$out/image.txt"
cp "$repo/benchmarks/omb/oci-pair.sh" "$out/runner.sh"
sed "s@/home/ubuntu/kafka-data-omb@/home/ubuntu/$name-kafka-data@" \
 "$repo/benchmarks/omb/kafka-rf1.properties" >"$out/kafka.properties"
kafka=/home/ubuntu/kafka-dist/kafka_2.13-3.6.1/bin
cluster_id=$("$kafka/kafka-storage.sh" random-uuid)
"$kafka/kafka-storage.sh" format -t "$cluster_id" -c "$out/kafka.properties"
sudo systemd-run --unit="$name-kafka" --uid=ubuntu --property=MemoryMax=9G \
 --property=AllowedCPUs=0,1 --setenv=KAFKA_HEAP_OPTS='-Xms1G -Xmx1G' \
 "$kafka/kafka-server-start.sh" "$out/kafka.properties"
sudo systemctl show "$name-kafka" -p MemoryMax -p AllowedCPUs >"$out/kafka-limits.txt"
for attempt in $(seq 1 60); do
 if timeout 1 bash -c '</dev/tcp/127.0.0.1/19092' 2>/dev/null; then break; fi
 sleep 1
done
cd "$repo"
OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh "$rate" localhost:19092 "$out/kafka.json"
sudo systemctl stop "$name-kafka"
sudo journalctl -u "$name-kafka" --no-pager >"$out/kafka-server.log"
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 15 ]]
cd /home/ubuntu
sudo docker run -d --name "$name-db" --cpuset-cpus=0,1 --memory=8g --network host \
 --env-file broker-omb.env -v "$name-data:/data" \
 -v "$candidate/libpg_broker.so:/opt/pgsql/lib/pg_broker.so:ro" pg-broker:omb-release container-db.sh
for migration in 0.1.1--0.1.2 0.1.2--0.1.3 0.1.3--0.1.4 0.1.4--0.1.5; do
 sudo docker cp "pg_broker--$migration.sql" "$name-db:/opt/pgsql/share/extension/"
done
for attempt in $(seq 1 60); do
 if sudo docker exec "$name-db" test -f /data/.broker-ready && \
 sudo docker exec "$name-db" psql -U postgres -Atc "SELECT extversion FROM pg_extension WHERE extname='pg_broker'" 2>/dev/null | grep -qx 0.1.1; then break; fi
 sleep 1
done
sudo docker exec "$name-db" psql -U postgres -v ON_ERROR_STOP=1 -c "ALTER EXTENSION pg_broker UPDATE TO '0.1.5'"
sudo docker run -d --name "$name-gateway" --cpuset-cpus=0,1 --memory=1g --network host \
 --env-file broker-omb.env -v "$candidate/pg-broker-gateway:/usr/local/bin/pg-broker-gateway:ro" pg-broker:omb-release
sudo docker inspect "$name-db" "$name-gateway" --format '{{.Name}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}' >"$out/broker-limits.txt"
cd "$repo"
OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh "$rate" localhost:9092 "$out/broker.json"
sudo docker stop "$name-gateway" >/dev/null
sudo docker logs "$name-gateway" >"$out/gateway.log" 2>&1
sudo docker kill --signal=SIGINT "$name-db" >/dev/null
sudo docker wait "$name-db" >/dev/null
sudo docker start "$name-db" >/dev/null
for attempt in $(seq 1 30); do
 if sudo docker exec "$name-db" pg_isready -U postgres >/dev/null; then break; fi
 sleep 1
done
sudo docker exec -i "$name-db" psql -U postgres -At < /home/ubuntu/binary015-verify.sql >"$out/storage.txt"
python3 - "$out" <<'PY'
import json, re, sys
from pathlib import Path
p = Path(sys.argv[1])
assert (p/'storage.txt').read_text().splitlines() == ['0.1.5|on|on|on|t|t', '16|t']
for system in ('kafka', 'broker'):
    d = json.loads((p/f'{system}.json').read_text())
    assert len(d['publishRate']) == 30, system
    # Preserve nonzero errors and warnings as results, never hide a failed load.
    print(system, 'max publish errors/s', max(d['publishErrorRate']),
          'client warnings/errors', len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b',
          (p/f'{system}.json.log').read_text())))
PY
cleanup
trap - EXIT
date -u +%FT%TZ >"$out/complete.txt"

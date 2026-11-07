#!/usr/bin/env bash
# Temporary, source/destination-scoped rule on the dedicated benchmark server.
set -euo pipefail
mode=${1:?Usage: oci-external-network.sh add|remove client-private-ip server-private-ip}
client=${2:?client IPv4 required}
server=${3:?server IPv4 required}
case "$mode" in add|remove) ;; *) exit 2 ;; esac
python3 - "$client" "$server" <<'PY'
import ipaddress, sys
addresses = [ipaddress.IPv4Address(v) for v in sys.argv[1:]]
assert addresses[0] != addresses[1]
for address in addresses:
    assert any(address in ipaddress.ip_network(cidr) for cidr in
               ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))
PY
[[ $(hostname) = pg-broker-omb-verification-1 ]]
# Never change the network while any pair runner owns the measurement host.
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -f ' kafka.Kafka ' >/dev/null
ip -j -4 address show | python3 -c '
import json, sys
assert sys.argv[1] in {a["local"] for i in json.load(sys.stdin) for a in i["addr_info"]}
' "$server"
rule=(-s "$client/32" -d "$server/32" -p tcp -m multiport --dports 9092,19092
      -m comment --comment pg-broker-omb-external-client -j ACCEPT)
if [[ "$mode" = add ]]; then
 sudo iptables -C INPUT "${rule[@]}" 2>/dev/null || sudo iptables -I INPUT 1 "${rule[@]}"
else
 if sudo iptables -C INPUT "${rule[@]}" 2>/dev/null; then
  sudo iptables -D INPUT "${rule[@]}"
 fi
fi
sudo iptables -S INPUT

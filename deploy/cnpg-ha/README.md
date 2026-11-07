# Multi-host verification deployment

This overlay requires at least three schedulable Kubernetes nodes. Each database
instance has **required hostname anti-affinity**; the two gateways also require
different hosts. An unschedulable third replica is preferable to silently putting
all durable copies on one host.

```sh
kubectl --context YOUR_TEST_CLUSTER apply -k deploy/cnpg-ha
kubectl --context YOUR_TEST_CLUSTER -n pg-broker wait cluster/broker-db --for=condition=Ready --timeout=600s
kubectl --context YOUR_TEST_CLUSTER -n pg-broker get pods -o wide
```

Build and distribute the two images to every test node, or use a registry with
immutable image references, before applying. The operator must watch `pg-broker`.
Use the base [CNPG guide](../cnpg/README.md) for image, TLS and client setup.

The tested infrastructure uses three K3s v1.35.1+k3s1 **server** nodes with
embedded etcd and private node networking. Losing one server therefore leaves
an API/etcd quorum. Installation follows the [K3s HA instructions](https://docs.k3s.io/datastore/ha-embedded).
Persist guest firewall rules for private node traffic across reboots, in addition
to cloud security rules. A temporary `iptables -I` rule disappears on reboot and
can prevent the old primary from reaching DNS or replication peers when it returns.
Keep the API, etcd and VXLAN ports restricted to the private cluster network.
Run CNPG 1.30.1 controllers redundantly on different nodes as well. On a dedicated
test cluster with the standard operator Deployment:

```sh
kubectl --context YOUR_TEST_CLUSTER -n cnpg-system patch deployment cnpg-controller-manager \
  --type merge --patch-file deploy/cnpg-ha/operator-patch.yaml
```

Hostname separation does not prove availability-zone separation. Choose zone
placement and storage failure domains separately for production.

## Hard host-loss drill

After running `scripts/cnpg-vm.sh seed`, identify the primary Pod's node and ensure
the client Pod and an SSH observer are on another node. Preserve the client Pod's
`/tmp/pg-broker-smoke.json` if moving it; that file identifies the original groups
whose offsets must survive. If `kubectl cp` preserves a different file owner,
set the copied checkpoint to the client container user before running `seed` again. Confirm all three database instances are healthy.

`scripts/cnpg-host-failure.py` uses OCI **STOP**, not a PostgreSQL switchover or
Pod deletion. Its guard only accepts VMs tagged
`purpose=pg-broker-ha-verification`, checks that the selected host contains the
current primary, and rejects a client/observer on that host. It waits for a
writable promoted survivor, resumes both original Kafka groups, proves new
writes work, powers the old host back on in a `finally` block, and checks that the
old primary rejoins as a standby.

```sh
python3 scripts/cnpg-host-failure.py \
  --observer OBSERVER_PUBLIC_IP \
  --victim-instance-id DISPOSABLE_INSTANCE_OCID \
  --victim-node DISPOSABLE_NODE_NAME \
  --ssh-key PATH_TO_TEST_KEY --known-hosts PATH_TO_KNOWN_HOSTS
```

The reported interval starts at the power-off request and ends at successful
client resume. It includes control-plane detection, promotion and client/group
recovery; it is a single observed recovery interval, not an RTO guarantee.
The test does not simulate an asymmetric network partition or simultaneous loss
of a quorum. Its `finally` restart is best-effort if the process or OCI API itself
fails; always inspect cloud resource state after interrupting fault injection.

# Backup and restore

The extension registers its six durable tables with PostgreSQL's
`pg_extension_config_dump`. A full logical database archive therefore includes
topics, logs, partition bounds, groups, membership and committed offsets. Install
the matching extension binary/control/versioned SQL on the restore server first;
a dump does not contain these files.

Use PostgreSQL 17 client tools and your normal PostgreSQL connection environment:

```sh
umask 077
pg_dump --format=custom --file=broker.dump broker
createdb broker_restore
pg_restore --exit-on-error --single-transaction --dbname=broker_restore broker.dump
```

Restore into a **fresh database**. Do not pre-create the extension there: the
archive creates it. Preserve referenced roles, or deliberately map ownership and
reapply the application's restricted function grant before opening the restored
gateway. Protect archives as application data and verify copies stored outside
the source host. The commands above do not upload, encrypt or schedule backups.

`tests/test_backup_restore.py` exercises a real archive and fresh database,
compares all extension table contents, resumes two independent Kafka groups from
different committed offsets, checks nonzero retention bounds and confirms the
next append continues its offset sequence.

For the disposable K3s/CNPG environment, after `scripts/cnpg-vm.sh seed`:

```sh
python3 scripts/cnpg-logical-restore.py
```

This creates the separate `broker-restore` CNPG cluster and gateway, restores a
full archive, and points both ordinary Kafka clients at that new endpoint to
resume their original groups. It refuses an existing target cluster and keeps
the archive under `.dev/backups/` with private permissions. The source remains
available. The restored test cluster remains for inspection; remove it and its
PVCs only after you finish inspecting this disposable environment.

A logical archive is a consistent database snapshot. Commits after the snapshot
are absent from the restored database. Quiesce clients or define an explicit
cutover boundary before switching production endpoints. Restored consumer
memberships may initially be stale; normal session expiry and generation fencing
apply, and external side effects must tolerate replay.

This is **not PITR**: continuous WAL archiving, object-storage backup scheduling,
off-site restore drills and a documented RPO/RTO remain separate operational work.
See PostgreSQL's [extension configuration tables](https://www.postgresql.org/docs/17/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES)
and [pg_dump](https://www.postgresql.org/docs/17/app-pgdump.html) documentation.

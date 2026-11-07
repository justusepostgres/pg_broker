//! Real single-node engine callbacks/recovery, deliberately no network/snapshots.
use super::{alpha_log::Store, alpha_machine::Machine, alpha_network::NoNetwork, Data, Next, Old};
use broker_cluster::{application::PartitionCommand, journal::JournalStorage};
use deadpool_postgres::{Manager, Pool};
use openraft_next::{Config, Raft, SnapshotPolicy};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio_postgres::{Config as PgConfig, NoTls};

fn pool() -> Pool {
    let config: PgConfig = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(4)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "engine-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
pub(super) async fn setup(pool: &Pool, key: &str) -> String {
    let c = pool.get().await.unwrap();
    let id: String = c
        .query_one(
            "INSERT INTO broker.topics(name,partitions) VALUES($1,1) RETURNING topic_id::text",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    c.execute(
        "INSERT INTO broker.partitions(topic,partition) VALUES($1,0)",
        &[&key],
    )
    .await
    .unwrap();
    id
}
pub(super) fn command(offset: u64) -> Data {
    let hex = include_str!("../../../../crates/cluster/tests/fixtures/append-batch.hex").trim();
    let bytes: Vec<_> = (0..hex.len())
        .step_by(2)
        .map(|n| u8::from_str_radix(&hex[n..n + 2], 16).unwrap())
        .collect();
    Data(
        serde_json::to_value(PartitionCommand::append(offset, 1700000000123, &bytes).unwrap())
            .unwrap(),
    )
}
async fn start(pool: Pool, key: &str, topic_id: &str) -> Raft<Next, Machine> {
    let store = JournalStorage::<Old>::claim(pool, key.into())
        .await
        .unwrap();
    let mut app = store.application();
    app.bind(key, topic_id, 0).await.unwrap();
    let config = Config {
        snapshot_policy: SnapshotPolicy::Never,
        max_payload_entries: 128,
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        NoNetwork,
        Store(Arc::new(Mutex::new(store))),
        Machine {
            app,
            topic: key.into(),
            topic_id: topic_id.into(),
        },
    )
    .await
    .unwrap()
}
async fn leader(raft: &Raft<Next, Machine>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while raft.current_leader().await != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) async fn offset(pool: &Pool, key: &str) -> i64 {
    pool.get()
        .await
        .unwrap()
        .query_one(
            "SELECT next_offset FROM broker.partitions WHERE topic=$1 AND partition=0",
            &[&key],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn real_client_callback_waits_for_application_and_restart_recovers() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let raft = start(pool.clone(), &key, &tid).await;
    raft.initialize(BTreeMap::from([(1, openraft_next::BasicNode::default())]))
        .await
        .unwrap();
    leader(&raft).await;
    let mut held = pool.get().await.unwrap();
    let tx = held.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM broker.partitions WHERE topic=$1 AND partition=0 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let writer = raft.clone();
    let mut pending = tokio::spawn(async move { writer.client_write(command(0)).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(offset(&pool, &key).await, 0);
    tx.commit().await.unwrap();
    drop(held);
    let response = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(offset(&pool, &key).await, 1);
    let applied: serde_json::Value = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT applied FROM broker.raft_partition_state WHERE group_key=$1",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(applied, serde_json::to_value(response.log_id).unwrap());
    raft.shutdown().await.unwrap();
    drop(raft);
    let reopened = start(pool.clone(), &key, &tid).await;
    reopened
        .wait_for_recovery(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    leader(&reopened).await;
    reopened.client_write(command(1)).await.unwrap();
    assert_eq!(offset(&pool, &key).await, 2);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn failed_application_does_not_report_a_successful_client_write() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let raft = start(pool.clone(), &key, &tid).await;
    raft.initialize(BTreeMap::from([(1, openraft_next::BasicNode::default())]))
        .await
        .unwrap();
    leader(&raft).await;
    let result = tokio::time::timeout(Duration::from_secs(10), raft.client_write(command(999)))
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(offset(&pool, &key).await, 0);
    raft.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn real_engine_recovers_a_large_committed_application_jump() {
    use openraft_old::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let mut store = JournalStorage::<Old>::claim(pool.clone(), key.clone())
        .await
        .unwrap();
    store.application().bind(&key, &tid, 0).await.unwrap();
    let id = |index| LogId::new(CommittedLeaderId::new(1, 1), index);
    store.save_vote(Vote::new_committed(1, 1)).await.unwrap();
    let membership = Membership::new(vec![BTreeSet::from([1])], None);
    store
        .append(&[Entry {
            log_id: id(0),
            payload: EntryPayload::Membership(membership),
        }])
        .await
        .unwrap();
    for first in (1..10001).step_by(256) {
        store
            .append(
                &(first..(first + 256).min(10001))
                    .map(|index| Entry {
                        log_id: id(index),
                        payload: EntryPayload::Blank,
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }
    store.commit(id(10000)).await.unwrap();
    drop(store);
    let raft = start(pool.clone(), &key, &tid).await;
    raft.wait_for_recovery(Some(Duration::from_secs(90)))
        .await
        .unwrap();
    let applied: serde_json::Value = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT applied FROM broker.raft_partition_state WHERE group_key=$1",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    assert!(applied["index"].as_u64().unwrap() >= 10000);
    assert_eq!(offset(&pool, &key).await, 0);
    raft.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn replaced_storage_owner_fences_a_running_engine() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let raft = start(pool.clone(), &key, &tid).await;
    raft.initialize(BTreeMap::from([(1, openraft_next::BasicNode::default())]))
        .await
        .unwrap();
    leader(&raft).await;
    raft.client_write(command(0)).await.unwrap();
    let replacement = JournalStorage::<Old>::claim(pool.clone(), key.clone())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), raft.client_write(command(1)))
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "a fenced engine must not acknowledge new writes"
    );
    assert_eq!(offset(&pool, &key).await, 1);
    raft.shutdown().await.unwrap();
    drop(replacement);
    let reopened = start(pool.clone(), &key, &tid).await;
    leader(&reopened).await;
    reopened.client_write(command(1)).await.unwrap();
    assert_eq!(offset(&pool, &key).await, 2);
    reopened.shutdown().await.unwrap();
}

//! Explicit disposable-PostgreSQL tests; no network-consensus claim.
use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Config, NoTls};

openraft::declare_raft_types!(pub TestConfig:
    D = Vec<u8>, R = (), SnapshotData = std::io::Cursor<Vec<u8>>
);
type Store = JournalStorage<TestConfig>;

fn pool() -> Pool {
    let config: Config = std::env::var("DATABASE_URL")
        .expect("disposable DATABASE_URL")
        .parse()
        .unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "journal-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn id(index: u64, term: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}
fn entry(index: u64, term: u64) -> Entry<TestConfig> {
    Entry {
        log_id: id(index, term),
        payload: EntryPayload::Normal(vec![0, 1, 255]),
    }
}

use openraft::{
    storage::{RaftLogStorage, RaftLogStorageExt},
    RaftLogReader,
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.11"]
async fn writer_trait_flushes_and_recovers_committed_prefix() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    RaftLogStorage::save_committed(&mut store, None)
        .await
        .unwrap();
    let vote = Vote::new_committed(1, 1);
    RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        store.blocking_append(vec![entry(0, 1), entry(1, 1)]),
    )
    .await
    .unwrap()
    .unwrap(); // Public OpenRaft helper waits for the actual flush callback.
    let mut reader = RaftLogStorage::get_log_reader(&mut store).await;
    assert_eq!(
        reader.try_get_log_entries(..).await.unwrap(),
        vec![entry(0, 1), entry(1, 1)]
    );
    RaftLogStorage::save_committed(&mut store, Some(id(0, 1)))
        .await
        .unwrap();
    assert!(RaftLogStorage::save_committed(&mut store, None)
        .await
        .is_err());
    assert!(RaftLogStorage::truncate(&mut store, id(0, 1))
        .await
        .is_err());
    RaftLogStorage::truncate(&mut store, id(1, 1))
        .await
        .unwrap();
    store.blocking_append(vec![entry(1, 2)]).await.unwrap();
    // As in the SQL journal tests, the test explicitly assumes an applied
    // prefix. A real node still needs durable application/snapshot support.
    RaftLogStorage::purge(&mut store, id(0, 1)).await.unwrap();
    let mut next = Store::claim(pool, key).await.unwrap();
    let state = RaftLogStorage::get_log_state(&mut next).await.unwrap();
    assert_eq!(state.last_log_id, Some(id(1, 2)));
    assert_eq!(state.last_purged_log_id, Some(id(0, 1)));
    assert_eq!(
        RaftLogStorage::read_committed(&mut next).await.unwrap(),
        Some(id(0, 1))
    );
    assert_eq!(
        RaftLogStorage::read_vote(&mut next).await.unwrap(),
        Some(vote)
    );
    assert!(store.blocking_append(vec![entry(2, 2)]).await.is_err());
    assert!(reader.try_get_log_entries(..).await.is_err());
    assert!(RaftLogStorage::save_committed(&mut store, None)
        .await
        .is_err());
    // Count bounds stop an infinite iterator before any write.
    assert!(next
        .blocking_append(std::iter::repeat_with(|| entry(2, 2)))
        .await
        .is_err());
    // A malformed later entry rolls back the entire append transaction.
    assert!(next
        .blocking_append(vec![entry(2, 2), entry(4, 2)])
        .await
        .is_err());
    assert_eq!(
        RaftLogStorage::get_log_state(&mut next)
            .await
            .unwrap()
            .last_log_id,
        Some(id(1, 2))
    );
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.11"]
async fn flush_completion_waits_for_database_and_failed_append_has_no_prefix() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let other = crate::pool();
    let mut blocker = other.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.query_one(
        "SELECT group_key FROM broker.raft_votes WHERE group_key=$1 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let write = tokio::spawn(async move {
        let result = store.blocking_append(vec![entry(0, 1)]).await;
        (store, result)
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let blocked: bool = tx
            .query_one("SELECT cardinality(pg_blocking_pids($1))>0", &[&pid])
            .await
            .unwrap()
            .get(0);
        if blocked {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !write.is_finished(),
        "flush acknowledged before database write"
    );
    tx.rollback().await.unwrap();
    let (mut store, result) = tokio::time::timeout(Duration::from_secs(3), write)
        .await
        .unwrap()
        .unwrap();
    result.unwrap();
    assert_eq!(
        store.try_get_log_entries(..).await.unwrap(),
        vec![entry(0, 1)]
    );
    // The callback/error path terminates; it neither hangs nor commits a prefix.
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        store.blocking_append(vec![entry(1, 1), entry(3, 1)])
    )
    .await
    .unwrap()
    .is_err());
    assert_eq!(
        store.try_get_log_entries(..).await.unwrap(),
        vec![entry(0, 1)]
    );
}

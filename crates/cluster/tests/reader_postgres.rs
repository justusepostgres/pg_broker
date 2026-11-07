use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Config, NoTls};
openraft::declare_raft_types!(pub TestConfig: D = String, R = (), SnapshotData = std::io::Cursor<Vec<u8>>);
type Store = JournalStorage<TestConfig>;
fn pool() -> Pool {
    let config: Config = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "reader-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}
fn entry(index: u64) -> Entry<TestConfig> {
    Entry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.11"]
async fn complete_and_limited_ranges_have_distinct_contracts_and_fencing() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let entries: Vec<_> = (0..4097).map(entry).collect();
    for chunk in entries.chunks(256) {
        store.append(chunk).await.unwrap();
    }
    let mut reader = store.reader();
    let mut other = reader.clone();
    assert_eq!(
        reader.try_get_log_entries(0..4096).await.unwrap().len(),
        4096
    );
    assert!(
        reader.try_get_log_entries(..).await.is_err(),
        "must not silently truncate full read"
    );
    assert_eq!(
        reader.limited_get_log_entries(0, 4097).await.unwrap().len(),
        256
    );
    assert_eq!(
        other.try_get_log_entries(4096..).await.unwrap(),
        vec![entry(4096)]
    );
    assert_eq!(
        other.try_get_log_entries(1..=2).await.unwrap(),
        vec![entry(1), entry(2)]
    );
    assert!(reader.try_get_log_entries(..0).await.unwrap().is_empty());
    store.commit(id(255)).await.unwrap();
    store.purge(id(255)).await.unwrap();
    assert!(reader.try_get_log_entries(0..10).await.unwrap().is_empty());
    assert!(reader.limited_get_log_entries(0, 10).await.is_err());
    let _successor = Store::claim(pool, key).await.unwrap();
    assert!(reader.try_get_log_entries(256..257).await.is_err());
    assert!(
        other.limited_get_log_entries(0, 0).await.is_err(),
        "empty stale reads must be fenced too"
    );
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.11"]
async fn byte_bounds_never_silently_shorten_complete_reads() {
    let mut store = Store::claim(pool(), key()).await.unwrap();
    for start in (0..75).step_by(8) {
        let entries: Vec<_> = (start..(start + 8).min(75))
            .map(|i| Entry {
                log_id: id(i),
                payload: EntryPayload::Normal("x".repeat(900_000)),
            })
            .collect();
        store.append(&entries).await.unwrap();
    }
    let mut reader = store.reader();
    assert_eq!(reader.try_get_log_entries(0..10).await.unwrap().len(), 10);
    let partial = reader.limited_get_log_entries(0, 10).await.unwrap();
    assert!(!partial.is_empty() && partial.len() < 10);
    assert!(serde_json::to_vec(&partial).unwrap().len() <= 8 * 1024 * 1024);
    assert!(
        reader.try_get_log_entries(..).await.is_err(),
        "64MiB full-read limit must report an error"
    );
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.11"]
async fn cancelled_reader_discards_connection_without_fencing_the_writer() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut reader = store.reader();
    let pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let blockers = crate::pool();
    let mut blocker = blockers.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.query_one(
        "SELECT group_key FROM broker.raft_votes WHERE group_key=$1 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let read = tokio::spawn(async move { reader.try_get_log_entries(..).await });
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
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();
    let new_pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(pid, new_pid);
    store.append(&[entry(0)]).await.unwrap();
    assert_eq!(
        store.reader().try_get_log_entries(..).await.unwrap(),
        vec![entry(0)]
    );
}
